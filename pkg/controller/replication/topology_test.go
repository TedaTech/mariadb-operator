package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newTestTopology(t *testing.T, mdb *mariadbv1alpha1.MariaDB) *singleClusterTopology {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "repl-password",
			Namespace: "default",
		},
		Data: map[string][]byte{"password": []byte("replpass")},
	}
	k8sClient := fake.NewClientBuilder().WithObjects(secret).Build()
	refResolver := refresolver.New(k8sClient)

	return &singleClusterTopology{
		mariadb:           mdb,
		userSqlReconciler: newUserSqlReconciler(mdb, refResolver, logf.Log),
		refResolver:       refResolver,
		logger:            logf.Log,
	}
}

func mariadbWithReplSecret() *mariadbv1alpha1.MariaDB {
	return &mariadbv1alpha1.MariaDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mariadb",
			Namespace: "default",
		},
		Spec: mariadbv1alpha1.MariaDBSpec{
			Replication: &mariadbv1alpha1.Replication{
				Enabled: true,
				ReplicationSpec: mariadbv1alpha1.ReplicationSpec{
					Replica: mariadbv1alpha1.ReplicaReplication{
						ReplPasswordSecretKeyRef: &mariadbv1alpha1.GeneratedSecretKeyRef{
							SecretKeySelector: mariadbv1alpha1.SecretKeySelector{
								LocalObjectReference: mariadbv1alpha1.LocalObjectReference{Name: "repl-password"},
								Key:                  "password",
							},
						},
					},
				},
			},
		},
	}
}

// TestConfigurePrimaryToleratedGtid1948FallsThrough pins the fall-through of a
// tolerated error 1948 in ConfigurePrimary.
//
// Error 1948 is raised when @@gtid_slave_pos is reset for a domain the binary log
// still has. Under current_pos the assignment is redundant, so the error used to
// be swallowed with an early return — which skipped DisableReadOnly right below
// it, and a node promoted into the primary starts read_only=1. The fix logs and
// falls through so the rest of the promotion runs; the test asserts ConfigurePrimary
// returns nil AND DisableReadOnly is still invoked.
func TestConfigurePrimaryToleratedGtid1948FallsThrough(t *testing.T) {
	cases := []struct {
		name       string
		resetErr   error
		wantErr    bool
		expectRest bool
	}{
		{
			name: "tolerated 1948 falls through and still disables read_only",
			resetErr: fmt.Errorf("Error 1948 (HY000): Specified value for @@gtid_slave_pos contains no " +
				"value for replication domain 0. This conflicts with the binary log which contains GTID 0-11-1176"),
			wantErr:    false,
			expectRest: true,
		},
		{
			name:       "a non-1948 reset error aborts before DisableReadOnly",
			resetErr:   errors.New("Error 1234 (HY000): something else"),
			wantErr:    true,
			expectRest: false,
		},
		{
			name:       "no reset error runs the full promotion",
			resetErr:   nil,
			wantErr:    false,
			expectRest: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := newMockedSQLClient(t)

			mock.ExpectQuery("SHOW REPLICA").WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow("x"))
			mock.ExpectExec("STOP SLAVE").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("RESET SLAVE").WillReturnResult(sqlmock.NewResult(0, 0))
			// The error that used to short-circuit the promotion.
			if tt.resetErr != nil {
				mock.ExpectExec("SET @@global.gtid_slave_pos").WillReturnError(tt.resetErr)
			} else {
				mock.ExpectExec("SET @@global.gtid_slave_pos").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			if tt.expectRest {
				// DisableReadOnly must still run after a tolerated error.
				mock.ExpectExec("SET @@global.read_only=0").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT COUNT").WithArgs("repl", "%").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("1"))
				mock.ExpectExec("ALTER USER").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("GRANT REPLICATION REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
			}

			topology := newTestTopology(t, mariadbWithReplSecret())

			err := topology.ConfigurePrimary(context.Background(), client)
			if tt.wantErr && err == nil {
				t.Fatal("ConfigurePrimary() error = nil, want a fatal reset error to abort the promotion")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ConfigurePrimary() error = %v, want nil", err)
			}
		})
	}
}
