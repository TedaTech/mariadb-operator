package replication

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	conditions "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestShouldWaitForPrimaryRole(t *testing.T) {
	replicationConfigured := metav1.Condition{
		Type:   mariadbv1alpha1.ConditionTypeReplicationConfigured,
		Status: metav1.ConditionTrue,
		Reason: mariadbv1alpha1.ConditionReasonReplicationConfigured,
	}
	tests := []struct {
		name            string
		configured      bool
		replication     *mariadbv1alpha1.ReplicationStatus
		primaryPodIndex int
		want            bool
	}{
		{
			name:            "never configured, no replication status",
			configured:      false,
			replication:     nil,
			primaryPodIndex: 0,
			want:            true,
		},
		{
			name:       "never configured, roles only for the replicas",
			configured: false,
			replication: &mariadbv1alpha1.ReplicationStatus{
				Roles: map[string]mariadbv1alpha1.ReplicationRole{
					"mariadb-repl-1": mariadbv1alpha1.ReplicationRoleReplica,
					"mariadb-repl-2": mariadbv1alpha1.ReplicationRoleReplica,
				},
			},
			primaryPodIndex: 0,
			want:            true,
		},
		{
			name:       "never configured, primary role unknown but assigned",
			configured: false,
			replication: &mariadbv1alpha1.ReplicationStatus{
				Roles: map[string]mariadbv1alpha1.ReplicationRole{
					"mariadb-repl-0": mariadbv1alpha1.ReplicationRoleUnknown,
				},
			},
			primaryPodIndex: 0,
			want:            false,
		},
		{
			name:       "never configured, role assigned to the primary of another index",
			configured: false,
			replication: &mariadbv1alpha1.ReplicationStatus{
				Roles: map[string]mariadbv1alpha1.ReplicationRole{
					"mariadb-repl-0": mariadbv1alpha1.ReplicationRoleReplica,
				},
			},
			primaryPodIndex: 1,
			want:            true,
		},
		{
			name:            "configured, no replication status",
			configured:      true,
			replication:     nil,
			primaryPodIndex: 0,
			want:            false,
		},
		{
			name:       "configured, roles only for the replicas",
			configured: true,
			replication: &mariadbv1alpha1.ReplicationStatus{
				Roles: map[string]mariadbv1alpha1.ReplicationRole{
					"mariadb-repl-1": mariadbv1alpha1.ReplicationRoleReplica,
				},
			},
			primaryPodIndex: 0,
			want:            false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mdb := &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mariadb-repl",
					Namespace: "default",
				},
				Status: mariadbv1alpha1.MariaDBStatus{
					Replication: tt.replication,
				},
			}
			if tt.configured {
				meta.SetStatusCondition(&mdb.Status.Conditions, replicationConfigured)
			}
			assert.Equal(t, tt.want, shouldWaitForPrimaryRole(mdb, tt.primaryPodIndex))
		})
	}
}

// TestFromPrimaryPodIndex covers the index the post-promotion switchover phases read.
//
// Once the promotion is committed, status.currentPrimaryPodIndex names the NEW primary. A
// phase that demotes "the current primary" by reading the status would then demote the node
// it just promoted, so the switchover pins where it started from instead.
func TestFromPrimaryPodIndex(t *testing.T) {
	statusIndex := 1
	pinned := 0

	cases := []struct {
		name    string
		req     *ReconcileRequest
		want    int
		wantErr bool
	}{
		{
			name: "pinned index wins over a status that has already moved",
			req: &ReconcileRequest{
				mariadb:                &mariadbv1alpha1.MariaDB{Status: mariadbv1alpha1.MariaDBStatus{CurrentPrimaryPodIndex: &pinned}},
				switchoverFromPodIndex: &statusIndex,
			},
			want: statusIndex,
		},
		{
			name: "falls back to the status outside a switchover",
			req: &ReconcileRequest{
				mariadb: &mariadbv1alpha1.MariaDB{Status: mariadbv1alpha1.MariaDBStatus{CurrentPrimaryPodIndex: &statusIndex}},
			},
			want: statusIndex,
		},
		{
			name:    "errors when neither is set",
			req:     &ReconcileRequest{mariadb: &mariadbv1alpha1.MariaDB{}},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.req.fromPrimaryPodIndex()
			if tt.wantErr {
				if err == nil {
					t.Fatal("fromPrimaryPodIndex() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("fromPrimaryPodIndex() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("fromPrimaryPodIndex() = %d, want %d", got, tt.want)
			}
		})
	}
}

func freshMariadbWithReplication() *mariadbv1alpha1.MariaDB {
	return &mariadbv1alpha1.MariaDB{
		Spec: mariadbv1alpha1.MariaDBSpec{
			Replication: &mariadbv1alpha1.Replication{Enabled: true},
		},
	}
}

// TestShouldSkipPrimaryReconciliation covers the decision that gates primary
// configuration on a fresh replication cluster.
//
// The status phase infers role=Primary for the currentPrimaryPodIndex as soon
// as currentPrimaryPodIndex is defaulted, which happens before ConfigurePrimary
// has ever run. Skipping primary configuration on role alone then leaves a
// fresh cluster with no replication user and no read_only assertion, so
// replicas never establish replication and the agent readiness probe never
// clears. The skip must additionally require that replication has been
// configured at least once.
func TestShouldSkipPrimaryReconciliation(t *testing.T) {
	configured := func() *mariadbv1alpha1.MariaDB {
		mdb := freshMariadbWithReplication()
		conditions.SetReplicationConfigured(&mdb.Status)
		return mdb
	}

	tests := []struct {
		name       string
		mdb        *mariadbv1alpha1.MariaDB
		roles      map[string]mariadbv1alpha1.ReplicationRole
		expectSkip bool
	}{
		{
			name:       "fresh cluster: primary role alone must not skip configuration",
			mdb:        freshMariadbWithReplication(),
			roles:      map[string]mariadbv1alpha1.ReplicationRole{"mariadb-0": mariadbv1alpha1.ReplicationRolePrimary},
			expectSkip: false,
		},
		{
			name:       "converged cluster: primary role skips reconfiguration",
			mdb:        configured(),
			roles:      map[string]mariadbv1alpha1.ReplicationRole{"mariadb-0": mariadbv1alpha1.ReplicationRolePrimary},
			expectSkip: true,
		},
		{
			name:       "role not yet assigned still skips",
			mdb:        freshMariadbWithReplication(),
			roles:      map[string]mariadbv1alpha1.ReplicationRole{},
			expectSkip: true,
		},
		{
			name:       "fresh cluster: unknown role does not skip",
			mdb:        freshMariadbWithReplication(),
			roles:      map[string]mariadbv1alpha1.ReplicationRole{"mariadb-0": mariadbv1alpha1.ReplicationRoleUnknown},
			expectSkip: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldSkipPrimaryReconciliation(tt.mdb, tt.roles, "mariadb-0", logf.Log)
			if got != tt.expectSkip {
				t.Errorf("shouldSkipPrimaryReconciliation() = %v, want %v", got, tt.expectSkip)
			}
		})
	}
}

// TestFromPrimaryPodIndex covers the index the post-promotion switchover phases read.
//
// Once the promotion is committed, status.currentPrimaryPodIndex names the NEW primary. A
// phase that demotes "the current primary" by reading the status would then demote the node
// it just promoted, so the switchover pins where it started from instead.
