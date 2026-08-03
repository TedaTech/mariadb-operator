package replication

import (
	"context"
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

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
