package replication

import (
	"context"
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	conditions "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

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

func mariadbWithPITR(enabled bool) *mariadbv1alpha1.MariaDB {
	mdb := &mariadbv1alpha1.MariaDB{
		Spec: mariadbv1alpha1.MariaDBSpec{
			Replication: &mariadbv1alpha1.Replication{Enabled: true},
		},
	}
	if enabled {
		mdb.Spec.PointInTimeRecoveryRef = &mariadbv1alpha1.LocalObjectReference{Name: "pitr"}
	}
	return mdb
}

func resetsMaster(opts []ConfigureReplicaOpt) bool {
	// The default ConfigureReplica applies when no option overrides it.
	resolved := ConfigureReplicaOpts{ResetMaster: true}
	for _, setOpt := range opts {
		setOpt(&resolved)
	}
	return resolved.ResetMaster
}

// TestGetReplicaOptsPreservesBinlogsUnderPITR covers the path steady-state reconciliation
// takes, which is the one that converges a demoted primary after a switchover.
//
// It used to return no options at all, so ConfigureReplica fell back to ResetMaster:true and
// wiped binary logs the archiver had not shipped yet. The PITR guard existed, but sat below
// the early return and only ever reached the forced-recovery path.
func TestGetReplicaOptsPreservesBinlogsUnderPITR(t *testing.T) {
	cases := []struct {
		name            string
		pitr            bool
		wantResetMaster bool
	}{
		{
			name:            "PITR enabled: binary logs must survive",
			pitr:            true,
			wantResetMaster: false,
		},
		{
			name:            "PITR disabled: the default still applies",
			pitr:            false,
			wantResetMaster: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := &ReplicationReconciler{}
			req := &ReconcileRequest{mariadb: mariadbWithPITR(tt.pitr)}

			// No ReconcilePodOpt, so forceReplicaConfiguration is false: the steady-state
			// path, which must not need an agent or a VolumeSnapshot to answer this.
			opts, err := r.getReplicaOpts(context.Background(), req, "mariadb-0", 0, logf.Log)
			if err != nil {
				t.Fatalf("getReplicaOpts() error = %v", err)
			}
			if got := resetsMaster(opts); got != tt.wantResetMaster {
				t.Errorf("ResetMaster = %v, want %v", got, tt.wantResetMaster)
			}
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
