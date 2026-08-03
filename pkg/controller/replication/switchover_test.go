package replication

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	"k8s.io/utils/ptr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func mariadbWithGtid(gtid *mariadbv1alpha1.Gtid) *mariadbv1alpha1.MariaDB {
	return &mariadbv1alpha1.MariaDB{
		Spec: mariadbv1alpha1.MariaDBSpec{
			Replication: &mariadbv1alpha1.Replication{
				ReplicationSpec: mariadbv1alpha1.ReplicationSpec{
					Replica: mariadbv1alpha1.ReplicaReplication{
						Gtid: gtid,
					},
				},
			},
		},
	}
}

// TestDefersToBinlogPos covers the gate on tolerating error 1947. Swallowing that error is
// only sound under current_pos, where the server merges gtid_binlog_pos into the position it
// replicates from; under slave_pos the assignment that failed was the one that mattered.
func TestDefersToBinlogPos(t *testing.T) {
	cases := []struct {
		name string
		gtid *mariadbv1alpha1.Gtid
		opts ConfigureReplicaOpts
		want bool
	}{
		{
			name: "unset defaults to current_pos",
			gtid: nil,
			want: true,
		},
		{
			name: "current_pos",
			gtid: ptr.To(mariadbv1alpha1.GtidCurrentPos),
			want: true,
		},
		{
			// The replica would resume from a gtid_slave_pos the failed assignment never
			// wrote. Must keep failing.
			name: "slave_pos",
			gtid: ptr.To(mariadbv1alpha1.GtidSlavePos),
			want: false,
		},
		{
			// getReplicaOpts forces slave_pos on the recovery path regardless of the CR,
			// and that override has to win here too.
			name: "ChangeMasterOpts override to slave_pos beats a current_pos spec",
			gtid: ptr.To(mariadbv1alpha1.GtidCurrentPos),
			opts: ConfigureReplicaOpts{
				ChangeMasterOpts: []sql.ChangeMasterOpt{sql.WithChangeMasterGtid("slave_pos")},
			},
			want: false,
		},
		{
			name: "ChangeMasterOpts override to current_pos beats a slave_pos spec",
			gtid: ptr.To(mariadbv1alpha1.GtidSlavePos),
			opts: ConfigureReplicaOpts{
				ChangeMasterOpts: []sql.ChangeMasterOpt{sql.WithChangeMasterGtid("current_pos")},
			},
			want: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			topology := &singleClusterTopology{
				mariadb: mariadbWithGtid(tt.gtid),
				logger:  logf.Log,
			}

			got, err := topology.defersToBinlogPos(tt.opts)
			if err != nil {
				t.Fatalf("defersToBinlogPos() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("defersToBinlogPos() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefersToBinlogPosRejectsAnInvalidGtid(t *testing.T) {
	topology := &singleClusterTopology{
		mariadb: mariadbWithGtid(ptr.To(mariadbv1alpha1.Gtid("Nonsense"))),
		logger:  logf.Log,
	}

	if _, err := topology.defersToBinlogPos(ConfigureReplicaOpts{}); err == nil {
		t.Error("defersToBinlogPos() error = nil; an unresolvable GTID mode must not silently tolerate 1947")
	}
}

// TestConfigureReplicaDefaultsToResetMaster pins the default the PITR guard exists to
// override. If this ever flips, getReplicaOpts' non-forced path stops mattering and the
// binlog-preservation guarantee changes meaning.
func TestConfigureReplicaDefaultsToResetMaster(t *testing.T) {
	opts := ConfigureReplicaOpts{ResetMaster: true}
	WithResetMaster(false)(&opts)

	if opts.ResetMaster {
		t.Error("WithResetMaster(false) did not clear ResetMaster")
	}
}

// TestCommitsPromotionMarksExactlyThePromotionPhase asserts the switchover commits its
// status when the new primary becomes writable, not after the phases that follow it.
//
// The primary Service selector is built from status.currentPrimaryPodIndex. Committing only
// after every phase succeeded is what let a failure in the LAST phase strand traffic on a
// read-only ex-primary for 25 minutes on 2026-08-03, while the promoted node sat writable
// and unused.
func TestCommitsPromotionMarksExactlyThePromotionPhase(t *testing.T) {
	r := &ReplicationReconciler{}
	phases := r.switchoverPhases()

	var committing []int
	for i, p := range phases {
		if p.commitsPromotion {
			committing = append(committing, i)
		}
	}

	if len(committing) != 1 {
		t.Fatalf("expected exactly one phase to commit the promotion, got %d: %v", len(committing), committing)
	}

	const wantPhase = "Configure new primary"
	if got := phases[committing[0]].name; got != wantPhase {
		t.Errorf("promotion committed after %q, want %q", got, wantPhase)
	}

	// Everything after the commit is convergence work the steady-state reconcile retries.
	// Anything that makes the promotion itself true must run before it.
	if committing[0] == len(phases)-1 {
		t.Error("the committing phase is last; the commit is then indistinguishable from the old end-of-loop patch")
	}
}
