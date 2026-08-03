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
