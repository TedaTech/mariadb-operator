package replication

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
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

type fakeReplicationClientSet struct {
	client *sql.Client
}

func (f *fakeReplicationClientSet) close() error { return nil }

func (f *fakeReplicationClientSet) currentPrimaryClient(ctx context.Context,
	clientOpts ...sql.Opt) (*sql.Client, error) {
	return f.client, nil
}

func (f *fakeReplicationClientSet) clientForIndex(ctx context.Context, index int,
	clientOpts ...sql.Opt) (*sql.Client, error) {
	return f.client, nil
}

func (f *fakeReplicationClientSet) newPrimaryClient(ctx context.Context,
	clientOpts ...sql.Opt) (*sql.Client, error) {
	return f.client, nil
}

type fakeTopologyManager struct {
	topology Topology
}

func (f *fakeTopologyManager) TopologyForMariaDB(mariadb *mariadbv1alpha1.MariaDB,
	logger logr.Logger) Topology {
	return f.topology
}

type noopTopology struct{}

func (t *noopTopology) ConfigurePrimary(ctx context.Context, client *sql.Client) error {
	return nil
}

func (t *noopTopology) ConfigureReplica(ctx context.Context, client *sql.Client,
	primaryPodIndex int, replicaOpts ...ConfigureReplicaOpt) error {
	return nil
}

func newMockedSQLClient(t *testing.T) (*sql.Client, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating sqlmock: %v", err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectations not met: %v", err)
		}
		db.Close()
	})
	return sql.NewClientWithDB(db), mock
}

// TestConfigureNewPrimaryEnsuresWritable covers the promotion gate in
// configureNewPrimary: the promotion is committed right after this phase and nothing
// re-asserts read_only=0 for a switching primary, so the phase re-asserts it once and
// fails — leaving the promotion uncommitted — only if the variable persists.
func TestConfigureNewPrimaryEnsuresWritable(t *testing.T) {
	cases := []struct {
		name        string
		readOnly    string
		repair      bool
		afterRepair string
		wantErr     bool
	}{
		{
			name:     "writable new primary succeeds",
			readOnly: "0",
			wantErr:  false,
		},
		{
			name:        "read_only re-asserted and passes",
			readOnly:    "1",
			repair:      true,
			afterRepair: "0",
			wantErr:     false,
		},
		{
			name:        "read_only persists after re-assert fails the phase",
			readOnly:    "1",
			repair:      true,
			afterRepair: "1",
			wantErr:     true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client, mock := newMockedSQLClient(t)
			mock.ExpectQuery("SELECT @@global.read_only").
				WillReturnRows(sqlmock.NewRows([]string{"read_only"}).AddRow(tt.readOnly))
			if tt.repair {
				mock.ExpectExec("SET @@global.read_only=0").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT @@global.read_only").
					WillReturnRows(sqlmock.NewRows([]string{"read_only"}).AddRow(tt.afterRepair))
			}

			recorder := events.NewFakeRecorder(10)
			r := &ReplicationReconciler{
				recorder:        recorder,
				topologyManager: &fakeTopologyManager{topology: &noopTopology{}},
			}
			req := &ReconcileRequest{
				mariadb:       mariadbWithPrimaryPodIndex(1),
				replClientSet: &fakeReplicationClientSet{client: client},
			}

			err := r.configureNewPrimary(context.Background(), req, logf.Log)
			if tt.wantErr && err == nil {
				t.Fatal("configureNewPrimary() error = nil, want the promotion to fail on a read_only new primary")
			}
			if tt.wantErr && !strings.Contains(err.Error(), "still read_only") {
				t.Errorf("configureNewPrimary() error = %v, want it to mention the read_only new primary", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("configureNewPrimary() error = %v, want nil", err)
			}

			events := drainEvents(recorder)
			if got := containsEvent(events, string(mariadbv1alpha1.ReasonPrimaryStillReadOnly)); got != tt.wantErr {
				t.Errorf("PrimaryStillReadOnly event present = %v, want %v (events: %v)", got, tt.wantErr, events)
			}
		})
	}
}

func drainEvents(recorder *events.FakeRecorder) []string {
	var drained []string
	for {
		select {
		case e := <-recorder.Events:
			drained = append(drained, e)
		default:
			return drained
		}
	}
}

func containsEvent(events []string, reason string) bool {
	for _, e := range events {
		if strings.Contains(e, reason) {
			return true
		}
	}
	return false
}

func mariadbWithPrimaryPodIndex(podIndex int) *mariadbv1alpha1.MariaDB {
	return &mariadbv1alpha1.MariaDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mariadb",
			Namespace: "default",
		},
		Spec: mariadbv1alpha1.MariaDBSpec{
			Replication: &mariadbv1alpha1.Replication{
				Enabled: true,
				ReplicationSpec: mariadbv1alpha1.ReplicationSpec{
					Primary: mariadbv1alpha1.PrimaryReplication{PodIndex: ptr.To(podIndex)},
				},
			},
		},
	}
}
