package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	condition "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	mariadbpod "github.com/mariadb-operator/mariadb-operator/v26/pkg/pod"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/wait"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

type switchoverPhase struct {
	name      string
	reconcile func(context.Context, *ReconcileRequest, logr.Logger) error
	// commitsPromotion marks the phase after which the new primary is writable and the
	// switchover can no longer be undone. See reconcileSwitchover.
	commitsPromotion bool
}

func isSwitchoverStale(mdb *mariadbv1alpha1.MariaDB) bool {
	return mdb.IsSwitchingPrimary() && !mdb.IsReplicationSwitchoverRequired()
}

func shouldReconcileSwitchover(mdb *mariadbv1alpha1.MariaDB) bool {
	if mdb.IsMaxScaleEnabled() || mdb.IsRestoringBackup() || mdb.IsResizingStorage() {
		return false
	}
	if !mdb.HasConfiguredReplica() {
		return false
	}
	return mdb.IsReplicationSwitchoverRequired()
}

// switchoverPhases returns the ordered steps of a primary switchover. Phases up to and
// including the one marked commitsPromotion establish the new primary; those after it
// reattach the rest of the topology to it.
func (r *ReplicationReconciler) switchoverPhases() []switchoverPhase {
	return []switchoverPhase{
		{
			name:      "Lock primary with read lock",
			reconcile: r.lockPrimaryWithReadLock,
		},
		{
			name:      "Set read_only in primary",
			reconcile: r.setPrimaryReadOnly,
		},
		{
			name:      "Wait sync",
			reconcile: r.waitSync,
		},
		{
			name:             "Configure new primary",
			reconcile:        r.configureNewPrimary,
			commitsPromotion: true,
		},
		{
			name:      "Connect replicas to new primary",
			reconcile: r.connectReplicasToNewPrimary,
		},
		{
			name:      "Change primary to replica",
			reconcile: r.changePrimaryToReplica,
		},
	}
}

func (r *ReplicationReconciler) reconcileSwitchover(ctx context.Context, req *ReconcileRequest, switchoverLogger logr.Logger) error {
	logger := switchoverLogger.WithValues("mariadb", req.mariadb.Name)

	currentPrimaryReady, err := r.currentPrimaryReady(ctx, req.mariadb, req.replClientSet)
	if err != nil {
		return fmt.Errorf("error getting current primary readiness: %v", err)
	}
	req.currentPrimaryReady = currentPrimaryReady

	if err := r.reconcileStaleSwitchover(ctx, req, logger); err != nil {
		return fmt.Errorf("error reconciling stale switchover: %v", err)
	}
	if !shouldReconcileSwitchover(req.mariadb) {
		return nil
	}

	replication := ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{})
	primary := req.mariadb.Status.CurrentPrimaryPodIndex
	newPrimary := *replication.Primary.PodIndex
	newPrimaryPodName := statefulset.PodName(req.mariadb.ObjectMeta, *replication.Primary.PodIndex)
	logger = logger.WithValues("primary", primary, "new-primary", newPrimary)

	// Pin the primary this switchover started from. The promotion is committed to status
	// midway through the phases below, after which Status.CurrentPrimaryPodIndex names the
	// NEW primary — but the remaining phases still have to act on the old one, so they read
	// this instead of the status.
	req.switchoverFromPodIndex = primary

	if err := r.patchStatus(ctx, req.mariadb, func(status *mariadbv1alpha1.MariaDBStatus) {
		condition.SetPrimarySwitching(&req.mariadb.Status, newPrimaryPodName)
	}); err != nil {
		return fmt.Errorf("error patching MariaDB status: %v", err)
	}

	// The promotion is committed to status as soon as the new primary is writable, not
	// after every phase has succeeded.
	//
	// The primary Service selector is built from Status.CurrentPrimaryPodIndex, so until
	// that field moves, traffic keeps going to the old primary — which phase 2 has just set
	// read_only on, and which only the LAST phase would ever unset. Committing at the end
	// therefore means any failure in the phases after the promotion takes writes down for
	// as long as it persists, even though a healthy writable primary already exists. That
	// is what happened on 2026-08-03: error 1947 in the final phase left a promoted,
	// writable mdb-0 unused for 25 minutes while the Service pointed at a read-only mdb-1.
	//
	// The phases after the commit reattach replicas and demote the old primary. They are
	// convergence work, retried by the steady-state reconcile if they fail here, and none
	// of them makes the promotion any more or less true.
	for _, p := range r.switchoverPhases() {
		if err := p.reconcile(ctx, req, logger.WithValues("phase", p.name)); err != nil {
			if apierrors.IsNotFound(err) {
				return err
			}
			return fmt.Errorf("error in %s switchover reconcile phase: %v", p.name, err)
		}
		if !p.commitsPromotion {
			continue
		}
		if err := r.patchStatus(ctx, req.mariadb, func(status *mariadbv1alpha1.MariaDBStatus) {
			status.UpdateCurrentPrimary(req.mariadb, newPrimary)
			condition.SetPrimarySwitched(&req.mariadb.Status)
		}); err != nil {
			return fmt.Errorf("error patching MariaDB status: %v", err)
		}

		logger.Info("Primary switched")
		r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonPrimarySwitched,
			mariadbv1alpha1.ActionReconciling, "Primary switched from index '%d' to index '%d'", *primary, newPrimary)
	}

	return nil
}

func (r *ReplicationReconciler) reconcileStaleSwitchover(ctx context.Context, req *ReconcileRequest,
	logger logr.Logger) error {
	if !isSwitchoverStale(req.mariadb) {
		return nil
	}
	if !req.currentPrimaryReady {
		logger.Info("Skipped stale switchover reconciliation due to primary's non ready status")
		return nil
	}
	currentPrimaryClient, err := req.replClientSet.currentPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting current primary client: %v", err)
	}

	logger.Info("Unlocking primary")
	if err := currentPrimaryClient.UnlockTables(ctx); err != nil {
		return fmt.Errorf("error unlocking primary: %v", err)
	}

	logger.Info("Disabling readonly in primary")
	if err := currentPrimaryClient.DisableReadOnly(ctx); err != nil {
		return fmt.Errorf("error disabling readonly in primary: %v", err)
	}

	if err := r.patchStatus(ctx, req.mariadb, func(status *mariadbv1alpha1.MariaDBStatus) {
		condition.SetPrimarySwitched(&req.mariadb.Status)
	}); err != nil {
		return fmt.Errorf("error patching MariaDB status: %v", err)
	}

	logger.Info("Stale switchover has been reset")
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationResetStaleSwitchover,
		mariadbv1alpha1.ActionReconciling, "Stale switchover has been reset")
	return nil
}

func (r *ReplicationReconciler) lockPrimaryWithReadLock(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	if !req.currentPrimaryReady {
		logger.Info("Skipped locking primary with read lock due to primary's non ready status")
		return nil
	}
	client, err := req.replClientSet.currentPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting current primary client: %v", err)
	}

	logger.Info("Locking primary with read lock")
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationPrimaryLock,
		mariadbv1alpha1.ActionReconciling, "Locking primary with read lock")
	return client.LockTablesWithReadLock(ctx)
}

func (r *ReplicationReconciler) setPrimaryReadOnly(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	if !req.currentPrimaryReady {
		logger.Info("Skipped enabling readonly mode in primary due to primary's non ready status")
		return nil
	}
	client, err := req.replClientSet.currentPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting current primary client: %v", err)
	}

	logger.Info("Enabling readonly mode in primary")
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationPrimaryReadonly,
		mariadbv1alpha1.ActionReconciling, "Enabling readonly mode in primary")
	return client.EnableReadOnly(ctx)
}

func (r *ReplicationReconciler) waitSync(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	if req.currentPrimaryReady {
		return r.waitForReplicaSync(ctx, req, logger)
	}
	return r.waitForNewPrimarySync(ctx, req, logger)
}

func (r *ReplicationReconciler) waitForReplicaSync(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	primaryPodIndex, err := req.fromPrimaryPodIndex()
	if err != nil {
		return err
	}
	if !req.currentPrimaryReady {
		logger.Info("Skipped waiting for replicas to be synced with primary due to primary's non ready status")
		return nil
	}

	primaryClient, err := req.replClientSet.clientForIndex(ctx, primaryPodIndex)
	if err != nil {
		return fmt.Errorf("error getting current primary client: %v", err)
	}
	primaryGtid, err := primaryClient.GtidBinlogPos(ctx)
	if err != nil {
		return fmt.Errorf("error getting primary GTID binlog pos: %v", err)
	}
	if primaryGtid == "" {
		return errors.New("primary GTID (gtid_binlog_pos) is empty")
	}

	logger.Info("Waiting for replicas to be synced with primary", "gtid", primaryGtid)
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationReplicaSync,
		mariadbv1alpha1.ActionReconciling, "Waiting for replicas to be synced with primary")
	replication := ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{})

	g := new(errgroup.Group)
	g.SetLimit(int(req.mariadb.Spec.Replicas))

	for i := 0; i < int(req.mariadb.Spec.Replicas); i++ {
		if i == primaryPodIndex {
			continue
		}
		g.Go(func() error {
			replClient, err := req.replClientSet.clientForIndex(ctx, i)
			if err != nil {
				return fmt.Errorf("error getting replica '%d' client: %v", i, err)
			}

			// This phase is not naturally re-entrant, and the switchover restarts from the
			// first phase whenever a later one fails. By then a later phase has repointed
			// this replica at the new primary and reset its gtid_slave_pos, so it will
			// never receive the old primary's GTID again — MASTER_GTID_WAIT burns the
			// whole syncTimeout, the routine restarts, and the loop is unbounded. A
			// replica that is no longer following this primary cannot be waited on, and
			// does not need to be: the barrier was passed on the attempt that moved it.
			if !r.replicaFollowsPrimary(ctx, req, i, primaryPodIndex, logger) {
				logger.Info("Replica no longer follows this primary, already repointed. Skipping sync wait", "replica", i)
				return nil
			}

			logger.V(1).Info("Syncing replica with primary GTID", "replica", i, "gtid", primaryGtid)
			syncTimeout := ptr.Deref(replication.Replica.SyncTimeout, metav1.Duration{Duration: 10 * time.Second}).Duration

			if err := replClient.WaitForReplicaGtid(ctx, primaryGtid, syncTimeout); err != nil {
				logger.Error(err, "Error waiting for GTID in replica", "gtid", primaryGtid, "replica", i)
				r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeWarning, mariadbv1alpha1.ReasonReplicationReplicaSyncErr,
					mariadbv1alpha1.ActionReconciling, "Error waiting for GTID '%s' in replica '%d': %v", primaryGtid, i, err)
				return err
			}

			logger.V(1).Info("Replica synced", "replica", i, "gtid", primaryGtid)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("error waiting for replica sync: %w", err)
	}

	req.replicasSynced = true
	return nil
}

func (r *ReplicationReconciler) waitForNewPrimarySync(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	replication := ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{})
	newPrimaryClient, err := req.replClientSet.newPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting new primary client: %v", err)
	}

	// This phase is not naturally re-entrant, and the switchover routine is
	// restarted from the beginning whenever a later phase fails or syncTimeout
	// expires. configureNewPrimary, which runs next, stops and resets all slaves
	// on this Pod — so on the second pass through there is no replica status
	// left to read, HasRelayLogEvents fails on a nil GTID IO position, the poll
	// never succeeds, and the routine restarts again. That loop is unbounded:
	// the Pod parks in `status.replication.roles: Unknown` and the switchover
	// never completes. A node that is no longer a replica has, by definition,
	// no relay log left to apply, so there is nothing to wait for.
	isReplica, err := newPrimaryClient.IsReplicationReplica(ctx)
	if err != nil {
		return fmt.Errorf("error checking whether new primary is still a replica: %v", err)
	}
	if !isReplica {
		logger.Info("New primary is no longer a replica, already promoted. Skipping sync wait")
		return nil
	}

	logger.Info("Waiting for new primary to be synced")
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationPrimaryNewSync,
		mariadbv1alpha1.ActionReconciling, "Waiting for new primary to be synced")

	syncTimeout := ptr.Deref(replication.Replica.SyncTimeout, metav1.Duration{Duration: 10 * time.Second}).Duration
	syncCtx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	if err := wait.PollUntilSuccessOrContextCancel(syncCtx, logger, func(ctx context.Context) error {
		status, err := newPrimaryClient.ReplicaStatus(ctx, logger)
		if err != nil {
			return fmt.Errorf("error getting new primary status: %v", err)
		}
		// SHOW REPLICA STATUS returned no rows, so every field is nil. Either
		// replication was torn down since the check above, or the IO thread
		// never received anything; both mean an empty relay log. Without this
		// the poll would burn the whole syncTimeout on HasRelayLogEvents
		// rejecting the nil GTID IO position, and restart the routine.
		if status.GtidIOPos == nil && status.GtidCurrentPos == nil {
			logger.Info("New primary reports no replica status, nothing to sync")
			return nil
		}
		gtidDomainId, err := newPrimaryClient.GtidDomainId(ctx)
		if err != nil {
			return fmt.Errorf("error getting GTID domain ID in new primary: %v", err)
		}
		hasRelayLogEvents, err := HasRelayLogEvents(status, *gtidDomainId, logger)
		if err != nil {
			return fmt.Errorf("error checking relay logs: %v", err)
		}
		if hasRelayLogEvents {
			return errors.New("relay log events detected")
		}
		return nil
	}); err != nil {
		logger.Error(err, "Error waiting for new primary to be synced")
		r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeWarning, mariadbv1alpha1.ReasonReplicationPrimaryNewSyncErr,
			mariadbv1alpha1.ActionReconciling, "Error waiting for new primary to be synced: %v", err)
		return err
	}

	logger.V(1).Info("New primary synced")
	return nil
}

func (r *ReplicationReconciler) configureNewPrimary(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	newPrimary := *ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{}).Primary.PodIndex
	newPrimaryClient, err := req.replClientSet.newPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting new primary client: %v", err)
	}

	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationPrimaryNew,
		mariadbv1alpha1.ActionReconciling, "Configuring new primary at index '%d'", newPrimary)

	topology := r.topologyManager.TopologyForMariaDB(req.mariadb, logger)

	if err := topology.ConfigurePrimary(ctx, newPrimaryClient); err != nil {
		return fmt.Errorf("error configuring new primary vars: %v", err)
	}

	// The promotion commits to status right after this phase, and steady-state
	// reconciliation skips a switching primary, so the commit point must prove the
	// new primary is writable.
	readOnly, err := newPrimaryClient.GetReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("error reading read_only in new primary: %v", err)
	}
	if readOnly {
		// One idempotent repair attempt, then verify: fail only if it persists.
		if err := newPrimaryClient.DisableReadOnly(ctx); err != nil {
			return fmt.Errorf("error disabling read_only in new primary: %v", err)
		}
		readOnly, err = newPrimaryClient.GetReadOnly(ctx)
		if err != nil {
			return fmt.Errorf("error reading read_only in new primary: %v", err)
		}
		if readOnly {
			r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeWarning, mariadbv1alpha1.ReasonPrimaryStillReadOnly,
				mariadbv1alpha1.ActionReconciling, "New primary at index '%d' is still read_only after configuration", newPrimary)
			return fmt.Errorf("new primary at index '%d' is still read_only after configuration", newPrimary)
		}
	}
	return nil
}

func (r *ReplicationReconciler) connectReplicasToNewPrimary(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	oldPrimary, err := req.fromPrimaryPodIndex()
	if err != nil {
		return err
	}

	newPrimary := *ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{}).Primary.PodIndex
	newPrimaryClient, err := req.replClientSet.newPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting new primary client: %v", err)
	}

	logger.Info("Connecting replicas to new primary")
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationReplicaConn,
		mariadbv1alpha1.ActionReconciling, "Connecting replicas to new primary at '%d'", newPrimary)

	replicaOpts, err := r.configureReplicaOpts(ctx, req, newPrimaryClient, logger)
	if err != nil {
		return fmt.Errorf("error getting replica options: %v", err)
	}

	g := new(errgroup.Group)
	g.SetLimit(int(req.mariadb.Spec.Replicas))

	for i := 0; i < int(req.mariadb.Spec.Replicas); i++ {
		if i == oldPrimary || i == newPrimary {
			continue
		}
		g.Go(func() error {
			key := types.NamespacedName{
				Name:      statefulset.PodName(req.mariadb.ObjectMeta, i),
				Namespace: req.mariadb.Namespace,
			}
			var pod corev1.Pod
			if err := r.Get(ctx, key, &pod); err != nil {
				logger.V(1).Info("Error getting Pod when connecting replicas to new primary", "pod", key.Name)
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("error getting pod: %w", err)
			}
			if !mariadbpod.PodReady(&pod) {
				// On the failover path this gate skips EVERY replica: they all
				// lost their IO thread when the primary died, so their
				// replication-aware readiness probe fails, so none of them is
				// repointed and all of them keep replicating from a primary
				// that no longer exists. That is precisely the state that has
				// to be repaired by hand — CHANGE MASTER on each survivor —
				// after an otherwise successful failover. It also starves the
				// new primary of connected replicas, which is what leaves it
				// classified as Unknown.
				//
				// Reachability is established for real by the SQL client below,
				// so all that is needed here is a Pod that is actually running.
				if req.currentPrimaryReady || pod.Status.Phase != corev1.PodRunning {
					logger.V(1).Info("Skipping non ready Pod when connecting replicas to new primary", "pod", key.Name)
					return nil
				}
			}

			replClient, err := req.replClientSet.clientForIndex(ctx, i)
			if err != nil {
				// One unreachable replica must not fail the phase: that
				// restarts the whole switchover from the top, and on a
				// permanently dead node it would never stop. The steady-state
				// reconcile picks this replica up once it is back.
				logger.Info("Skipping unreachable replica when connecting to new primary", "replica", i, "err", err)
				return nil
			}
			topology := r.topologyManager.TopologyForMariaDB(req.mariadb, logger.WithValues("replica", i))

			if err := topology.ConfigureReplica(ctx, replClient, newPrimary, replicaOpts...); err != nil {
				return fmt.Errorf("error configuring replica '%d': %v", i, err)
			}

			return nil
		})
	}

	return g.Wait()
}

func (r *ReplicationReconciler) changePrimaryToReplica(ctx context.Context, req *ReconcileRequest, logger logr.Logger) error {
	if !req.currentPrimaryReady {
		logger.Info("Skipped changing primary to be a replica due to primary's non ready status")
		return nil
	}

	// Deliberately not currentPrimaryClient: the promotion has already been committed, so
	// the status now names the NEW primary and this phase would demote the node it just
	// promoted.
	currentPrimary, err := req.fromPrimaryPodIndex()
	if err != nil {
		return err
	}
	currentPrimaryClient, err := req.replClientSet.clientForIndex(ctx, currentPrimary)
	if err != nil {
		return fmt.Errorf("error getting current primary client: %v", err)
	}
	newPrimary := *ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{}).Primary.PodIndex
	newPrimaryClient, err := req.replClientSet.newPrimaryClient(ctx)
	if err != nil {
		return fmt.Errorf("error getting new primary client: %v", err)
	}

	logger.Info("Change primary to be a replica")
	r.recorder.Eventf(
		req.mariadb,
		nil,
		corev1.EventTypeNormal,
		mariadbv1alpha1.ReasonReplicationPrimaryToReplica,
		mariadbv1alpha1.ActionReconciling,
		"Unlocking primary '%d' and configuring it to be a replica. New primary at '%d'",
		currentPrimary,
		newPrimary,
	)

	replicaOpts, err := r.configureReplicaOpts(ctx, req, newPrimaryClient, logger)
	if err != nil {
		return fmt.Errorf("error getting replica options: %v", err)
	}

	logger.Info("Unlocking primary")
	r.recorder.Eventf(req.mariadb, nil, corev1.EventTypeNormal, mariadbv1alpha1.ReasonReplicationPrimaryLock,
		mariadbv1alpha1.ActionReconciling, "Unlocking primary")
	if err := currentPrimaryClient.UnlockTables(ctx); err != nil {
		return fmt.Errorf("error unlocking primary: %v", err)
	}

	topology := r.topologyManager.TopologyForMariaDB(req.mariadb, logger)

	return topology.ConfigureReplica(
		ctx,
		currentPrimaryClient,
		newPrimary,
		replicaOpts...,
	)
}

func (r *ReplicationReconciler) configureReplicaOpts(ctx context.Context, req *ReconcileRequest, primaryClient *sql.Client,
	logger logr.Logger) ([]ConfigureReplicaOpt, error) {
	var replicaOpts []ConfigureReplicaOpt

	if req.replicasSynced {
		// gtid_current_pos, not gtid_binlog_pos. log_slave_updates is off in single-cluster
		// topology, so a node that has been replicating records what it applied in
		// gtid_slave_pos and nothing of it reaches its own binary log. Read at the moment
		// of promotion this node is exactly that: gtid_binlog_pos holds only whatever it
		// wrote locally, which is behind the stream every replica has already consumed.
		// Handing that out rewinds them, and rewinds the demoted primary past its own
		// binary log — the assignment MariaDB rejects with error 1947.
		//
		// gtid_current_pos is the union of the two, so it is the only reading that means
		// "everything this node has applied".
		primaryGtid, err := primaryClient.GtidCurrentPos(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting primary current position: %v", err)
		}
		logger.Info("Configuring replicas with primary GTID", "gtid", primaryGtid)
		replicaOpts = append(replicaOpts, WithGtidSlavePos(primaryGtid))
	} else {
		replicaOpts = append(replicaOpts, WithResetGtidSlavePos())
	}

	// avoid deleting binary logs during archival to prevent drifting from object storage
	if req.mariadb.IsPointInTimeRecoveryEnabled() {
		replicaOpts = append(replicaOpts, WithResetMaster(false))
	}
	return replicaOpts, nil
}

func (r *ReplicationReconciler) currentPrimaryReady(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
	clientSet replicationClientSet) (bool, error) {
	if mariadb.Status.CurrentPrimaryPodIndex == nil {
		return false, errors.New("'status.currentPrimaryPodIndex' must be set")
	}
	_, err := clientSet.clientForIndex(ctx, *mariadb.Status.CurrentPrimaryPodIndex, sql.WithTimeout(1*time.Second))
	return err == nil, nil
}
