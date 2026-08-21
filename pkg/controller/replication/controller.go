package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	agentclient "github.com/mariadb-operator/mariadb-operator/v26/pkg/agent/client"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/builder"
	conditions "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/controller/configmap"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/controller/secret"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/controller/service"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/environment"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Option func(*ReplicationReconciler)

func WithRefResolver(rr *refresolver.RefResolver) Option {
	return func(r *ReplicationReconciler) {
		r.refResolver = rr
	}
}

func WithSecretReconciler(sr *secret.SecretReconciler) Option {
	return func(rr *ReplicationReconciler) {
		rr.secretReconciler = sr
	}
}

func WithServiceReconciler(sr *service.ServiceReconciler) Option {
	return func(rr *ReplicationReconciler) {
		rr.serviceReconciler = sr
	}
}

type ReplicationReconciler struct {
	client.Client
	recorder            events.EventRecorder
	builder             *builder.Builder
	env                 *environment.OperatorEnv
	topologyManager     *TopologyManager
	refResolver         *refresolver.RefResolver
	secretReconciler    *secret.SecretReconciler
	configMapreconciler *configmap.ConfigMapReconciler
	serviceReconciler   *service.ServiceReconciler
}

func NewReplicationReconciler(client client.Client, recorder events.EventRecorder, builder *builder.Builder, env *environment.OperatorEnv,
	topologyManager *TopologyManager, opts ...Option) (*ReplicationReconciler, error) {
	r := &ReplicationReconciler{
		Client:          client,
		recorder:        recorder,
		builder:         builder,
		env:             env,
		topologyManager: topologyManager,
	}
	for _, setOpt := range opts {
		setOpt(r)
	}
	if r.refResolver == nil {
		r.refResolver = refresolver.New(client)
	}
	if r.secretReconciler == nil {
		reconciler, err := secret.NewSecretReconciler(client, builder)
		if err != nil {
			return nil, err
		}
		r.secretReconciler = reconciler
	}
	if r.configMapreconciler == nil {
		r.configMapreconciler = configmap.NewConfigMapReconciler(client, builder)
	}
	if r.serviceReconciler == nil {
		r.serviceReconciler = service.NewServiceReconciler(client)
	}
	return r, nil
}

type ReconcileRequest struct {
	mariadb             *mariadbv1alpha1.MariaDB
	key                 types.NamespacedName
	replClientSet       *ReplicationClientSet
	agentClientSet      *agentclient.ClientSet
	currentPrimaryReady bool
	replicasSynced      bool
	// switchoverFromPodIndex is the primary a switchover in progress started from. It is
	// pinned before the phases run because the promotion is committed to status partway
	// through them, and the phases that follow still act on the old primary.
	switchoverFromPodIndex *int
}

// fromPrimaryPodIndex returns the primary a switchover started from, falling back to the
// status for callers reached outside a switchover.
func (r *ReconcileRequest) fromPrimaryPodIndex() (int, error) {
	if r.switchoverFromPodIndex != nil {
		return *r.switchoverFromPodIndex, nil
	}
	if r.mariadb.Status.CurrentPrimaryPodIndex == nil {
		return 0, errors.New("'status.currentPrimaryPodIndex' must be set")
	}
	return *r.mariadb.Status.CurrentPrimaryPodIndex, nil
}

func (r *ReconcileRequest) Close() error {
	if r.replClientSet != nil {
		r.replClientSet.close()
	}
	return nil
}

func (r *ReplicationReconciler) NewReconcileRequest(ctx context.Context, mdb *mariadbv1alpha1.MariaDB) (*ReconcileRequest, error) {
	replClientSet, err := NewReplicationClientSet(mdb, r.refResolver)
	if err != nil {
		return nil, fmt.Errorf("error creating mariadb clientset: %v", err)
	}
	agentClientSet, err := agentclient.NewClientSet(ctx, mdb, r.env, r.refResolver)
	if err != nil {
		return nil, fmt.Errorf("error getting agent clientset: %v", err)
	}
	return &ReconcileRequest{
		mariadb:             mdb,
		key:                 client.ObjectKeyFromObject(mdb),
		replClientSet:       replClientSet,
		agentClientSet:      agentClientSet,
		currentPrimaryReady: false,
		replicasSynced:      false,
	}, nil
}

func (r *ReplicationReconciler) Reconcile(ctx context.Context, mdb *mariadbv1alpha1.MariaDB) (ctrl.Result, error) {
	if !mdb.IsReplicationEnabled() {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx).WithName("replication")
	switchoverLogger := log.FromContext(ctx).WithName("switchover")

	req, err := r.NewReconcileRequest(ctx, mdb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error creating reconcile request: %v", err)
	}
	defer req.Close()

	if mdb.IsReplicationSwitchoverRequired() {
		return ctrl.Result{}, r.reconcileSwitchover(ctx, req, switchoverLogger)
	}
	if result, err := r.reconcileReplication(ctx, req, logger); !result.IsZero() || err != nil {
		return result, err
	}
	return ctrl.Result{}, r.reconcileSwitchover(ctx, req, switchoverLogger)
}

func (r *ReplicationReconciler) reconcileReplication(ctx context.Context, req *ReconcileRequest, logger logr.Logger) (ctrl.Result, error) {
	if result, err := r.shouldReconcileReplication(ctx, req, logger); !result.IsZero() || err != nil {
		return result, err
	}
	for _, i := range r.replicationPodIndexes(req) {
		if result, err := r.ReconcileReplicationInPod(ctx, req, i, logger); !result.IsZero() || err != nil {
			return result, err
		}
	}
	if !req.mariadb.HasConfiguredReplication() {
		if err := r.patchStatus(ctx, req.mariadb, func(status *mariadbv1alpha1.MariaDBStatus) {
			conditions.SetReplicationConfigured(status)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *ReplicationReconciler) shouldReconcileReplication(ctx context.Context, req *ReconcileRequest,
	logger logr.Logger) (ctrl.Result, error) {
	if req.mariadb.Status.CurrentPrimaryPodIndex == nil {
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}
	if req.mariadb.IsSwitchingPrimary() {
		return ctrl.Result{}, nil
	}
	if req.mariadb.IsMaxScaleEnabled() {
		mxs, err := r.refResolver.MaxScale(ctx, req.mariadb.Spec.MaxScaleRef, req.mariadb.Namespace)
		if err != nil {
			// MaxScale is not present, so no conflict can occur. Safe to proceed with replication reconciliation.
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, fmt.Errorf("error getting MaxScale: %v", err)
		}
		if mxs.IsSwitchingPrimary() {
			logger.Info("MaxScale is switching primary. Requeuing..")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	return ctrl.Result{}, nil
}

func (r *ReplicationReconciler) replicationPodIndexes(req *ReconcileRequest) []int {
	podIndexes := []int{
		*req.mariadb.Status.CurrentPrimaryPodIndex,
	}
	for i := 0; i < int(req.mariadb.Spec.Replicas); i++ {
		if i != *req.mariadb.Status.CurrentPrimaryPodIndex {
			podIndexes = append(podIndexes, i)
		}
	}
	return podIndexes
}

type ReconcilePodOpts struct {
	forceReplicaConfiguration bool
	volumeSnapshotKey         *types.NamespacedName
}

type ReconcilePodOpt func(*ReconcilePodOpts)

func WithForceReplicaConfiguration(reconcile bool) ReconcilePodOpt {
	return func(rpo *ReconcilePodOpts) {
		rpo.forceReplicaConfiguration = reconcile
	}
}

func WithVolumeSnapshotKey(key *types.NamespacedName) ReconcilePodOpt {
	return func(rpo *ReconcilePodOpts) {
		rpo.volumeSnapshotKey = key
	}
}

func (r *ReplicationReconciler) ReconcileReplicationInPod(ctx context.Context, req *ReconcileRequest, podIndex int,
	logger logr.Logger, reconcilePodOpts ...ReconcilePodOpt) (ctrl.Result, error) {
	opts := ReconcilePodOpts{}
	for _, setOpt := range reconcilePodOpts {
		setOpt(&opts)
	}

	primaryPodIndex := *req.mariadb.Status.CurrentPrimaryPodIndex
	replStatus := ptr.Deref(req.mariadb.Status.Replication, mariadbv1alpha1.ReplicationStatus{})
	replRoles := replStatus.Roles
	pod := statefulset.PodName(req.mariadb.ObjectMeta, podIndex)
	topology := r.topologyManager.TopologyForMariaDB(req.mariadb, logger.WithValues("pod", pod))

	if primaryPodIndex == podIndex {
		if shouldSkipPrimaryReconciliation(req.mariadb, replRoles, pod, logger) {
			return ctrl.Result{}, r.assertPrimaryWritable(ctx, req, pod, logger)
		}
		client, err := req.replClientSet.currentPrimaryClient(ctx)
		if err != nil {
			logger.V(1).Info("error getting current primary client", "err", err, "pod", pod)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		if err := topology.ConfigurePrimary(ctx, client); err != nil {
			return ctrl.Result{}, fmt.Errorf("error configuring primary: %v", err)
		}
		return ctrl.Result{}, r.assertPrimaryWritable(ctx, req, pod, logger)
	}

	if !opts.forceReplicaConfiguration {
		role, ok := replRoles[pod]
		if ok && role == mariadbv1alpha1.ReplicationRoleReplica {
			// "Replica" says nothing about WHICH primary this Pod follows. A
			// replica left pointing at a primary that no longer exists still
			// reports Slave_SQL_Running=Yes and stays classified Replica
			// forever, so this early return is the reason a survivor of a
			// failover has to be reattached by hand. Re-point it instead.
			stale, err := r.replicaFollowsWrongPrimary(ctx, req, podIndex, primaryPodIndex, pod, logger)
			if err == nil && stale {
				logger.Info("Replica follows a stale primary, reconfiguring", "pod", pod)
			} else {
				// Full replica configuration is skipped — but read_only is a
				// runtime SET GLOBAL that is not persisted, and it is only ever
				// written on a role change. A replica that merely restarts, and
				// an ex-primary demoted before this status was written, both
				// come back writable and stay that way indefinitely. A stray
				// write there does not replicate and diverges the cluster
				// permanently, so re-assert it every reconcile. A pod the spec
				// designates as primary is only transiently classified as a
				// replica (role status lags a promotion): forcing read_only on
				// it would fight the promotion.
				if podIsSpecPrimary(req, podIndex) {
					return ctrl.Result{}, nil
				}
				return ctrl.Result{}, r.assertReplicaReadOnly(ctx, req, podIndex, pod, logger)
			}
		}
	}

	client, err := req.replClientSet.clientForIndex(ctx, podIndex)
	if err != nil {
		logger.V(1).Info("error getting replica client", "err", err, "pod", pod)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	replicaOpts, err := r.getReplicaOpts(ctx, req, pod, podIndex, logger, reconcilePodOpts...)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error getting replica opts: %v", err)
	}
	if err := topology.ConfigureReplica(ctx, client, primaryPodIndex, replicaOpts...); err != nil {
		return ctrl.Result{}, fmt.Errorf("error configuring replica: %v", err)
	}
	return ctrl.Result{}, nil
}

// replicaMasterHost returns the host a replica is replicating from, empty when it is not
// replicating at all.
func (r *ReplicationReconciler) replicaMasterHost(ctx context.Context, req *ReconcileRequest,
	podIndex int, pod string, logger logr.Logger) (string, error) {
	client, err := req.replClientSet.clientForIndex(ctx, podIndex)
	if err != nil {
		logger.V(1).Info("error getting replica client to check its primary", "err", err, "pod", pod)
		return "", err
	}
	host, err := client.ReplicaMasterHost(ctx)
	if err != nil {
		logger.V(1).Info("error reading replica master host", "err", err, "pod", pod)
		return "", err
	}
	return host, nil
}

// primaryHost returns the host the operator would configure replicas to follow for a given
// primary index — the same expression ConfigureReplica uses.
func (r *ReplicationReconciler) primaryHost(req *ReconcileRequest, primaryPodIndex int) string {
	return statefulset.PodFQDNWithService(req.mariadb.ObjectMeta, primaryPodIndex, req.mariadb.InternalServiceKey().Name)
}

// replicaFollowsWrongPrimary reports whether an already-configured replica is
// replicating from something other than the current primary.
//
// Deliberately conservative: any error, and any empty Master_Host, reports
// false. Reconfiguring a replica resets its binlog and restarts replication, so
// the only acceptable false answer here is "leave it alone".
func (r *ReplicationReconciler) replicaFollowsWrongPrimary(ctx context.Context, req *ReconcileRequest,
	podIndex, primaryPodIndex int, pod string, logger logr.Logger) (bool, error) {
	host, err := r.replicaMasterHost(ctx, req, podIndex, pod, logger)
	if err != nil {
		return false, err
	}
	if host == "" {
		return false, nil
	}
	want := r.primaryHost(req, primaryPodIndex)
	if host == want {
		return false, nil
	}
	logger.V(1).Info("Replica follows an unexpected primary", "pod", pod, "master", host, "want", want)
	return true, nil
}

// replicaFollowsPrimary reports whether a replica is replicating from primaryPodIndex right
// now.
//
// The opposite bias to replicaFollowsWrongPrimary, because it gates a wait rather than a
// reconfiguration: anything that cannot be determined reports true, and the caller waits
// exactly as it would have. Only a replica positively observed to be following something
// else — or nothing at all, which no amount of waiting will change — reports false.
func (r *ReplicationReconciler) replicaFollowsPrimary(ctx context.Context, req *ReconcileRequest,
	podIndex, primaryPodIndex int, logger logr.Logger) bool {
	pod := statefulset.PodName(req.mariadb.ObjectMeta, podIndex)
	host, err := r.replicaMasterHost(ctx, req, podIndex, pod, logger)
	if err != nil {
		return true
	}
	return host == r.primaryHost(req, primaryPodIndex)
}

// assertReplicaReadOnly makes read_only=ON true of an already-configured
// replica. It reads before writing so a converged cluster costs one cheap
// SELECT per reconcile and never touches the server, and it is deliberately
// non-fatal: a replica that cannot be reached is a problem for the rest of the
// reconcile loop to surface, not a reason to abort it here.
func (r *ReplicationReconciler) assertReplicaReadOnly(ctx context.Context, req *ReconcileRequest, podIndex int,
	pod string, logger logr.Logger) error {
	client, err := req.replClientSet.clientForIndex(ctx, podIndex)
	if err != nil {
		logger.V(1).Info("error getting replica client to assert read_only", "err", err, "pod", pod)
		return nil
	}
	readOnly, err := client.IsSystemVariableEnabled(ctx, "read_only")
	if err != nil {
		logger.V(1).Info("error reading read_only from replica", "err", err, "pod", pod)
		return nil
	}
	if readOnly {
		return nil
	}
	logger.Info("Replica is writable, re-asserting read_only", "pod", pod)
	if err := client.EnableReadOnly(ctx); err != nil {
		return fmt.Errorf("error enabling read_only in replica '%s': %v", pod, err)
	}
	return nil
}

func (r *ReplicationReconciler) getReplicaOpts(ctx context.Context, req *ReconcileRequest, pod string, index int,
	logger logr.Logger, reconcilePodOpts ...ReconcilePodOpt) ([]ConfigureReplicaOpt, error) {
	opts := ReconcilePodOpts{}
	for _, setOpt := range reconcilePodOpts {
		setOpt(&opts)
	}

	// avoid deleting binary logs during archival to prevent drifting from object storage.
	//
	// This has to be decided before the early return below, not after it. Steady-state
	// reconciliation takes that return, so it used to hand ConfigureReplica no options at
	// all and fall back to its ResetMaster:true default — silently dropping binary logs
	// that the archiver had not shipped yet. That path is how a demoted primary gets
	// converged after a switchover, so it is precisely the one that must keep them.
	var pitrOpts []ConfigureReplicaOpt
	if req.mariadb.IsPointInTimeRecoveryEnabled() {
		pitrOpts = append(pitrOpts, WithResetMaster(false))
	}
	if !opts.forceReplicaConfiguration {
		return pitrOpts, nil
	}

	var gtid string
	if opts.volumeSnapshotKey != nil {
		var snapshot volumesnapshotv1.VolumeSnapshot
		if err := r.Get(ctx, *opts.volumeSnapshotKey, &snapshot); err != nil {
			return nil, fmt.Errorf("error getting %s VolumeSnapshot: %v", (*opts.volumeSnapshotKey).Name, err)
		}
		snapshotGtid, ok := snapshot.Annotations[metadata.GtidAnnotation]
		if !ok {
			return nil, fmt.Errorf("could not find GTID annotation %s in VolumeSnapshot", metadata.GtidAnnotation)
		}

		gtid = snapshotGtid
		logger.Info("Got replica GTID from VolumeSnapshot", "pod", pod, "gtid", gtid, "snapshot", snapshot.Name)
	} else {
		agentClient, err := req.agentClientSet.ClientForIndex(index)
		if err != nil {
			return nil, fmt.Errorf("error getting agent client: %v", err)
		}
		agentGtid, err := agentClient.Replication.GetGtid(ctx)
		if err != nil {
			return nil, fmt.Errorf("error requesting GTID to agent: %v", err)
		}

		gtid = agentGtid
		logger.Info("Got replica GTID from agent", "pod", pod, "gtid", gtid)
	}

	changeMasterGtid, err := mariadbv1alpha1.GtidSlavePos.MariaDBFormat()
	if err != nil {
		return nil, fmt.Errorf("error getting change master GTID: %v", err)
	}
	replicaOpts := []ConfigureReplicaOpt{
		WithGtidSlavePos(gtid),
		WithChangeMasterOpts(
			sql.WithChangeMasterGtid(changeMasterGtid),
		),
	}
	return append(replicaOpts, pitrOpts...), nil
}

func (r *ReplicationReconciler) patchStatus(ctx context.Context, mariadb *mariadbv1alpha1.MariaDB,
	patcher func(*mariadbv1alpha1.MariaDBStatus)) error {
	patch := client.MergeFrom(mariadb.DeepCopy())
	patcher(&mariadb.Status)
	return r.Status().Patch(ctx, mariadb, patch)
}

// podIsSpecPrimary reports whether the spec designates this pod index as primary. Role
// status is inferred independently and can lag a promotion, so a pod the spec promotes must
// never be treated as a replica here — in particular, must never be forced read_only.
func podIsSpecPrimary(req *ReconcileRequest, podIndex int) bool {
	replication := ptr.Deref(req.mariadb.Spec.Replication, mariadbv1alpha1.Replication{})
	return replication.Primary.PodIndex != nil && *replication.Primary.PodIndex == podIndex
}

// shouldReassertPrimaryWritable gates the read_only=OFF assertion on the current primary.
// Explicitly requested read-only (maintenance mode) must survive, and an in-flight
// switchover has its own handling of the primary's read_only.
func shouldReassertPrimaryWritable(mdb *mariadbv1alpha1.MariaDB) bool {
	return !mdb.IsReadOnlyEnabled() && !mdb.IsSwitchingPrimary() && !mdb.IsReplicationSwitchoverRequired()
}

// assertPrimaryWritable makes read_only=OFF true of the current primary. The mirror of
// assertReplicaReadOnly: read_only is a runtime SET GLOBAL written only on role changes, so
// a primary that restarts into read_only — or one that a failed switchover or the
// maintenance reconciler left read-only — stays read-only forever: role inference
// classifies the pod as primary and the steady-state ConfigurePrimary path is skipped.
// Deliberately non-fatal, same as its mirror.
func (r *ReplicationReconciler) assertPrimaryWritable(ctx context.Context, req *ReconcileRequest, pod string,
	logger logr.Logger) error {
	if !shouldReassertPrimaryWritable(req.mariadb) {
		return nil
	}
	client, err := req.replClientSet.currentPrimaryClient(ctx)
	if err != nil {
		logger.V(1).Info("error getting primary client to assert read_only", "err", err, "pod", pod)
		return nil
	}
	readOnly, err := client.GetReadOnly(ctx)
	if err != nil {
		logger.V(1).Info("error reading read_only from primary", "err", err, "pod", pod)
		return nil
	}
	if !readOnly {
		return nil
	}
	logger.Info("Primary is read_only, re-asserting writability", "pod", pod)
	if err := client.DisableReadOnly(ctx); err != nil {
		return fmt.Errorf("error disabling read_only in primary '%s': %v", pod, err)
	}
	return nil
}

func shouldSkipPrimaryReconciliation(mariadb *mariadbv1alpha1.MariaDB, replRoles map[string]mariadbv1alpha1.ReplicationRole,
	pod string, logger logr.Logger) bool {
	role, ok := replRoles[pod]
	if !ok {
		logger.V(1).Info("Primary Pod role not yet assigned. Skipping reconciliation...", "pod", pod)
		return true
	}
	if mariadb.IsMultiClusterReplica() {
		return role == mariadbv1alpha1.ReplicationRolePrimaryReplica
	}
	if !mariadb.HasConfiguredReplication() {
		// The spec-primary pod reports role=Primary (via the currentPrimaryPodIndex
		// inference) as soon as status is defaulted, before ConfigurePrimary has
		// ever run. Skipping on role alone leaves a fresh cluster without a
		// configured primary - no replication user, read_only never asserted - so
		// replicas can never connect and never become ready. Configure the primary
		// at least once before trusting the role-based fast path.
		return false
	}
	return role == mariadbv1alpha1.ReplicationRolePrimary
}
