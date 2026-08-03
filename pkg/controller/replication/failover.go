package replication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	mdbpod "github.com/mariadb-operator/mariadb-operator/v26/pkg/pod"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	mdbsts "github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type FailoverHandler struct {
	client      client.Client
	refResolver *refresolver.RefResolver
	mariadb     *mariadbv1alpha1.MariaDB
	logger      logr.Logger
}

func NewFailoverHandler(client client.Client, mariadb *mariadbv1alpha1.MariaDB,
	logger logr.Logger) *FailoverHandler {
	return &FailoverHandler{
		client:      client,
		refResolver: refresolver.New(client),
		mariadb:     mariadb,
		logger:      logger,
	}
}

// candidateOpts gates which replicas may be considered for promotion.
//
// The defaults are the strict rules that apply during a switchover, where the
// current primary is alive and every replica should therefore be fully healthy.
// They are wrong during a failover, where the primary is gone by definition:
// see WithPrimaryDown.
type candidateOpts struct {
	requirePodReady bool
	requireIOThread bool
}

// CandidateOpt customises promotion-candidate selection.
type CandidateOpt func(*candidateOpts)

// WithPrimaryDown relaxes the two gates a dead primary necessarily trips.
//
// Pod readiness for a replica is replication-aware: the agent's readiness probe
// reports failure when Seconds_Behind_Master cannot be determined, and
// Seconds_Behind_Master is NULL whenever the IO thread is not running. When the
// primary dies, every replica loses its IO thread within seconds, so every
// replica goes NotReady — before the kubelet has even marked the primary Pod
// NotReady (bound by node-monitor-grace-period). By the time failover is
// triggered there is nothing left that satisfies the strict gates, and failover
// is silently skipped for as long as the primary stays down. Requiring a
// running IO thread on the failover path is likewise self-contradictory: there
// is no primary left for it to connect to.
//
// The safety property that actually matters is preserved either way: a
// candidate must still have its SQL thread running and its relay log fully
// applied, so it cannot be promoted while holding unapplied events, and
// ranking still picks the furthest-advanced GTID among the survivors.
func WithPrimaryDown() CandidateOpt {
	return func(o *candidateOpts) {
		o.requirePodReady = false
		o.requireIOThread = false
	}
}

// FurthestAdvancedReplica finds a candidate to be promoted as primary, taking into account replica status.
func (f *FailoverHandler) FurthestAdvancedReplica(ctx context.Context, opts ...CandidateOpt) (string, error) {
	pods, err := mdbpod.ListMariaDBSecondaryPods(ctx, f.client, f.mariadb)
	if err != nil {
		return "", fmt.Errorf("error listing secondary Pods: %v", err)
	}
	f.logger.Info("Finding candidates to be promoted to primary")

	candidateOpts := candidateOpts{
		requirePodReady: true,
		requireIOThread: true,
	}
	for _, setOpt := range opts {
		setOpt(&candidateOpts)
	}

	candidates := f.findCandidates(ctx, pods, candidateOpts)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].name < candidates[j].name
	})
	if len(candidates) == 0 {
		return "", errors.New("no promotion candidates were found")
	}
	f.logger.Info("Found promotion candidates", "candidates", getCandidateNames(candidates))

	furthestAdvanced := f.furthestAdvancedCandidate(candidates)
	if furthestAdvanced == nil {
		return "", errors.New("no furthest advanced candidate was found")
	}
	return furthestAdvanced.name, nil
}

type promotionCandidate struct {
	name           string
	gtidCurrentPos *replication.Gtid
}

// podEligible decides whether a Pod may be considered at all, before any SQL
// connection is attempted. It returns a reason suitable for logging when the
// Pod is rejected.
func podEligible(pod *corev1.Pod, opts candidateOpts) (string, bool) {
	if opts.requirePodReady {
		if !mdbpod.PodReady(pod) {
			return "Pod not ready", false
		}
		return "", true
	}
	// Readiness is not usable here (see WithPrimaryDown), but a Pod that is not
	// Running cannot serve as a primary either. Real reachability is verified by
	// the SQL connection the caller opens next.
	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Sprintf("Pod not running (phase %q)", pod.Status.Phase), false
	}
	return "", true
}

func (f *FailoverHandler) findCandidates(ctx context.Context, pods []corev1.Pod, opts candidateOpts) []promotionCandidate {
	candidates := make([]promotionCandidate, 0, len(pods))
	for _, pod := range pods {
		podLogger := f.logger.WithValues("name", pod.Name)

		if reason, ok := podEligible(&pod, opts); !ok {
			podLogger.Info(reason + ". Skipping...")
			continue
		}
		podIndex, err := mdbsts.PodIndex(pod.Name)
		if err != nil {
			podLogger.Info("Invalid Pod name. Skipping...", "err", err)
			continue
		}

		sqlClient, err := sql.NewInternalClientWithPodIndex(ctx, f.mariadb, f.refResolver, *podIndex, sql.WithTimeout(3*time.Second))
		if err != nil {
			podLogger.Info("Unable to create SQL connection. Skipping...", "err", err)
			continue
		}
		defer sqlClient.Close()

		status, err := sqlClient.ReplicaStatus(ctx, podLogger)
		if err != nil {
			podLogger.Info("Unable to get replica status Skipping...", "err", err)
			continue
		}

		if opts.requireIOThread {
			slaveIORunning := ptr.Deref(status.SlaveIORunning, false)
			if !slaveIORunning {
				podLogger.Info("IO thread not running. Skipping...")
				continue
			}
		}
		slaveSQLRunning := ptr.Deref(status.SlaveSQLRunning, false)
		if !slaveSQLRunning {
			podLogger.Info("SQL thread not running. Skipping...")
			continue
		}

		gtidDomainId, err := sqlClient.GtidDomainId(ctx)
		if err != nil {
			podLogger.Info("Error getting GTID domain ID. Skipping...", "err", err)
			continue
		}

		hasRelayLogEvents, err := HasRelayLogEvents(status, *gtidDomainId, podLogger)
		if err != nil {
			podLogger.Info("Error checking relay log events. Skipping...", "err", err)
			continue
		}
		if hasRelayLogEvents {
			podLogger.Info("Detected events in relay log. Skipping...")
			continue
		}

		if status.GtidCurrentPos == nil {
			podLogger.Info("GTID current position not set. Skipping...")
			continue
		}
		gtidCurrentPos, err := replication.ParseGtidWithDomainId(*status.GtidCurrentPos, *gtidDomainId, f.logger)
		if err != nil {
			podLogger.Info("Error parsing GTID current position. Skipping...", "err", err)
			continue
		}

		candidates = append(candidates, promotionCandidate{
			name:           pod.Name,
			gtidCurrentPos: gtidCurrentPos,
		})
	}
	return candidates
}

func (f *FailoverHandler) furthestAdvancedCandidate(candidates []promotionCandidate) *promotionCandidate {
	var furthestAdvanced *promotionCandidate
	for i := range candidates {
		c := &candidates[i]
		candidateLogger := f.logger.WithValues("candidate", c.name)

		if c.gtidCurrentPos == nil {
			candidateLogger.Info("GTID position not set. Skipping...")
			continue
		}
		if furthestAdvanced == nil {
			furthestAdvanced = c
			continue
		}

		greaterThan, err := c.gtidCurrentPos.GreaterThan(furthestAdvanced.gtidCurrentPos)
		if err != nil {
			candidateLogger.Info("Error comparing GTID values. Skipping...", "err", err)
			continue
		}
		if greaterThan {
			furthestAdvanced = c
		}
	}
	return furthestAdvanced
}

func getCandidateNames(candidates []promotionCandidate) []string {
	names := make([]string, len(candidates))
	for i, c := range candidates {
		names[i] = c.name
	}
	return names
}
