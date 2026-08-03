package replication

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func strictOpts() candidateOpts {
	return candidateOpts{requirePodReady: true, requireIOThread: true}
}

func primaryDownOpts() candidateOpts {
	o := strictOpts()
	WithPrimaryDown()(&o)
	return o
}

func TestWithPrimaryDownRelaxesBothGates(t *testing.T) {
	opts := primaryDownOpts()
	if opts.requirePodReady {
		t.Error("requirePodReady = true; a replica cannot stay ready once the primary is gone")
	}
	if opts.requireIOThread {
		t.Error("requireIOThread = true; an IO thread cannot run against a dead primary")
	}
}

func pod(phase corev1.PodPhase, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:      phase,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

func TestPodEligible(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		opts candidateOpts
		want bool
	}{
		{
			name: "switchover accepts a ready pod",
			pod:  pod(corev1.PodRunning, true),
			opts: strictOpts(),
			want: true,
		},
		{
			// The whole point of the failover path: a replica goes NotReady the
			// moment the primary dies, because its readiness probe cannot
			// determine lag. Rejecting it here is what silently skipped
			// failover entirely.
			name: "switchover rejects a not-ready pod",
			pod:  pod(corev1.PodRunning, false),
			opts: strictOpts(),
			want: false,
		},
		{
			name: "failover accepts a running but not-ready pod",
			pod:  pod(corev1.PodRunning, false),
			opts: primaryDownOpts(),
			want: true,
		},
		{
			name: "failover still rejects a pending pod",
			pod:  pod(corev1.PodPending, false),
			opts: primaryDownOpts(),
			want: false,
		},
		{
			name: "failover still rejects a failed pod",
			pod:  pod(corev1.PodFailed, false),
			opts: primaryDownOpts(),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, got := podEligible(tc.pod, tc.opts)
			if got != tc.want {
				t.Errorf("podEligible = %v (%q); want %v", got, reason, tc.want)
			}
			if !got && reason == "" {
				t.Error("rejected without a reason; the log line would be empty")
			}
		})
	}
}
