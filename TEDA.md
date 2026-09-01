# TeDa Tech fork of mariadb-operator

This fork started for one reason: **on upstream `26.3.0`, automatic primary
failover does not happen.** Not "happens slowly", not "happens with a warning" —
the node hosting the primary dies, `autoFailover` is enabled, the delay elapses,
and nothing at all occurs. No promotion, no event, no non-verbose log line.

It now carries two further fixes, both in point-in-time recovery and both
unrelated to failover:

- **`spec.archiveTimeout` does not bound anything**, so the RPO is unbounded.
  See commit 7.
- **Enabling PITR breaks primary switchover**, and the failure strands writes on
  a read-only ex-primary. See commit 8.

And three more, found in production on 2026-08-18 when a failover left the new
primary `read_only=1`, the demoted ex-primary unwedged and writable, and a
replica permanently stuck `Init`ing. See commits 9–13:

- **A failover can commit a `read_only=1` primary.** Upstream tolerates error
  1948 by *returning early*, skipping the very step that makes the node
  writable. See commit 9.
- **A retried backup object is double-compressed** and becomes unrestorable
  (`wrong chunk magic at offset 0x0`). See commit 12.
- **A failed `pb-init` job wedges replica recovery forever** — no re-fire, no
  event, just `Ready=False` and a pod whose init container waits on a field that
  is only cleared after the job completes. See commit 13.

Base: upstream tag `v26.6.0`. Working branch: `teda/26.6.0`.
Image: `ghcr.io/tedatech/mariadb-operator:26.6.0-teda.N`.

Everything here is a bug fix against upstream behaviour. There are no new API
fields, no CRD changes, and no new configuration — the CRDs from upstream
`26.6.0` are used unmodified.

## The one cause

A replica's readiness probe is **replication-aware**. It fails on a nil
`Seconds_Behind_Master` (`pkg/agent/handler/replication/probe.go`), and
`pkg/sql/sql.go` sets that nil whenever a replication thread is not running:

> `Seconds_Behind_Master` may be empty when any of the replication threads are
> not running. Do not treat nil as 0!

Kill the primary and every replica goes NotReady within seconds. Every code path
that gates on `PodReady` therefore fails at exactly the moment it is needed.

Commits 1–6 below look like six unrelated bugs. They are one mistake, repeated.
That is also why raising `maxLagSeconds` does not help at any value: the nil
check returns before the threshold is ever compared.

Commits 7 and 8 have a different cause — see their sections. Commits 9–11 are
the aftermath of a failover that *completes* (the commit-1–6 path); 12 and 13
are recovery machinery failures, independent of the rest. 14 is a regression
in this fork's own commit 4, shipped with `26.6.0-teda.2` but never exercised
in CI until 2026-08-21 — see its section.

## What we carry

| # | Commit | Upstream |
|---|---|---|
| 1 | `fix(failover): select a promotion candidate when the primary is down` | [#1846](https://github.com/mariadb-operator/mariadb-operator/issues/1846) (open, filed against 26.6.0), [#1628](https://github.com/mariadb-operator/mariadb-operator/issues/1628) (open), [PR #1658](https://github.com/mariadb-operator/mariadb-operator/pull/1658) (open, unmerged) |
| 2 | `fix(switchover): make waitForNewPrimarySync re-entrant` | [#1654](https://github.com/mariadb-operator/mariadb-operator/issues/1654) — closed `not_planned` by the stale bot |
| 3 | `fix(switchover): reconnect replicas whose primary is gone` | not filed |
| 4 | `fix(status): infer Primary from currentPrimaryPodIndex` | not filed |
| 5 | `fix(replication): repair a replica following a dead primary` | not filed |
| 6 | `fix(replication): re-assert read_only on replicas every reconcile` | [#1719](https://github.com/mariadb-operator/mariadb-operator/issues/1719) |
| 7 | `fix(binlog): rotate the active binary log when archival is overdue` | not filed |
| 8 | `fix(switchover): survive a switchover with point-in-time recovery enabled` | [#1669](https://github.com/mariadb-operator/mariadb-operator/issues/1669) is the mirror image (open, stale-botted); the 1947 case is not filed |
| 9 | `fix(switchover): fall through a tolerated 1948 instead of skipping read_only reset` | upstream `e3e07c62` (in 26.6.0) created the tolerance; the swallow is not filed |
| 10 | `fix(switchover): fail the promotion phase when the new primary is still read_only` | not filed |
| 11 | `fix(replication): re-assert read_only=OFF on the primary every reconcile` | [#1719](https://github.com/mariadb-operator/mariadb-operator/issues/1719) is the mirror image (demoted node stays writable; patch 6 covers it) |
| 12 | `fix(backup): make compression idempotent to stop double-compressed backups` | not filed |
| 13 | `fix(init): re-fire a failed PhysicalBackup init job instead of waiting forever` | not filed |
| 14 | `fix(replication): configure the primary at least once on a fresh cluster` | matches upstream [#1692](https://github.com/mariadb-operator/mariadb-operator/issues/1692) and [#1730](https://github.com/mariadb-operator/mariadb-operator/issues/1730) (both open); a regression in this fork's own commit 4, caught by CI on 2026-08-21 |

Commits 1 and 3 are the two that make failover complete at all. 5 and 6 are the
ones that act in steady state on a healthy cluster — check those first if a
rebase misbehaves. 7 and 8 are independent of the other six, and of each other.
9–11 are one incident (the new primary stays `read_only`), 12 and 13 are the
recovery-machinery defects that made the same incident worse.

### 7 — `archiveTimeout` does not bound the RPO

Separate defect, separate subsystem, same shape: a knob whose name promises
something the code never delivers.

`archiveBinaryLogs` deliberately drops the active binary log from the list it
ships (`pkg/binlog/archiver.go`, `binlogs = binlogs[:len(binlogs)-1]`), and
**nothing in the operator or the agent ever issues `FLUSH BINARY LOGS`**.
`spec.archiveTimeout` is only a `context.WithTimeout` on the upload. So binary
logs reach object storage only when MariaDB rotates on its own — at
`max_binlog_size` (1 GiB by default) or on restart. Everything written since the
last rotation is unrecoverable, for an unbounded period.

It is worse than a plain outage because it looks healthy. A no-op cycle still
logs `Archiving binary logs` and `Binlog archival done`; the "already archived"
and "object exists" skips are both at `V(1)`. Observed in production 2026-08-03:
`BinlogsArchived=True`, a successful cycle logged every 10 minutes, and
`lastArchivedTime` two days old on `…-bin.000001` at position 374 — a binary log
containing no transactions at all.

The fix rotates the active binary log when the time since `lastArchivedTime`
exceeds `archiveTimeout`, which makes the field mean what it says. Rotation is
guarded on `@@gtid_binlog_pos` having moved since this archiver last rotated:
MariaDB rotates unconditionally, so an idle primary would otherwise accumulate
one empty binary log per interval forever.

Only the primary runs the archiver (`shouldArchiveBinlogs` returns early on
replicas), so this issues no SQL anywhere else.

**Config-only fallback if this is ever dropped:** a small `max_binlog_size` in
`spec.myCnf`. That ties the RPO to write volume instead of time, which is
strictly worse — a quiet database gets the worst coverage — but it needs no
patched image.

### 8 — enabling PITR breaks switchover

Turning point-in-time recovery on makes every primary switchover fail, and the
failure takes writes down until a human intervenes. Four parts, one incident.

**The trigger.** `configureReplicaOpts` adds `WithResetMaster(false)` when PITR
is enabled, so the demoted primary keeps its binary logs — correct, archival
must not lose them. `ConfigureReplica` then reaches `SET GLOBAL gtid_slave_pos`
without the `RESET MASTER` that normally clears the binlog first, and assigns
the *new* primary's `gtid_binlog_pos`, which is **behind** the demoted primary's
own binlog:

```
Error 1947 (HY000): Specified GTID 0-10-1291069 conflicts with the binary log
which contains a more recent GTID 0-11-1294159. If MASTER_GTID_POS=CURRENT_POS
is used, the binlog position will override the new value of @@gtid_slave_pos
```

It is behind because `log_slave_updates` is **off** in single-cluster topology
(`internal/controller/mariadb_controller.go`, enabled only for multi-cluster).
A replicating node records what it applied in `gtid_slave_pos` and none of it
reaches its own binary log — so the new primary's `gtid_binlog_pos`, read at the
moment of promotion, holds only its local writes. The fix reads
`gtid_current_pos` instead, the union of the two, and tolerates 1947 when
`ResetMaster` is suppressed **and** the effective `MASTER_USE_GTID` is
`current_pos`, where the server merges the binlog position in and the error is
advisory. Under `slave_pos` it still fails: there the assignment decides where
replication resumes. `getReplicaOpts` forces `slave_pos` on the recovery path,
so the mode is resolved exactly as `changeMaster` resolves it, overrides
included.

Upstream fixed the mirror image and stopped there. `e3e07c62` swallows **1948**
in `ConfigurePrimary`, naming *"(multi-cluster, PITR)"* as the trigger and
noting it *"will completely block switchover/failover operations"*. 1947, in
`ConfigureReplica`, was left alone — `grep -rn 1947` across upstream finds
nothing. `c37902db` (26.3.0) introduced the `ResetMaster(false)` suppression
that creates the situation in the first place.

**Why one error became a 25-minute outage.** The switchover used to commit
`status.currentPrimaryPodIndex` only after *all six* phases succeeded. The
primary Service selector is built from that field
(`reconcilePrimaryService`), so until it moved, traffic kept going to the old
primary — which phase 2 sets `read_only` on and only phase 6 would ever unset.
Observed in production 2026-08-03: `mdb-0` was promoted and writable at
17:23:20 and stayed unused for 25 minutes while the Service pointed at a
read-only `mdb-1`, because phase 6 failed with 1947.

The promotion is now committed as soon as the new primary is writable. The
phases after it reattach replicas and demote the old primary; they are
convergence work the steady-state reconcile retries, and none of them makes the
promotion any more or less true. They read a pinned `switchoverFromPodIndex`,
since the status no longer names the node they act on.

**The convergence path had to be fixed for that to be safe.** `getReplicaOpts`
returned early on `!forceReplicaConfiguration` *before* applying the PITR guard,
so steady-state reconciliation passed `ConfigureReplica` no options and fell
back to `ResetMaster: true` — deleting binary logs the archiver had not shipped
yet. That is exactly the path that converges a demoted primary, so it is the one
that must keep them. The guard now sits above the early return.

**Also re-entrancy.** `waitForReplicaSync` waits for replicas to reach the old
primary's GTID. Once a later phase has repointed them at the new primary they
can never receive it, so `MASTER_GTID_WAIT` burns the whole `syncTimeout` and
the routine restarts — unbounded. It now skips a replica observed to be
following someone else. This mirrors patch 2, which covers only
`waitForNewPrimarySync`, the failover branch; the wedge above went through the
sibling function, which patch 2 never touched.

### 9 — a failover can commit a `read_only=1` primary (commits 9–11)

The 2026-08-18 incident: the switchover *completed* — `Primary switched`, the
-​primary Service moved, the cluster reported ready — and writes still failed
with `ERROR 1290 (HY000): server is running with the --read-only option`,
because the **new** primary was `read_only=1`. A replica's `read_only` is only
ever cleared by `ConfigurePrimary` (patch 6 keeps replicas read-only), and
this one never cleared it.

Three independent reasons no code path fixed it:

**Commit 9 — upstream's 1948 tolerance skips `DisableReadOnly`.**
`singleClusterTopology.ConfigurePrimary` handles the tolerated error 1948
(`gtid_slave_pos` has no value for the replication domain, see the quote in
defect 8) with an early `return nil` *inside the `isReplica` block* —
swallowing the error and skipping the `DisableReadOnly` that comes after it.
So a promoted node that was a replica (and therefore `read_only=1`) stays
read-only, while promotion carries on as if nothing happened: the 1948
tolerance itself is what defeats the failover. The comment upstream shipped
with the tolerance says it must not block switchover operations — it just
chose the wrong way to not block them. The fix logs and falls through;
`DisableReadOnly` and the replication-user SQL always run.

Upstream also shipped the other half of this story: `26.6.0` introduced a
maintenance `read_only` reconciler that *would* re-assert `read_only=0` on the
current primary — but it is gated on the cluster being `Ready`, which it is
not while a recovery is wedged, and its reconcile is skipped entirely while
replicas are being recovered. The steady-state replication path that could
also fix it is skipped for a node whose role already reads `Primary`.

**Commit 10 — the promotion is committed even when the new primary is still
read-only.** `configureNewPrimary` now reads `@@global.read_only` back after
`ConfigurePrimary` and fails the phase if it is still ON. The promotion is
committed only after that phase succeeds (see defect 8), so a wedge here
re-tries the configuration instead of blessing it. Events carry
`PrimaryStillReadOnly`.

**Commit 11 — nothing ever re-asserts `read_only=0` on the primary.** After
the incident the cluster converges *only* if some path turns the new primary
writable again. `assertPrimaryWritable` mirrors the replica assertion from
patch 6: one cheap read of `@@global.read_only` per reconcile, `DisableReadOnly`
only when it is ON, non-fatal on an unreachable pod. It runs on the primary
path even when `ConfigurePrimary` is skipped, and stays off during maintenance
read-only and while a switchover (which manages the primary's `read_only`
itself) is pending.

The same commit guards the mirror: the replica-path assertion (patch 6) never
runs against a pod the spec designates as primary, because role status lags a
promotion and could otherwise force the promoted node back to read-only.

**What the incident was not.** The ex-primary was never reattached — but not
because the operator lacks the machinery. The switchover skips demoting an
unreachable old primary, and the wedged replica recovery (commit 13)
short-circuited the reconcile before the steady-state path that would have
reconfigured the returning pod ever ran. That path falls through to a full
`ConfigureReplica` for any non-primary pod without replication — verified in
source; it was starved, not broken. No `Error 1236` was ever observed, and the
suspicion that GTID is seeded from the new primary's binlog is contradicted by
the code (`getReplicaOpts` seeds from the replica's own agent GTID, or clears
and learns from the primary). **No fix was needed and none was made.** What
remains is untested in a live drill with a returning ex-primary — see the
Known gap below.

### 10 — a retried backup is double-compressed (commit 12)

`compressFile` replaces the plain file in place (`file.tmp` → `file`), and the
backup command re-runs `Compress` over the same path whenever its container is
restarted (`RestartPolicy: OnFailure`). The second pass compresses the stream
the first pass produced: `gzip(gzip(xbstream))` reaches object storage.
Restore unwraps exactly one layer, so `mariadb-backup`'s `mbstream` reads a
gzip header where it expects an xbstream chunk and dies with:

```
xb_stream_read_chunk(): wrong chunk magic at offset 0x0
```

That is what made the 2026-08-18 recovery backup permanently corrupt, which in
turn made the `pb-init` job fail, which wedged the recovery (commit 13). Any
backup — scheduled or recovery — could hit it; nothing else flagged it,
because the archive looked fine and only restore noticed.

The fix skips compressing a file that already starts with a gzip (`1f 8b`) or
bzip2 (`BZh`) header. A plain mariadb-backup stream starts with the xbstream
header, never either magic, so a genuine plain backup is never skipped. The
also-obvious fix, `RestartPolicy: Never`, is applied by users, not by us; the
sniff protects whatever the policy is.

### 11 — a failed `pb-init` job wedges replica recovery (commit 13)

Replica recovery pins the pod's init container on
`status.replication.replicaToRecover`, which is cleared only after the
recovery's `pb-init` restore job completes. `reconcileAndWaitForInitJob`
treated a **Failed** job (e.g. `BackoffLimitExceeded`) exactly like a running
one: "not complete, requeue in 1s" — forever. Nothing deleted or re-fired the
job, the field was never cleared, the pod stayed `Init:0/1` ("Waiting for
replica recovery"), and the cluster reported `Ready=False, ReplicaRecovering`,
which in turn froze binlog archival (a quiet PITR gap) and short-circuited the
reconciles that would have repaired the rest of the topology. The only way out
was a human deleting the stale PhysicalBackup.

Now a failed init job is deleted and re-created, up to 5 per-pod retries
recorded in a `k8s.mariadb.com/pb-init-retry-<pod>` annotation on the MariaDB
resource. A transient failure (bad network to object storage, a just-now
corrupt object that a re-taken backup fixes) converges unattended. Above the
cap the job is left in place as evidence and a `InitJobFailed` warning event
is emitted — re-firing a job that fails for a deterministic reason would only
churn.

### 12 — a fresh replication cluster never becomes Ready (commit 14)

The `a9e4cf1f` role inference (defect 4) classifies a pod as
`ReplicationRolePrimary` whenever its index equals
`Status.CurrentPrimaryPodIndex` and it has no connected replicas. That is the
desired post-failover signal — but `CurrentPrimaryPodIndex` is defaulted from
`spec.replication.primary.podIndex` on the **first status pass**, before
`ConfigurePrimary` has ever run. A brand-new cluster therefore reports its
spec-primary as `Primary` immediately, and `shouldSkipPrimaryReconciliation`
skipped primary configuration forever: no replication user, no `read_only`
assertion. Replicas configured against that unset-up primary, replication
never established, `Seconds_Behind_Master` stayed NULL, and the agent
readiness probe (the "one cause" above) kept every pod NotReady. The operator
logged `Configuring replica` for pods -1/-2 every reconcile and never
`Configuring primary` — production never saw this because new clusters are
bootstrapped with a `PhysicalBackup` init (a different path); the integration
suite saw it on the first CI run of `teda/26.6.0` (2026-08-21): 7 specs timed
out waiting for `mariadb-repl` to become Ready. Upstream is unaffected (no
such fallback) and its CI at `26.6.0` is green; the same fresh-replication
symptom is tracked upstream in #1692 and #1730, both still open.

The fix gates the role-based skip on `HasConfiguredReplication()`, the
condition `reconcileReplication` sets only after the first full, successful
pod pass. A fresh cluster always runs `ConfigurePrimary` at least once;
a converged cluster — and the post-failover case commit 4 was written for —
behave exactly as before. The multi-cluster branch is untouched.

## Evidence

kind, 5 nodes, k8s v1.35, 3-replica MariaDB with `autoFailover: true`, then
`docker stop` on the node hosting the primary:

| | `podIndex` moves | failover log lines | writes return |
|---|---|---|---|
| upstream `26.3.0`, after 7 min | no | **0** | no — both survivors `read_only=1` |
| this fork | yes, in **17s** | yes | yes |

Upgrading is not a fix. Every one of the defects was re-verified against
`26.6.0` when this branch was rebased onto it on 2026-08-03:

| Defect | Status in `26.6.0` |
|---|---|
| 1 failover candidate selection | `pkg/controller/replication/failover.go` is **byte-identical** to `26.3.0` |
| 2 `waitForNewPrimarySync` re-entrancy | unchanged logic; #1654 still closed `not_planned` |
| 3 reconnect replicas whose primary is gone | still gated on Pod readiness |
| 4 role inference | still reports `Unknown` for a primary with no connected replicas; `26.6.0` only added the multi-cluster `PrimaryReplica` case |
| 5 replica following a dead primary | the early return on `role == Replica` is **verbatim** |
| 6 `read_only` re-assertion | same early return, same gap |
| 7 `archiveTimeout` | `pkg/binlog/archiver.go` is **byte-identical** to `26.3.0` |
| 8 PITR breaks switchover | `26.6.0` fixed the **1948** half (`e3e07c62`) and left 1947 unhandled; the end-of-loop status commit and the `getReplicaOpts` early return are both unchanged |
| 9–13 post-failover wedges | all introduced after this fork's rebase; defects 9 (1948 fall-through) and 11 (maintenance `read_only` reconciler gated on `Ready`) exist upstream but the others are not fixed there — the forked code paths are fork-only |

Defect 8 was found *after* that rebase, in production, on this fork. `26.6.0`
made it visible rather than causing it: picking up the 1948 tolerance is what
let the promotion phase succeed, so the failure moved to the phase after it.

What `26.6.0` did change in this area is a refactor: `replConfigClient` became
`topologyManager.TopologyForMariaDB(...)`. Patches 2–6 were re-applied against
it; 4 moved into the extracted `getReplicationRole` helper, which now takes
`podIndex` directly.

**Known gap.** Unattended post-failover recovery is still not drilled end to
end. The original drill fixture had no `PhysicalBackup`, so after reattachment
the replicas hit `Error 1236 ... GTID not in the master's binlog` — the
promotion `RESET MASTER` trap. Upstream's designed answer is
`spec.replication.replica.recovery` + `bootstrapFrom`, which our production
clusters do configure, but the full reattachment of a *returning ex-primary*
after a failover has **not** been proven in a drill. Commits 12 and 13 make
the recovery path reachable and self-healing; 9–11 make the promoted primary
writable. What remains unproven is the last half hour of a failover: the old
primary coming back and re-joining as a replica. Until the drill covers it,
treat a real failover as something to watch rather than something to trust.

**Latent watch items, deliberately not code:** the steady-state reattach of a
pod whose `CHANGE MASTER` keeps failing error-loops with `Ready=False` and
does not escalate to replica recovery (the recovery trigger needs a
`LastIOErrno` in status, which a never-configured pod does not have); and the
switchover phases that silently skip fencing a non-ready old primary keep no
record that they did. Both are covered by the same drill gap above.

## Releasing

Push a tag matching `[0-9]+.[0-9]+.[0-9]+-teda.[0-9]+`.
`.github/workflows/teda-release.yml` builds `linux/amd64` and pushes to ghcr;
nothing else in the repo triggers on that pattern. The ghcr package must stay
**public**: the operator writes its own image reference into every new HA
cluster's `spec.replication.agent.image` and `initContainer.image`, and those
Pods run in tenant namespaces that hold no pull secret.

## Rebasing onto a newer upstream release

1. Check each row above first — a fixed defect should be dropped, not carried.
   Verify against the new tag's source, not the issue's state; #1654 is closed
   and still broken.
2. Branch `teda/<new-version>` from the new tag and cherry-pick the commits
   from the table above, in order. They are deliberately one-per-defect so a
   partial rebase is meaningful.
3. Re-run the kind drill. A rebase that compiles proves nothing here — every one
   of these defects is invisible until a node dies.
4. Tag `<new-version>-teda.1` and repin in the infra repo
   (`kubernetes/platform-services/cozystack-service-packages/cozystack-mariadb-operator-package.yaml`).
   There is no image automation on this pin, on purpose: a new operator build
   must never reach production without a drill.

Consumer-side notes, rollout gates and rollback live in `docs/mariadb-operator-fork.md`
in the infra repo.
