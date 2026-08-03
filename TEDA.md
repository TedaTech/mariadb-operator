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

Commits 7 and 8 have a different cause — see their sections.

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

Commits 1 and 3 are the two that make failover complete at all. 5 and 6 are the
ones that act in steady state on a healthy cluster — check those first if a
rebase misbehaves. 7 and 8 are independent of the other six, and of each other.

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

Defect 8 was found *after* that rebase, in production, on this fork. `26.6.0`
made it visible rather than causing it: picking up the 1948 tolerance is what
let the promotion phase succeed, so the failure moved to the phase after it.

What `26.6.0` did change in this area is a refactor: `replConfigClient` became
`topologyManager.TopologyForMariaDB(...)`. Patches 2–6 were re-applied against
it; 4 moved into the extracted `getReplicationRole` helper, which now takes
`podIndex` directly.

**Known gap.** The drill fixture had no `PhysicalBackup`, so after reattachment
the replicas hit `Error 1236 ... GTID not in the master's binlog` — the promotion
`RESET MASTER` trap. Upstream's designed answer is
`spec.replication.replica.recovery` + `bootstrapFrom`, which our production
clusters do configure, but unattended post-failover reseed has **not** been
drilled end to end yet.

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
2. Branch `teda/<new-version>` from the new tag and cherry-pick the six commits
   in order. They are deliberately one-per-defect so a partial rebase is
   meaningful.
3. Re-run the kind drill. A rebase that compiles proves nothing here — every one
   of these defects is invisible until a node dies.
4. Tag `<new-version>-teda.1` and repin in the infra repo
   (`kubernetes/platform-services/cozystack-service-packages/cozystack-mariadb-operator-package.yaml`).
   There is no image automation on this pin, on purpose: a new operator build
   must never reach production without a drill.

Consumer-side notes, rollout gates and rollback live in `docs/mariadb-operator-fork.md`
in the infra repo.
