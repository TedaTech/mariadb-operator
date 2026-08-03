# TeDa Tech fork of mariadb-operator

This fork started for one reason: **on upstream `26.3.0`, automatic primary
failover does not happen.** Not "happens slowly", not "happens with a warning" —
the node hosting the primary dies, `autoFailover` is enabled, the delay elapses,
and nothing at all occurs. No promotion, no event, no non-verbose log line.

It now carries a second, unrelated fix: **`spec.archiveTimeout` does not bound
anything**, so point-in-time recovery has an unbounded RPO. See commit 7.

Base: upstream tag `v26.3.0`. Working branch: `teda/26.3.0`.
Image: `ghcr.io/tedatech/mariadb-operator:26.3.0-teda.N`.

Everything here is a bug fix against upstream behaviour. There are no new API
fields, no CRD changes, and no new configuration — the CRDs from upstream
`26.3.0` are used unmodified.

## The one cause

A replica's readiness probe is **replication-aware**. It fails on a nil
`Seconds_Behind_Master` (`pkg/agent/handler/replication/probe.go`), and
`pkg/sql/sql.go` sets that nil whenever a replication thread is not running:

> `Seconds_Behind_Master` may be empty when any of the replication threads are
> not running. Do not treat nil as 0!

Kill the primary and every replica goes NotReady within seconds. Every code path
that gates on `PodReady` therefore fails at exactly the moment it is needed.

The six commits below look like six unrelated bugs. They are one mistake,
repeated. That is also why raising `maxLagSeconds` does not help at any value:
the nil check returns before the threshold is ever compared.

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

Commits 1 and 3 are the two that make failover complete at all. 5 and 6 are the
ones that act in steady state on a healthy cluster — check those first if a
rebase misbehaves. 7 is independent of the other six.

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

## Evidence

kind, 5 nodes, k8s v1.35, 3-replica MariaDB with `autoFailover: true`, then
`docker stop` on the node hosting the primary:

| | `podIndex` moves | failover log lines | writes return |
|---|---|---|---|
| upstream `26.3.0`, after 7 min | no | **0** | no — both survivors `read_only=1` |
| this fork | yes, in **17s** | yes | yes |

Upgrading is not a fix: `26.6.0` is the newest tag and #1846 is filed against it.

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
