# Failure and visibility contract

## Publication boundary

1. `stage` validates and stores one canonical intent at committed revision R.
2. It calculates the final document contribution set after all edits/deletions.
3. One job is recorded per changed target. Empty targets require removal only.
4. Workers claim jobs under short `BEGIN IMMEDIATE` transactions, then call reducers outside locks.
5. `complete` saves a result only when batch revision R is still current and the claim's random token and unexpired lease match.
6. `commit` requires every job ready and revision R still current. Documents, outputs, reducer version, revision increment, and batch state update in one SQLite transaction.

Readers see old outputs until the commit. Separate `documents()` and `outputs()` calls can straddle a concurrent commit; each call is individually consistent but the pair is not a combined snapshot API. `outputs()` reads all outputs in one query.

## What can happen after failure

- Callback raises: its claim returns to pending. Other ready results remain available.
- Process dies before callback: claim expires and another worker may run it.
- Process dies after provider success but before `complete`: provider may be called again. A journal cannot solve that distributed uncertainty by itself.
- Lease expires while provider still runs: a new worker may call concurrently. Old completion is rejected, even if nobody reclaimed it yet.
- Process dies after `complete`: ready result survives; no need to re-run that job.
- Commit fails: SQLite rolls back the whole publication, leaving jobs ready for retry.
- Another batch commits: old work cannot complete or commit. Explicit abort/restage is required.
- A worker observes all jobs ready just after another commits: repeated commit returns the original committed revision.

The stable job key hashes target, reducer version, and sorted document contributions. It is stable across identical work, including different batch IDs. Reducer behavior must be determined by those inputs/version; update the version when other influences change. Provider-specific idempotency is optional and outside this package's guarantees.

No background thread renews leases. Use a generous lease or `heartbeat` through another connection. Persistent leases use wall-clock time; major system clock jumps can accelerate or delay reclamation. Local SQLite locking, durable filesystem behavior, and ordinary clock discipline are assumed. Sudden hardware failures can exceed SQLite/OS durability guarantees.

## Revision conflicts

Concurrency is conservative: even independent batches affecting disjoint targets conflict if one commits first. This avoids accidental lost updates without requiring complex target-level merge semantics. Workers inside one batch can run concurrently. Reducing conflict granularity is future work, not a current promise.

## Retention and idempotency

Committed documents store only current contributions. Journals intentionally store historical intent/inputs to support recovery and duplicate-ID checking. Abort removes job data but retains intent. Forget deletes all batch rows but permits that ID to be reused. Applications handling delayed retries must keep their own deduplication records after forgetting journals.

A logical document deletion is not legal/privacy erasure. Plan retention policies for the database, WAL, backups, logs, external LLMs, and downstream indexes separately.
