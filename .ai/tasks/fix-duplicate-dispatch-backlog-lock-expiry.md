# Task: Duplicate dispatch under backlog or lock expiry

## Status

Complete (2026-11-20). Workflow: Bug Fix. Requested by the user ("fix
duplicate dispatch when there's a backlog or a lock expires"); originally a
Medium item from the production bug check.

## Problem (as documented, to be confirmed)

`business-rules/message-delivery.md` (Delivery Semantics) and
`components/outbox-dispatch-runtime.md`:

- Backlog re-claim: while more than 100 rows are claimable, the poller loops
  with no delay, claiming 101 rows per pass into the unbounded in-memory
  queue (`MemoryMessageQueue`, 10 concurrent handlers). Rows still queued
  when their lock (`MessageLockExpirationTimeSpan`, default 5 min) expires are
  claimed and enqueued again in the same process -> duplicate dispatch.
- Lock expiry during processing: a handler that outlasts the lock lets the
  poller (this or another process) re-claim and dispatch the row again.

Delivery is documented as at-least-once; subscribers should be idempotent.
The goal is to stop the library itself from producing avoidable duplicates.

## Confirmed evidence (planner, 2026-11-20)

- Poller claims 101 rows with lock = poller now + span
  (`OutboxProcessorHostedService.cs` ~L75); loops without delay when > 100
  (~L82); nothing paces claiming to dispatch progress.
- `MemoryMessageQueue`: unbounded, no dedup, 10 concurrent handlers; shared
  by the poller and immediate dispatch.
- Immediate rows are locked at save time, before commit
  (`OutboxDbContext.ToOutboxMessageEntity`); queue wait and caller
  transaction length count against the lock.
- The handler publishes without checking the row still exists or is owned;
  `RemoveAsync` = DELETE WHERE Id; `UpdateAsync` rewrites all columns
  including `LockExpiresAt`. No concurrency token.
- A stale failing handler resets a re-claimer's lock to its old, expired
  value -> a third dispatch is possible; it also overwrites `Attemps`.
- Same process: needs drain time > 30 min (next poll) or a claim loop past
  the lock span. Cross-instance: drain time > lock span.
- Docs saying the hosted service has no tests are outdated
  (`EventPublisherAdversarialTests`, SQL Server `HostedServiceTests`).

## Proposed plan (pending user decisions)

1. `MemoryMessageQueue` in-process dedup by Id (queued or in flight).
2. Poller backpressure: after a full batch wait until its claimed messages
   are handled (bounded by `MessageLockExpirationTimeSpan`).
3. `EntityFrameworkOutboxStore.UpdateAsync`: do not write `LockExpiresAt`.
Deferred candidates: ownership token (model change), lock renewal or
re-lock at dispatch start (`IOutboxStore` change).

## User decisions (2026-11-20)

- D1 queue dedup by Id: yes.
- D2 poller backpressure: yes, hard-coded timings (no new options).
- D3 `UpdateAsync` stops writing `LockExpiresAt`: yes.
- D4 ownership token: not selected (deferred).
- D5 lock renewal: YES (user chose it over the recommendation to defer).
  Design (API shape: new `IOutboxStore` member vs optional interface,
  conditional renewal, interval) to be planned and confirmed with the user.

## Lock renewal design (planner, 2026-11-20)

4. Optional public `IOutboxLockRenewal<TKey>.RenewLocksAsync(expectedLocks,
   lockExpiration)` implemented by `EntityFrameworkOutboxStore`: conditional
   `UPDATE ... JOIN (VALUES ...)` with an expected-lock match, explicit
   datetime2 parameters, `OUTPUT` of stored values, 1000-id chunks, no
   READPAST. `MemoryMessageQueue` tracks queued and in-flight entries (the D1
   structure) with their own expected lock, via an internal interface. The
   hosted service renews every span/3 (floor 100 ms) with GetUtcNow() + span;
   failures are logged and retried. A miss revokes a queued copy and warns
   for an in-flight one. A dedup hit refreshes the expected lock; revoke and
   confirm compare-and-set against the expected value sent. D3 is a
   prerequisite (lock writers only move forward).

Confirmed: claims store `datetime`-rounded values and return them through
OUTPUT; immediate rows are stored exactly as datetime2 (sample migration);
test doubles implement `IOutboxStore`/`IMessageQueue`, so adding interface
members is breaking; packages at 0.4.0, no changelog.
Hypothesis: EF8 `SqlQueryRaw` handles non-composed `UPDATE ... OUTPUT`.

Adopted by the orchestrator (planner recommendations, no material business
choice): R3 lost ownership -> drop queued copy, in-flight handler continues
with a warning; R5 interval span/3 with 100 ms floor; R6 renewal only with
the built-in queue and a store implementing the interface (documented).
User decisions (2026-11-20):
- R1: add `RenewLocksAsync` as a REQUIRED member of public `IOutboxStore`
  (breaking for custom stores; user chose it over the optional interface).
  The hosted service always renews (no runtime capability check).
- R2: expected-lock match within ±5 ms.
- R4: no cap on renewal; warn each tick for in-flight messages older than one
  lock span.
- R7: keep version 0.4.0 (no bump in this branch).

## Out of scope / requires idempotent subscribers

Crash or failed delete after the handler; cross-instance handler running
beyond the lock; heavy cross-instance queue wait; multi-subscriber partial
failure; long caller transactions committing after the lock span.

## Progress

- [x] Planning (planner)
- [x] Implementation with tests first (developer, 2026-11-20)
  - D3: `EntityFrameworkOutboxStore.UpdateAsync` marks `LockExpiresAt` not
    modified (the EF UPDATE no longer contains the column).
  - D1: `MemoryMessageQueue` tracks entries by Id (`ConcurrentDictionary`;
    entry = message, own `ExpectedLock` copy, Started/Revoked/Removed flags,
    guarded by a lock on the entry; the message is never mutated). Duplicate
    enqueue skipped (Debug log); a newer `LockExpiresAt` moves `ExpectedLock`
    and clears Revoked. Entry removed in the `finally` after the handler.
    Revoked queued entries are skipped at dequeue (no handler call; the
    signal/concurrency semaphores stay balanced). New internal
    `IMessageLockTracker<TKey>` (src/XDev.Outbox/IMessageLockTracker.cs),
    implemented explicitly: `GetTrackedLocks`, `IsTracked`, CAS
    `ConfirmRenewal`/`Revoke`. 10-way concurrency unchanged.
  - R1: required `IOutboxStore<TKey>.RenewLocksAsync(IReadOnlyDictionary<TKey,
    DateTime> expectedLocks, DateTime lockExpiration, CancellationToken = default)
    -> Task<IReadOnlyDictionary<TKey, DateTime>>` with XML docs. EF store: one
    `UPDATE t ... OUTPUT INSERTED.Id, INSERTED.LockExpiresAt FROM <table> AS t
    WITH (ROWLOCK) INNER JOIN (VALUES ...) ... BETWEEN DATEADD(ms,±5)` per
    1000-id chunk; `@lockExpiration`/`@eN` as DateTime2, ids Int/BigInt/
    UniqueIdentifier (others inferred); read via `Database.SqlQueryRaw<T>`
    (internal nested `RenewedLock`); confirmed working on EF 8/9/10/11 incl.
    hostile/brace table names. Null input throws ArgumentNullException; empty
    input returns empty without a command. Test double `RecordingStore`
    updated (returns empty); no other `IOutboxStore` implementations exist
    (samples use the EF store).
  - Hosted service: renewal loop (`RunLockRenewalAsync`) started in
    `ExecuteAsync` when the queue implements the tracker, awaited in a
    `finally`; interval span/3, floor 100 ms; lock = `GetUtcNow()` + span; new
    scope per tick; confirm/revoke per result; warnings for queued drop,
    in-flight loss, and each tick for in-flight > span; non-cancellation
    exceptions logged as warnings ("Failed to renew ...") and retried; no
    store call when nothing is tracked. D2: after a full batch (> 100 rows)
    the poller waits (outside the claim scope) until none of that batch is
    tracked, checking every 1 s, bounded by the span. ≤ 100 path unchanged.
  - Deviations: (a) backpressure and renewal only apply with a queue that
    implements the internal tracker (the built-in one); other queues keep the
    old loop-immediately behavior. (b) A newer lock on a duplicate enqueue
    also clears a revocation (the process owns the row again). (c) The test
    harness stops `MemoryMessageQueue` by cancellation, not `StopAsync`: the
    pre-existing "double dispose / NRE on stop" risk reproduces
    deterministically when `StopAsync` is called after the receive loop
    started (NRE at `RunReceiveTaskAsync` finally); not fixed (out of scope).
  - Tests: core `MemoryMessageQueueDedupTests` (6),
    `LockRenewalHostedServiceTests` (11), `PollerBackpressureTests` (3); SQL
    `OutboxStoreTests.UpdateAsync_StaleMessage...` (1), `LockRenewalTests.cs`
    (store 12, Guid 1, end-to-end 2), adversarial table names (+1 x 4
    contexts). `RecordingMessageQueue` now records `LockExpiresAt`.
  - Fail-before: 3 dedup tests failed (3 guards passed); D3 test failed (lock
    moved back to the stale value); all 19 SQL renewal tests failed against a
    stub; 12 of 14 hosted-service/backpressure tests failed before the loop
    (the 2 passing are guards: nothing tracked, ≤ 100 path).
  - Results: build 0 errors, no warnings in changed files (netstandard2.0/2.1
    built). Core 46 (was 26), EFCore 1075 net8 / 1074 net9-11 (unchanged),
    SQL Server 362 (was 342), 0 skipped, on net8-11. Flakiness: timing tests
    re-run 5x (core, 4 TFMs in parallel) and 3x (SQL renewal/hosted/update,
    4 TFMs) with no failures; race test exercised both outcomes (renewal 4 /
    claim 26). No leftover XDevOutboxSmoke_ databases.
  - Risks: renewal retried by a retrying execution strategy after a commit
    whose acknowledgement was lost misses (stored = new lock) and revokes a
    still-owned queued copy (delivery delayed until the lock expires, no
    duplicate); `OUTPUT` without `INTO` fails on tables with triggers (same as
    the claim); RCSI databases not tested.
- [x] Adversarial testing (tester, 2026-11-20)
  - Added tests only (no src change): core
    `tests/XDev.Outbox.Tests/TesterLockRenewalAdversarialTests.cs` (13 run + 1
    skipped defect repro), SQL
    `tests/XDev.Outbox.EFCore.SqlServer.Tests/TesterLockRenewalAdversarialTests.cs`
    (12 run + 1 skipped defect repro).
  - Verified: revoke racing dequeue (3000 msgs, ~1450 dropped while queued,
    ~5 revoked in flight per run) - each message runs once or is dropped, never
    both, queue keeps running; 8 concurrent producers of duplicate ids never
    overlap and the tracking map returns to empty; sync-throw / faulted /
    canceled / null-task handlers release the id; renewal failing longer than
    the span -> poller re-claims the same rows, each handled once, newer lock
    renewed afterwards; revoked queued copy stays dropped on a same-lock
    enqueue and is owned again on a newer lock; null renewal result retried;
    frozen TimeProvider used for the renewal lock; shutdown during backpressure
    wait and during a renewal call is prompt with no unobserved exceptions.
    SQL: chunk boundaries 999/1000/1001/2000/2001; NULL lock not renewed; two
    hosts + a competing expired-lock poller, 400 rows, 400 ms handler, 3 s
    span: exactly once, competitor claims 0 (default and RCSI); RCSI
    renewal-vs-claim race (exactly one wins, both outcomes seen); in-flight
    immediate CLR copy + re-claimed poller (Expando) copy handled once and
    renewal resumes from the claimed lock; trigger table: claim and renewal
    both fail with 334 (known limitation).
  - Fail-before (scratch worktree at HEAD, src hash identical before/after):
    two-host test (competitor claimed 82) and immediate+poller-copy test fail.
  - DEFECT 1: span/3 above Task.Delay's max (~49.7 days; span > ~149 days,
    > ~74.5 days on .NET Framework) -> renewal loop spins (80,531 "Failed to
    renew" warnings in 500 ms, from startup). Repro test skipped.
  - DEFECT 2: renewal is one autocommit per 1000-id chunk; a failure in a
    later chunk throws after earlier chunks committed, the service confirms
    nothing, and the next tick misses -> 990 queued copies dropped and 10
    in-flight revoked (no other claimer). Same mechanism as the known
    lost-acknowledgement risk, without needing a retrying strategy.
    Repro test skipped.
  - Pre-existing (not this change): `MemoryMessageQueue` receive task faults
    on cancellation when faulted handler tasks are still in `RunningTasks`
    and is never awaited -> UnobservedTaskException.
  - Results: Core 60 (59 passed, 1 skipped), EFCore 1075/1074 unchanged, SQL
    375 (374 passed, 1 skipped) on net8-11; core timing tests 5x and SQL
    renewal tests 3x on 4 TFMs in parallel green after fixing one test-only
    start-up race; no leftover XDevOutboxSmoke_ databases.
- [x] Review (reviewer, 2026-11-20): APPROVE WITH NON-BLOCKING COMMENTS.
  - F1 MEDIUM (to fix): renewal is per-chunk autocommit; if a later chunk
    fails, committed chunks' entries keep the old expected lock and are
    revoked on the next tick (queued copies dropped, in-flight no longer
    renewed -> cross-instance duplicate possible). Same root cause as the
    lost-acknowledgement retry risk. Fix: after a failed/unknown tick also
    accept the attempted lockExpiration on the next tick (and/or one
    transaction per call); test with a fault on the second chunk.
  - F2 MEDIUM (to fix, docs + test double): a custom store returning an
    empty result drops every queued message. Document that a missing id means
    "owned elsewhere" and that a store that cannot renew must throw (e.g.
    NotSupportedException); make RecordingStore not model the empty stub.
  - F3 LOW (to fix): false "lost its lock while its handler is running"
    warning when the DELETE commits just before a tick; re-check tracking
    and/or reword/lower the level.
  - F4 LOW (to fix): span/3 above Task.Delay's maximum makes the loop spin;
    clamp the interval and/or delay in the general catch.
  - F5 LOW (to fix): public docs should say renewal and backpressure only
    happen with the built-in MemoryMessageQueue.
  - Open questions: RCSI (tester checking); datetime2 precision < 3 not
    supported (document); spans < ~300 ms cannot be kept alive (document);
    handlers outliving their lock during shutdown (accepted out of scope).
- [x] Developer corrections (developer, 2026-11-20)
  - F1: fixed in `OutboxProcessorHostedService` (any store; store contract
    unchanged: one expected value per id, ±5 ms). A renewal call that throws
    or returns null records its `lockExpiration` as an uncertain attempt for
    every id sent in that call (`_uncertainLocks`, only touched by the
    sequential renewal loop; keyed by id with the tracker's expected lock at
    the time). Nothing is revoked in a failed tick. Next tick: pass 0 sends the
    expected locks; ids that miss are retried with each remembered attempt
    (one store call per candidate, oldest first, same new lockExpiration);
    only ids missing every candidate are revoked. Confirm/revoke still CAS
    against the snapshot's expected lock. Attempts are forgotten when the id
    is renewed, revoked, untracked or its expected lock moved (re-claim);
    at most 8 per id (older ones are locks expired ~1.7 spans ago, except with
    clamped intervals for spans > ~74 days). EF store left as one autocommit
    per chunk (not one transaction): a user transaction would need
    execution-strategy wrapping (retrying strategies reject it) and would
    hold row locks across chunks. CORRECTION (re-review N1): this round only
    covered a lost acknowledgement when the call throws; under a retrying
    execution strategy EF re-runs the committed statement, which misses and
    returns normally, so those ids were revoked. Covered since round 2.
  - F4: interval clamped to int.MaxValue ms (~24.8 days; valid for
    Task.Delay on every target incl. .NET Framework); the general catch now
    waits 100 ms before looping, so no failure can spin. Backpressure wait
    checked: each delay is min(remaining, 1 s) and `remaining <= 0` returns,
    so no Task.Delay/TimeSpan overflow (span + now overflow for absurd spans
    is pre-existing in the poller's claim).
  - F2: `IOutboxStore.RenewLocksAsync` remarks: missing id = owned elsewhere
    (queued copy dropped, running handler warned), a store that cannot renew
    must throw (e.g. NotSupportedException) - thrown/null is logged and
    retried, nothing dropped; only called with the built-in queue; the next
    call may send a failed call's attempted value as expected. RecordingStore
    now throws NotSupportedException (its tests use CapturingQueue, never
    renew).
  - F3: on an in-flight revocation the service re-checks `IsTracked`; if the
    handler already finished it logs Debug, otherwise the Warning (reworded:
    "the row was deleted or claimed elsewhere ... Unless the handler itself
    deleted the row, the message may be dispatched again"). Kept Warning.
  - F5: remarks on `MemoryMessageQueue`, `OutboxProcessorHostedService` and
    `IOutboxStore`: renewal/backpressure only with the built-in queue; spans
    < ~300 ms cannot be kept alive (100 ms floor); `LockExpiresAt` needs ms
    precision (`datetime` or `datetime2(3)`+).
  - Tests: un-skipped `RenewalLoop_LockSpanWhoseThirdExceedsTheTaskDelayMaximum...`
    (core) and `RenewalTick_FailingOnItsSecondChunk...` (SQL). New core tests
    in `LockRenewalHostedServiceTests`: failed call that applied the lock
    (lost ack) -> next tick retries with the attempted lock, nothing dropped;
    partial application + a second failed tick -> all renewed; failed call
    then row claimed elsewhere -> dropped after trying every candidate;
    handler finishing during a missing renewal -> no lost-lock warning (guard,
    passes before too via CAS).
  - Fail-before (fix toggled off temporarily): 3 new F1 core tests and the
    SQL repro failed (990 dropped, 10 in-flight lost); F4 repro failed with
    the clamp off (5 warnings in 500 ms with the catch delay; 80k before).
  - Results: build 0 errors, no warnings in changed files (netstandard2.0/2.1
    built). Core 64 (0 skipped), EFCore 1075 net8 / 1074 net9-11, SQL Server
    375 (0 skipped), on net8-11. Flakiness: core renewal/backpressure/queue
    tests (38) 3x and SQL renewal/hosted/update tests (30) 3x on 4 TFMs, all
    green. No leftover XDevOutboxSmoke_ databases; no test processes left.
- [x] Re-review of corrections (reviewer, 2026-11-20): APPROVE WITH
  NON-BLOCKING COMMENTS; F1–F5 resolved, no D1/D2/D3 regression.
  N1 MEDIUM (to fix): under a retrying execution strategy a renewal that
  committed is re-run with the old expected value, misses, returns normally
  -> revoked; fix: retry misses once with the tick's own lockExpiration.
  N2 MEDIUM: add SQL lost-ack tests (fault after execution, with/without a
  retrying strategy). N3 LOW: IOutboxStore remarks wording.
  Q1 (residual risk, accepted as inherent without an ownership token, D4
  deferred by the user): a remembered value can match another claimer's
  lock only if renewal failed for about 2/3 of a span and the other claim
  landed within ±5 ms of the failed attempt.
- [x] Developer corrections, round 2 (developer, 2026-11-20)
  - N1: `OutboxProcessorHostedService` candidates per id are now: expected
    lock, remembered failed attempts (oldest first, max 8), then THIS tick's
    own `lockExpiration` (values already tried are skipped, so a frozen clock
    adds no call). A message missed with everything else is retried once in
    the same tick with the tick's own lock (the row holds it when a retrying
    strategy re-ran a committed statement; a match renews to the same value),
    then confirmed with the stored value. Confirm/revoke still CAS against the
    snapshot's expected lock. Own lock is last per id, so it also covers a
    re-run of a remembered-attempt pass. Bound: at most one extra call per
    tick (max 1 + 8 + 1 calls), only when there are misses. A failure in any
    pass revokes nothing and remembers the tick's lock for the ids it sent.
    Consequence: a genuinely lost message costs one extra store call before it
    is revoked.
  - N3: `IOutboxStore.RenewLocksAsync` remarks: later calls may send, as the
    expected value, a lock an earlier call tried to store (outcome unknown:
    threw, or re-run after commit), and a call may send its own
    `lockExpiration` (renews to the same value, returned as usual).
  - Tests: SQL `RenewalFaultAfterExecutionTests` (new file; theory, real
    hosted service + MemoryMessageQueue + EF store, 30 rows: 10 in flight, 20
    queued, 3 s span; renewal faulted AfterExecution once) (a) no retrying
    strategy -> call throws, remembered path; (b) `SmokeRetryingExecutionStrategy`
    -> EF re-ran the statement (same parameters), no "Failed to renew"; both:
    no dropped/lost logs, every lock unexpired after span + 1 s, each message
    dispatched exactly once, all rows deleted. Core (`LockRenewalHostedServiceTests`):
    re-run after lost commit (ids 1-12) -> same-tick own-lock retry, nothing
    dropped; later pass fails after pass 0 partly succeeded -> nothing revoked,
    recovered next tick; 10 failed ticks -> next tick sends expected + 8 most
    recent attempts + own lock (10 calls), oldest 2 not tried. Updated 3
    existing tests whose call counts/filters change by design (lost queued /
    in-flight message now gets the own-lock retry call in the same tick).
  - Fail-before (own-lock candidate disabled temporarily): SQL (b) failed (20
    queued dropped, 10 in-flight lost); (a) passed (remembered path). Core: the
    re-run test, the cap test and the 2 updated count tests failed.
  - Results: build 0 errors, no warnings in changed files (netstandard2.0/2.1
    built). Core 67 (was 64), EFCore 1075 net8 / 1074 net9-11 (unchanged), SQL
    Server 377 (was 375), 0 skipped, on net8-11. Flakiness: core renewal/
    backpressure/queue/tester tests (41) 3x and SQL renewal/tester/hosted/
    update tests (33) 3x on 4 TFMs, all green. No leftover XDevOutboxSmoke_
    databases; no test processes left.
  - Residual risk (same class as remembered attempts): the own-lock retry could
    match another instance's claim only if this process's lock had already
    expired and that claim stored a lock within ±5 ms of this tick's value.
  - Knowledge candidate: `components/outbox-dispatch-runtime.md` (renewal loop)
    says remembered attempts cover lost commit acknowledgements; it should also
    mention the same-tick own-lock retry (retrying execution strategies).
- [x] Verification (orchestrator, after round 2): build 0 errors
  (netstandard included); net8–net11 Core 67, EFCore 1075/1074, SQL Server
  377, all passed, 0 skipped; 0 leftover smoke DBs; `git diff --check`
  clean; no debug output. Round-2 change (own-lock retry) is the fix the
  reviewer proposed for N1; not re-reviewed separately.
- [x] Knowledge updates: `components/outbox-dispatch-runtime.md`,
  `business-rules/message-delivery.md`, `database/outbox-messages.md`,
  `architecture.md`, `project.md`.
