# Native Rebase: Durable Intent And Snapshot Preparation

Status: normal benchmark Rebase is unchanged and native publication is disabled.
Explicit helpers persist intent, prepare its two snapshots and inspect receipts;
this is not the complete native Rebase or recovery implementation.

Base adapter: `c2737beaf503dd1f3208efc457d369040f0aca40` (`mo-opt`). The normal
`_rebase_impl` and generic benchmark API remain unchanged. Kernel R1 and R2
integration have independent source identities and acceptance requirements.

## Invariants And Ownership

`RebaseAttemptStore` owns one fresh connection per call. `prepare` commits a
`PREPARING` row in the existing benchmark metadata database before returning a
fixed source token, generation and resource names. `prepare` itself sends no
snapshot or publication. The source branch row is locked while the intent is
assembled; source inventory is scoped by account and database ID. Later
publication must revalidate both the token and registry relation.

The immutable intent also binds both old base endpoints, their database IDs,
snapshot UUIDs/timestamps, and the base log sequence. The adapter uses its
existing lineage resolver once and passes the two observed registrations to
`prepare`; a changed source or upstream generation rejects that stale
resolution. In the intent transaction, both base snapshots must exist in the
current account and match their recorded database names. No two-way/no-base
fallback is allowed on this restricted native path. Recovery decodes these
original endpoints, never chooses a new fork point from current history.
This is identity validation, not yet snapshot lifetime/GC protection; the
kernel must protect and validate the exact endpoints before publication.
Incomplete intents from the earlier, unreleased draft are rejected without
inventing bases or deleting resources. The production benchmark path is
unchanged and does not invoke this preparation helper.

An attempt ID is a full 128-bit identifier. Its candidate and snapshot names
are allocated once and remain within the server's 64-character limit. Epoch
exhaustion and generation overflow fail before the intent insert. Failed
pre-commit preparation rolls back. Once COMMIT is sent, an exception means
`RebaseAttemptUnknown(attempt_id)`, not a retry, rollback claim or cleanup.

Intent preparation mutates only benchmark metadata. The separately invoked
snapshot stage creates only its two preallocated snapshots. The metadata
database and its permissions remain owned by the benchmark instance, not by
an unprivileged external caller.
No credentials or connection strings are recorded in the intent.

## Inspection Contract

- `list_rebase_attempts(branch, after=None, limit=100)` returns at most 100 rows
  with an explicit ID-ordered continuation cursor. It does not create tables.
- `recover_rebase(attempt_id)` first reads the immutable intent, then uses a
  fresh RC transaction to lock the branch registration before the attempt row
  and reread the original result and its at most two snapshot receipts. Both
  locking reads use NOWAIT: a busy publisher returns `UNKNOWN`, not a stalled
  recovery operation or rollback inference. Lock-wait timeout, deadlock and
  uncertain recovery-transaction completion also remain `UNKNOWN`; other
  read errors propagate. Absent, `PREPARING` or `UNKNOWN` records return
  `UNKNOWN`. A stored `COMMITTED` result survives an advanced or removed
  current branch registration; recovery never derives its outcome from the
  current pointer. Changed immutable identity or terminal evidence is rejected.
- Corrupt identity, generation, state or result data is rejected. Neither
  inspection method publishes, retries, aborts, drops or reclaims anything.

Locked confirmation still cannot establish that an independent snapshot request
has ended. It cannot transition a pending row to `ABORTED` or authorize resource
deletion. The preparation stage uses non-locking inspection only to discover the
intent before its separate branch/attempt-locked request admission; this is not
a second recovery or publishing API.

## Required Publication Integration

Before enabling native Rebase, implement and validate all of the following:

1. Record confirmed snapshot identities and terminal request evidence in the
   persistent attempt. A request without a proven terminal outcome remains
   `UNKNOWN`, including when the client dies or the snapshot is not yet visible.
2. In the existing Pessimistic+RC publication transaction, lock the branch row
   before the attempt row. Validate generation, database identity and fixed
   inventory token. Commit epoch reservation, replayed candidate, new base,
   history, pointer CAS, source retirement and `COMMITTED` result together.
3. The locked confirmation path must pass real-client competition and process
   recovery tests. An eventual transition to `ABORTED` requires publication
   rollback and all independent snapshot requests to have confirmed terminal
   states. Missing metadata, disconnect or lock timeout is not such evidence.
4. Keep old generations and snapshots until historical and active references
   prove reclamation safe. No sweeper or unbounded automatic retry is added.

The common catalog producer coverage, shutdown upgrade and cross-CN conflict
proof are prerequisites; intent persistence alone does not close the lost-table
counterexample or qualify R2 concurrency safety.

## Design Decision And Verification

Selected persistent database records over current-pointer inference (which
cannot prove an earlier success) or local client files (which do not survive
client loss and cannot commit atomically with publication). A general branch
service or background recovery manager is not required for this benchmark-owned
protocol. These decisions follow the restricted R2 plan; this slice does not
select a different publication algorithm.

Offline tests exercise DB-API call ordering, parameter binding, boundaries,
commit uncertainty, connection cleanup and read-only inspection. They do not
prove transaction conflicts, WAL recovery or actual adapter process death.
Those real-database and real-process acceptance gates remain `NOT_RUN` for
this module. Performance is not measured at this stage.

The locked-recovery tests additionally cover branch-before-attempt NOWAIT,
pending-to-committed rereads, missing/advanced registration, both busy lock
positions, release uncertainty, immutable-identity/terminal-result corruption,
and retained pending resources. These are offline owner/ordering oracles, not
proof that two actual database transactions conflict or that a killed client
recovers safely. No ABORTED writer, publication or resource reclamation is
enabled by this slice.

## Snapshot Request Owner

The next isolated stage adds at most two request rows per attempt in the same
benchmark metadata database, keyed by `(attempt_id, role)`. This keeps the
immutable intent and terminal publication result separate from independently
committed snapshot receipts without an ALTER of already-created attempt rows.
Only `feature` and `upstream` roles are accepted. Their names and database IDs
come from the committed intent; no retry generates a new resource name.

Before sending CREATE SNAPSHOT, a fresh metadata transaction locks the branch
registration row and then the original attempt row, checks its generation and
database identity, and inserts `REQUESTED`. It commits before opening the
autocommit snapshot-request connection. A failed/unknown reservation commit
sends no snapshot request. A pre-existing REQUESTED row is UNKNOWN and must not
be reissued; a valid CONFIRMED row may be reused without another CREATE.
Upstream preparation requires a confirmed feature receipt.

Before inserting a new REQUESTED row, the same metadata owner checks both
original base snapshot identities against the immutable intent. Missing or
rebound names stop this preparation, including between the feature and upstream
requests; already-confirmed feature resources are retained. Reusing a confirmed
receipt does not create another request. This early check does not pin snapshots
against later GC or replace the publication transaction's protected endpoint
validation. A read error is not ABORTED evidence and never triggers cleanup.

After a successful snapshot response, query its UUID, timestamp and database ID
from the current account's snapshot catalog. Verify level/name/ID against the
intent and persist a CONFIRMED receipt under the same branch-before-attempt
lock order. Receipt confirmation does not publish a branch and therefore does
not require the current branch generation to remain unchanged; the later
publication transaction must still validate it. Any failure after REQUESTED
was committed conservatively retains the record and resources. In particular,
client death before send is indistinguishable on restart from an in-flight
request; catalog absence is not proof of rollback.

Inspection may return the two bounded receipts but still reports UNKNOWN for
an unpublished attempt, even when both snapshots are confirmed. No ABORTED,
snapshot deletion, retry scheduler or publication is introduced. The normal
benchmark Rebase stays unchanged. The component and its offline ordering/error
tests may be developed independently; real request and adapter-process exits
remain required before R2 acceptance. This owner design is accepted for the
disabled adapter stage, not evidence of runtime correctness.

The focused offline suites cover acknowledged reservation before request,
branch/attempt lock order, independent autocommit requests, confirmed receipt
reuse, commit uncertainty, stale generations, corruption and connection release.
The adapter metadata cases also check both child/parent orientations, bases in
older generations, registration changes during lineage resolution, and recovery
without re-reading current history. These local DB-API/source integration tests
do not close lifetime, transaction, process-recovery or publication acceptance.
No result from this preparation stage is a performance sample.
