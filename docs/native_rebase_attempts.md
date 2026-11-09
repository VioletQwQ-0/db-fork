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
- `recover_rebase(attempt_id)` uses a new connection and reads only the original
  record. Absent, `PREPARING` or `UNKNOWN` records return `UNKNOWN`. A stored
  `COMMITTED` result is returned without consulting the current branch pointer,
   which may already have moved again. Inspection also reads at most two
   snapshot receipts; read errors propagate.
- Corrupt identity, generation, state or result data is rejected. Neither
  inspection method publishes, retries, aborts, drops or reclaims anything.

This deliberately incomplete read-only reconciliation cannot establish that a
publisher or snapshot request has ended. It cannot transition a pending row to
`ABORTED` or authorize resource deletion.

## Required Publication Integration

Before enabling native Rebase, implement and validate all of the following:

1. Record confirmed snapshot identities and terminal request evidence in the
   persistent attempt. A request without a proven terminal outcome remains
   `UNKNOWN`, including when the client dies or the snapshot is not yet visible.
2. In the existing Pessimistic+RC publication transaction, lock the branch row
   before the attempt row. Validate generation, database identity and fixed
   inventory token. Commit epoch reservation, replayed candidate, new base,
   history, pointer CAS, source retirement and `COMMITTED` result together.
3. Locked recovery confirms the same attempt without replay. It may mark
   `ABORTED` only after publication rollback and all independent snapshot
   requests have confirmed terminal states. It must not infer rollback from
   missing metadata, a disconnected client or a lock timeout.
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

The focused offline suite contains 34 tests, including the original 18 intent
cases. It verifies acknowledged reservation before request, branch/attempt lock
order, two independent autocommit requests, confirmed receipt reuse, commit
uncertainty, stale generations, receipt corruption and connection release.
All pass locally; real snapshot execution, competing publication and actual
adapter subprocess exits remain NOT_RUN. No result is a performance sample.
