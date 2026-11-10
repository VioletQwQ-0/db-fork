# Native Rebase: Durable Intent And Snapshot Preparation

Status: normal benchmark Rebase is unchanged and native publication is disabled.
Explicit helpers persist intent, prepare its two snapshots, inspect receipts
and implement the disabled candidate publisher; this is not the complete native
Rebase or recovery implementation.

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

## Native Generation Identity

The allocated candidate name is opaque to the legacy `branch_of_db` parser.
That parser alone cannot relate a previous native upstream generation to its
current generation during subsequent Rebase or Merge. Resolve the bounded set
of relevant native names through their embedded attempt IDs and the existing
attempt-table primary key, not by scanning history or adding a mapping table.
Only immutable intent plus a matching COMMITTED result may identify a published
generation. Verify the exact allocated name, metadata namespace and generation;
pending/missing records remain UNKNOWN, and corrupt/aborted identities fail.
Never fall back to string-derived branch identity for a recognized native name.

The lineage caller resolves at most four names in one read, uses an operation-
local map, and keeps the legacy path unchanged when no native names occur.
The known benchmark root is never classified as a native generation, even if
its chosen literal name happens to match that shape.
Old generation identity survives current-pointer advances or physical database
retirement because its successful attempt remains durable. This read-only
identity slice is not a GC authorization, native publication or proof of the
future base/history-writing protocol. Real repeated Rebase, subsequent Merge
and lifecycle acceptance remain required before enabling R2.

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

## Disabled Atomic Publication Implementation

Design decision for the next slice: add explicit MO-only publication of one
already-prepared attempt. Do not route normal benchmark Rebase into it yet.
The server EXPECT candidate is test-only and default-disabled; the client does
not activate or bypass that gate. Conflict policy (`fail`, `ours` meaning
upstream, or `theirs` meaning feature) is fixed in the durable intent before
snapshot preparation. A callback is rejected before opening a connection.
Earlier unpublished draft intents without that new optional field mean fail,
never an inferred policy from the current caller.

Use a fresh existing RC transaction and the established branch-then-attempt
lock order. Require the original source registration and PREPARING intent,
canonical candidate name, both CONFIRMED receipts and all four original
snapshot identities. A current pointer or a visible snapshot is not a terminal
request receipt. Existing candidates are retained and rejected, not dropped.

Resolve the source's data-table names in that same transaction, omitting only
the adapter's `_bb_commits`. Unexpected reserved metadata tables are rejected.
The kernel EXPECT check rejects stale inventories, including races after this
name read. Run native Rebase with all those user tables and the fixed four
snapshots; verify its complete typed summary and recheck all four snapshot
UUID/TS/DB identities after native admission has acquired their protection.
If a name was rebound before admission, roll the whole candidate back.

The candidate inherits upstream history, but its new base is the actual fixed
U endpoint retained by R1, not a fictitious candidate pre-replay snapshot.
Write one rebase marker whose base and previous base both name that U snapshot.
Append feature commit/merge rows after the original base sequence, with original
commit IDs and historical snapshot/database names. Use server-side INSERT SELECT
and ROW_NUMBER, not Python row replay. Allocate marker/append sequences above
both relevant histories and reject overflow. Record the candidate's schema
fingerprint with the marker, using the same pure row decoder as legacy history.
Do not call autonomous `_add_commit`, `_register`, snapshot or drop helpers.

Then update the branch row by exact old database/generation CAS, retire the
fixed source with DROP in this same transaction and conditionally write its
COMMITTED result. Acknowledged COMMIT is the publication point. The result
contains candidate ID/generation, the exact used U database/snapshot/timestamp,
the native summary and client-observed native SQL duration. Overall elapsed,
preparation and cleanup are measured separately by callers; no performance
claim is made by this component.

The real-client fixture reuses the existing SQL integration cluster rather than
introducing another service lifecycle. Its exclusive test-only EXPECT switch
is restored at fixture exit. A frozen wrapper stages only the actual native
attempt module, minimal package initializer, its tiny acceptance script and
pure-Python PyMySQL sources. The host Python child shares the worker cgroup;
its interpreter and source hashes are recorded. Native MO continues using the
unchanged verified rootfs and CGo libraries; no host Python libraries are bound
into the server namespace. An atomic ready receipt starts the child, and its
atomic terminal receipt releases the fixture. Both process return codes and
the exact adapter hash must agree before the wrapper can accept the result.
Timeout/error terminates and reaps only these exact children.

That fixture checks two publications, actual U base markers, original commit
identities, reads through retired database snapshots, Merge after the second
Rebase and precise successful cleanup. It does not by itself test process
death, cross-CN, whole MatrixOneToolSuite routing or uncertain-result recovery.

The tiny real native-attempt client has passed that selected main path against
the fixed test-only kernel candidate, including clean process exit and explicit
resource cleanup. Its first run exposed and fixed recovery's assumption that
the outer DB-API fetchall container was a list; the exact zero/one-row branch
checks now also accept PyMySQL's tuple container. This is not kill/unknown-
commit or cross-CN acceptance. Normal benchmark routing remains unchanged.

An uncertain COMMIT returns UNKNOWN and performs no retry or cleanup. Explicit
publication refuses a terminal attempt and directs callers to recover the
original result rather than republish it. An acknowledged pre-commit rollback
also does not authorize speculative snapshot cleanup; pending attempts remain
retained until the separate durable abort-proof path is implemented. This is a
conservative unfinished recovery state, not full R2 acceptance.

Review decision: PASS for this disabled implementation slice. Offline DB-API
tests must cover same-owner order, policy binding, fixed endpoint identity,
history/base correctness, exact CAS/affected rows, overflow, rollback at each
mutation, and unknown commit without retry. Real adapter subprocess tests,
continuous Rebase/Merge/history and recovery are mandatory before enabling the
normal path. A model PASS does not establish storage atomicity or completion.

## Deterministic Real-Client Failure Acceptance

The next test-only slice reuses that same private cluster and starts actual
adapter subprocesses. A bounded plaintext MySQL packet relay is necessary to
distinguish a server COMMIT from receipt of its response; a Python exception
after conn.commit() would not establish the requested lost-response condition.
The relay is fixture code only, binds loopback, admits at most8 connections and
limits a packet to1MiB. It forwards authentication without recording it and
logs only COM_QUERY SQL from the private test. It does not parse general SQL.

Three observable barriers drive exact-child SIGKILL, never scheduler sleeps:
- snapshot: the exact preallocated CREATE SNAPSHOT has a server OK held before
  forwarding to the child; its durable request remains REQUESTED;
- before_commit: after the publication success-row UPDATE, hold its COMMIT
  request before forwarding; a fresh recovery must see the busy publisher as
  UNKNOWN, then child death/backend closure must leave source/epoch/pointer
  unchanged and candidate absent;
- after_commit: forward that COMMIT and hold its server OK before the child
  receives it; a direct independent query must prove COMMITTED before killing
  the child, then a fresh Python process must return that exact durable result
  without another publication or cleanup.

Each barrier is acknowledged by the relay, including whether the request was
forwarded/OK withheld. Recovery starts a new interpreter with only connection
configuration, metadata DB and original attempt ID; no returned in-memory
intent/result is supplied. Record exact owned PIDs and reap/close all relay
threads/connections even on failure. Busy-state observation uses NOWAIT and
bounded polling only for actual asynchronous lock release after disconnect.
It is not a retry of a publication.

The pending snapshot case stays UNKNOWN even if a snapshot is visible. The
pending pre-commit case also remains conservatively UNKNOWN with the current
recover interface; this does not qualify a durable ABORTED/production cleanup
path. Fixture teardown can use its independently observed terminal server OK,
dead clients/closed backend and released publication locks to remove only its
private resources. It is not authorization for production recovery to infer
rollback from disconnect or catalog absence.

Add a two-PK-table conflict case: first sorted table has a valid feature change,
second has conflicting edits. The existing native applied-table hook records
that the first table was actually processed before the error. Independently
assert complete source/upstream rows, original epoch/registration, no candidate
and PREPARING attempt with both confirmed receipts. No full-data scale or new
service fixture is needed.

Review decision: PASS for these test-only owner/transport closures. Unit tests
must prove fragmentation, unchanged pass-through, request-vs-response barriers,
wrong server response refusal and exact shutdown. Real-process evidence is
still required; proxy unit tests do not replace it. Existing resource gates and
exclusive lease remain in force, and production R2 stays disabled.

## Actual ToolSuite Call-Chain Acceptance

The ToolSuite fixture uses the unchanged MatrixOneToolSuite constructor, branch and
commit API, schema fingerprint, real ResultCollector and generation resolution.
Its native publication is the existing explicit MO helper chain (prepare,
request snapshots, publish), not the ordinary legacy rebase route. No generic
API/default routing or server activation is changed by this fixture.

Create tiny explicit-PK data and an unchanged duplicate/NULL bag, make actual
fork/commit history, publish twice after independent upstream/feature edits,
then Merge using the real Suite method. Check complete rows, old commit refs,
the first durable result after the branch advances, base direction and the
second delta window. Two further source branches conflict on one key and
prove ours=upstream and theirs=feature. Track SQL-issued fixture snapshots and
known databases exactly; no database/snapshot cleanup follows failed evidence.
Success closes the Suite before explicit resource teardown and confirms every
owned user snapshot/database absent. This is still not production GC or a
performance experiment. Linux client dependencies are pinned by wheel hashes;
no database, driver, ledger, ResultCollector or protobuf module is mocked.
