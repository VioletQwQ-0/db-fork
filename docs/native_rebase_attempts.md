# Native Rebase: Durable Intent Stage

Status: publication disabled. This stage persists intent and reads committed
metadata; it is not the complete native Rebase or recovery implementation.

Base adapter: `c2737beaf503dd1f3208efc457d369040f0aca40` (`mo-opt`). The normal
`_rebase_impl` and generic benchmark API remain unchanged. Kernel R1 and R2
integration have independent source identities and acceptance requirements.

## Invariants And Ownership

`RebaseAttemptStore` owns one fresh connection per call. `prepare` commits a
`PREPARING` row in the existing benchmark metadata database before returning a
fixed source token, generation and resource names. No snapshot or publication
is sent by this stage. The source branch row is locked while the intent is
assembled; source inventory is scoped by account and database ID. Later
publication must revalidate both the token and registry relation.

An attempt ID is a full 128-bit identifier. Its candidate and snapshot names
are allocated once and remain within the server's 64-character limit. Epoch
exhaustion and generation overflow fail before the intent insert. Failed
pre-commit preparation rolls back. Once COMMIT is sent, an exception means
`RebaseAttemptUnknown(attempt_id)`, not a retry, rollback claim or cleanup.

Only benchmark metadata is mutated. The metadata database and its permissions
remain owned by the benchmark instance, not by an unprivileged external caller.
No credentials or connection strings are recorded in the intent.

## Inspection Contract

- `list_rebase_attempts(branch, after=None, limit=100)` returns at most 100 rows
  with an explicit ID-ordered continuation cursor. It does not create tables.
- `recover_rebase(attempt_id)` uses a new connection and reads only the original
  record. Absent, `PREPARING` or `UNKNOWN` records return `UNKNOWN`. A stored
  `COMMITTED` result is returned without consulting the current branch pointer,
  which may already have moved again. Read errors propagate.
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
