"""Durable intent and read-only reconciliation for native MO Rebase.

This module does not invoke Rebase, send snapshot requests, retire databases or
delete resources. Publication integration remains disabled until kernel and
real-process acceptance pass. Pending intent is never proof of rollback.
"""

from contextlib import closing
from dataclasses import asdict, dataclass
import hashlib
import json
import re
import uuid


ATTEMPTS_TABLE = "rebase_attempts"
ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    attempt_id VARCHAR(32) NOT NULL PRIMARY KEY,
    branch VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    intent_json TEXT NOT NULL,
    result_json TEXT
)
"""


class RebaseAttemptUnknown(RuntimeError):
    """An intent COMMIT may have succeeded; inspect this ID, never reissue it."""

    def __init__(self, attempt_id):
        self.attempt_id = attempt_id
        super().__init__(f"Rebase attempt {attempt_id} has an unknown outcome")


def _identifier(value):
    if not isinstance(value, str) or not value or len(value) > 64 or "\x00" in value:
        raise ValueError("invalid Rebase metadata identifier")
    return "`" + value.replace("`", "``") + "`"


def _attempt_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("invalid Rebase attempt ID")
    return value


@dataclass(frozen=True)
class RebaseIntent:
    attempt_id: str
    branch: str
    source_db: str
    source_id: int
    source_epoch: int
    old_generation: int
    candidate_db: str
    new_generation: int
    upstream_db: str
    upstream_id: int
    feature_snapshot: str
    upstream_snapshot: str
    protocol: int = 1

    def __post_init__(self):
        _attempt_id(self.attempt_id)
        for value in (self.branch, self.source_db, self.candidate_db, self.upstream_db,
                      self.feature_snapshot, self.upstream_snapshot):
            _identifier(value)
        for value in (self.source_id, self.source_epoch, self.upstream_id):
            if type(value) is not int or not 0 < value < 2 ** 64:
                raise ValueError("invalid Rebase identity or epoch")
        if self.source_epoch == 2 ** 64 - 1 or self.source_id == self.upstream_id:
            raise ValueError("exhausted epoch or identical database identity")
        if (type(self.old_generation) is not int or not 0 <= self.old_generation < 2 ** 31 - 1
                or type(self.new_generation) is not int
                or self.new_generation != self.old_generation + 1):
            raise ValueError("invalid Rebase generation")
        if (type(self.protocol) is not int or self.protocol != 1
                or self.candidate_db in (self.source_db, self.upstream_db)
                or self.source_db == self.upstream_db
                or self.feature_snapshot == self.upstream_snapshot):
            raise ValueError("invalid Rebase intent protocol or candidate")


class RebaseAttemptStore:
    """MO-specific metadata owner, using fresh DB-API connections per operation."""

    def __init__(self, connect, meta_db):
        self._connect = connect
        quoted = _identifier(meta_db)
        self._attempts = f"{quoted}.`{ATTEMPTS_TABLE}`"
        self._branches = f"{quoted}.`branches`"

    def prepare(self, branch, upstream_branch, attempt_id=None):
        """Persist fixed token and resource names before any snapshot request.

        This is not a publish operation. Its returned intent must be passed to
        the later snapshot/publication protocol without generating new names.
        """
        _identifier(branch)
        _identifier(upstream_branch)
        attempt_id = _attempt_id(attempt_id if attempt_id is not None else uuid.uuid4().hex)
        with closing(self._connect()) as conn:
            if not conn.get_autocommit():
                raise ValueError("intent preparation requires a fresh autocommit connection")
            with conn.cursor() as cur:
                cur.execute(ATTEMPTS_DDL.format(table=self._attempts))
                cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            conn.begin()
            commit_started = False
            try:
                with conn.cursor() as cur:
                    # Preparing only locks the branch row; no attempt lock is
                    # acquired and then followed by a branch lock.
                    cur.execute(f"SELECT db, gen FROM {self._branches} WHERE name=%s FOR UPDATE", (branch,))
                    source = cur.fetchone()
                    if source is None:
                        raise ValueError("unknown source branch")
                    cur.execute(f"SELECT db, gen FROM {self._branches} WHERE name=%s", (upstream_branch,))
                    upstream = cur.fetchone()
                    if upstream is None or source[0] == upstream[0]:
                        raise ValueError("unknown upstream or identical branch")
                    # Tenant scope is explicit even for a system-account client.
                    cur.execute("SELECT d.dat_id, i.epoch FROM mo_catalog.mo_database d "
                                "JOIN mo_catalog.mo_database_inventory i "
                                "ON i.account_id=d.account_id AND i.database_id=d.dat_id "
                                "WHERE d.account_id=current_account_id() AND d.datname=%s", (source[0],))
                    rows = cur.fetchall()
                    if len(rows) != 1:
                        raise ValueError("missing or ambiguous source inventory")
                    source_id, epoch = rows[0]
                    cur.execute("SELECT dat_id FROM mo_catalog.mo_database "
                                "WHERE account_id=current_account_id() AND datname=%s", (upstream[0],))
                    rows = cur.fetchall()
                    if len(rows) != 1:
                        raise ValueError("missing or ambiguous upstream identity")
                    gen = int(source[1])
                    prefix = hashlib.sha256((self._attempts + "\x00" + branch).encode()).hexdigest()[:12]
                    intent = RebaseIntent(
                        attempt_id, branch, source[0], int(source_id), int(epoch), gen,
                        f"bb_r2_{prefix}_{gen + 1}_{attempt_id}", gen + 1,
                        upstream[0], int(rows[0][0]), f"bb_r2_f_{attempt_id}", f"bb_r2_u_{attempt_id}")
                    cur.execute(f"INSERT INTO {self._attempts} (attempt_id,branch,state,intent_json) "
                                "VALUES (%s,%s,%s,%s)",
                                (attempt_id, branch, "PREPARING", json.dumps(asdict(intent), sort_keys=True)))
                    if cur.rowcount != 1:
                        raise RuntimeError("Rebase intent was not inserted exactly once")
                commit_started = True
                try:
                    conn.commit()
                except Exception as exc:
                    raise RebaseAttemptUnknown(attempt_id) from exc
                return intent
            except BaseException:
                if not commit_started:
                    try:
                        conn.rollback()
                    except Exception as exc:
                        raise RebaseAttemptUnknown(attempt_id) from exc
                raise

    def list_attempts(self, branch, after=None, limit=100):
        """Return a bounded page, including its explicit continuation cursor."""
        _identifier(branch)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("attempt page limit must be between 1 and 100")
        if after is not None:
            _attempt_id(after)
        predicate = "branch=%s" + (" AND attempt_id>%s" if after else "")
        params = (branch, after, limit + 1) if after else (branch, limit + 1)
        with closing(self._connect()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                        f"WHERE {predicate} ORDER BY attempt_id LIMIT %s", params)
            rows = cur.fetchall()
        page = [self._decode(row) for row in rows[:limit]]
        if any(item["intent"]["branch"] != branch for item in page):
            raise ValueError("persisted attempt belongs to a different branch")
        return {"attempts": page, "next": page[-1]["attempt_id"] if len(rows) > limit else None}

    def recover(self, attempt_id):
        """Read committed terminal evidence without publishing or cleaning.

        Pending/absent rows remain UNKNOWN. Determining ABORTED for an active
        or rolled-back publisher needs the later locked terminal protocol and
        snapshot request receipts; this read-only stage never guesses it.
        """
        _attempt_id(attempt_id)
        with closing(self._connect()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                        "WHERE attempt_id=%s", (attempt_id,))
            rows = cur.fetchall()
        if not rows:
            return {"attempt_id": attempt_id, "state": "UNKNOWN", "result": None}
        if len(rows) != 1:
            raise ValueError("ambiguous Rebase attempt identity")
        outcome = self._decode(rows[0])
        if outcome["state"] not in ("COMMITTED", "ABORTED"):
            outcome["state"] = "UNKNOWN"
        return outcome

    @staticmethod
    def _decode(row):
        attempt_id, state, raw_intent, raw_result = row
        intent = RebaseIntent(**json.loads(raw_intent))
        if attempt_id != intent.attempt_id or state not in ("PREPARING", "UNKNOWN", "COMMITTED", "ABORTED"):
            raise ValueError("invalid persisted Rebase attempt")
        result = json.loads(raw_result) if raw_result is not None else None
        if state == "COMMITTED" and not isinstance(result, dict):
            raise ValueError("committed Rebase attempt has no result")
        if state not in ("COMMITTED", "ABORTED") and result is not None:
            raise ValueError("pending Rebase attempt has a terminal result")
        return {"attempt_id": attempt_id, "state": state, "intent": asdict(intent), "result": result}
