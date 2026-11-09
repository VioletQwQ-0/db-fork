"""Offline DB-API contract tests, not database or process-recovery evidence."""

from dataclasses import asdict, replace
import json
import unittest
from unittest.mock import MagicMock, Mock

from dblib.matrixone_rebase import (
    RebaseAttemptStore,
    RebaseAttemptUnknown,
    RebaseIntent,
)


ATTEMPT = "0123456789abcdef0123456789abcdef"


def intent(attempt_id=ATTEMPT):
    return RebaseIntent(attempt_id, "feature", "bench_feature", 17, 3, 0,
                        "candidate_" + attempt_id, 1, "bench", 11,
                        "feature_" + attempt_id, "upstream_" + attempt_id)


def record(state="PREPARING", value=None, result=None):
    value = value or intent()
    return (value.attempt_id, state, json.dumps(asdict(value)),
            json.dumps(result) if result is not None else None)


class AttemptStoreTest(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.conn.get_autocommit.return_value = True
        self.cur = self.conn.cursor.return_value.__enter__.return_value
        self.cur.fetchone.side_effect = [("bench_feature", 0), ("bench", 0)]
        self.cur.fetchall.side_effect = [[(17, 3)], [(11,)]]
        self.cur.rowcount = 1
        self.connect = Mock(return_value=self.conn)
        self.store = RebaseAttemptStore(self.connect, "bench_meta")

    def test_prepare_commits_one_fixed_intent_without_snapshot_or_publication(self):
        events = Mock()
        events.attach_mock(self.cur.execute, "execute")
        events.attach_mock(self.conn.begin, "begin")
        events.attach_mock(self.conn.commit, "commit")
        result = self.store.prepare("feature", "main", ATTEMPT)
        self.assertEqual((result.source_id, result.source_epoch, result.old_generation), (17, 3, 0))
        self.assertIn(ATTEMPT, result.candidate_db)
        self.assertLessEqual(len(result.candidate_db), 64)
        statements = [call.args for call in self.cur.execute.call_args_list]
        self.assertEqual(len(statements), 7)
        self.assertIn("FOR UPDATE", statements[2][0])
        self.assertIn("current_account_id()", statements[4][0])
        self.assertIn("current_account_id()", statements[5][0])
        self.assertEqual(statements[-1][1][:3], (ATTEMPT, "feature", "PREPARING"))
        self.assertEqual(json.loads(statements[-1][1][3]), asdict(result))
        self.assertFalse(any("SNAPSHOT" in sql[0] or "REBASE" in sql[0]
                             or "DROP" in sql[0] for sql in statements))
        names = [call[0] for call in events.mock_calls]
        self.assertEqual(names[:3], ["execute", "execute", "begin"])
        self.assertEqual(names[-2:], ["execute", "commit"])
        self.conn.rollback.assert_not_called()
        self.conn.close.assert_called_once()
        self.connect.assert_called_once_with()

    def test_commit_loss_is_unknown_without_retry_or_rollback(self):
        self.conn.commit.side_effect = OSError("lost commit response")
        with self.assertRaises(RebaseAttemptUnknown) as caught:
            self.store.prepare("feature", "main", ATTEMPT)
        self.assertEqual(caught.exception.attempt_id, ATTEMPT)
        self.conn.commit.assert_called_once()
        self.conn.rollback.assert_not_called()
        self.connect.assert_called_once()
        self.conn.close.assert_called_once()

    def test_failed_insert_rolls_back_without_retry(self):
        def execute(sql, params=None):
            if sql.startswith("INSERT"):
                raise RuntimeError("duplicate attempt")
        self.cur.execute.side_effect = execute
        with self.assertRaisesRegex(RuntimeError, "duplicate attempt"):
            self.store.prepare("feature", "main", ATTEMPT)
        self.conn.rollback.assert_called_once()
        self.conn.commit.assert_not_called()
        self.connect.assert_called_once()
        self.conn.close.assert_called_once()

    def test_failed_rollback_remains_unknown(self):
        self.cur.rowcount = 0
        self.conn.rollback.side_effect = OSError("lost rollback response")
        with self.assertRaises(RebaseAttemptUnknown) as caught:
            self.store.prepare("feature", "main", ATTEMPT)
        self.assertEqual(caught.exception.attempt_id, ATTEMPT)
        self.conn.commit.assert_not_called()

    def test_missing_source_or_upstream_rolls_back(self):
        for rows in ((None,), (("bench_feature", 0), None),
                     (("bench_feature", 0), ("bench_feature", 0))):
            with self.subTest(rows=rows):
                self.setUp()
                self.cur.fetchone.side_effect = rows
                with self.assertRaises(ValueError):
                    self.store.prepare("feature", "main", ATTEMPT)
                self.conn.rollback.assert_called_once()
                self.conn.commit.assert_not_called()

    def test_missing_or_duplicate_inventory_fails_closed(self):
        for rows in ([], [(17, 3), (17, 4)], [(17, 0)], [(17, 2 ** 64 - 1)]):
            with self.subTest(rows=rows):
                self.setUp()
                self.cur.fetchall.side_effect = [rows, [(11,)]]
                with self.assertRaises(ValueError):
                    self.store.prepare("feature", "main", ATTEMPT)
                self.assertFalse(any(call.args[0].startswith("INSERT")
                                     for call in self.cur.execute.call_args_list))
                self.conn.rollback.assert_called_once()

    def test_invalid_prepare_inputs_do_not_open_connection(self):
        for args in (("", "main", ATTEMPT), ("feature", "main", "bad"),
                     ("feature", "x" * 65, ATTEMPT)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.store.prepare(*args)
        self.connect.assert_not_called()

    def test_nonfresh_connection_cannot_own_intent_transaction(self):
        self.conn.get_autocommit.return_value = False
        with self.assertRaises(ValueError):
            self.store.prepare("feature", "main", ATTEMPT)
        self.cur.execute.assert_not_called()
        self.conn.begin.assert_not_called()
        self.conn.close.assert_called_once()

    def test_identifiers_are_quoted_and_branch_names_are_bound(self):
        store = RebaseAttemptStore(self.connect, "meta`quoted")
        branch = "branch'with`punctuation"
        self.cur.fetchall.side_effect = [[record(value=replace(intent(), branch=branch))]]
        store.list_attempts(branch)
        sql, params = self.cur.execute.call_args.args
        self.assertIn("`meta``quoted`.`rebase_attempts`", sql)
        self.assertNotIn(branch, sql)
        self.assertEqual(params, (branch, 101))

    def test_missing_or_preparing_attempt_stays_unknown_without_mutation(self):
        for rows in ([], [record()], [record("UNKNOWN")]):
            with self.subTest(rows=rows):
                self.setUp()
                self.cur.fetchall.side_effect = [rows]
                outcome = self.store.recover(ATTEMPT)
                self.assertEqual(outcome["state"], "UNKNOWN")
                self.assertIsNone(outcome["result"])
                self.assertEqual(self.cur.execute.call_count, 1)
                self.assertTrue(self.cur.execute.call_args.args[0].startswith("SELECT"))
                self.conn.commit.assert_not_called()
                self.conn.rollback.assert_not_called()
                self.conn.close.assert_called_once()

    def test_committed_result_does_not_depend_on_current_branch_generation(self):
        result = {"candidate_db": "original", "generation": 1}
        self.cur.fetchall.side_effect = [[record("COMMITTED", result=result)]]
        outcome = self.store.recover(ATTEMPT)
        self.assertEqual(outcome["state"], "COMMITTED")
        self.assertEqual(outcome["result"], result)
        self.assertNotIn("branches", self.cur.execute.call_args.args[0])
        self.assertEqual(self.cur.execute.call_count, 1)

    def test_read_failure_is_not_reclassified_as_aborted(self):
        self.cur.execute.side_effect = OSError("server unavailable")
        with self.assertRaises(OSError):
            self.store.recover(ATTEMPT)
        self.conn.close.assert_called_once()

    def test_corrupt_attempt_does_not_publish_or_clean(self):
        bad_intent = asdict(intent())
        bad_intent["new_generation"] = 7
        rows = [record("COMMITTED"), record("UNKNOWN", result={}),
                ("f" * 32, "PREPARING", record()[2], None),
                (ATTEMPT, "PREPARING", json.dumps(bad_intent), None),
                record("NOT_A_STATE")]
        for row in rows:
            with self.subTest(row=row):
                self.setUp()
                self.cur.fetchall.side_effect = [[row]]
                with self.assertRaises(ValueError):
                    self.store.recover(ATTEMPT)
                self.assertEqual(self.cur.execute.call_count, 1)

    def test_duplicate_rows_are_not_terminal_evidence(self):
        self.cur.fetchall.side_effect = [[record(), record()]]
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.store.recover(ATTEMPT)

    def test_list_is_bounded_and_exposes_continuation(self):
        second = replace(intent(), attempt_id="f" * 32)
        self.cur.fetchall.side_effect = [[record(), record(value=second)]]
        page = self.store.list_attempts("feature", limit=1)
        self.assertEqual(len(page["attempts"]), 1)
        self.assertEqual(page["next"], ATTEMPT)
        self.assertEqual(self.cur.execute.call_args.args[1], ("feature", 2))
        self.cur.fetchall.side_effect = [[record(value=second)]]
        page = self.store.list_attempts("feature", after=ATTEMPT, limit=1)
        self.assertIsNone(page["next"])
        self.assertEqual(self.cur.execute.call_args.args[1], ("feature", ATTEMPT, 2))

    def test_list_rejects_cross_branch_corruption(self):
        self.cur.fetchall.side_effect = [[record(value=replace(intent(), branch="other"))]]
        with self.assertRaises(ValueError):
            self.store.list_attempts("feature")

    def test_invalid_page_arguments_do_not_open_connection(self):
        for kwargs in ({"limit": 0}, {"limit": 101}, {"limit": True}, {"after": "bad"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.store.list_attempts("feature", **kwargs)
        self.connect.assert_not_called()

    def test_generation_and_identity_boundaries(self):
        for values in ({"source_epoch": 2 ** 64 - 1}, {"source_id": True},
                       {"new_generation": True}, {"protocol": True},
                       {"old_generation": 2 ** 31 - 1, "new_generation": 2 ** 31},
                       {"upstream_id": 17}, {"candidate_db": "bench"},
                       {"upstream_snapshot": intent().feature_snapshot}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                replace(intent(), **values)


if __name__ == "__main__":
    unittest.main()
