"""Real MatrixOneToolSuite acceptance for the disabled native helper chain.

Run only against the exclusive Go bridge. All normal branch/commit/history/
Merge methods and the collector are real. No benchmark/default route changes.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import traceback

import pymysql


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    temporary.replace(path)


def execute(bridge):
    ready = json.loads((bridge / "ready.json").read_text())
    if set(ready) != {"port"} or type(ready["port"]) is not int or not 0 < ready["port"] < 65536:
        raise ValueError("invalid private bridge endpoint")
    os.environ.update(MO_HOST="127.0.0.1", MO_PORT=str(ready["port"]), MO_USER="dump", MO_PASSWORD="111")
    from dblib import matrixone as mo
    from dblib import matrixone_rebase as native
    from dblib import result_pb2 as rslt
    from dblib.db_api import Ref
    from dblib.result_collector import ResultCollector

    log = (bridge / "adapter-sql.jsonl").open("x")
    snapshots, databases = set(), set()
    queries = 0
    connect = pymysql.connect
    suite = None
    suite_connection = None
    raw = None

    class Observed(pymysql.cursors.Cursor):
        def execute(self, query, args=None):
            nonlocal queries
            queries += 1
            if queries > 4000:
                raise RuntimeError("ToolSuite fixture SQL budget exceeded")
            log.write(json.dumps({"sql": query, "params": args}, default=str) + "\n")
            log.flush()
            result = super().execute(query, args)
            match = re.match(r"^CREATE SNAPSHOT (`(?:``|[^`])+`|[a-zA-Z0-9_]+)\s+FOR DATABASE ", query, re.I)
            if match:
                name = match[1]
                snapshots.add(name[1:-1].replace("``", "`") if name.startswith("`") else name)
            return result

    def observed_connect(*args, **kwargs):
        # Instrumentation only: the original PyMySQL factory and all real
        # connection options remain unchanged; no SQL/result is substituted.
        kwargs.setdefault("cursorclass", Observed)
        return connect(*args, **kwargs)

    def check(actual, expected, context):
        if actual != expected:
            raise AssertionError(f"{context}: expected {expected!r}, got {actual!r}")

    def sql(query, params=None):
        with raw.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else cur.rowcount

    def verb(result):
        if not result.ok:
            raise AssertionError(f"real Suite {result.op} failed: {result.error}")
        return result.value

    def rows(ref):
        result = suite.exec(["SELECT id,v FROM items ORDER BY id"], refs=[ref], raise_on_error=True)[0]
        verb(result)
        return tuple(tuple(row) for row in result.rows)

    def change(ref, statements):
        for result in suite.exec(statements, refs=[ref], raise_on_error=True):
            verb(result)

    def native_publish(branch, policy="fail"):
        intent = suite._prepare_native_rebase_attempt(Ref(branch), Ref("main"), policy)
        databases.add(intent.candidate_db)
        receipt = suite._request_native_rebase_snapshots(intent.attempt_id)
        check([item["state"] for item in receipt["snapshots"]], ["CONFIRMED", "CONFIRMED"], "real Suite snapshot receipts")
        result = suite._publish_native_rebase_attempt(intent.attempt_id)
        check(suite.recover_rebase(intent.attempt_id)["result"], result, "real Suite recovery")
        check(sql("SELECT count(*) FROM mo_catalog.mo_database WHERE account_id=current_account_id() AND datname=%s",
                  (intent.source_db,)), ((0,),), "real Suite source retired")
        return intent, result

    pymysql.connect = observed_connect
    try:
        raw = mo.connect(None, read_timeout=30, write_timeout=30)
        database = "nr_actual_suite"
        databases.update((database, mo.meta_db_name(database)))
        sql(f"CREATE DATABASE {native._identifier(database)}")
        sql(f"CREATE TABLE {database}.items(id int primary key,v int)")
        sql(f"CREATE TABLE {database}.history(v int)")
        sql(f"INSERT INTO {database}.items VALUES(1,10)")
        sql(f"INSERT INTO {database}.history VALUES(1),(1),(NULL)")
        collector = ResultCollector(run_id="native-toolsuite", output_dir=str(bridge / "collector"))
        suite_connection = mo.connect(None, read_timeout=30, write_timeout=30)
        suite = mo.MatrixOneToolSuite(suite_connection, collector, database)
        verb(suite.commit("main", "base", raise_on_error=True))
        verb(suite.branch("feature", "main", raise_on_error=True))
        databases.add(mo.branch_db_name(database, "feature"))
        change("feature", ["UPDATE items SET v=11 WHERE id=1", "INSERT INTO items VALUES(4,40)"])
        first_commit = verb(suite.commit("feature", "feature one", raise_on_error=True))
        change("main", ["INSERT INTO items VALUES(2,20)"])
        verb(suite.commit("main", "upstream one", raise_on_error=True))
        first, one = native_publish("feature")
        check(rows("feature"), ((1, 11), (2, 20), (4, 40)), "real Suite first publication")
        marker = suite._base_row(one["candidate_db"])
        check((marker["db"], marker["snap"], marker["prev_db"], marker["prev_snap"]),
              (database, first.upstream_snapshot, database, first.upstream_snapshot), "real Suite fixed U marker")
        change("feature", ["UPDATE items SET v=41 WHERE id=4"])
        second_commit = verb(suite.commit("feature", "feature two", raise_on_error=True))
        change("main", ["INSERT INTO items VALUES(3,30)", "INSERT INTO items VALUES(5,50)"])
        verb(suite.commit("main", "upstream two", raise_on_error=True))
        second, two = native_publish("feature")
        check((second.feature_base.database, second.feature_base.snapshot,
               second.upstream_base.database, second.upstream_base.snapshot),
              (database, first.upstream_snapshot, database, first.upstream_snapshot), "real Suite second delta window")
        expected = ((1, 11), (2, 20), (3, 30), (4, 41), (5, 50))
        check(rows("feature"), expected, "real Suite second publication")
        check(rows("feature@" + first_commit), ((1, 11), (4, 40)), "real Suite old generation commit one")
        check(rows("feature@" + second_commit), ((1, 11), (2, 20), (4, 41)), "real Suite old generation commit two")
        check(suite.recover_rebase(first.attempt_id)["result"], one, "real Suite old success after advance")
        commits = verb(suite.log("feature", limit=100, raise_on_error=True))
        for commit in (first_commit, second_commit):
            check(sum(item["commit_hash"] == commit for item in commits), 1, "real Suite preserved commit identity")
        verb(suite.merge("main", "feature", on_conflict="theirs", raise_on_error=True))
        check(rows("main"), expected, "real Suite Merge after two native publications")
        for branch, value in (("keep_u", 200), ("keep_f", 300)):
            verb(suite.branch(branch, "main", raise_on_error=True))
            databases.add(mo.branch_db_name(database, branch))
            change(branch, [f"UPDATE items SET v={value} WHERE id=1"])
            verb(suite.commit(branch, "conflicting feature", raise_on_error=True))
        change("main", ["UPDATE items SET v=100 WHERE id=1"])
        verb(suite.commit("main", "conflicting upstream", raise_on_error=True))
        for branch, policy, value in (("keep_u", "ours", 100), ("keep_f", "theirs", 300)):
            _, outcome = native_publish(branch, policy)
            check(outcome["native_summary"]["conflict_keys"], 1, "real Suite conflict count")
            check(rows(branch), ((1, value),) + expected[1:], "real Suite conflict direction")
        check(rows("main"), ((1, 100),) + expected[1:], "native policies must not modify upstream")
        check(sql(f"SELECT count(*),sum(v=1),sum(v is null) FROM {native._identifier(two['candidate_db'])}.history"),
              ((3, 2, 1),), "real Suite unchanged no-PK bag")
        if not collector.results or any(result.status != rslt.OpStatus.OK for result in collector.results):
            raise AssertionError("real result collector contains missing or failed Suite operations")
        database_state = sql("SELECT db FROM " + native._identifier(suite._meta_db) + ".branches")
        databases.update(row[0] for row in database_state)
        databases.update(suite._temp_dbs.values())
        # No active operation or independent request remains. Fixture-local
        # teardown is not production reference-safe garbage collection.
        suite.close_connection()
        suite = None
        for name in sorted(snapshots):
            sql(f"DROP SNAPSHOT {native._identifier(name)}")
        for name in sorted(databases, reverse=True):
            sql(f"DROP DATABASE IF EXISTS {native._identifier(name)}")
        for relation, column, resources in (("mo_catalog.mo_database", "datname", databases),
                                            ("mo_catalog.mo_snapshots", "sname", snapshots)):
            check(sql(f"SELECT {column} FROM {relation} WHERE {column} IN (" +
                      ",".join("%s" for _ in resources) + ")", tuple(sorted(resources))), (), "real Suite exact cleanup")
        return {"status": "PASS_REAL_MATRIXONE_TOOLSUITE_NATIVE_REBASE", "publications": 4,
                "historical_reads": 2, "policies": 2, "merge_verified": True, "cleanup_complete": True,
                "suite_sha256": hashlib.sha256(Path(mo.__file__).read_bytes()).hexdigest(),
                "adapter_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
                "collector_operations": len(collector.results), "sql_statements": queries,
                "native_default_route_enabled": False}
    finally:
        if suite_connection is not None and suite_connection.open:
            # Do not call suite.close_connection on failed evidence: it may
            # delete owned historical-read databases that must be retained.
            suite_connection.close()
        if raw is not None:
            raw.close()
        pymysql.connect = connect
        log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bridge", type=Path, required=True)
    args = parser.parse_args()
    try:
        value = execute(args.bridge)
    except BaseException as exc:
        traceback.print_exc(file=sys.stderr)
        save(args.bridge / "result.json", {"status": "FAIL", "category": type(exc).__name__, "cleanup_complete": False})
        return 1
    save(args.bridge / "result.json", value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
