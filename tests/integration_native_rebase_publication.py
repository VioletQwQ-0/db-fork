"""Tiny real-client acceptance against the exclusive native-Rebase SQL fixture.

Only the fixture's private databases/snapshots are touched. Failure preserves
them; successful verification removes the explicit resource list. Not a bench.
"""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import traceback

import pymysql

from dblib import matrixone_rebase as native


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    temporary.replace(path)


def execute(bridge):
    config = json.loads((bridge / "ready.json").read_text())
    if set(config) != {"port"} or type(config["port"]) is not int or not 0 < config["port"] < 65536:
        raise ValueError("invalid private fixture endpoint")
    query_log = (bridge / "adapter-sql.jsonl").open("x")

    class ObservedCursor(pymysql.cursors.Cursor):
        def execute(self, query, args=None):
            query_log.write(json.dumps({"sql": query, "params": args}, default=str) + "\n")
            query_log.flush()
            return super().execute(query, args)

    def connect():
        return pymysql.connect(host="127.0.0.1", port=config["port"], user="dump", password="111",
                               autocommit=True, connect_timeout=10, read_timeout=30, write_timeout=30,
                               cursorclass=ObservedCursor)

    try:
        conn = connect()
    except BaseException:
        query_log.close()
        raise
    snapshots = []
    databases = []
    prefix = "nr_adapter_pub"
    upstream, feature, metadata = prefix + "_u", prefix + "_f", prefix + "_meta"
    store = native.RebaseAttemptStore(connect, metadata)

    def sql(query, params=None):
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else cur.rowcount

    def snapshot(database, name):
        sql(f"CREATE SNAPSHOT {native._identifier(name)} FOR DATABASE {native._identifier(database)}")
        snapshots.append(name)
        return name

    def state(database, fixed=None):
        expression = native._identifier(database) + ".`items`" + (" {snapshot=%s}" if fixed else "")
        return sql("SELECT id,v FROM " + expression + " ORDER BY id", (fixed,) if fixed else None)

    def check(actual, expected, context):
        if actual != expected:
            raise AssertionError(f"{context}: expected {expected!r}, got {actual!r}")

    def history(database, seq, kind, name, previous=None):
        sql(f"INSERT INTO {native._identifier(database)}.`_bb_commits` "
            "(seq,id,kind,message,snap,db,prev_snap,prev_db,ts,schema_json) "
            "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,NOW(6),NULL)", (
                seq, "fixture_" + str(seq), kind, name, database,
                previous[1] if previous else None, previous[0] if previous else None))

    def prepare_and_snapshot(source, generation, feature_base, upstream_base, sequence):
        intent = store.prepare("feature", "main", source_registration=(source, generation),
                               upstream_registration=(upstream, 0), feature_base=feature_base,
                               upstream_base=upstream_base, base_sequence=sequence)
        receipts = store.request_snapshots(intent.attempt_id)
        check([r["state"] for r in receipts["snapshots"]], ["CONFIRMED", "CONFIRMED"], "snapshot receipts")
        snapshots.extend((intent.feature_snapshot, intent.upstream_snapshot))
        databases.append(intent.candidate_db)
        return intent

    try:
        for database in (upstream, feature, metadata):
            databases.append(database)
        sql(f"CREATE DATABASE {upstream}")
        sql(f"CREATE TABLE {upstream}.items(id int primary key,v int)")
        sql(f"CREATE TABLE {upstream}.history(v int)")
        sql(f"CREATE TABLE {upstream}._bb_commits("
            "seq bigint primary key,id varchar(40) not null,kind varchar(8) not null,message text,"
            "snap varchar(64) not null,db varchar(64) not null,prev_snap varchar(64),prev_db varchar(64),"
            "ts datetime(6),schema_json text,key _bb_commit_id(id))")
        sql(f"INSERT INTO {upstream}.items VALUES(1,10)")
        sql(f"INSERT INTO {upstream}.history VALUES(1),(1),(NULL)")
        base_u, base_f = prefix + "_bu", prefix + "_bf"
        history(upstream, 10, "root", base_u)
        snapshot(upstream, base_u)
        sql(f"DATA BRANCH CREATE DATABASE {feature} FROM {upstream} {{snapshot=%s}}", (base_u,))
        history(feature, 20, "fork", base_f, (upstream, base_u))
        snapshot(feature, base_f)
        sql(f"UPDATE {feature}.items SET v=11 WHERE id=1")
        sql(f"INSERT INTO {feature}.items VALUES(4,40)")
        historical_f = prefix + "_feature_commit"
        history(feature, 30, "commit", historical_f)
        snapshot(feature, historical_f)
        sql(f"INSERT INTO {upstream}.items VALUES(2,20)")
        history(upstream, 40, "commit", prefix + "_upstream_commit")
        snapshot(upstream, prefix + "_upstream_commit")
        sql(f"CREATE DATABASE {metadata}")
        sql(f"CREATE TABLE {metadata}.branches(name varchar(64) primary key,db varchar(64),gen int)")
        sql(f"INSERT INTO {metadata}.branches VALUES('main',%s,0),('feature',%s,0)", (upstream, feature))
        first = prepare_and_snapshot(feature, 0, (feature, base_f), (upstream, base_u), 20)
        sql(f"INSERT INTO {upstream}.items VALUES(3,30)")
        one = store.publish(first.attempt_id)
        check(state(one["candidate_db"]), ((1, 11), (2, 20), (4, 40)), "first native result")
        check(state(upstream), ((1, 10), (2, 20), (3, 30)), "later upstream write")
        check(state(feature, historical_f), ((1, 11), (4, 40)), "retired original history")
        check(store.recover(first.attempt_id)["result"], one, "first durable result")
        check(sql(f"SELECT gen,db FROM {metadata}.branches WHERE name='feature'"),
              ((1, one["candidate_db"]),), "first pointer")
        check(sql("SELECT count(*) FROM mo_catalog.mo_database WHERE datname=%s", (feature,)), ((0,),), "source retirement")
        marker_rows = sql(f"SELECT seq,db,snap,prev_db,prev_snap FROM {native._identifier(one['candidate_db'])}._bb_commits WHERE kind='rebase'")
        check(len(marker_rows), 1, "first base marker cardinality")
        marker, marker_db, marker_snap, previous_db, previous_snap = marker_rows[0]
        check((marker_db, marker_snap, previous_db, previous_snap),
              (upstream, first.upstream_snapshot, upstream, first.upstream_snapshot), "actual U base")
        current = one["candidate_db"]
        sql(f"UPDATE {native._identifier(current)}.items SET v=41 WHERE id=4")
        historical_second = prefix + "_feature_commit2"
        history(current, 50, "commit", historical_second)
        snapshot(current, historical_second)
        sql(f"INSERT INTO {upstream}.items VALUES(5,50)")
        fixed_base = (upstream, first.upstream_snapshot)
        second = prepare_and_snapshot(current, 1, fixed_base, fixed_base, marker)
        two = store.publish(second.attempt_id)
        expected = ((1, 11), (2, 20), (3, 30), (4, 41), (5, 50))
        check(state(two["candidate_db"]), expected, "second native result")
        check(store.recover(first.attempt_id)["result"], one, "old success survives advanced pointer")
        check(store.recover(second.attempt_id)["result"], two, "second durable result")
        check(state(current, historical_second), ((1, 11), (2, 20), (4, 41)), "retired second history")
        check(sql(f"SELECT id FROM {native._identifier(two['candidate_db'])}._bb_commits "
                  "WHERE kind='commit' ORDER BY seq"),
              (("fixture_40",), ("fixture_30",), ("fixture_50",)), "preserved commit identities")
        sql(f"DATA BRANCH MERGE {native._identifier(two['candidate_db'])}.items INTO {upstream}.items WHEN CONFLICT FAIL")
        check(state(upstream), expected, "merge after second rebase")
        check(sql(f"SELECT count(*),sum(v=1),sum(v is null) FROM {native._identifier(two['candidate_db'])}.history"),
              ((3, 2, 1),), "duplicate/NULL bag retained")
        # All fixed/historical reads and durable confirmations precede cleanup.
        for name in reversed(snapshots):
            sql(f"DROP SNAPSHOT {native._identifier(name)}")
        for database in reversed(databases):
            sql(f"DROP DATABASE IF EXISTS {native._identifier(database)}")
        remaining = sql("SELECT datname FROM mo_catalog.mo_database WHERE datname IN (" +
                        ",".join("%s" for _ in databases) + ")", tuple(databases))
        check(remaining, (), "precise database cleanup")
        return {"status": "PASS_REAL_NATIVE_ADAPTER_PUBLICATION", "publications": 2,
                "historical_reads": 2, "merge_verified": True, "cleanup_complete": True,
                "adapter_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()}
    finally:
        conn.close()
        query_log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bridge", type=Path, required=True)
    bridge = parser.parse_args().bridge
    try:
        result = execute(bridge)
    except BaseException as exc:
        traceback.print_exc(file=sys.stderr)
        save(bridge / "result.json", {"status": "FAIL", "category": type(exc).__name__, "cleanup_complete": False})
        return 1
    save(bridge / "result.json", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
