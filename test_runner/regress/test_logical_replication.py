import time
from functools import partial
from random import choice
from string import ascii_lowercase

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    logical_replication_sync,
    wait_for_last_flush_lsn,
)
from fixtures.types import Lsn
from fixtures.utils import query_scalar, wait_until


def random_string(n: int):
    return "".join([choice(ascii_lowercase) for _ in range(n)])


def test_logical_replication(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start(
        "test_logical_replication", config_lines=["log_statement=all"]
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("create table t(pk integer primary key, payload integer)")
    cur.execute(
        "CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));"
    )
    cur.execute("create publication pub1 for table t, replication_example")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, payload integer)")
    vanilla_pg.safe_psql(
        "CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120), testcolumn1 int, testcolumn2 int, testcolumn3 int);"
    )
    connstr = endpoint.connstr().replace("'", "''")
    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    logical_replication_sync(vanilla_pg, endpoint)

    # insert some data
    cur.execute("insert into t values (generate_series(1,1000), 0)")

    # Wait logical replication to sync
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 1000

    # now stop subscriber...
    vanilla_pg.stop()

    # ... and insert some more data which should be delivered to subscriber after restart
    cur.execute("insert into t values (generate_series(1001,2000), 0)")

    # Restart compute
    endpoint.stop()
    endpoint.start()

    # start subscriber
    vanilla_pg.start()

    # Wait logical replication to sync
    logical_replication_sync(vanilla_pg, endpoint)

    # Check that subscribers receives all data
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 2000

    # Test that save/restore of RewriteMappingFile works. Partial copy of
    # rewrite.sql test.
    log.info("checking rewriteheap")
    vanilla_pg.stop()
    cmds = """
INSERT INTO replication_example(somedata) VALUES (1);

BEGIN;
INSERT INTO replication_example(somedata) VALUES (2);
ALTER TABLE replication_example ADD COLUMN testcolumn1 int;
INSERT INTO replication_example(somedata, testcolumn1) VALUES (3,  1);
COMMIT;

BEGIN;
INSERT INTO replication_example(somedata) VALUES (3);
ALTER TABLE replication_example ADD COLUMN testcolumn2 int;
INSERT INTO replication_example(somedata, testcolumn1, testcolumn2) VALUES (4,  2, 1);
COMMIT;

VACUUM FULL pg_am;
VACUUM FULL pg_amop;
VACUUM FULL pg_proc;
VACUUM FULL pg_opclass;
VACUUM FULL pg_type;
VACUUM FULL pg_index;
VACUUM FULL pg_database;


-- repeated rewrites that fail
BEGIN;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
ROLLBACK;

-- repeated rewrites that succeed
BEGIN;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
COMMIT;

-- repeated rewrites in different transactions
VACUUM FULL pg_class;
VACUUM FULL pg_class;

-- reindexing of important relations / indexes
REINDEX TABLE pg_class;
REINDEX INDEX pg_class_oid_index;
REINDEX INDEX pg_class_tblspc_relfilenode_index;

INSERT INTO replication_example(somedata, testcolumn1) VALUES (5, 3);

BEGIN;
INSERT INTO replication_example(somedata, testcolumn1) VALUES (6, 4);
ALTER TABLE replication_example ADD COLUMN testcolumn3 int;
INSERT INTO replication_example(somedata, testcolumn1, testcolumn3) VALUES (7, 5, 1);
COMMIT;
"""
    endpoint.safe_psql_many([q for q in cmds.splitlines() if q != "" and not q.startswith("-")])

    # refetch rewrite files from pageserver
    endpoint.stop()
    endpoint.start()

    vanilla_pg.start()
    logical_replication_sync(vanilla_pg, endpoint)
    eq_q = "select testcolumn1, testcolumn2, testcolumn3 from replication_example order by 1, 2, 3"
    assert vanilla_pg.safe_psql(eq_q) == endpoint.safe_psql(eq_q)
    log.info("rewriteheap synced")

    # test that removal of repl slots works across restart
    vanilla_pg.stop()
    time.sleep(1)  # wait for conn termination; active slots can't be dropped
    endpoint.safe_psql("select pg_drop_replication_slot('sub1');")
    endpoint.safe_psql("insert into t values (2001, 1);")  # forces WAL flush
    # wait for drop message to reach safekeepers (it is not transactional)
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop()
    endpoint.start()
    # it must be gone (but walproposer slot still exists, hence 1)
    assert endpoint.safe_psql("select count(*) from pg_replication_slots")[0][0] == 1


# Test that neon.logical_replication_max_snap_files works
def test_obsolete_slot_drop(neon_simple_env: NeonEnv, vanilla_pg):
    def slot_removed(ep):
        assert (
            endpoint.safe_psql(
                "select count(*) from pg_replication_slots where slot_name = 'stale_slot'"
            )[0][0]
            == 0
        )

    env = neon_simple_env

    env.neon_cli.create_branch("test_logical_replication", "empty")
    # set low neon.logical_replication_max_snap_files
    endpoint = env.endpoints.create_start(
        "test_logical_replication",
        config_lines=["log_statement=all", "neon.logical_replication_max_snap_files=1"],
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # create obsolete slot
    cur.execute("select pg_create_logical_replication_slot('stale_slot', 'pgoutput');")
    assert (
        endpoint.safe_psql(
            "select count(*) from pg_replication_slots where slot_name = 'stale_slot'"
        )[0][0]
        == 1
    )

    # now insert some data and create and start live subscriber to create more .snap files
    # (in most cases this is not needed as stale_slot snap will have higher LSN than restart_lsn anyway)
    cur.execute("create table t(pk integer primary key, payload integer)")
    cur.execute("create publication pub1 for table t")

    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, payload integer)")
    connstr = endpoint.connstr().replace("'", "''")
    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    wait_until(number_of_iterations=10, interval=2, func=partial(slot_removed, endpoint))


# Tests that walsender correctly blocks until WAL is downloaded from safekeepers
def test_lr_with_slow_safekeeper(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("init")
    endpoint = env.endpoints.create_start("init")
    sk = env.safekeepers[0]

    with endpoint.connect().cursor() as cur:
        cur.execute("create table wal_generator (id serial primary key, data text)")
        cur.execute(
            """
INSERT INTO wal_generator (data)
SELECT repeat('A', 1024) -- Generates a kilobyte of data per row
FROM generate_series(1, 16384) AS seq; -- Inserts enough rows to exceed 16MB of data
"""
        )
        cur.execute("create table t(a int)")
        cur.execute("create publication pub for table t")
        cur.execute("insert into t values (1)")

    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(a int)")
    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub")
    logical_replication_sync(vanilla_pg, endpoint)
    vanilla_pg.stop()

    # Pause the safekeepers so that they can't send WAL (except to pageserver)
    for sk in env.safekeepers:
        sk_http = sk.http_client()
        sk_http.configure_failpoints([("sk-pause-send", "return")])

    # Insert a 2
    with endpoint.connect().cursor() as cur:
        cur.execute("insert into t values (2)")

    endpoint.stop_and_destroy()

    # This new endpoint should contain [1, 2], but it can't access WAL from safekeeper
    endpoint = env.endpoints.create_start("init")
    with endpoint.connect().cursor() as cur:
        cur.execute("select * from t")
        res = [r[0] for r in cur.fetchall()]
        assert res == [1, 2]

    # Reconnect subscriber
    vanilla_pg.start()
    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"alter subscription sub1 connection '{connstr}'")

    time.sleep(5)
    # Make sure the 2 isn't replicated
    assert [r[0] for r in vanilla_pg.safe_psql("select * from t")] == [1]

    # Re-enable WAL download
    for sk in env.safekeepers:
        sk_http = sk.http_client()
        sk_http.configure_failpoints([("sk-pause-send", "off")])

    logical_replication_sync(vanilla_pg, endpoint)
    assert [r[0] for r in vanilla_pg.safe_psql("select * from t")] == [1, 2]

    log_path = vanilla_pg.pgdatadir / "pg.log"
    with open(log_path, "r") as log_file:
        logs = log_file.read()
        assert "could not receive data from WAL stream" not in logs


# Test compute start at LSN page of which starts with contrecord
# https://github.com/neondatabase/neon/issues/5749
def test_wal_page_boundary_start(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("init")
    endpoint = env.endpoints.create_start("init")
    tenant_id = endpoint.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = endpoint.safe_psql("show neon.timeline_id")[0][0]

    cur = endpoint.connect().cursor()
    cur.execute("create table t(key int, value text)")
    cur.execute("CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int);")
    cur.execute("insert into replication_example values (1, 2)")
    cur.execute("create publication pub1 for table replication_example")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, value text)")
    vanilla_pg.safe_psql("CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int);")

    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")
    logical_replication_sync(vanilla_pg, endpoint)
    vanilla_pg.stop()

    with endpoint.cursor() as cur:
        # measure how much space logical message takes. Sometimes first attempt
        # creates huge message and then it stabilizes, have no idea why.
        for _ in range(3):
            lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
            log.info(f"current_lsn={lsn_before}")
            # Non-transactional logical message doesn't write WAL, only XLogInsert's
            # it, so use transactional. Which is a bit problematic as transactional
            # necessitates commit record. Alternatively we can do smth like
            #   select neon_xlogflush(pg_current_wal_insert_lsn());
            # but isn't much better + that particular call complains on 'xlog flush
            # request 0/282C018 is not satisfied' as pg_current_wal_insert_lsn skips
            # page headers.
            payload = "blahblah"
            cur.execute(f"select pg_logical_emit_message(true, 'pref', '{payload}')")
            lsn_after_by_curr_wal_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
            lsn_diff = lsn_after_by_curr_wal_lsn - lsn_before
            logical_message_base = lsn_after_by_curr_wal_lsn - lsn_before - len(payload)
            log.info(
                f"before {lsn_before}, after {lsn_after_by_curr_wal_lsn}, lsn diff is {lsn_diff}, base {logical_message_base}"
            )

        # and write logical message spanning exactly as we want
        lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"current_lsn={lsn_before}")
        curr_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        offs = int(curr_lsn) % 8192
        till_page = 8192 - offs
        payload_len = (
            till_page - logical_message_base - 8
        )  # not sure why 8 is here, it is deduced from experiments
        log.info(f"current_lsn={curr_lsn}, offs {offs}, till_page {till_page}")

        # payload_len above would go exactly till the page boundary; but we want contrecord, so make it slightly longer
        payload_len += 8

        cur.execute(f"select pg_logical_emit_message(true, 'pref', 'f{'a' * payload_len}')")
        supposedly_contrecord_end = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"supposedly_page_boundary={supposedly_contrecord_end}")
        # The calculations to hit the page boundary are very fuzzy, so just
        # ignore test if we fail to reach it.
        if not (int(supposedly_contrecord_end) % 8192 == 32):
            pytest.skip("missed page boundary, bad luck")

        cur.execute("insert into replication_example values (2, 3)")

    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop().start()

    cur = endpoint.connect().cursor()
    # this should flush current wal page
    cur.execute("insert into replication_example values (3, 4)")
    vanilla_pg.start()
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql(
        "select sum(somedata) from replication_example"
    ) == endpoint.safe_psql("select sum(somedata) from replication_example")


# Test that WAL redo works for fairly large records.
#
# See https://github.com/neondatabase/neon/pull/6534. That wasn't a
# logical replication bug as such, but without logical replication,
# records passed ot the WAL redo process are never large enough to hit
# the bug.
def test_large_records(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("init")
    endpoint = env.endpoints.create_start("init")

    cur = endpoint.connect().cursor()
    cur.execute("CREATE TABLE reptbl(id int, largeval text);")
    cur.execute("alter table reptbl replica identity full")
    cur.execute("create publication pub1 for table reptbl")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE reptbl(id int, largeval text);")

    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Test simple insert, update, delete. But with very large values
    value = random_string(10_000_000)
    cur.execute(f"INSERT INTO reptbl VALUES (1, '{value}')")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(1, value)]

    # Test delete, and reinsert another value
    cur.execute("DELETE FROM reptbl WHERE id = 1")
    cur.execute(f"INSERT INTO reptbl VALUES (2, '{value}')")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(2, value)]

    value = random_string(10_000_000)
    cur.execute(f"UPDATE reptbl SET largeval='{value}'")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(2, value)]

    endpoint.stop()
    endpoint.start()
    cur = endpoint.connect().cursor()
    value = random_string(10_000_000)
    cur.execute(f"UPDATE reptbl SET largeval='{value}'")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(2, value)]


#
# Check that slots are not inherited in brnach
#
def test_slots_and_branching(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant, timeline = env.neon_cli.create_tenant()
    env.pageserver.http_client()

    main_branch = env.endpoints.create_start("main", tenant_id=tenant)
    main_cur = main_branch.connect().cursor()

    # Create table and insert some data
    main_cur.execute("select pg_create_logical_replication_slot('my_slot', 'pgoutput')")

    wait_for_last_flush_lsn(env, main_branch, tenant, timeline)

    # Create branch ws.
    env.neon_cli.create_branch("ws", "main", tenant_id=tenant)
    ws_branch = env.endpoints.create_start("ws", tenant_id=tenant)

    # Check that we can create slot with the same name
    ws_cur = ws_branch.connect().cursor()
    ws_cur.execute("select pg_create_logical_replication_slot('my_slot', 'pgoutput')")


def test_replication_shutdown(neon_simple_env: NeonEnv):
    # Ensure Postgres can exit without stuck when a replication job is active + neon extension installed
    env = neon_simple_env
    env.neon_cli.create_branch("test_replication_shutdown_publisher", "empty")
    pub = env.endpoints.create("test_replication_shutdown_publisher")

    env.neon_cli.create_branch("test_replication_shutdown_subscriber")
    sub = env.endpoints.create("test_replication_shutdown_subscriber")

    pub.respec(skip_pg_catalog_updates=False)
    pub.start()

    sub.respec(skip_pg_catalog_updates=False)
    sub.start()

    pub.wait_for_migrations()
    sub.wait_for_migrations()

    with pub.cursor() as cur:
        cur.execute(
            "CREATE ROLE mr_whiskers WITH PASSWORD 'cat' LOGIN INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser"
        )
        cur.execute("CREATE DATABASE neondb WITH OWNER mr_whiskers")
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE neondb TO neon_superuser")

        # If we don't do this, creating the subscription will fail later on PG16
        pub.edit_hba(["host all mr_whiskers 0.0.0.0/0 md5"])

    with sub.cursor() as cur:
        cur.execute(
            "CREATE ROLE mr_whiskers WITH PASSWORD 'cat' LOGIN INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser"
        )
        cur.execute("CREATE DATABASE neondb WITH OWNER mr_whiskers")
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE neondb TO neon_superuser")

    with pub.cursor(dbname="neondb", user="mr_whiskers", password="cat") as cur:
        cur.execute("CREATE PUBLICATION pub FOR ALL TABLES")
        cur.execute("CREATE TABLE t (a int)")
        cur.execute("INSERT INTO t VALUES (10), (20)")
        cur.execute("SELECT * from t")
        res = cur.fetchall()
        assert [r[0] for r in res] == [10, 20]

    with sub.cursor(dbname="neondb", user="mr_whiskers", password="cat") as cur:
        cur.execute("CREATE TABLE t (a int)")

        pub_conn = f"host=localhost port={pub.pg_port} dbname=neondb user=mr_whiskers password=cat"
        query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
        log.info(f"Creating subscription: {query}")
        cur.execute(query)

        with pub.cursor(dbname="neondb", user="mr_whiskers", password="cat") as pcur:
            pcur.execute("INSERT INTO t VALUES (30), (40)")

        def check_that_changes_propagated():
            cur.execute("SELECT * FROM t")
            res = cur.fetchall()
            log.info(res)
            assert len(res) == 4
            assert [r[0] for r in res] == [10, 20, 30, 40]

        wait_until(10, 0.5, check_that_changes_propagated)
