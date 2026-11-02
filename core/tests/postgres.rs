//! PostgreSQL tests (DB-1, DB-2, DB-3, MIG-3, MIG-4, LIVE-2). Need `DM_TEST_PG_URL`.

mod common;

use common::*;

const PG: &str = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml";

/// DB-1 + DB-2: all mapped types, keys, indexes and FKs PostgreSQL -> PostgreSQL.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn db3_postgres_to_postgres_all_types() {
    let _ = PG;
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_db3_src").await;
    let dst = fresh_pg("dm_db3_dst").await;
    pg_exec(&src, PG_SEED).await;
    let summary = migrate(&src, &dst, &settings(dir.path())).await;
    assert_eq!(summary.containers, 5);
    assert_eq!(summary.rows, 2 + 4 + 40 + 2 + 3);

    let source = read_all(&src).await;
    let target = read_all(&dst).await;
    assert_same_data(&source, &target, PG_CONTAINERS);

    // exact representations in the target
    let r = pg_query_strings(
        &dst,
        "SELECT c_numeric::text, c_text, c_bytes::text, c_json::text, c_arr::text, c_mood, c_tstz AT TIME ZONE 'UTC', c_ts::text, c_time::text FROM all_types WHERE id = 1",
    )
    .await;
    assert_eq!(r[0][0].as_deref(), Some("12345678901234567890.123456"));
    assert!(r[0][1].as_deref().unwrap().contains("\u{1e}\u{1f}"));
    assert_eq!(r[0][2].as_deref(), Some("\\x00ff1e1f"));
    assert_eq!(r[0][3].as_deref(), Some(r#"{"k": [1, "x", null], "n": {"m": true}}"#));
    assert_eq!(r[0][4].as_deref(), Some("[1, 2, 3]"));
    assert_eq!(r[0][5].as_deref(), Some("happy"));
    assert_eq!(r[0][6].as_deref(), Some("2024-05-06 05:08:09"));
    assert_eq!(r[0][7].as_deref(), Some("2024-05-06 07:08:09.123456"));
    assert_eq!(r[0][8].as_deref(), Some("23:59:58.5"));

    // constraints and indexes recreated after the load
    let fks = pg_query_strings(&dst, "SELECT count(*) FROM pg_constraint WHERE contype = 'f'").await;
    assert_eq!(fks[0][0].as_deref(), Some("2"));
    let idx = pg_query_strings(&dst, "SELECT indexdef FROM pg_indexes WHERE tablename = 'users' AND indexdef LIKE '%(email)%'").await;
    assert_eq!(idx.len(), 1);
    let users = &target["users"].0;
    assert_eq!(users.foreign_keys[0].reference_container_name, "public.user_types");

    // running again is idempotent (merge on primary key); table without key is appended
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_eq!(pg_count(&dst, "orders").await, 40);
}

/// FN-6: `?schema=` selects the source schema and the target schema.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn fn6_schema_selection() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_fn6_src").await;
    let dst = fresh_pg("dm_fn6_dst").await;
    pg_exec(&src, "CREATE SCHEMA sales; CREATE TABLE sales.items (id INT PRIMARY KEY, n TEXT); INSERT INTO sales.items VALUES (1, 'a'); CREATE TABLE public.other (id INT PRIMARY KEY);").await;
    migrate(&format!("{}?schema=sales", src), &format!("{}?schema=imported", dst), &settings(dir.path())).await;
    assert_eq!(pg_count(&dst, "imported.items").await, 1);
    let other = pg_query_strings(&dst, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'other'").await;
    assert_eq!(other[0][0].as_deref(), Some("0"));
}

/// MIG-4: PostgreSQL -> SQLite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn mig4_postgres_to_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_mig4_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = sqlite_url(dir.path(), "t.db");
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, PG_CONTAINERS);
}

/// MIG-3: SQLite -> PostgreSQL.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn mig3_sqlite_to_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "s.db");
    sqlite_exec(&src, SQLITE_SEED);
    let dst = fresh_pg("dm_mig3_dst").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SQLITE_CONTAINERS);
    let fks = pg_query_strings(&dst, "SELECT count(*) FROM pg_constraint WHERE contype = 'f'").await;
    assert_eq!(fks[0][0].as_deref(), Some("2"));
}

/// LIVE-2 (+ LIVE-4..8, LIVE-13, LIVE-6): logical replication PostgreSQL -> SQLite, including
/// changes made while the snapshot is taken, key changes, and resume after restart.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn live2_postgres_logical_replication() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_live2_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = sqlite_url(dir.path(), "live.db");
    let mut s = settings(dir.path());
    s.live.slot_name = "dm_live2_slot".into();
    s.live.publication_name = "dm_live2_pub".into();

    let run = start_live(&src, &dst, &s).await;
    pg_exec(
        &src,
        "INSERT INTO user_types (type_name) VALUES ('Guest');
         INSERT INTO users (username, email, user_type_id) VALUES ('guest', 'g@example.com', 3);
         UPDATE users SET username = 'renamed' WHERE id = 1;
         BEGIN; UPDATE orders SET amount = amount + 1 WHERE id <= 10; DELETE FROM orders WHERE id = 40; COMMIT;
         UPDATE all_types SET id = 3, c_text = 'moved' WHERE id = 2;
         UPDATE all_types SET c_json = '{\"changed\": 1}', c_arr = '{4,5}' WHERE id = 1;",
    )
    .await;
    eventually("changes replicated", || {
        let dst = dst.clone();
        async move {
            let p = dst.strip_prefix("sqlite://").unwrap().to_string();
            let c = rusqlite::Connection::open(p).unwrap();
            let n: i64 = c.query_row("SELECT count(*) FROM all_types WHERE id = 3", [], |r| r.get(0)).unwrap();
            let o: i64 = c.query_row("SELECT count(*) FROM orders", [], |r| r.get(0)).unwrap();
            let j: i64 = c.query_row("SELECT count(*) FROM all_types WHERE c_json LIKE '%changed%'", [], |r| r.get(0)).unwrap();
            n == 1 && o == 39 && j == 1
        }
    })
    .await;
    let source = read_all(&src).await;
    assert_same_data(&source, &read_all(&dst).await, &["user_types", "users", "orders", "all_types"]);
    let summary = run.finish().await.live.unwrap();
    assert!(summary.events >= 16, "{summary:?}");

    // resume: changes while stopped arrive, the snapshot is not repeated
    pg_exec(&src, "INSERT INTO users (username, email, user_type_id) VALUES ('later', 'l@example.com', 2);").await;
    sqlite_exec(&dst, "DELETE FROM orders WHERE id = 1;");
    let run = start_live(&src, &dst, &s).await;
    eventually("resumed", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "users") == 6 }
    })
    .await;
    run.finish().await;
    assert_eq!(sqlite_count(&dst, "orders"), 38);
    let slot = pg_query_strings(&src, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'dm_live2_slot'").await;
    assert_eq!(slot[0][0].as_deref(), Some("1"));
}

/// LIVE-3 into PostgreSQL: SQLite triggers -> PostgreSQL upserts / deletes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn live3_sqlite_to_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "s.db");
    sqlite_exec(&src, SQLITE_SEED);
    let dst = fresh_pg("dm_live3_dst").await;
    let run = start_live(&src, &dst, &settings(dir.path())).await;
    sqlite_exec(&src, "UPDATE users SET avatar = x'abcd', balance = '5.55' WHERE id = 2; DELETE FROM orders WHERE id > 25; INSERT INTO user_types VALUES (9, 'New');");
    eventually("changes replicated", || {
        let dst = dst.clone();
        async move { pg_count(&dst, "orders").await == 25 && pg_count(&dst, "user_types").await == 3 }
    })
    .await;
    eventually("update replicated", || {
        let dst = dst.clone();
        async move { pg_query_strings(&dst, "SELECT balance::text FROM users WHERE id = 2").await[0][0].as_deref() == Some("5.55") }
    })
    .await;
    run.finish().await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SQLITE_CONTAINERS);
}

/// PERF-11: a large table is read by several key-range readers on one snapshot; every row
/// arrives exactly once, also when rows change during the copy.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn perf11_parallel_key_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_perf11_src").await;
    let dst = fresh_pg("dm_perf11_dst").await;
    pg_exec(&src, "CREATE TABLE big (id BIGINT PRIMARY KEY, v TEXT); INSERT INTO big SELECT g * 3, md5(g::text) FROM generate_series(-20000, 130000) g; ANALYZE big;").await;
    let mut s = settings(dir.path());
    s.parallel_ranges = 4;
    s.pagination_limit = 1000;
    migrate(&src, &dst, &s).await;
    let q = "SELECT count(*)::text, sum(id)::text, md5(string_agg(v, ',' ORDER BY id)) FROM big";
    assert_eq!(pg_query_strings(&src, q).await, pg_query_strings(&dst, q).await);
    assert_eq!(pg_count(&dst, "big").await, 150_001);
}

/// VIEW-5 / VIEW-6 / VIEW-9 on PostgreSQL: filter and sort in the database, joins with duplicate
/// column names, read-only queries, cancel request.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn view6_postgres_browse_and_query() {
    use rustygrator_core::browse::{Query, QueryRunner, Sort};
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_view6").await;
    pg_exec(&src, PG_SEED).await;
    let mut db = open(&src, &settings(dir.path())).await;
    let orders = definition(&mut db, "public.orders").await;
    let q = Query::Table { def: orders, filter: Some("note IS NULL".into()), sort: Some(Sort { column: "id".into(), descending: true }) };
    let ids: Vec<String> = browse(&mut db, &q).await.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids, ["40", "35", "30", "25", "20", "15", "10", "5"]);

    let q = Query::Text("SELECT u.id, t.id, t.type_name, a.c_numeric, a.c_json, a.c_arr, a.c_mood FROM users u JOIN user_types t ON t.id = u.user_type_id LEFT JOIN all_types a ON a.id = u.id ORDER BY u.id;".into());
    let def = db.describe(&q).await.unwrap();
    assert_eq!(def.properties_info.iter().map(|p| p.property_name.as_str()).collect::<Vec<_>>(), ["id", "id", "type_name", "c_numeric", "c_json", "c_arr", "c_mood"]);
    let rows = browse(&mut db, &q).await;
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][..4], ["1", "1", "Admin", "12345678901234567890.123456"]);
    assert_eq!(rows[0][5], "[1, 2, 3]");
    assert_eq!(rows[0][6], "happy");

    let def = db.describe(&Query::Text("DELETE FROM orders RETURNING id".into())).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let r = db.fetch(&Query::Text("DELETE FROM orders RETURNING id".into()), &def, std::sync::Arc::new(rustygrator_core::migrator::batching::BatchSizer::fixed(10)), tx).await;
    assert!(r.is_err(), "read-only transaction rejects writes");
    assert_eq!(pg_count(&src, "orders").await, 40);

    let q = Query::Text("SELECT pg_sleep(60)::text".into());
    let def = db.describe(&q).await.unwrap();
    let canceller = db.canceller();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let fetch = tokio::spawn(async move { db.fetch(&q, &def, std::sync::Arc::new(rustygrator_core::migrator::batching::BatchSizer::fixed(10)), tx).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    canceller.cancel().await;
    let r = tokio::time::timeout(std::time::Duration::from_secs(10), fetch).await.expect("cancel request stops the query").unwrap();
    let r = r.unwrap_err().to_string();
    assert!(r.contains("cancel"), "{r}");
}

/// CONN-3: `sslmode` in the connection string switches TLS on; a server without TLS (the test
/// container) is refused instead of silently connecting unencrypted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn conn3_postgres_sslmode() {
    let dir = tempfile::tempdir().unwrap();
    let url = fresh_pg("dm_conn3").await;
    let sep = if url.contains('?') { '&' } else { '?' };
    let plain = format!("{url}{sep}sslmode=disable");
    rustygrator_core::db::database::connect(conn(&plain), settings(dir.path())).await.expect("sslmode=disable connects");
    let tls = format!("{url}{sep}sslmode=require");
    let r = rustygrator_core::db::database::connect(conn(&tls), settings(dir.path())).await;
    assert!(r.is_err(), "sslmode=require must not fall back to an unencrypted connection");
}
