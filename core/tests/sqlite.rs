//! SQLite tests (DB-7, DB-8, DB-9, LIVE-3 and the generic pipeline). They need no external
//! database and always run.

mod common;

use common::*;
use rustygrator_core::models::migration_execution_settings::{RunMode, TransferMode};

fn seeded(dir: &std::path::Path) -> (String, String) {
    let src = sqlite_url(dir, "source.db");
    let dst = sqlite_url(dir, "target.db");
    sqlite_exec(&src, SQLITE_SEED);
    (src, dst)
}

/// DB-7 + DB-8: every column, foreign key and index survives SQLite -> SQLite.
#[tokio::test(flavor = "multi_thread")]
async fn db9_sqlite_to_sqlite_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    let summary = migrate(&src, &dst, &settings(dir.path())).await;
    assert_eq!(summary.containers, 3);
    assert_eq!(summary.rows, 2 + 4 + 30);

    let source = read_all(&src).await;
    let target = read_all(&dst).await;
    assert_same_data(&source, &target, SQLITE_CONTAINERS);

    let (users, _) = &target["users"];
    assert_eq!(users.foreign_keys.len(), 1);
    assert_eq!(users.foreign_keys[0].reference_container_name, "user_types");
    assert!(users.indexes.iter().any(|i| i.columns == ["email"] && !i.unique));
    assert!(target["user_types"].0.indexes.iter().any(|i| i.columns == ["type_name"] && i.unique));
    // REFERENCES without a column list resolves to the parent's primary key
    assert_eq!(target["orders"].0.foreign_keys[0].reference_columns, ["id"]);
}

/// PERF-14: fast settings during the load, normal settings restored afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn perf14_sqlite_load_settings_are_restored() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    sqlite_exec(&dst, "PRAGMA journal_mode = WAL;");
    migrate(&src, &dst, &settings(dir.path())).await;
    let c = rusqlite::Connection::open(dst.strip_prefix("sqlite://").unwrap()).unwrap();
    let journal: String = c.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    assert_eq!(journal, "wal");
    let ok: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
}

/// Re-running a migration does not duplicate rows (idempotent load).
#[tokio::test(flavor = "multi_thread")]
async fn db9_rerun_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    let s = settings(dir.path());
    migrate(&src, &dst, &s).await;
    sqlite_exec(&src, "UPDATE users SET email = 'changed@example.com' WHERE id = 1;");
    migrate(&src, &dst, &s).await;
    assert_eq!(sqlite_count(&dst, "users"), 4);
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SQLITE_CONTAINERS);
}

/// FN-2 / FN-4: transfer through dbc files; FN-7: custom temp dir; separate provide / receive
/// processes.
#[tokio::test(flavor = "multi_thread")]
async fn fn2_file_transfer_and_separate_processes() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    let mut s = settings(dir.path());
    s.transfer = TransferMode::File;
    s.dbc.keep_temp = true;
    migrate(&src, &dst, &s).await;
    assert!(s.temp_dir.join("users_data.dbc").exists());
    assert!(s.temp_dir.join("definition_users.dmb").exists());
    assert!(s.temp_dir.join("migration_order.dmb").exists());
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SQLITE_CONTAINERS);
    // without --keep-temp the files are removed after success (FMT-14)
    let dst_b = sqlite_url(dir.path(), "target_b.db");
    let mut s2 = s.clone();
    s2.dbc.keep_temp = false;
    s2.temp_dir = dir.path().join("dbc2");
    migrate(&src, &dst_b, &s2).await;
    assert_eq!(std::fs::read_dir(&s2.temp_dir).unwrap().count(), 0);
    assert_same_data(&read_all(&src).await, &read_all(&dst_b).await, SQLITE_CONTAINERS);

    // provider and receiver as two runs sharing only the temp directory
    let dst2 = sqlite_url(dir.path(), "target2.db");
    let mut p = settings(dir.path());
    p.temp_dir = dir.path().join("handover");
    p.mode = RunMode::Provide;
    let mut m = rustygrator_core::migrator::migrator::Migrator::connect(Some(conn(&src)), None, p.clone()).await.unwrap();
    assert_eq!(m.run().await.unwrap().rows, 36);

    // FMT-13: a wrong key or a changed file stops before any row reaches the receiver
    let mut r = p.clone();
    r.mode = RunMode::Receive;
    let mut wrong = r.clone();
    wrong.dbc.key = rustygrator_core::dbc::crypto::KeySource::Passphrase("not the right passphrase".into());
    let mut m = rustygrator_core::migrator::migrator::Migrator::connect(None, Some(conn(&dst2)), wrong).await.unwrap();
    assert!(m.run().await.unwrap_err().to_string().contains("wrong key"));
    let data = p.temp_dir.join("orders_data.dbc");
    let original = std::fs::read(&data).unwrap();
    let mut changed = original.clone();
    let n = changed.len();
    changed[n - 60] ^= 1;
    std::fs::write(&data, &changed).unwrap();
    let mut m = rustygrator_core::migrator::migrator::Migrator::connect(None, Some(conn(&dst2)), r.clone()).await.unwrap();
    assert!(m.run().await.is_err());
    let has_tables = rusqlite::Connection::open(dst2.strip_prefix("sqlite://").unwrap())
        .unwrap()
        .query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table'", [], |r| r.get::<_, i64>(0))
        .unwrap();
    assert_eq!(has_tables, 0, "nothing may be written before every file is verified");
    std::fs::write(&data, &original).unwrap();

    let mut m = rustygrator_core::migrator::migrator::Migrator::connect(None, Some(conn(&dst2)), r).await.unwrap();
    assert_eq!(m.run().await.unwrap().rows, 36);
    assert_same_data(&read_all(&src).await, &read_all(&dst2).await, SQLITE_CONTAINERS);
    // receive removes the handed-over files after success
    assert!(!data.exists());
}

/// LIVE-9 include / exclude lists also apply to one-off migrations.
#[tokio::test(flavor = "multi_thread")]
async fn live9_filters_containers() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    let mut s = settings(dir.path());
    s.filter.exclude = vec!["orders".into()];
    let summary = migrate(&src, &dst, &s).await;
    assert_eq!(summary.containers, 2);
    let target = read_all(&dst).await;
    assert!(!target.contains_key("orders"));
    assert_same_data(&read_all(&src).await, &target, &["user_types", "users"]);
}

/// LIVE-3, LIVE-4..8, LIVE-13, LIVE-6: snapshot, stream inserts / updates / deletes / key
/// changes, stop gracefully, resume from the checkpoint without a new snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn live3_sqlite_change_capture_and_resume() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    let s = settings(dir.path());

    let run = start_live(&src, &dst, &s).await;
    sqlite_exec(
        &src,
        "INSERT INTO user_types VALUES (3, 'Guest');
         INSERT INTO users (id, username, email, user_type_id, avatar) VALUES (5, 'guest', 'g@example.com', 3, x'0102');
         UPDATE users SET username = 'renamed', balance = '1.25' WHERE id = 1;
         UPDATE users SET username = 'twice' WHERE id = 1;
         DELETE FROM orders WHERE id = 30;
         UPDATE orders SET id = 129 WHERE id = 29;",
    );
    eventually("changes applied", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "users") == 5 && sqlite_count(&dst, "orders") == 29 && sqlite_count(&dst, "user_types") == 3 }
    })
    .await;
    // wait for the key change too, then compare everything
    eventually("pk change applied", || {
        let dst = dst.clone();
        async move {
            let p = dst.strip_prefix("sqlite://").unwrap().to_string();
            rusqlite::Connection::open(p).unwrap().query_row("SELECT count(*) FROM orders WHERE id = 129", [], |r| r.get::<_, i64>(0)).unwrap() == 1
        }
    })
    .await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SQLITE_CONTAINERS);
    let summary = run.finish().await;
    let live = summary.live.unwrap();
    assert!(live.events >= 6, "{live:?}");
    assert_eq!(live.errors, 0);

    // changes while rustygrator is stopped are picked up after restart; a row removed only in
    // the target stays removed, proving the snapshot was not repeated
    sqlite_exec(&dst, "DELETE FROM orders WHERE id = 1;");
    sqlite_exec(&src, "INSERT INTO users (id, username, email, user_type_id) VALUES (6, 'later', 'l@example.com', 2);");
    let run = start_live(&src, &dst, &s).await;
    eventually("resumed", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "users") == 6 }
    })
    .await;
    run.finish().await;
    assert_eq!(sqlite_count(&dst, "orders"), 28);
    // acknowledged changelog entries are cleaned up
    assert_eq!(sqlite_count(&src, "_data_mover_changelog"), 0);
}

/// VIEW-5: filter and sort run in the database; VIEW-6: free SQL with expression columns.
#[tokio::test(flavor = "multi_thread")]
async fn view5_view6_sqlite_table_filter_sort_and_query() {
    use rustygrator_core::{browse::{Query, Sort}, migrator::migrator_steps::DataProvider, models::migration_execution_settings::ContainerFilter};
    let dir = tempfile::tempdir().unwrap();
    let (src, _) = seeded(dir.path());
    let mut db = open(&src, &settings(dir.path())).await;
    let defs = db.get_definitions(&ContainerFilter::default()).await.unwrap();
    let orders = defs.iter().find(|d| d.container_name == "orders").unwrap().clone();

    let all = browse(&mut db, &Query::Table { def: orders.clone(), filter: None, sort: None }).await;
    assert_eq!(all.len(), 30);
    let q = Query::Table { def: orders, filter: Some("user_id = 2".into()), sort: Some(Sort { column: "id".into(), descending: true }) };
    let rows = browse(&mut db, &q).await;
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, ["29", "25", "21", "17", "13", "9", "5", "1"]);

    let q = Query::Text("SELECT u.username, count(*) AS n FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.id ORDER BY u.id;".into());
    let def = rustygrator_core::browse::QueryRunner::describe(&mut db, &q).await.unwrap();
    assert_eq!(def.properties_info.iter().map(|p| p.property_name.as_str()).collect::<Vec<_>>(), ["username", "n"]);
    let rows = browse(&mut db, &q).await;
    assert_eq!(rows[0], ["admin_user1", "7"]);
    assert_eq!(rows.len(), 4);
}

/// VIEW-9: the query editor cannot change data.
#[tokio::test(flavor = "multi_thread")]
async fn view9_sqlite_rejects_writes() {
    use rustygrator_core::browse::{Query, QueryRunner};
    let dir = tempfile::tempdir().unwrap();
    let (src, _) = seeded(dir.path());
    let mut db = open(&src, &settings(dir.path())).await;
    for q in ["DELETE FROM orders", "UPDATE users SET email = 'x'", "DROP TABLE orders"] {
        assert!(db.describe(&Query::Text(q.into())).await.is_err(), "{q}");
    }
    assert_eq!(sqlite_count(&src, "orders"), 30);
}

/// VIEW-2: the cursor only moves when the reader takes the next batch; VIEW-6: cancel stops a
/// running query.
#[tokio::test(flavor = "multi_thread")]
async fn view2_view6_sqlite_fetch_waits_for_reader_and_cancels() {
    use rustygrator_core::{browse::{Query, QueryRunner}, migrator::batching::BatchSizer};
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "big.db");
    sqlite_exec(&src, "CREATE TABLE t (id INTEGER PRIMARY KEY); WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 100000) INSERT INTO t SELECT n FROM g;");
    let mut db = open(&src, &settings(dir.path())).await;
    let q = Query::Text("SELECT id FROM t".into());
    let def = db.describe(&q).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let fetch = tokio::spawn(async move { db.fetch(&q, &def, std::sync::Arc::new(BatchSizer::fixed(100)), tx).await });
    let first = rx.recv().await.unwrap();
    assert_eq!(first.rows, 100);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!fetch.is_finished(), "the reader took one batch, the query must still be waiting");
    drop(rx);
    assert!(fetch.await.unwrap().is_err(), "dropping the reader stops the query");

    // a query that would run for a long time without returning rows is interrupted
    let mut db = open(&src, &settings(dir.path())).await;
    let q = Query::Text("WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g) SELECT count(*) FROM g".into());
    let def = db.describe(&q).await.unwrap();
    let canceller = db.canceller();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let fetch = tokio::spawn(async move { db.fetch(&q, &def, std::sync::Arc::new(BatchSizer::fixed(100)), tx).await });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    canceller.cancel().await;
    let r = tokio::time::timeout(std::time::Duration::from_secs(10), fetch).await.expect("cancel stops the query").unwrap();
    assert!(r.unwrap_err().to_string().contains("interrupt"));
}

/// MAPP-2: migration order and per-table progress can be read from the migrator.
#[tokio::test(flavor = "multi_thread")]
async fn mapp2_progress_per_table() {
    use rustygrator_core::migrator::migrator::Migrator;
    let dir = tempfile::tempdir().unwrap();
    let (src, dst) = seeded(dir.path());
    let mut m = Migrator::connect(Some(conn(&src)), Some(conn(&dst)), settings(dir.path())).await.unwrap();
    let stats = m.stats();
    m.run().await.unwrap();
    assert_eq!(*stats.order.lock().unwrap(), ["user_types", "users", "orders"]);
    let tables = stats.tables.lock().unwrap();
    assert_eq!(tables["orders"].rows, 30);
    assert!(tables.values().all(|t| t.done && t.phase == "transfer" && t.bytes > 0));
}
