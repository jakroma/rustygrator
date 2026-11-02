//! MySQL tests (MY-1..MY-8, LIVE-16). They need the MySQL server of
//! e2e/docker-compose.test.yml (`DM_TEST_MYSQL_URL`, binlog in ROW format).

mod common;

use common::*;

const TABLES: [&str; 4] = ["customers", "products", "orders", "order_items"];

/// MY-2 + MY-3 (MIG-18): MySQL -> MySQL keeps every value, key, index and foreign key; a re-run
/// merges instead of duplicating.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MySQL (DM_TEST_MYSQL_URL), run via e2e/docker-compose.test.yml"]
async fn my3_mysql_to_mysql_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mysql("dm_my_src").await;
    mysql_exec(&src, &dump("shop/mysql.sql")).await;
    let dst = fresh_mysql("dm_my_dst").await;
    let s = settings(dir.path());
    migrate(&src, &dst, &s).await;
    let source = read_all(&src).await;
    let target = read_all(&dst).await;
    assert_same_data(&source, &target, &TABLES);
    assert_eq!(target["order_items"].0.foreign_keys.len(), 2);
    assert!(target["orders"].0.indexes.iter().any(|i| i.columns == ["status"]));
    // DECIMAL(10,2) keeps its scale, enum values arrive as text
    assert_eq!(target["products"].0.properties_info[3].source_type.as_deref(), Some("decimal(10,2)"));

    mysql_exec(&src, "UPDATE customers SET name = 'Anna K.' WHERE id = 1").await;
    migrate(&src, &dst, &s).await;
    assert_eq!(mysql_count(&dst, "customers").await, 6);
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, &TABLES);
}

/// MY-3 (MIG-15): the receiver creates a database that does not exist yet; file mode (dbc) too.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MySQL (DM_TEST_MYSQL_URL), run via e2e/docker-compose.test.yml"]
async fn my3_sqlite_to_new_mysql_database_through_files() {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "shop.db");
    sqlite_exec(&src, &dump("shop/sqlite.sql"));
    let dst = fresh_mysql("dm_my_created").await;
    mysql_exec(&dst, "DROP DATABASE dm_my_created").await;
    let mut s = settings(dir.path());
    s.transfer = rustygrator_core::models::migration_execution_settings::TransferMode::File;
    migrate(&src, &dst, &s).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, &TABLES);
}

/// LIVE-16: binlog changes (insert, update, key change, delete) reach SQLite; the checkpoint
/// lets a restarted run continue without a new snapshot.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MySQL (DM_TEST_MYSQL_URL), run via e2e/docker-compose.test.yml"]
async fn live16_mysql_binlog_to_sqlite() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mysql("dm_live16").await;
    mysql_exec(&src, &dump("shop/mysql.sql")).await;
    let dst = sqlite_url(dir.path(), "live.db");
    let s = settings(dir.path());
    let run = start_live(&src, &dst, &s).await;
    mysql_exec(
        &src,
        "INSERT INTO customers VALUES (7, 'new@example.com', 'Nowy', TRUE, 1.25, '2024-06-01 00:00:00');
         UPDATE products SET price = 210.00, tags = '[\"office\", \"usb\", \"sale\"]' WHERE id = 1;
         UPDATE orders SET status = 'sent' WHERE id = 3;
         DELETE FROM order_items WHERE order_id = 6;
         UPDATE customers SET id = 60 WHERE id = 6;",
    )
    .await;
    eventually("binlog changes replicated", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "order_items") == 8 && sqlite_count(&dst, "customers") == 7 }
    })
    .await;
    run.finish().await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, &TABLES);

    // restart: resumes from the checkpoint (no snapshot), picks up what changed while stopped
    mysql_exec(&src, "DELETE FROM customers WHERE id = 7").await;
    let run = start_live(&src, &dst, &s).await;
    eventually("change made while stopped replicated", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "customers") == 6 }
    })
    .await;
    run.finish().await;
}

/// VIEW-5, VIEW-6, VIEW-9 for MySQL: table pages with filter / sort, queries, writes refused,
/// a running query cancelled with KILL QUERY.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MySQL (DM_TEST_MYSQL_URL), run via e2e/docker-compose.test.yml"]
async fn view6_mysql_browse_and_query() {
    use rustygrator_core::browse::{Query, QueryRunner, Sort};
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mysql("dm_my_view6").await;
    mysql_exec(&src, &dump("shop/mysql.sql")).await;
    let mut db = open(&src, &settings(dir.path())).await;
    let orders = definition(&mut db, "dm_my_view6.orders").await;
    let q = Query::Table { def: orders, filter: Some("note IS NULL".into()), sort: Some(Sort { column: "id".into(), descending: true }) };
    let ids: Vec<String> = browse(&mut db, &q).await.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids, ["5", "3", "1"]);

    let q = Query::Text("SELECT c.name, o.status, i.unit_price FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items i ON i.order_id = o.id WHERE o.id = 4 ORDER BY i.product_id".into());
    let rows = browse(&mut db, &q).await;
    assert_eq!(rows, [["Zofia Żółć-Gęślą", "paid", "189.99"], ["Zofia Żółć-Gęślą", "paid", "100.00"]]);

    for write in ["DELETE FROM orders", "DROP TABLE orders", "SELECT * FROM orders INTO OUTFILE '/tmp/x'"] {
        assert!(db.describe(&Query::Text(write.into())).await.is_err(), "{write}");
    }
    assert_eq!(mysql_count(&src, "orders").await, 6);

    let q = Query::Text("SELECT SLEEP(60)".into());
    let def = db.describe(&q).await.unwrap();
    let canceller = db.canceller();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let fetch = tokio::spawn(async move { db.fetch(&q, &def, std::sync::Arc::new(rustygrator_core::migrator::batching::BatchSizer::fixed(10)), tx).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    canceller.cancel().await;
    // SLEEP() returns 1 when interrupted instead of failing; either way it ends at once
    tokio::time::timeout(std::time::Duration::from_secs(10), fetch).await.expect("KILL QUERY stops the query").unwrap().ok();
}
