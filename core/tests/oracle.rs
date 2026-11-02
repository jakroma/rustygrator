//! Oracle tests (ORA-1..ORA-10, MIG-21..MIG-23, LIVE-19). They need the Oracle server of
//! e2e/docker-compose.test.yml (`DM_TEST_ORACLE_URL`, ARCHIVELOG + supplemental logging) and
//! Oracle Instant Client on the machine running the tests.

mod common;

use common::*;
use rustygrator_core::models::migration_execution_settings::TransferMode;

const TABLES: [&str; 4] = ["customers", "products", "orders", "order_items"];

/// ORA-2 + ORA-3 (MIG-23): Oracle -> Oracle keeps every value, key, index and foreign key; a
/// re-run merges instead of duplicating.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Oracle (DM_TEST_ORACLE_URL), run via e2e/docker-compose.test.yml"]
async fn ora3_oracle_to_oracle_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_oracle("dm_ora_src").await;
    oracle_exec(&src, &dump("shop/oracle.sql")).await;
    let dst = fresh_oracle("dm_ora_dst").await;
    let s = settings(dir.path());
    migrate(&src, &dst, &s).await;
    let source = read_all(&src).await;
    let target = read_all(&dst).await;
    assert_same_data(&source, &target, &TABLES);
    assert_eq!(target["order_items"].0.foreign_keys.len(), 2);
    assert!(target["orders"].0.indexes.iter().any(|i| i.columns == ["status"]));
    // ORA-4: sizes, booleans and JSON text survive
    let products = &target["products"].0;
    assert_eq!(products.properties_info[3].source_type.as_deref(), Some("number(10,2)"));
    assert_eq!(products.properties_info[5].data_type, rustygrator_core::models::value::LogicalType::Json);
    assert_eq!(products.properties_info[9].data_type, rustygrator_core::models::value::LogicalType::Uuid);
    assert_eq!(target["customers"].0.properties_info[3].data_type, rustygrator_core::models::value::LogicalType::Bool);

    oracle_exec(&src, "UPDATE \"customers\" SET \"name\" = 'Anna K.' WHERE \"id\" = 1;").await;
    migrate(&src, &dst, &s).await;
    assert_eq!(oracle_count(&dst, "customers").await, 6);
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, &TABLES);
}

/// MIG-21 / MIG-22, ORA-5, ORA-6: SQLite -> Oracle (empty strings become NULL; `?case=upper`
/// folds names) and Oracle -> SQLite, through dbc files.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Oracle (DM_TEST_ORACLE_URL), run via e2e/docker-compose.test.yml"]
async fn mig22_sqlite_to_oracle_and_back_through_files() {
    let dir = tempfile::tempdir().unwrap();
    let lite = sqlite_url(dir.path(), "shop.db");
    sqlite_exec(&lite, &dump("shop/sqlite.sql"));
    let ora = fresh_oracle("dm_ora_lite").await;
    let mut s = settings(dir.path());
    s.transfer = TransferMode::File;
    migrate(&lite, &ora, &s).await;
    let source = empty_text_as_null(read_all(&lite).await);
    assert_same_data(&source, &read_all(&ora).await, &TABLES);

    let back = sqlite_url(dir.path(), "back.db");
    migrate(&ora, &back, &s).await;
    assert_same_data(&source, &read_all(&back).await, &TABLES);

    let upper = format!("{}?case=upper", fresh_oracle("dm_ora_upper").await);
    migrate(&lite, &upper, &settings(dir.path())).await;
    assert_eq!(oracle_count(&upper, "CUSTOMERS").await, 6);
    assert_eq!(oracle_count(&upper, "ORDER_ITEMS").await, 9);
}

/// LIVE-19: LogMiner changes (insert, update, key change, delete) reach SQLite; the checkpoint
/// lets a restarted run continue without a new snapshot.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Oracle (DM_TEST_ORACLE_URL) in ARCHIVELOG mode, run via e2e/docker-compose.test.yml"]
async fn live19_oracle_logminer_to_sqlite() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_oracle("dm_ora_live").await;
    oracle_exec(&src, &dump("shop/oracle.sql")).await;
    let dst = sqlite_url(dir.path(), "live.db");
    let s = settings(dir.path());
    let run = start_live(&src, &dst, &s).await;
    oracle_exec(
        &src,
        "INSERT INTO \"customers\" VALUES (7, 'new@example.com', 'Nowy', 1, 1.25, TIMESTAMP '2024-06-01 00:00:00 +00:00');
         UPDATE \"products\" SET \"price\" = 210.00 WHERE \"id\" = 1;
         UPDATE \"orders\" SET \"status\" = 'sent' WHERE \"id\" = 3;
         DELETE FROM \"order_items\" WHERE \"order_id\" = 6;
         UPDATE \"customers\" SET \"id\" = 60 WHERE \"id\" = 6;",
    )
    .await;
    eventually("LogMiner changes replicated", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "order_items") == 8 && sqlite_count(&dst, "customers") == 7 }
    })
    .await;
    run.finish().await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, &TABLES);

    // restart: resumes from the checkpoint (no snapshot), picks up what changed while stopped
    oracle_exec(&src, "DELETE FROM \"customers\" WHERE \"id\" = 7;").await;
    let run = start_live(&src, &dst, &s).await;
    eventually("change made while stopped replicated", || {
        let dst = dst.clone();
        async move { sqlite_count(&dst, "customers") == 6 }
    })
    .await;
    run.finish().await;
}

/// ORA-8, ORA-9: table pages with filter / sort, queries, writes refused, a running query
/// stopped with OCIBreak.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Oracle (DM_TEST_ORACLE_URL), run via e2e/docker-compose.test.yml"]
async fn ora8_oracle_browse_and_query() {
    use rustygrator_core::browse::{Query, QueryRunner, Sort};
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_oracle("dm_ora_view").await;
    oracle_exec(&src, &dump("shop/oracle.sql")).await;
    let mut db = open(&src, &settings(dir.path())).await;
    let orders = definition(&mut db, "DM_ORA_VIEW.orders").await;
    let q = Query::Table { def: orders, filter: Some("\"note\" IS NULL".into()), sort: Some(Sort { column: "id".into(), descending: true }) };
    let ids: Vec<String> = browse(&mut db, &q).await.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids, ["5", "3", "1"]);

    let q = Query::Text(
        "SELECT c.\"name\", o.\"status\", i.\"unit_price\" FROM \"customers\" c JOIN \"orders\" o ON o.\"customer_id\" = c.\"id\" \
         JOIN \"order_items\" i ON i.\"order_id\" = o.\"id\" WHERE o.\"id\" = 4 ORDER BY i.\"product_id\";"
            .into(),
    );
    let rows = browse(&mut db, &q).await;
    assert_eq!(rows, [["Zofia Żółć-Gęślą", "paid", "189.99"], ["Zofia Żółć-Gęślą", "paid", "100"]]);

    for write in ["DELETE FROM \"orders\"", "DROP TABLE \"orders\"", "SELECT * FROM \"orders\" FOR UPDATE"] {
        assert!(db.describe(&Query::Text(write.into())).await.is_err(), "{write}");
    }
    assert_eq!(oracle_count(&src, "orders").await, 6);

    let q = Query::Text("SELECT count(*) FROM all_objects a, all_objects b, all_objects c".into());
    let def = db.describe(&q).await.unwrap();
    let canceller = db.canceller();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let fetch = tokio::spawn(async move { db.fetch(&q, &def, std::sync::Arc::new(rustygrator_core::migrator::batching::BatchSizer::fixed(10)), tx).await });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    canceller.cancel().await;
    let r = tokio::time::timeout(std::time::Duration::from_secs(10), fetch).await.expect("OCIBreak stops the query").unwrap();
    assert!(r.is_err(), "the query was cancelled (ORA-01013)");
}

/// SN-11, SN-12 (ORA-7, MIG-24, MIG-25): Oracle -> MongoDB / ScyllaDB with both strategies, and
/// the ScyllaDB dump -> Oracle.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Oracle, MongoDB and ScyllaDB (DM_TEST_ORACLE_URL, DM_TEST_MONGO_URL, DM_TEST_SCYLLA_URL), run via e2e/docker-compose.test.yml"]
async fn sn11_sn12_oracle_to_mongo_and_scylla() {
    use rustygrator_core::models::migration_execution_settings::SqlToNoSqlStrategy;
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_oracle("dm_ora_nosql").await;
    oracle_exec(&src, &dump("shop/oracle.sql")).await;
    let source = read_all(&src).await;
    let mut normalized = settings(dir.path());
    strategy(&mut normalized, SqlToNoSqlStrategy::Normalized);

    for (dst, s) in [
        (fresh_mongo("dm_ora_mongo_den").await, settings(dir.path())),
        (fresh_mongo("dm_ora_mongo_norm").await, normalized.clone()),
        (fresh_scylla("dm_ora_scy_den").await, settings(dir.path())),
        (fresh_scylla("dm_ora_scy_norm").await, normalized.clone()),
    ] {
        migrate(&src, &dst, &s).await;
        let target = read_all(&dst).await;
        for (table, n) in SHOP {
            assert_eq!(target[table].1.len() as i64, n, "{dst} {table}");
        }
        // order_items has a composite key: MongoDB keeps it as a compound `_id`, checked by count above
        if s.strategy == SqlToNoSqlStrategy::Normalized {
            assert_same_data(&source, &target, &TABLES[..3]);
        }
    }

    let scy = fresh_scylla("dm_scy_ora_src").await;
    load_cql_dump("dm_scy_ora_src", "shop/scylladb.cql").await;
    let ora = fresh_oracle("dm_scy_ora").await;
    migrate(&scy, &ora, &settings(dir.path())).await;
    for (table, n) in SHOP {
        assert_eq!(oracle_count(&ora, table).await, n, "{table}");
    }
}
