//! DUMP-1..DUMP-3: the shop data set of `dumps/shop/` loaded into every database and migrated.
//! SQLite runs everywhere; the others need the databases of e2e/docker-compose.test.yml.

mod common;

use common::*;

const TABLES: [&str; 4] = ["customers", "products", "orders", "order_items"];

fn assert_shop_counts(counts: &[(&str, i64)]) {
    for ((table, want), (t, got)) in SHOP.iter().zip(counts) {
        assert_eq!((table, want), (t, got));
    }
}

/// DUMP-2: the SQLite dump loads and survives SQLite -> SQLite unchanged (types, unicode, nulls,
/// blobs, composite keys, foreign key order).
#[tokio::test(flavor = "multi_thread")]
async fn dump2_sqlite_shop() {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "shop.db");
    let dst = sqlite_url(dir.path(), "copy.db");
    sqlite_exec(&src, &dump("shop/sqlite.sql"));
    let summary = migrate(&src, &dst, &settings(dir.path())).await;
    assert_eq!(summary.rows, SHOP.iter().map(|(_, n)| *n as u64).sum::<u64>());
    let counts: Vec<(&str, i64)> = TABLES.iter().map(|t| (*t, sqlite_count(&dst, t))).collect();
    assert_shop_counts(&counts);
    let (source, target) = (read_all(&src).await, read_all(&dst).await);
    assert_same_data(&source, &target, &TABLES);
    assert_eq!(target["order_items"].0.identifier_names(), ["order_id", "product_id"]);
}

/// DUMP-2: PostgreSQL dump -> SQLite and back into a second PostgreSQL database.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (DM_TEST_PG_URL), run via e2e/docker-compose.test.yml"]
async fn dump2_postgres_shop() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_dump_pg").await;
    pg_exec(&src, &dump("shop/postgresql.sql")).await;
    let lite = sqlite_url(dir.path(), "shop.db");
    let back = fresh_pg("dm_dump_pg_back").await;
    migrate(&src, &lite, &settings(dir.path())).await;
    migrate(&lite, &back, &settings(dir.path())).await;
    let source = read_all(&src).await;
    assert_same_data(&source, &read_all(&lite).await, &TABLES);
    assert_same_data(&source, &read_all(&back).await, &TABLES);
}

/// DUMP-2 + MIG-14..MIG-17: MySQL dump -> PostgreSQL, SQLite, MongoDB, and the PostgreSQL dump -> MySQL.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MySQL, PostgreSQL and MongoDB (DM_TEST_MYSQL_URL, DM_TEST_PG_URL, DM_TEST_MONGO_URL), run via e2e/docker-compose.test.yml"]
async fn dump2_mysql_shop() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mysql("dm_dump_my").await;
    mysql_exec(&src, &dump("shop/mysql.sql")).await;
    let source = read_all(&src).await;

    let pg = fresh_pg("dm_dump_my_pg").await;
    migrate(&src, &pg, &settings(dir.path())).await;
    assert_same_data(&source, &read_all(&pg).await, &TABLES);

    let lite = sqlite_url(dir.path(), "shop.db");
    migrate(&src, &lite, &settings(dir.path())).await;
    assert_same_data(&source, &read_all(&lite).await, &TABLES);

    let mongo = fresh_mongo("dm_dump_my_mongo").await;
    migrate(&src, &mongo, &settings(dir.path())).await;
    let db = mongo_db(&mongo).await;
    for (table, n) in SHOP {
        assert_eq!(db.collection::<mongodb::bson::Document>(table).count_documents(None, None).await.unwrap(), n as u64, "{table}");
    }

    let pg_src = fresh_pg("dm_dump_pg_src").await;
    pg_exec(&pg_src, &dump("shop/postgresql.sql")).await;
    let my = fresh_mysql("dm_dump_pg_my").await;
    migrate(&pg_src, &my, &settings(dir.path())).await;
    assert_same_data(&read_all(&pg_src).await, &read_all(&my).await, &TABLES);
}

/// DUMP-2: MongoDB dump (nested items, decimals, binary, uuid) -> SQLite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MongoDB (DM_TEST_MONGO_URL), run via e2e/docker-compose.test.yml"]
async fn dump2_mongo_shop() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_dump_mongo").await;
    load_mongo_dump(&src, "shop/mongodb.json").await;
    let lite = sqlite_url(dir.path(), "shop.db");
    migrate(&src, &lite, &settings(dir.path())).await;
    for (table, n) in &SHOP[..3] {
        assert_eq!(sqlite_count(&lite, table), *n, "{table}");
    }
    assert_same_data(&read_all(&src).await, &read_all(&lite).await, &["customers"]);
}

/// DUMP-2: ScyllaDB dump -> SQLite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs ScyllaDB (DM_TEST_SCYLLA_URL), run via e2e/docker-compose.test.yml"]
async fn dump2_scylla_shop() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_scylla("dm_dump_scylla").await;
    load_cql_dump("dm_dump_scylla", "shop/scylladb.cql").await;
    let lite = sqlite_url(dir.path(), "shop.db");
    migrate(&src, &lite, &settings(dir.path())).await;
    let counts: Vec<(&str, i64)> = TABLES.iter().map(|t| (*t, sqlite_count(&lite, t))).collect();
    assert_shop_counts(&counts);
    assert_same_data(&read_all(&src).await, &read_all(&lite).await, &TABLES);
}

/// DUMP-4 + MIG-21, MIG-22, MIG-24, MIG-25, SN-11: Oracle dump -> PostgreSQL, MySQL, MongoDB;
/// PostgreSQL, MySQL and MongoDB dumps -> Oracle.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Oracle, PostgreSQL, MySQL and MongoDB (DM_TEST_ORACLE_URL, DM_TEST_PG_URL, DM_TEST_MYSQL_URL, DM_TEST_MONGO_URL), run via e2e/docker-compose.test.yml"]
async fn dump4_oracle_shop() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_oracle("dm_dump_ora").await;
    oracle_exec(&src, &dump("shop/oracle.sql")).await;
    let source = read_all(&src).await;
    let counts: Vec<(&str, i64)> = futures_util::future::join_all(TABLES.iter().map(|t| async { (*t, oracle_count(&src, t).await) })).await;
    assert_shop_counts(&counts);

    let pg = fresh_pg("dm_dump_ora_pg").await;
    migrate(&src, &pg, &settings(dir.path())).await;
    assert_same_data(&source, &read_all(&pg).await, &TABLES);

    let my = fresh_mysql("dm_dump_ora_my").await;
    migrate(&src, &my, &settings(dir.path())).await;
    assert_same_data(&source, &read_all(&my).await, &TABLES);

    let mongo = fresh_mongo("dm_dump_ora_mongo").await;
    migrate(&src, &mongo, &settings(dir.path())).await;
    let db = mongo_db(&mongo).await;
    for (table, n) in SHOP {
        assert_eq!(db.collection::<mongodb::bson::Document>(table).count_documents(None, None).await.unwrap(), n as u64, "{table}");
    }

    let pg_src = fresh_pg("dm_dump_pg_ora_src").await;
    pg_exec(&pg_src, &dump("shop/postgresql.sql")).await;
    let ora = fresh_oracle("dm_dump_pg_ora").await;
    migrate(&pg_src, &ora, &settings(dir.path())).await;
    assert_same_data(&empty_text_as_null(read_all(&pg_src).await), &read_all(&ora).await, &TABLES);

    let my_src = fresh_mysql("dm_dump_my_ora_src").await;
    mysql_exec(&my_src, &dump("shop/mysql.sql")).await;
    let ora = fresh_oracle("dm_dump_my_ora").await;
    migrate(&my_src, &ora, &settings(dir.path())).await;
    assert_same_data(&empty_text_as_null(read_all(&my_src).await), &read_all(&ora).await, &TABLES);

    let mongo_src = fresh_mongo("dm_dump_mongo_ora_src").await;
    load_mongo_dump(&mongo_src, "shop/mongodb.json").await;
    let ora = fresh_oracle("dm_dump_mongo_ora").await;
    migrate(&mongo_src, &ora, &settings(dir.path())).await;
    for (table, n) in &SHOP[..3] {
        assert_eq!(oracle_count(&ora, table).await, *n, "{table}");
    }
}
