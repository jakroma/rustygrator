//! ScyllaDB tests (DB-10..DB-12, SCY-1..SCY-10, MIG-7..MIG-13, SN-5..SN-8, LIVE-15).
//! Need `DM_TEST_SCYLLA_URL` (and PostgreSQL / MongoDB for the cross-database ones).

mod common;

use common::*;
use rustygrator_core::models::migration_execution_settings::SqlToNoSqlStrategy;
use scylla::value::CqlValue;

fn udt_field<'a>(v: &'a CqlValue, name: &str) -> Option<&'a CqlValue> {
    match v {
        CqlValue::UserDefinedType { fields, .. } => fields.iter().find(|(n, _)| n == name).and_then(|(_, v)| v.as_ref()),
        _ => None,
    }
}

/// MIG-7 + SN-5: PostgreSQL -> ScyllaDB denormalized (frozen UDT, nested), SCY-9 indexes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn mig7_sn5_postgres_to_scylla_denormalized() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_mig7_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = fresh_scylla("dm_mig7").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, PG_CONTAINERS);

    let users = scylla_rows("SELECT user_type FROM dm_mig7.users WHERE id = 1").await;
    let ut = users[0].columns[0].as_ref().unwrap();
    assert_eq!(udt_field(ut, "type_name"), Some(&CqlValue::Text("Admin".into())));
    let orders = scylla_rows("SELECT \"user\" FROM dm_mig7.orders WHERE id = 2").await;
    let user = orders[0].columns[0].as_ref().unwrap();
    // seed: order g belongs to user 1 + g % 4, so order 2 to user 3 (standard_user1, Standard)
    assert_eq!(udt_field(user, "username"), Some(&CqlValue::Text("standard_user1".into())));
    assert_eq!(udt_field(udt_field(user, "user_type").unwrap(), "type_name"), Some(&CqlValue::Text("Standard".into())));
    let idx = scylla_rows("SELECT index_name FROM system_schema.indexes WHERE keyspace_name = 'dm_mig7' AND table_name = 'users'").await;
    assert!(idx.len() >= 2, "email index and FK index expected, got {idx:?}");
    // idempotent re-run
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_eq!(scylla_rows("SELECT count(*) FROM dm_mig7.orders").await[0].columns[0], Some(CqlValue::BigInt(40)));
}

/// SN-6 + SCY-8: normalized copy, key override.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn sn6_scy8_postgres_to_scylla_normalized_with_key_override() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_sn6_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = fresh_scylla("dm_sn6").await;
    let mut s = settings(dir.path());
    strategy(&mut s, SqlToNoSqlStrategy::Normalized);
    s.scylla.key_overrides.insert("orders".into(), (vec!["user_id".into()], vec!["id".into()]));
    migrate(&src, &dst, &s).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, PG_CONTAINERS);
    let cols = scylla_rows("SELECT column_name, kind FROM system_schema.columns WHERE keyspace_name = 'dm_sn6' AND table_name = 'orders'").await;
    let kind = |c: &str| cols.iter().find(|r| r.columns[0] == Some(CqlValue::Text(c.into()))).map(|r| r.columns[1].clone());
    assert_eq!(kind("user_id"), Some(Some(CqlValue::Text("partition_key".into()))));
    assert_eq!(kind("id"), Some(Some(CqlValue::Text("clustering".into()))));
    assert!(kind("user").is_none(), "normalized: nothing embedded");
}

/// MIG-8 (DB-10, SCY-2..SCY-4): ScyllaDB -> PostgreSQL with collections, UDTs, composite keys.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn mig8_scylla_to_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let src = seed_scylla("dm_mig8").await;
    let dst = fresh_pg("dm_mig8_dst").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SCYLLA_CONTAINERS);
    let r = pg_query_strings(&dst, "SELECT addr->>'city', tags::text, big::text, amount::text FROM events WHERE seq = 1 AND device = 'd1'").await;
    assert_eq!(r[0], vec![Some("Gdańsk".into()), Some(r#"["x", "y"]"#.into()), Some("123456789012345678901234567890".into()), Some("12345.678900".into())]);
    assert_eq!(pg_count(&dst, "events").await, 303);
}

/// MIG-9 + SN-7 / SN-8 and MIG-10: SQLite <-> ScyllaDB.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn mig9_mig10_sqlite_and_scylla() {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "s.db");
    sqlite_exec(&src, SQLITE_SEED);
    let denorm = fresh_scylla("dm_sn7").await;
    migrate(&src, &denorm, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&denorm).await, SQLITE_CONTAINERS);
    let o = scylla_rows("SELECT \"user\" FROM dm_sn7.orders WHERE id = 1").await;
    assert_eq!(udt_field(o[0].columns[0].as_ref().unwrap(), "username"), Some(&CqlValue::Text("admin_user2".into())));
    let norm = fresh_scylla("dm_sn8").await;
    let mut s = settings(dir.path());
    strategy(&mut s, SqlToNoSqlStrategy::Normalized);
    migrate(&src, &norm, &s).await;
    assert_same_data(&read_all(&src).await, &read_all(&norm).await, SQLITE_CONTAINERS);

    let scy = seed_scylla("dm_mig10").await;
    let back = sqlite_url(dir.path(), "back.db");
    migrate(&scy, &back, &settings(dir.path())).await;
    assert_same_data(&read_all(&scy).await, &read_all(&back).await, SCYLLA_CONTAINERS);
}

/// MIG-11 (SCY-10) and MIG-12: MongoDB <-> ScyllaDB; stable nested shapes become list / UDT.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MongoDB + ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn mig11_mig12_mongo_and_scylla() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_mig11").await;
    seed_mongo(&src).await;
    let dst = fresh_scylla("dm_mig11").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, MONGO_CONTAINERS);
    let cols = scylla_rows("SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = 'dm_mig11' AND table_name = 'users'").await;
    let ty = |c: &str| cols.iter().find(|r| r.columns[0] == Some(CqlValue::Text(c.into()))).and_then(|r| r.columns[1].clone());
    assert_eq!(ty("tags"), Some(CqlValue::Text("list<text>".into())));
    assert_eq!(ty("profile"), Some(CqlValue::Text("frozen<users_profile>".into())));

    let scy = seed_scylla("dm_mig12").await;
    let mongo = fresh_mongo("dm_mig12").await;
    migrate(&scy, &mongo, &settings(dir.path())).await;
    // composite key -> _id sub-document; values compared through the MongoDB provider
    let d = mongo_db(&mongo).await.collection::<mongodb::bson::Document>("events").count_documents(None, None).await.unwrap();
    assert_eq!(d, 303);
    let dev = mongo_db(&mongo).await.collection::<mongodb::bson::Document>("devices").find_one(mongodb::bson::doc! {"name": "sensor 'a'"}, None).await.unwrap().unwrap();
    assert_eq!(dev.get_i32("age").unwrap(), 3);
}

/// MIG-13: ScyllaDB -> ScyllaDB keeps collections and UDTs.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn mig13_scylla_to_scylla() {
    let dir = tempfile::tempdir().unwrap();
    let src = seed_scylla("dm_mig13_src").await;
    let dst = fresh_scylla("dm_mig13_dst").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, SCYLLA_CONTAINERS);
    let cols = scylla_rows("SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = 'dm_mig13_dst' AND table_name = 'events'").await;
    let ty = |c: &str| cols.iter().find(|r| r.columns[0] == Some(CqlValue::Text(c.into()))).and_then(|r| r.columns[1].clone());
    assert_eq!(ty("readings"), Some(CqlValue::Text("list<int>".into())));
    assert_eq!(ty("addr"), Some(CqlValue::Text("frozen<events_addr>".into())));
}

/// LIVE-15: ScyllaDB CDC -> PostgreSQL, with resume.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + ScyllaDB, run via e2e/docker-compose.test.yml"]
async fn live15_scylla_cdc_to_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let src = seed_scylla("dm_live15").await;
    let dst = fresh_pg("dm_live15_dst").await;
    let s = settings(dir.path());
    let run = start_live(&src, &dst, &s).await;
    let session = scylla_session().await;
    cql(
        &session,
        "INSERT INTO dm_live15.devices (id, name, age) VALUES (33333333-3333-3333-3333-333333333333, 'new', 7);
         UPDATE dm_live15.devices SET name = 'renamed', age = null WHERE id = 11111111-1111-1111-1111-111111111111;
         DELETE FROM dm_live15.devices WHERE id = 22222222-2222-2222-2222-222222222222;
         DELETE FROM dm_live15.events WHERE device = 'd1' AND ts = '2024-01-02 03:04:06+0000' AND seq = 2",
    )
    .await;
    eventually("CDC deltas applied", || {
        let dst = dst.clone();
        async move {
            let names = pg_query_strings(&dst, "SELECT name, age::text FROM devices ORDER BY name").await;
            names == vec![vec![Some("new".to_string()), Some("7".to_string())], vec![Some("renamed".to_string()), None]] && pg_count(&dst, "events").await == 302
        }
    })
    .await;
    run.finish().await;
    cql(&session, "INSERT INTO dm_live15.devices (id, name) VALUES (44444444-4444-4444-4444-444444444444, 'after restart')").await;
    let run = start_live(&src, &dst, &s).await;
    eventually("resumed from the CDC watermark", || {
        let dst = dst.clone();
        async move { pg_count(&dst, "devices").await == 3 }
    })
    .await;
    run.finish().await;
}

/// VIEW-5 / VIEW-6 / VIEW-9 on ScyllaDB: filter on the partition key with sort, CQL SELECT
/// queries, other statements rejected.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs ScyllaDB (DM_TEST_SCYLLA_URL), run via e2e/docker-compose.test.yml"]
async fn view6_scylla_browse_and_query() {
    use rustygrator_core::browse::{Query, QueryRunner, Sort};
    let dir = tempfile::tempdir().unwrap();
    let src = seed_scylla("dm_view6").await;
    let mut db = open(&src, &settings(dir.path())).await;
    let events = definition(&mut db, "events").await;
    let seq = events.column_index("seq").unwrap();
    let q = Query::Table { def: events, filter: Some("device = 'd1'".into()), sort: Some(Sort { column: "ts".into(), descending: true }) };
    let seqs: Vec<String> = browse(&mut db, &q).await.into_iter().map(|r| r[seq].clone()).collect();
    assert_eq!(seqs, ["2", "1"]);

    let rows = browse(&mut db, &Query::Text("SELECT name, age FROM devices WHERE id = 11111111-1111-1111-1111-111111111111;".into())).await;
    assert_eq!(rows, [["sensor 'a'", "3"]]);
    assert!(db.describe(&Query::Text("TRUNCATE devices".into())).await.is_err());
}
