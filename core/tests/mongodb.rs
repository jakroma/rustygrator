//! MongoDB tests (DB-4, DB-5, DB-6, MIG-1, MIG-2, MIG-5, MIG-6, SN-1..SN-4, LIVE-1, LIVE-11).
//! Need `DM_TEST_MONGO_URL` (a replica set, for change streams) and most also `DM_TEST_PG_URL`.

mod common;

use common::*;
use rustygrator_core::models::migration_execution_settings::SqlToNoSqlStrategy;
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};

async fn find_one(url: &str, coll: &str, filter: Document) -> Document {
    mongo_db(url).await.collection::<Document>(coll).find_one(filter, None).await.unwrap().unwrap_or_else(|| panic!("no document in {coll}"))
}

async fn count(url: &str, coll: &str) -> u64 {
    mongo_db(url).await.collection::<Document>(coll).count_documents(None, None).await.unwrap()
}

async fn index_keys(url: &str, coll: &str) -> Vec<Document> {
    let c = mongo_db(url).await.collection::<Document>(coll);
    c.list_indexes(None).await.unwrap().try_collect::<Vec<_>>().await.unwrap().into_iter().map(|m| m.keys).collect()
}

/// MIG-1 + SN-1: PostgreSQL -> MongoDB, denormalized (nested embedding over two levels).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + MongoDB, run via e2e/docker-compose.test.yml"]
async fn mig1_sn1_postgres_to_mongo_denormalized() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_mig1_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = fresh_mongo("dm_mig1").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, PG_CONTAINERS);

    let user = find_one(&dst, "users", doc! {"_id": 1}).await;
    assert!(!user.contains_key("id"), "primary key is stored as _id");
    assert_eq!(user.get_document("user_type").unwrap().get_str("type_name").unwrap(), "Admin");
    let order = find_one(&dst, "orders", doc! {"user_id": 2}).await;
    let embedded_user = order.get_document("user").unwrap();
    assert_eq!(embedded_user.get_str("username").unwrap(), "admin_user2");
    assert_eq!(embedded_user.get_document("user_type").unwrap().get_str("type_name").unwrap(), "Admin");
    // validator from the definition rejects documents of the wrong shape
    let bad = mongo_db(&dst).await.collection::<Document>("user_types").insert_one(doc! {"_id": 99, "type_name": 5}, None).await;
    assert!(bad.is_err());
    assert!(index_keys(&dst, "users").await.iter().any(|k| k.contains_key("email")));
    assert!(index_keys(&dst, "users").await.iter().any(|k| k.contains_key("user_type_id")));

    // idempotent re-run
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_eq!(count(&dst, "orders").await, 40);
}

/// SN-2: PostgreSQL -> MongoDB, normalized (references stay plain, indexed fields).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + MongoDB, run via e2e/docker-compose.test.yml"]
async fn sn2_postgres_to_mongo_normalized() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_sn2_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = fresh_mongo("dm_sn2").await;
    let mut s = settings(dir.path());
    strategy(&mut s, SqlToNoSqlStrategy::Normalized);
    migrate(&src, &dst, &s).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, PG_CONTAINERS);
    let user = find_one(&dst, "users", doc! {"_id": 1}).await;
    assert_eq!(user.get_i32("user_type_id").unwrap(), 1);
    assert!(!user.contains_key("user_type"));
    assert!(index_keys(&dst, "users").await.iter().any(|k| k.contains_key("user_type_id")));
}

/// MIG-5 + SN-3 / SN-4: SQLite -> MongoDB with both strategies.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MongoDB, run via e2e/docker-compose.test.yml"]
async fn mig5_sn3_sn4_sqlite_to_mongo() {
    let dir = tempfile::tempdir().unwrap();
    let src = sqlite_url(dir.path(), "s.db");
    sqlite_exec(&src, SQLITE_SEED);
    let denorm = fresh_mongo("dm_sn3").await;
    migrate(&src, &denorm, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&denorm).await, SQLITE_CONTAINERS);
    let order = find_one(&denorm, "orders", doc! {"_id": 1}).await;
    // order 1 -> user 2 -> user type 1
    assert_eq!(order.get_document("user").unwrap().get_document("user_type").unwrap().get_str("type_name").unwrap(), "Admin");

    let norm = fresh_mongo("dm_sn4").await;
    let mut s = settings(dir.path());
    strategy(&mut s, SqlToNoSqlStrategy::Normalized);
    migrate(&src, &norm, &s).await;
    assert_same_data(&read_all(&src).await, &read_all(&norm).await, SQLITE_CONTAINERS);
    assert!(!find_one(&norm, "orders", doc! {"_id": 1}).await.contains_key("user"));
}

/// MIG-2 (DB-4): MongoDB -> PostgreSQL with schema analysis.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + MongoDB, run via e2e/docker-compose.test.yml"]
async fn mig2_mongo_to_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_mig2").await;
    seed_mongo(&src).await;
    let dst = fresh_pg("dm_mig2_dst").await;
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, MONGO_CONTAINERS);
    let types = pg_query_strings(
        &dst,
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position",
    )
    .await;
    let t = |c: &str| types.iter().find(|r| r[0].as_deref() == Some(c)).map(|r| (r[1].clone().unwrap(), r[2].clone().unwrap())).unwrap();
    assert_eq!(t("_id"), ("character varying".into(), "NO".into()));
    assert_eq!(t("username"), ("text".into(), "NO".into()));
    assert_eq!(t("profile").0, "jsonb");
    assert_eq!(t("tags").0, "jsonb");
    assert_eq!(t("visits"), ("bigint".into(), "YES".into()));
    assert_eq!(t("score").0, "double precision");
    assert_eq!(t("created_at").0, "timestamp with time zone");
    let city = pg_query_strings(&dst, "SELECT profile->>'city' FROM users WHERE username = 'admin_user1'").await;
    assert_eq!(city[0][0].as_deref(), Some("Gdańsk"));
    let idx = pg_query_strings(&dst, "SELECT count(*) FROM pg_indexes WHERE tablename = 'users' AND indexdef LIKE '%(email)%'").await;
    assert_eq!(idx[0][0].as_deref(), Some("1"));
}

/// MIG-6: MongoDB -> SQLite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MongoDB, run via e2e/docker-compose.test.yml"]
async fn mig6_mongo_to_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_mig6").await;
    seed_mongo(&src).await;
    let dst = sqlite_url(dir.path(), "t.db");
    migrate(&src, &dst, &settings(dir.path())).await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, MONGO_CONTAINERS);
}

/// LIVE-1: MongoDB change stream -> PostgreSQL, with resume after restart.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + MongoDB replica set, run via e2e/docker-compose.test.yml"]
async fn live1_mongo_change_stream_to_postgres() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_live1").await;
    seed_mongo(&src).await;
    let dst = fresh_pg("dm_live1_dst").await;
    let s = settings(dir.path());
    let run = start_live(&src, &dst, &s).await;
    let users = mongo_db(&src).await.collection::<Document>("users");
    users.insert_one(doc! {"username": "live", "email": "live@example.com", "visits": 9}, None).await.unwrap();
    users.update_one(doc! {"username": "admin_user1"}, doc! {"$set": {"email": "new@example.com"}}, None).await.unwrap();
    users.delete_one(doc! {"username": "standard_user1"}, None).await.unwrap();
    eventually("changes streamed", || {
        let dst = dst.clone();
        async move {
            pg_count(&dst, "users").await == 4
                && pg_query_strings(&dst, "SELECT email FROM users WHERE username = 'admin_user1'").await[0][0].as_deref() == Some("new@example.com")
                && pg_query_strings(&dst, "SELECT count(*) FROM users WHERE username = 'live'").await[0][0].as_deref() == Some("1")
        }
    })
    .await;
    assert_same_data(&read_all(&src).await, &read_all(&dst).await, &["user_types"]);
    run.finish().await;

    users.insert_one(doc! {"username": "after_restart", "email": "a@example.com"}, None).await.unwrap();
    let run = start_live(&src, &dst, &s).await;
    eventually("resumed from resume token", || {
        let dst = dst.clone();
        async move { pg_count(&dst, "users").await == 5 }
    })
    .await;
    run.finish().await;
}

/// LIVE-11: live PostgreSQL -> MongoDB keeps the denormalized embedding up to date.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + MongoDB, run via e2e/docker-compose.test.yml"]
async fn live11_postgres_to_mongo_denormalized() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_pg("dm_live11_src").await;
    pg_exec(&src, PG_SEED).await;
    let dst = fresh_mongo("dm_live11").await;
    let mut s = settings(dir.path());
    s.live.slot_name = "dm_live11_slot".into();
    s.live.publication_name = "dm_live11_pub".into();
    let run = start_live(&src, &dst, &s).await;
    pg_exec(&src, "UPDATE user_types SET type_name = 'Administrator' WHERE id = 1; INSERT INTO orders (user_id, amount) VALUES (3, 1.00);").await;
    eventually("embedded documents refreshed", || {
        let dst = dst.clone();
        async move {
            let o = mongo_db(&dst).await.collection::<Document>("orders").find_one(doc! {"user_id": 2}, None).await.unwrap().unwrap();
            let nested = o.get_document("user").ok().and_then(|u| u.get_document("user_type").ok()).and_then(|t| t.get_str("type_name").ok()).map(String::from);
            let new_order = mongo_db(&dst).await.collection::<Document>("orders").find_one(doc! {"_id": 41_i64}, None).await.unwrap();
            nested.as_deref() == Some("Administrator")
                && new_order.and_then(|d| d.get_document("user").ok().map(|u| u.get_str("username").unwrap_or("").to_string())).as_deref() == Some("standard_user1")
        }
    })
    .await;
    pg_exec(&src, "DELETE FROM orders WHERE user_id = 4; DELETE FROM users WHERE id = 4;").await;
    eventually("delete streamed", || {
        let dst = dst.clone();
        async move { count(&dst, "users").await == 3 && count(&dst, "orders").await == 31 }
    })
    .await;
    run.finish().await;
    let user = find_one(&dst, "users", doc! {"_id": 1}).await;
    assert_eq!(user.get_document("user_type").unwrap().get("type_name"), Some(&Bson::String("Administrator".into())));
}

/// PERF-11: a large collection is read by several `_id` range cursors; every document once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL + MongoDB, run via e2e/docker-compose.test.yml"]
async fn perf11_mongo_parallel_id_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_perf11").await;
    let coll = mongo_db(&src).await.collection::<Document>("big");
    for chunk in 0..12 {
        let docs: Vec<Document> = (0..10_000).map(|i| doc! {"n": chunk * 10_000 + i, "s": format!("v{}", i)}).collect();
        coll.insert_many(docs, None).await.unwrap();
    }
    let dst = fresh_pg("dm_perf11_mongo_dst").await;
    let mut s = settings(dir.path());
    s.parallel_ranges = 4;
    s.pagination_limit = 1000;
    migrate(&src, &dst, &s).await;
    let r = pg_query_strings(&dst, "SELECT count(*)::text, count(DISTINCT n)::text, sum(n)::text FROM big").await;
    assert_eq!(r[0], vec![Some("120000".into()), Some("120000".into()), Some((0..120_000i64).sum::<i64>().to_string())]);
}

/// VIEW-5 / VIEW-6 on MongoDB: filter and sort documents, find(..) and aggregate(..) queries.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs MongoDB (DM_TEST_MONGO_URL), run via e2e/docker-compose.test.yml"]
async fn view6_mongodb_browse_and_query() {
    use rustygrator_core::browse::{Query, Sort};
    let dir = tempfile::tempdir().unwrap();
    let src = fresh_mongo("dm_view6").await;
    seed_mongo(&src).await;
    let mut db = open(&src, &settings(dir.path())).await;
    let users = definition(&mut db, "users").await;
    let name = users.column_index("username").unwrap();
    let q = Query::Table { def: users, filter: Some(r#"{"email": {"$regex": "^admin"}}"#.into()), sort: Some(Sort { column: "username".into(), descending: true }) };
    let names: Vec<String> = browse(&mut db, &q).await.into_iter().map(|r| r[name].clone()).collect();
    assert_eq!(names, ["admin_user2", "admin_user1"]);

    let rows = browse(&mut db, &Query::Text(r#"db.users.find({"visits": {"$gte": 3}}, {"username": 1, "_id": 0}).sort({"username": 1}).limit(5)"#.into())).await;
    assert_eq!(rows, [["admin_user1"], ["admin_user2"]]);
    let rows = browse(&mut db, &Query::Text(r#"db.users.aggregate([{"$group": {"_id": "$user_type", "n": {"$sum": 1}}}, {"$project": {"_id": 0, "n": 1}}])"#.into())).await;
    assert_eq!(rows, [["2"], ["2"]]);
}
