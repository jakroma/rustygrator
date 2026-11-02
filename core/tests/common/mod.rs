//! Shared helpers for integration tests.
//!
//! External databases come from environment variables (set by `e2e/docker-compose.test.yml`):
//! * `DM_TEST_PG_URL`    e.g. `postgresql://user:password@127.0.0.1:5433/postgres`
//! * `DM_TEST_MONGO_URL` e.g. `mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true`
//! * `DM_TEST_SCYLLA_URL` e.g. `scylla://127.0.0.1:9042/`
//! * `DM_TEST_MYSQL_URL` e.g. `mysql://root:password@127.0.0.1:3306/`
//! * `DM_TEST_ORACLE_URL` e.g. `oracle://system:password@127.0.0.1:1521/FREEPDB1`
//!
//! SQLite needs nothing. Tests that need another database are `#[ignore]`d and run with
//! `cargo test -- --include-ignored` inside docker-compose.

#![allow(dead_code)]

use std::{collections::HashMap, path::Path, time::Duration};

use rustygrator_core::{
    db::database::{connect, DatabaseConnectionType},
    dbc::{
        batch::ColumnBatch,
        crypto::{DbcCrypto, KeySource},
    },
    migrator::{
        batching::BatchSizer,
        migrator::{MigrationSummary, Migrator},
        migrator_steps::DataProvider,
    },
    models::{
        change::LiveState,
        connection::MigrationConnection,
        data_definitions::{short_name, DataDefinition},
        migration_execution_settings::{ContainerFilter, MigrationExecutionSettings, SqlToNoSqlStrategy},
        value::{LogicalType, Row, Value},
    },
    DMResult,
};
use tokio::sync::{mpsc, watch};

pub fn pg_base() -> String {
    std::env::var("DM_TEST_PG_URL").expect("DM_TEST_PG_URL must be set (run the tests through e2e/docker-compose.test.yml)")
}

pub fn mongo_base() -> String {
    std::env::var("DM_TEST_MONGO_URL").expect("DM_TEST_MONGO_URL must be set (run the tests through e2e/docker-compose.test.yml)")
}

pub fn mysql_base() -> String {
    std::env::var("DM_TEST_MYSQL_URL").expect("DM_TEST_MYSQL_URL must be set (run the tests through e2e/docker-compose.test.yml)")
}

async fn mysql_conn(url: &str) -> mysql_async::Conn {
    let c = MigrationConnection::new(&url.to_string()).unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(c.host)
        .tcp_port(c.port)
        .user(Some(c.user))
        .pass(Some(c.password))
        .db_name(Some(c.database).filter(|d| !d.is_empty()))
        .init(vec!["SET time_zone = '+00:00'"]);
    mysql_async::Conn::new(opts).await.expect("connect to MySQL")
}

/// Creates an empty MySQL database for a test and returns its URL.
pub async fn fresh_mysql(name: &str) -> String {
    use mysql_async::prelude::Queryable;
    let mut admin = mysql_conn(&with_database(&mysql_base(), "mysql")).await;
    admin.query_drop(format!("DROP DATABASE IF EXISTS `{name}`")).await.unwrap();
    admin.query_drop(format!("CREATE DATABASE `{name}` DEFAULT CHARACTER SET utf8mb4")).await.unwrap();
    with_database(&mysql_base(), name)
}

/// Runs one or more statements.
pub async fn mysql_exec(url: &str, sql: &str) {
    use mysql_async::prelude::Queryable;
    mysql_conn(url).await.query_drop(sql).await.unwrap_or_else(|e| panic!("{e:?}\n{sql}"));
}

pub async fn mysql_count(url: &str, table: &str) -> i64 {
    use mysql_async::prelude::Queryable;
    mysql_conn(url).await.query_first(format!("SELECT count(*) FROM `{table}`")).await.unwrap().unwrap()
}

/// `DM_TEST_ORACLE_URL`: an administrator (e.g. `SYSTEM`) of the pluggable database the tests
/// create their schemas in.
pub fn oracle_base() -> String {
    std::env::var("DM_TEST_ORACLE_URL").expect("DM_TEST_ORACLE_URL must be set (run the tests through e2e/docker-compose.test.yml)")
}

fn oracle_conn(url: &str) -> oracle::Connection {
    let c = MigrationConnection::new(&url.to_string()).unwrap();
    oracle::Connection::connect(c.user, c.password, format!("{}:{}/{}", c.host, c.port, c.database)).expect("connect to Oracle")
}

/// Creates an empty schema (user `name`, password `password`) with the rights a migration and
/// LogMiner need, and returns its URL.
pub async fn fresh_oracle(name: &str) -> String {
    let name = name.to_uppercase();
    let base = oracle_base();
    let admin = name.clone();
    tokio::task::spawn_blocking(move || {
        let c = oracle_conn(&base);
        let _ = c.execute(&format!("DROP USER {admin} CASCADE"), &[]);
        for sql in [
            format!("CREATE USER {admin} IDENTIFIED BY password QUOTA UNLIMITED ON USERS"),
            format!("GRANT CREATE SESSION, CREATE TABLE, LOGMINING, SELECT ANY TRANSACTION, SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO {admin}"),
        ] {
            c.execute(&sql, &[]).unwrap_or_else(|e| panic!("{e}\n{sql}"));
        }
    })
    .await
    .unwrap();
    let mut u = url::Url::parse(&oracle_base()).unwrap();
    u.set_username(&name.to_lowercase()).unwrap();
    u.set_password(Some("password")).unwrap();
    u.to_string()
}

/// Runs a script: statements end with `;` at the end of a line; committed at the end.
pub async fn oracle_exec(url: &str, script: &str) {
    let (url, script) = (url.to_string(), script.to_string());
    tokio::task::spawn_blocking(move || {
        let c = oracle_conn(&url);
        let mut statement = String::new();
        for line in script.lines().filter(|l| !l.trim_start().starts_with("--")) {
            statement.push_str(line);
            statement.push('\n');
            if line.trim_end().ends_with(';') {
                let sql = statement.trim().trim_end_matches(';').to_string();
                c.execute(&sql, &[]).unwrap_or_else(|e| panic!("{e}\n{sql}"));
                statement.clear();
            }
        }
        c.commit().unwrap();
    })
    .await
    .unwrap();
}

pub async fn oracle_count(url: &str, table: &str) -> i64 {
    let (url, table) = (url.to_string(), table.to_string());
    tokio::task::spawn_blocking(move || oracle_conn(&url).query_row_as::<i64>(&format!("SELECT count(*) FROM \"{table}\""), &[]).unwrap()).await.unwrap()
}

/// ORA-5: Oracle has no empty string; `''` from other databases arrives as NULL.
pub fn empty_text_as_null(mut data: HashMap<String, (DataDefinition, Vec<Row>)>) -> HashMap<String, (DataDefinition, Vec<Row>)> {
    for (_, rows) in data.values_mut() {
        for v in rows.iter_mut().flatten() {
            if matches!(v, Value::String(s) if s.is_empty()) {
                *v = Value::Null;
            }
        }
    }
    data
}

/// A file of `dumps/` (DUMP-1).
pub fn dump(name: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../dumps").join(name)).unwrap_or_else(|e| panic!("dumps/{name}: {e}"))
}

/// Loads `dumps/shop/mongodb.json` ({collection: [extended JSON documents]}).
pub async fn load_mongo_dump(url: &str, name: &str) {
    let db = mongo_db(url).await;
    let all: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&dump(name)).unwrap();
    for (collection, docs) in all {
        let docs: Vec<mongodb::bson::Document> = docs
            .as_array()
            .unwrap()
            .iter()
            .map(|d| mongodb::bson::Bson::try_from(d.clone()).unwrap().as_document().unwrap().clone())
            .collect();
        db.collection::<mongodb::bson::Document>(&collection).insert_many(docs, None).await.unwrap();
    }
}

/// Runs a CQL dump (one statement per line, `--` comments) in `keyspace`.
pub async fn load_cql_dump(keyspace: &str, name: &str) {
    let s = scylla_session().await;
    cql(&s, &format!("CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}")).await;
    cql(&s, &format!("USE {keyspace}")).await;
    for statement in dump(name).lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with("--")) {
        cql(&s, statement.trim_end_matches(';')).await;
    }
}

/// Shop data set row counts.
pub const SHOP: [(&str, i64); 4] = [("customers", 6), ("products", 5), ("orders", 6), ("order_items", 9)];

fn with_database(base: &str, db: &str) -> String {
    let mut u = url::Url::parse(base).expect("valid url");
    u.set_path(&format!("/{}", db));
    u.to_string()
}

async fn pg_client(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &{
            let u = url::Url::parse(url).unwrap();
            format!(
                "host={} port={} user={} password={} dbname={}",
                u.host_str().unwrap(),
                u.port().unwrap_or(5432),
                u.username(),
                u.password().unwrap_or(""),
                u.path().trim_start_matches('/')
            )
        },
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect to PostgreSQL");
    tokio::spawn(conn);
    client
}

/// Creates an empty database for a test and returns its URL.
pub async fn fresh_pg(name: &str) -> String {
    let admin = pg_client(&pg_base()).await;
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1",
            &[&name],
        )
        .await
        .unwrap();
    admin.batch_execute(&format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", name)).await.unwrap();
    admin.batch_execute(&format!("CREATE DATABASE \"{}\"", name)).await.unwrap();
    with_database(&pg_base(), name)
}

pub async fn pg_exec(url: &str, sql: &str) {
    pg_client(url).await.batch_execute(sql).await.unwrap_or_else(|e| panic!("{e:?}\n{sql}"));
}

pub async fn pg_count(url: &str, table: &str) -> i64 {
    pg_client(url).await.query_one(&format!("SELECT count(*) FROM {}", table), &[]).await.unwrap().get(0)
}

pub async fn pg_query_strings(url: &str, sql: &str) -> Vec<Vec<Option<String>>> {
    let rows = pg_client(url).await.simple_query(sql).await.unwrap();
    rows.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).map(String::from)).collect()),
            _ => None,
        })
        .collect()
}

pub async fn fresh_mongo(name: &str) -> String {
    let url = with_database(&mongo_base(), name);
    let client = mongodb::Client::with_uri_str(&url).await.unwrap();
    client.database(name).drop(None).await.unwrap();
    url
}

pub async fn mongo_db(url: &str) -> mongodb::Database {
    let c = MigrationConnection::new(&url.to_string()).unwrap();
    mongodb::Client::with_uri_str(url).await.unwrap().database(&c.database)
}

pub fn sqlite_url(dir: &Path, name: &str) -> String {
    format!("sqlite://{}", dir.join(name).display())
}

pub fn sqlite_exec(url: &str, sql: &str) {
    let path = url.strip_prefix("sqlite://").unwrap();
    rusqlite::Connection::open(path).unwrap().execute_batch(sql).unwrap_or_else(|e| panic!("{e:?}\n{sql}"));
}

pub fn sqlite_count(url: &str, table: &str) -> i64 {
    let path = url.strip_prefix("sqlite://").unwrap();
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM \"{}\"", table), [], |r| r.get(0))
        .unwrap()
}

pub const TEST_KEY: &str = "integration test passphrase";

pub fn settings(temp: &Path) -> MigrationExecutionSettings {
    let mut s = MigrationExecutionSettings::new(2, 7); // small pages exercise batching
    s.batch.min_rows = 3; // adaptive batching on, starting at 7 rows
    s.dbc.block_rows = 16; // several blocks per file
    s.dbc.key = KeySource::Passphrase(TEST_KEY.into());
    s.temp_dir = temp.join("dbc");
    s.live.batch_wait = Duration::from_millis(200);
    s.live.stats_interval = Duration::from_secs(1);
    s.live.max_retries = 3;
    s
}

pub async fn open(url: &str, s: &MigrationExecutionSettings) -> DatabaseConnectionType {
    connect(MigrationConnection::new(&url.to_string()).unwrap(), s.clone()).await.unwrap()
}

pub fn conn(url: &str) -> MigrationConnection {
    MigrationConnection::new(&url.to_string()).unwrap()
}

pub async fn migrate(from: &str, to: &str, s: &MigrationExecutionSettings) -> MigrationSummary {
    let mut m = Migrator::connect(Some(conn(from)), Some(conn(to)), s.clone()).await.unwrap();
    m.run().await.unwrap()
}

/// Every container read back through rustygrator's own provider: short name -> (definition, rows).
pub async fn read_all(url: &str) -> HashMap<String, (DataDefinition, Vec<Row>)> {
    let s = MigrationExecutionSettings::new(2, 1000);
    let mut db = open(url, &s).await;
    let defs = db.get_definitions(&ContainerFilter::default()).await.unwrap();
    let mut out = HashMap::new();
    for def in defs {
        let (tx, mut rx) = mpsc::channel::<ColumnBatch>(4);
        let collect = async {
            let mut rows = Vec::new();
            while let Some(b) = rx.recv().await {
                rows.extend((0..b.rows).map(|i| b.row_values(i)));
            }
            rows
        };
        let (read, rows) = tokio::join!(db.read_data(&def, std::sync::Arc::new(BatchSizer::fixed(1000)), tx), collect);
        read.unwrap();
        out.insert(short_name(&def.container_name).to_string(), (def, rows));
    }
    out
}

fn norm(v: &Value, t: LogicalType, source_type: Option<&str>) -> Value {
    let v = match v.clone().coerce(t) {
        Ok(v) => v,
        Err(_) => v.clone(),
    };
    match (v, source_type) {
        (Value::String(s), Some("character")) => Value::String(s.trim_end().to_string()),
        // numerically equal: databases pad decimals to their column scale (MySQL DECIMAL(p,s))
        (Value::Decimal(s), _) if s.contains('.') => {
            let t = s.trim_end_matches('0').trim_end_matches('.');
            Value::Decimal(if t == "-0" { "0".into() } else { t.to_string() })
        }
        (Value::F32(f), _) => Value::String(format!("{:.4}", f)),
        (Value::F64(f), _) => Value::String(format!("{:.6}", f)),
        // PostgreSQL / MySQL times have microsecond precision (ScyllaDB: nanoseconds)
        (Value::Time(t), _) => Value::String(t.format("%H:%M:%S%.6f").to_string()),
        // MongoDB dates have millisecond precision
        (Value::DateTime(t), _) => Value::String(t.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
        (Value::DateTimeTz(t), _) => Value::String(t.format("%Y-%m-%d %H:%M:%S%.3f%z").to_string()),
        (other, _) => other,
    }
}

/// Asserts that `target` holds the same records as `source` for the given containers. Columns
/// are matched by name; a single primary key may be stored as `_id` (MongoDB). Extra target
/// columns (e.g. embedded documents) are ignored.
pub fn assert_same_data(
    source: &HashMap<String, (DataDefinition, Vec<Row>)>,
    target: &HashMap<String, (DataDefinition, Vec<Row>)>,
    containers: &[&str],
) {
    for name in containers {
        let (sd, srows) = source.get(*name).unwrap_or_else(|| panic!("source has no {name}"));
        let (td, trows) = target.get(*name).unwrap_or_else(|| panic!("target has no {name}: {:?}", target.keys()));
        assert_eq!(srows.len(), trows.len(), "row count of {name}");
        let sid = sd.identifier_names();
        let map: Vec<usize> = sd
            .properties_info
            .iter()
            .map(|p| {
                td.column_index(&p.property_name)
                    .or_else(|| (sid.len() == 1 && p.is_identifier).then(|| td.column_index("_id")).flatten())
                    .unwrap_or_else(|| panic!("{name}: target has no column {}", p.property_name))
            })
            .collect();
        let key_cols: Vec<usize> = if sid.is_empty() { (0..sd.properties_info.len()).collect() } else { sd.identifier_indexes() };
        let key = |row: &Row, mapping: Option<&[usize]>| -> String {
            key_cols
                .iter()
                .map(|&i| {
                    let p = &sd.properties_info[i];
                    let v = match mapping {
                        Some(m) => &row[m[i]],
                        None => &row[i],
                    };
                    format!("{:?}", norm(v, p.data_type, p.source_type.as_deref()))
                })
                .collect::<Vec<_>>()
                .join("|")
        };
        let mut s_sorted: Vec<&Row> = srows.iter().collect();
        s_sorted.sort_by_key(|r| key(r, None));
        let mut t_sorted: Vec<&Row> = trows.iter().collect();
        t_sorted.sort_by_key(|r| key(r, Some(&map)));
        for (s, t) in s_sorted.iter().zip(t_sorted.iter()) {
            for (i, p) in sd.properties_info.iter().enumerate() {
                let sv = norm(&s[i], p.data_type, p.source_type.as_deref());
                let tv = norm(&t[map[i]], p.data_type, p.source_type.as_deref());
                assert_eq!(sv, tv, "{name}.{} differs (source row {:?})", p.property_name, s);
            }
        }
    }
}

/// Polls `check` until it returns true (or panics after 60 s).
pub async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..300 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// Live migration running in the background.
pub struct LiveRun {
    pub stop: watch::Sender<bool>,
    pub handle: tokio::task::JoinHandle<DMResult<MigrationSummary>>,
    pub checkpoint: std::path::PathBuf,
}

pub async fn start_live(from: &str, to: &str, s: &MigrationExecutionSettings) -> LiveRun {
    let mut s = s.clone();
    s.live.enabled = true;
    let checkpoint = s.checkpoint_file();
    let mut m = Migrator::connect(Some(conn(from)), Some(conn(to)), s).await.unwrap();
    let (stop, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { m.run_with_shutdown(rx).await });
    let crypto = DbcCrypto::new(KeySource::Passphrase(TEST_KEY.into()));
    for _ in 0..300 {
        if matches!(LiveState::load(&checkpoint, &crypto), Ok(Some(LiveState { snapshot_done: true, .. }))) {
            return LiveRun { stop, handle, checkpoint };
        }
        // a run that ended before the snapshot was done failed: show why
        if handle.is_finished() {
            panic!("live run ended early: {:?}", handle.await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out waiting for: initial snapshot");
}

impl LiveRun {
    pub async fn finish(self) -> MigrationSummary {
        self.stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(30), self.handle)
            .await
            .expect("live run stops gracefully")
            .unwrap()
            .unwrap()
    }
}

pub fn strategy(s: &mut MigrationExecutionSettings, st: SqlToNoSqlStrategy) {
    s.strategy = st;
}

// ---------------------------------------------------------------- seed data

/// PostgreSQL schema covering every mapped type, foreign keys (two levels), indexes, a table
/// without primary key and values that broke the old dbc format.
pub const PG_SEED: &str = r#"
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE user_types (id SERIAL PRIMARY KEY, type_name VARCHAR(50) NOT NULL UNIQUE);
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    user_type_id INT NOT NULL REFERENCES user_types(id),
    created_at TIMESTAMP DEFAULT TIMESTAMP '2024-01-02 03:04:05'
);
CREATE INDEX users_email_idx ON users (email);
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount NUMERIC(12, 2),
    note TEXT
);
CREATE TABLE all_types (
    id INT PRIMARY KEY,
    c_small SMALLINT, c_big BIGINT, c_real REAL, c_double DOUBLE PRECISION,
    c_numeric NUMERIC(30, 6), c_bool BOOLEAN, c_text TEXT, c_char CHAR(3),
    c_date DATE, c_time TIME, c_ts TIMESTAMP, c_tstz TIMESTAMPTZ, c_uuid UUID,
    c_json JSONB, c_bytes BYTEA, c_arr INT[], c_mood mood
);
CREATE TABLE no_pk (a INT, b TEXT);

INSERT INTO user_types (type_name) VALUES ('Admin'), ('Standard');
INSERT INTO users (username, email, user_type_id) VALUES
 ('admin_user1', 'admin1@example.com', 1), ('admin_user2', 'admin2@example.com', 1),
 ('standard_user1', 'standard1@example.com', 2), ('standard_user2', 'standard2@example.com', 2);
INSERT INTO orders (user_id, amount, note)
SELECT 1 + (g % 4), (g * 1.25)::numeric(12,2), CASE WHEN g % 5 = 0 THEN NULL ELSE 'order ' || g END
FROM generate_series(1, 40) g;
INSERT INTO all_types VALUES
 (1, -32768, 9223372036854775807, 1.5, -2.25, '12345678901234567890.123456', true,
  E'quote " apostrophe '' sep \x1e\x1f newline \n end', 'ab', '2024-02-29', '23:59:58.5',
  '2024-05-06 07:08:09.123456', '2024-05-06 07:08:09+02', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
  '{"k": [1, "x", null], "n": {"m": true}}', '\x00ff1e1f', '{1,2,3}', 'happy'),
 (2, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO no_pk VALUES (1, 'x'), (1, 'x'), (NULL, 'y');
"#;

pub const PG_CONTAINERS: &[&str] = &["user_types", "users", "orders", "all_types", "no_pk"];

/// SQLite schema with declared types, foreign keys and an index.
pub const SQLITE_SEED: &str = r#"
CREATE TABLE user_types (id INTEGER PRIMARY KEY, type_name TEXT NOT NULL UNIQUE);
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email TEXT NOT NULL,
    user_type_id INTEGER NOT NULL REFERENCES user_types(id),
    created_at DATETIME,
    balance DECIMAL(10,2),
    active BOOLEAN,
    avatar BLOB,
    score REAL
);
CREATE INDEX users_email_idx ON users(email);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users,
    amount REAL,
    ordered_on DATE
);
INSERT INTO user_types VALUES (1, 'Admin'), (2, 'Standard');
INSERT INTO users VALUES
 (1, 'admin_user1', 'admin1@example.com', 1, '2024-01-02 03:04:05', '10.50', 1, x'00ff', 1.5),
 (2, 'admin_user2', 'admin2@example.com', 1, '2024-01-03 03:04:05', '0.00', 0, NULL, NULL),
 (3, 'standard_user1', 'standard1@example.com', 2, NULL, NULL, NULL, NULL, -2.75),
 (4, 'standard ''quoted''', 'standard2@example.com', 2, '2024-01-04T05:06:07', '99.99', 1, x'1e1f', 0);
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 30)
INSERT INTO orders SELECT n, 1 + (n % 4), n * 1.5, date('2024-01-01', '+' || n || ' days') FROM g;
"#;

pub const SQLITE_CONTAINERS: &[&str] = &["user_types", "users", "orders"];

pub async fn seed_mongo(url: &str) {
    use mongodb::bson::{doc, oid::ObjectId, DateTime};
    let db = mongo_db(url).await;
    let admin = ObjectId::new();
    let standard = ObjectId::new();
    db.collection("user_types")
        .insert_many(vec![doc! {"_id": admin, "type_name": "Admin"}, doc! {"_id": standard, "type_name": "Standard"}], None)
        .await
        .unwrap();
    db.collection("users")
        .insert_many(
            vec![
                doc! {"username": "admin_user1", "email": "admin1@example.com", "user_type": admin, "created_at": DateTime::from_millis(1_704_164_645_000_i64), "tags": ["a", "b"], "profile": {"age": 30, "city": "Gdańsk"}, "score": 1.5, "visits": 3},
                doc! {"username": "admin_user2", "email": "admin2@example.com", "user_type": admin, "created_at": DateTime::from_millis(1_704_251_045_000_i64), "visits": 5_000_000_000_i64},
                doc! {"username": "standard_user1", "email": "standard1@example.com", "user_type": standard, "score": null, "visits": 1},
                doc! {"username": "standard_user2", "email": "standard2@example.com", "user_type": standard, "score": 7, "extra": "only here"},
            ],
            None,
        )
        .await
        .unwrap();
    db.collection::<mongodb::bson::Document>("users")
        .create_index(mongodb::IndexModel::builder().keys(doc! {"email": 1}).build(), None)
        .await
        .unwrap();
}

pub const MONGO_CONTAINERS: &[&str] = &["user_types", "users"];

// ---------------------------------------------------------------- ScyllaDB

pub fn scylla_base() -> String {
    std::env::var("DM_TEST_SCYLLA_URL").expect("DM_TEST_SCYLLA_URL must be set (run the tests through e2e/docker-compose.test.yml)")
}

pub async fn scylla_session() -> scylla::client::session::Session {
    let c = MigrationConnection::new(&with_database(&scylla_base(), "system")).unwrap();
    scylla::client::session_builder::SessionBuilder::new().known_nodes(c.hosts(9042)).build().await.expect("connect to ScyllaDB")
}

pub async fn cql(s: &scylla::client::session::Session, statement: &str) {
    for part in statement.split(";\n").map(str::trim).filter(|p| !p.is_empty()) {
        s.query_unpaged(part.to_string(), ()).await.unwrap_or_else(|e| panic!("{e}\n{part}"));
    }
}

/// Drops the keyspace and returns the URL pointing at it (the receiver creates it).
pub async fn fresh_scylla(keyspace: &str) -> String {
    let s = scylla_session().await;
    cql(&s, &format!("DROP KEYSPACE IF EXISTS {}", keyspace)).await;
    with_database(&scylla_base(), keyspace)
}

pub async fn scylla_rows(statement: &str) -> Vec<scylla::value::Row> {
    let s = scylla_session().await;
    s.query_unpaged(statement.to_string(), ()).await.unwrap().into_rows_result().unwrap().rows::<scylla::value::Row>().unwrap().map(|r| r.unwrap()).collect()
}

/// Source keyspace with collections, UDTs, composite keys and special CQL types (vnodes, so CDC
/// can be enabled).
pub async fn seed_scylla(keyspace: &str) -> String {
    let url = fresh_scylla(keyspace).await;
    let s = scylla_session().await;
    cql(
        &s,
        &format!(
            "CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}};
             CREATE TYPE {ks}.address (city text, zip int);
             CREATE TABLE {ks}.events (device text, ts timestamp, seq int, value double, tags set<text>, attrs map<text, int>, readings list<int>,
                 addr frozen<address>, big varint, amount decimal, eid timeuuid, dur duration, ip inet, note text, PRIMARY KEY ((device), ts, seq));
             CREATE TABLE {ks}.devices (id uuid PRIMARY KEY, name text, age smallint, active boolean, born date, wake time, avatar blob);
             CREATE INDEX ON {ks}.devices (name);
             INSERT INTO {ks}.devices (id, name, age, active, born, wake, avatar) VALUES (11111111-1111-1111-1111-111111111111, 'sensor ''a''', 3, true, '2024-02-29', '07:30:00.123456789', 0x00ff1e);
             INSERT INTO {ks}.devices (id, name) VALUES (22222222-2222-2222-2222-222222222222, 'sensor b');
             INSERT INTO {ks}.events (device, ts, seq, value, tags, attrs, readings, addr, big, amount, eid, dur, ip, note)
                 VALUES ('d1', '2024-01-02 03:04:05.678+0000', 1, 1.5, {{'x', 'y'}}, {{'k': 1}}, [3, 1, 2], {{city: 'Gdańsk', zip: 80}},
                         123456789012345678901234567890, 12345.678900, 50554d6e-29bb-11e5-b345-feff819cdc9f, 1mo2d3s, '10.0.0.1', 'first');
             INSERT INTO {ks}.events (device, ts, seq, value) VALUES ('d1', '2024-01-02 03:04:06+0000', 2, -2.25);
             INSERT INTO {ks}.events (device, ts, seq) VALUES ('d2', '2024-01-03 00:00:00+0000', 1)",
            ks = keyspace
        ),
    )
    .await;
    for i in 0..300 {
        cql(&s, &format!("INSERT INTO {}.events (device, ts, seq, value, note) VALUES ('bulk{}', '2024-02-01 00:00:00+0000', {}, {}.5, 'n{}')", keyspace, i % 17, i, i, i)).await;
    }
    url
}

pub const SCYLLA_CONTAINERS: &[&str] = &["events", "devices"];

// ---------------------------------------------------------------- browsing (section 9.4)

/// Runs `q` (describe + fetch in small batches) and returns every row as text.
pub async fn browse(db: &mut DatabaseConnectionType, q: &rustygrator_core::browse::Query) -> Vec<Vec<String>> {
    use rustygrator_core::{browse::QueryRunner, migrator::batching::BatchSizer};
    let def = db.describe(q).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let fetch = db.fetch(q, &def, std::sync::Arc::new(BatchSizer::fixed(7)), tx);
    let collect = async {
        let mut out = Vec::new();
        while let Some(b) = rx.recv().await {
            for r in 0..b.rows {
                out.push((0..b.columns.len()).map(|c| b.cell(r, c).to_text().into_owned()).collect());
            }
        }
        out
    };
    let (n, rows) = tokio::join!(fetch, collect);
    assert_eq!(n.unwrap() as usize, rows.len());
    rows
}

/// Definition of one container of `db`.
pub async fn definition(db: &mut DatabaseConnectionType, name: &str) -> DataDefinition {
    use rustygrator_core::{migrator::migrator_steps::DataProvider, models::migration_execution_settings::ContainerFilter};
    db.get_definitions(&ContainerFilter::default()).await.unwrap().into_iter().find(|d| d.container_name == name).expect("container exists")
}
