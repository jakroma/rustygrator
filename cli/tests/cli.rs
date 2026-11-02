//! End-to-end tests of the `rustygrator` binary.

use std::process::Command;

fn rustygrator() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_rustygrator"));
    c.env("RUST_LOG", "info");
    c
}

fn run(args: &[&str]) {
    let out = rustygrator().args(args).output().expect("run rustygrator");
    assert!(
        out.status.success(),
        "rustygrator {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn sqlite_count(path: &std::path::Path, table: &str) -> i64 {
    rusqlite::Connection::open(path).unwrap().query_row(&format!("SELECT count(*) FROM {}", table), [], |r| r.get(0)).unwrap()
}

#[test]
fn cli_sqlite_to_sqlite_through_files() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("s.db");
    rusqlite::Connection::open(&src)
        .unwrap()
        .execute_batch(
            "CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id));
             INSERT INTO a VALUES (1, 'x'), (2, 'y'); INSERT INTO b VALUES (1, 1), (2, 2), (3, 1);",
        )
        .unwrap();
    let dst = dir.path().join("t.db");
    let temp = dir.path().join("tmp");
    run(&[
        "-f", &format!("sqlite://{}", src.display()),
        "-t", &format!("sqlite://{}", dst.display()),
        "--transfer", "file",
        "--temp-dir", temp.to_str().unwrap(),
        "-p", "2",
        "--keep-temp",
    ]);
    assert_eq!(sqlite_count(&dst, "a"), 2);
    assert_eq!(sqlite_count(&dst, "b"), 3);
    assert!(temp.join("migration_order.dmb").exists());
    assert!(temp.join("a_data.dbc").exists());
}

/// FMT-11: provider and receiver as separate processes sharing a key file.
#[test]
fn cli_provide_and_receive_with_key_file() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("s.db");
    rusqlite::Connection::open(&src)
        .unwrap()
        .execute_batch("CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, NULL);")
        .unwrap();
    let key = dir.path().join("dm.key");
    std::fs::write(&key, b"0123456789abcdef0123456789abcdef").unwrap();
    let temp = dir.path().join("handover");
    let t = temp.to_str().unwrap();
    let k = key.to_str().unwrap();
    run(&["--mode", "provide", "-f", &format!("sqlite://{}", src.display()), "--temp-dir", t, "--key-file", k, "--compression", "zstd"]);
    let dst = dir.path().join("t.db");
    // without the key the receiver refuses to start
    let out = rustygrator().args(["--mode", "receive", "-t", &format!("sqlite://{}", dst.display()), "--temp-dir", t]).env_remove("DM_DBC_KEY").output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--key-file"), "{}", String::from_utf8_lossy(&out.stderr));
    run(&["--mode", "receive", "-t", &format!("sqlite://{}", dst.display()), "--temp-dir", t, "--key-file", k]);
    assert_eq!(sqlite_count(&dst, "a"), 3);
}

#[test]
fn cli_rejects_unknown_database() {
    let out = rustygrator().args(["-f", "db2://u:p@h:1/d", "-t", "sqlite://x.db"]).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("NotSupportedDb"), "{}", String::from_utf8_lossy(&out.stderr));
}

/// The sample dump goes PostgreSQL -> SQLite -> MongoDB -> PostgreSQL through the CLI, so every
/// supported database is both provider and receiver.
#[test]
#[ignore = "needs PostgreSQL + MongoDB, run via e2e/docker-compose.test.yml"]
fn cli_dump_through_all_databases() {
    let pg = std::env::var("DM_TEST_PG_URL").expect("DM_TEST_PG_URL");
    let mongo = std::env::var("DM_TEST_MONGO_URL").expect("DM_TEST_MONGO_URL");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let with_db = |base: &str, db: &str| {
        let mut u = url::Url::parse(base).unwrap();
        u.set_path(&format!("/{}", db));
        u.to_string()
    };
    let src = with_db(&pg, "dm_cli_src");
    let back = with_db(&pg, "dm_cli_back");
    let mongo_url = with_db(&mongo, "dm_cli");
    let pg_admin = pg.clone();
    rt.block_on(async {
        let connect = |u: String| async move {
            let p = url::Url::parse(&u).unwrap();
            let (c, conn) = tokio_postgres::connect(
                &format!(
                    "host={} port={} user={} password={} dbname={}",
                    p.host_str().unwrap(),
                    p.port().unwrap_or(5432),
                    p.username(),
                    p.password().unwrap_or(""),
                    p.path().trim_start_matches('/')
                ),
                tokio_postgres::NoTls,
            )
            .await
            .unwrap();
            tokio::spawn(conn);
            c
        };
        let admin = connect(pg_admin).await;
        for db in ["dm_cli_src", "dm_cli_back"] {
            // separate calls: several statements in one call run as one transaction, and DROP DATABASE cannot
            admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await.unwrap();
            admin.batch_execute(&format!("CREATE DATABASE {db}")).await.unwrap();
        }
        let dump = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../dumps/postgresql_dumps.sql")).unwrap();
        connect(src.clone()).await.batch_execute(&dump).await.unwrap();
        mongodb::Client::with_uri_str(&mongo_url).await.unwrap().database("dm_cli").drop(None).await.unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let sqlite = dir.path().join("hop.db");
    let sqlite_url = format!("sqlite://{}", sqlite.display());
    let temp = dir.path().join("tmp");
    let t = temp.to_str().unwrap();
    run(&["-f", &src, "-t", &sqlite_url, "--temp-dir", t]);
    assert_eq!(sqlite_count(&sqlite, "users"), 4);
    run(&["-f", &sqlite_url, "-t", &mongo_url, "--temp-dir", t, "--strategy", "denormalized"]);
    run(&["-f", &mongo_url, "-t", &back, "--temp-dir", t, "--exclude", "nothing"]);

    let rows = rt.block_on(async {
        let p = url::Url::parse(&back).unwrap();
        let (c, conn) = tokio_postgres::connect(
            &format!(
                "host={} port={} user={} password={} dbname=dm_cli_back",
                p.host_str().unwrap(),
                p.port().unwrap_or(5432),
                p.username(),
                p.password().unwrap_or("")
            ),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(conn);
        // users came back with the embedded user type as a json column
        c.query("SELECT username, user_type->>'type_name' FROM users ORDER BY username", &[]).await.unwrap()
    });
    let got: Vec<(String, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0], ("admin_user1".to_string(), "Admin".to_string()));
    assert_eq!(got[3], ("standard_user2".to_string(), "Standard".to_string()));
}

/// ORA-1: an Oracle connection that cannot be made (no Instant Client on this machine, or no
/// server) fails with an Oracle error instead of a crash.
#[test]
fn ora1_oracle_connection_errors_are_clear() {
    let out = rustygrator().args(["-f", "oracle://u:p@127.0.0.1:1/svc", "-t", "sqlite://x.db"]).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("Oracle Instant Client not found") || err.contains("ORA-12541"), "{err}");
}
