//! End-to-end benchmarks of rustygrator (PERF-2..PERF-8).
//!
//! Every scenario generates a repeatable data set in the source database (fixed formulas /
//! fixed seed, PERF-3), runs the `rustygrator` binary as a child process and records wall time,
//! rows/s, MB/s, peak memory (RSS), CPU time and the size of the dbc files (PERF-5). Results are
//! printed as a table and written as JSON; with `--baseline` a run fails when a scenario is more
//! than 10 % slower or uses more than 10 % more memory (PERF-6). A live (CDC) scenario measures
//! events/s and lag percentiles under constant write load (PERF-4) and a memory scenario checks
//! that peak memory does not grow with the table size (PERF-8).
//!
//! Databases come from `BENCH_PG_URL`, `BENCH_MONGO_URL`, `BENCH_SCYLLA_URL` (SQLite always);
//! paths whose databases are not configured are skipped.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use clap::{Parser, ValueEnum};
use futures_util::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Profile {
    /// a few minutes: 10k rows on every path, one of each special data set, short live run
    Quick,
    /// everything: 10k / 1M / 10M rows on every path, wide / large / fk / nested at 1M
    Full,
}

#[derive(Parser)]
#[command(about = "rustygrator end-to-end benchmarks")]
struct Args {
    /// rustygrator binary
    #[arg(long, default_value = "target/release/rustygrator")]
    bin: PathBuf,
    #[arg(long, value_enum, default_value = "quick")]
    profile: Profile,
    /// Only these paths (MIG-1 .. MIG-13, PG-PG, SQLITE-SQLITE, LIVE, MEMORY)
    #[arg(long, value_delimiter = ',')]
    paths: Vec<String>,
    /// Results (JSON)
    #[arg(long, default_value = "e2e/bench-results/results.json")]
    out: PathBuf,
    /// Compare with this baseline and fail on > 10 % regressions
    #[arg(long)]
    baseline: Option<PathBuf>,
    /// Write the results to the baseline file
    #[arg(long)]
    save_baseline: bool,
    /// Transfer mode used for the runs (file also reports dbc sizes)
    #[arg(long, default_value = "file")]
    transfer: String,
    /// Scratch directory (SQLite files, dbc files)
    #[arg(long, default_value = "target/bench-work")]
    work_dir: PathBuf,
    /// Allowed regression against the baseline, in percent
    #[arg(long, default_value = "10")]
    tolerance: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Pg,
    Mongo,
    Sqlite,
    Scylla,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Shape {
    /// 5 columns
    Narrow,
    /// 100 columns
    Wide,
    /// 1 MiB text values
    Large,
    /// customers + orders with a foreign key (denormalized strategy)
    Fk,
    /// MongoDB documents with nested objects and arrays
    Nested,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Narrow => "narrow",
            Shape::Wide => "wide",
            Shape::Large => "large",
            Shape::Fk => "fk",
            Shape::Nested => "nested",
        }
    }
    /// Approximate logical bytes per row (for MB/s).
    fn row_bytes(self) -> u64 {
        match self {
            Shape::Narrow => 70,
            Shape::Wide => 33 * 8 + 33 * 32 + 33 * 8,
            Shape::Large => 1 << 20,
            Shape::Fk => 60,
            Shape::Nested => 120,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Dataset {
    shape: Shape,
    rows: u64,
}

impl Dataset {
    fn label(&self) -> String {
        let r = match self.rows {
            n if n >= 1_000_000 && n % 1_000_000 == 0 => format!("{}m", n / 1_000_000),
            n if n >= 1000 && n % 1000 == 0 => format!("{}k", n / 1000),
            n => n.to_string(),
        };
        format!("{}-{}", self.shape.name(), r)
    }
    fn db_name(&self) -> String {
        format!("dmb_src_{}", self.label().replace('-', "_"))
    }
    /// Rows the migration moves (fk: customers + orders).
    fn total_rows(&self) -> u64 {
        match self.shape {
            Shape::Fk => self.rows + self.rows / 10,
            _ => self.rows,
        }
    }
}

#[derive(Clone)]
struct Path_ {
    id: &'static str,
    from: Kind,
    to: Kind,
}

const PATHS: &[Path_] = &[
    Path_ { id: "MIG-1", from: Kind::Pg, to: Kind::Mongo },
    Path_ { id: "MIG-2", from: Kind::Mongo, to: Kind::Pg },
    Path_ { id: "MIG-3", from: Kind::Sqlite, to: Kind::Pg },
    Path_ { id: "MIG-4", from: Kind::Pg, to: Kind::Sqlite },
    Path_ { id: "MIG-5", from: Kind::Sqlite, to: Kind::Mongo },
    Path_ { id: "MIG-6", from: Kind::Mongo, to: Kind::Sqlite },
    Path_ { id: "MIG-7", from: Kind::Pg, to: Kind::Scylla },
    Path_ { id: "MIG-8", from: Kind::Scylla, to: Kind::Pg },
    Path_ { id: "MIG-9", from: Kind::Sqlite, to: Kind::Scylla },
    Path_ { id: "MIG-10", from: Kind::Scylla, to: Kind::Sqlite },
    Path_ { id: "MIG-11", from: Kind::Mongo, to: Kind::Scylla },
    Path_ { id: "MIG-12", from: Kind::Scylla, to: Kind::Mongo },
    Path_ { id: "MIG-13", from: Kind::Scylla, to: Kind::Scylla },
    Path_ { id: "PG-PG", from: Kind::Pg, to: Kind::Pg },
    Path_ { id: "SQLITE-SQLITE", from: Kind::Sqlite, to: Kind::Sqlite },
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunResult {
    path: String,
    dataset: String,
    rows: u64,
    seconds: f64,
    rows_per_s: f64,
    mb_per_s: f64,
    peak_rss_mb: f64,
    cpu_s: f64,
    cpu_percent: f64,
    dbc_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveResult {
    path: String,
    events: u64,
    events_per_s: f64,
    lag_p50_ms: f64,
    lag_p95_ms: f64,
    lag_p99_ms: f64,
    peak_rss_mb: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Report {
    runs: Vec<RunResult>,
    live: Vec<LiveResult>,
    memory_check: Option<String>,
}

type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Env {
    pg: Option<String>,
    mongo: Option<String>,
    scylla: Option<String>,
    work: PathBuf,
}

fn with_db(base: &str, db: &str) -> String {
    let mut u = url::Url::parse(base).expect("valid url");
    u.set_path(&format!("/{}", db));
    u.to_string()
}

impl Env {
    fn available(&self, k: Kind) -> bool {
        match k {
            Kind::Pg => self.pg.is_some(),
            Kind::Mongo => self.mongo.is_some(),
            Kind::Scylla => self.scylla.is_some(),
            Kind::Sqlite => true,
        }
    }
    fn url(&self, k: Kind, db: &str) -> String {
        match k {
            Kind::Pg => with_db(self.pg.as_ref().unwrap(), db),
            Kind::Mongo => with_db(self.mongo.as_ref().unwrap(), db),
            Kind::Scylla => with_db(self.scylla.as_ref().unwrap(), db),
            Kind::Sqlite => format!("sqlite://{}", self.work.join(format!("{}.db", db)).display()),
        }
    }
}

// ------------------------------------------------------------------ helpers per database

async fn pg(url: &str) -> R<tokio_postgres::Client> {
    let u = url::Url::parse(url)?;
    let (c, conn) = tokio_postgres::connect(
        &format!(
            "host={} port={} user={} password={} dbname={}",
            u.host_str().unwrap_or("localhost"),
            u.port().unwrap_or(5432),
            u.username(),
            u.password().unwrap_or(""),
            u.path().trim_start_matches('/')
        ),
        tokio_postgres::NoTls,
    )
    .await?;
    tokio::spawn(conn);
    Ok(c)
}

async fn scylla_session(env: &Env) -> R<scylla::client::session::Session> {
    let c = rustygrator_core::models::connection::MigrationConnection::new(&with_db(env.scylla.as_ref().unwrap(), "system"))?;
    Ok(scylla::client::session_builder::SessionBuilder::new().known_nodes(c.hosts(9042)).build().await?)
}

/// Empties the target.
async fn reset(env: &Env, k: Kind, db: &str) -> R<()> {
    match k {
        Kind::Pg => {
            let admin = pg(env.pg.as_ref().unwrap()).await?;
            admin.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1", &[&db]).await?;
            admin.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).await?;
            admin.batch_execute(&format!("CREATE DATABASE {db}")).await?;
        }
        Kind::Mongo => {
            mongodb::Client::with_uri_str(&env.url(k, db)).await?.database(db).drop(None).await?;
        }
        Kind::Scylla => {
            scylla_session(env).await?.query_unpaged(format!("DROP KEYSPACE IF EXISTS {db}"), ()).await?;
        }
        Kind::Sqlite => {
            let p = env.work.join(format!("{}.db", db));
            let _ = std::fs::remove_file(p);
        }
    }
    Ok(())
}

/// Tiny deterministic generator (fixed seed, PERF-3).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn text(rng: &mut Rng, len: usize) -> String {
    const ABC: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    (0..len).map(|_| ABC[(rng.next() % ABC.len() as u64) as usize] as char).collect()
}

// ------------------------------------------------------------------ data generation

async fn source_count(env: &Env, k: Kind, d: &Dataset) -> u64 {
    let db = d.db_name();
    let table = if d.shape == Shape::Fk { "orders" } else { d.shape.name() };
    let r: R<u64> = async {
        Ok(match k {
            Kind::Pg => pg(&env.url(k, &db)).await?.query_one(&format!("SELECT count(*) FROM {table}"), &[]).await?.get::<_, i64>(0) as u64,
            Kind::Sqlite => {
                let p = env.work.join(format!("{}.db", db));
                if !p.exists() {
                    return Ok(0);
                }
                rusqlite::Connection::open(p)?.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get::<_, i64>(0))? as u64
            }
            Kind::Mongo => mongodb::Client::with_uri_str(&env.url(k, &db)).await?.database(&db).collection::<mongodb::bson::Document>(table).estimated_document_count(None).await?,
            Kind::Scylla => {
                let s = scylla_session(env).await?;
                let r = s.query_unpaged(format!("SELECT count(*) FROM {db}.{table}"), ()).await?.into_rows_result()?;
                r.first_row::<(i64,)>()?.0 as u64
            }
        })
    }
    .await;
    r.unwrap_or(0)
}

fn pg_sql(d: &Dataset) -> String {
    let n = d.rows;
    match d.shape {
        Shape::Narrow => format!(
            "CREATE TABLE narrow (id bigint PRIMARY KEY, name text NOT NULL, amount numeric(12,2), created timestamp, flag boolean);
             INSERT INTO narrow SELECT g, 'name ' || md5(g::text), (g % 100000) * 1.01, timestamp '2024-01-01' + g * interval '1 second', g % 2 = 0 FROM generate_series(1, {n}) g;"
        ),
        Shape::Wide => {
            let cols: Vec<String> = (1..100).map(|i| format!("c{i} {}", ["integer", "text", "double precision"][i % 3])).collect();
            let vals: Vec<String> = (1..100)
                .map(|i| match i % 3 {
                    0 => format!("(g * {i}) % 100000"),
                    1 => format!("md5((g + {i})::text)"),
                    _ => format!("g * {i} * 0.5"),
                })
                .collect();
            format!("CREATE TABLE wide (id bigint PRIMARY KEY, {}); INSERT INTO wide SELECT g, {} FROM generate_series(1, {n}) g;", cols.join(", "), vals.join(", "))
        }
        Shape::Large => format!(
            "CREATE TABLE large (id bigint PRIMARY KEY, payload text); INSERT INTO large SELECT g, repeat(md5(g::text), 32768) FROM generate_series(1, {n}) g;"
        ),
        Shape::Fk => format!(
            "CREATE TABLE customers (id bigint PRIMARY KEY, name text, tier integer);
             INSERT INTO customers SELECT g, 'customer ' || g, g % 5 FROM generate_series(1, {c}) g;
             CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL REFERENCES customers(id), amount numeric(12,2), note text);
             INSERT INTO orders SELECT g, 1 + g % {c}, (g % 1000) * 1.25, md5(g::text) FROM generate_series(1, {n}) g;",
            c = (n / 10).max(1)
        ),
        Shape::Nested => unreachable!("nested data sets exist only in MongoDB"),
    }
}

fn sqlite_sql(d: &Dataset) -> String {
    let n = d.rows;
    let series = |n: u64| format!("WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < {n})");
    match d.shape {
        Shape::Narrow => format!(
            "CREATE TABLE narrow (id INTEGER PRIMARY KEY, name TEXT NOT NULL, amount DECIMAL(12,2), created DATETIME, flag BOOLEAN);
             {} INSERT INTO narrow SELECT n, printf('name %08x%08x', n * 2654435761 % 4294967296, n), (n % 100000) * 1.01, datetime('2024-01-01', '+' || n || ' seconds'), n % 2 = 0 FROM g;",
            series(n)
        ),
        Shape::Wide => {
            let cols: Vec<String> = (1..100).map(|i| format!("c{i} {}", ["INTEGER", "TEXT", "REAL"][i % 3])).collect();
            let vals: Vec<String> = (1..100)
                .map(|i| match i % 3 {
                    0 => format!("(n * {i}) % 100000"),
                    1 => format!("printf('%032d', n * {i})"),
                    _ => format!("n * {i} * 0.5"),
                })
                .collect();
            format!("CREATE TABLE wide (id INTEGER PRIMARY KEY, {}); {} INSERT INTO wide SELECT n, {} FROM g;", cols.join(", "), series(n), vals.join(", "))
        }
        Shape::Large => format!("CREATE TABLE large (id INTEGER PRIMARY KEY, payload TEXT); {} INSERT INTO large SELECT n, printf('%.*c', 1048576, 'x') FROM g;", series(n)),
        Shape::Fk => {
            let c = (n / 10).max(1);
            format!(
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier INTEGER);
                 {} INSERT INTO customers SELECT n, 'customer ' || n, n % 5 FROM g;
                 CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers(id), amount DECIMAL(12,2), note TEXT);
                 {} INSERT INTO orders SELECT n, 1 + n % {c}, (n % 1000) * 1.25, printf('note %d', n) FROM g;",
                series(c),
                series(n)
            )
        }
        Shape::Nested => unreachable!(),
    }
}

fn mongo_doc(d: &Dataset, i: u64, rng: &mut Rng) -> mongodb::bson::Document {
    use mongodb::bson::doc;
    match d.shape {
        Shape::Narrow => doc! {"_id": i as i64, "name": format!("name {}", text(rng, 32)), "amount": (i % 100000) as f64 * 1.01, "created": mongodb::bson::DateTime::from_millis(1_704_067_200_000 + i as i64 * 1000), "flag": i % 2 == 0},
        Shape::Wide => {
            let mut d = doc! {"_id": i as i64};
            for c in 1..100u64 {
                match c % 3 {
                    0 => d.insert(format!("c{c}"), ((i * c) % 100000) as i32),
                    1 => d.insert(format!("c{c}"), text(rng, 32)),
                    _ => d.insert(format!("c{c}"), (i * c) as f64 * 0.5),
                };
            }
            d
        }
        Shape::Large => doc! {"_id": i as i64, "payload": "x".repeat(1 << 20)},
        Shape::Nested => doc! {"_id": i as i64, "name": text(rng, 16), "profile": {"age": (i % 90) as i32, "city": text(rng, 8)}, "tags": [text(rng, 5), text(rng, 5)], "score": (i % 1000) as f64 / 10.0},
        Shape::Fk => unreachable!(),
    }
}

async fn generate(env: &Env, k: Kind, d: &Dataset) -> R<()> {
    if source_count(env, k, d).await == d.rows {
        return Ok(());
    }
    let db = d.db_name();
    eprintln!("generating {} in {:?}", d.label(), k);
    reset(env, k, &db).await?;
    match k {
        Kind::Pg => pg(&env.url(k, &db)).await?.batch_execute(&format!("{} ANALYZE;", pg_sql(d))).await?,
        Kind::Sqlite => rusqlite::Connection::open(env.work.join(format!("{}.db", db)))?.execute_batch(&sqlite_sql(d))?,
        Kind::Mongo => {
            let coll = mongodb::Client::with_uri_str(&env.url(k, &db)).await?.database(&db).collection::<mongodb::bson::Document>(d.shape.name());
            let mut rng = Rng(42);
            let chunk = if d.shape == Shape::Large { 4 } else { 10_000 };
            let mut i = 1;
            while i <= d.rows {
                let end = (i + chunk).min(d.rows + 1);
                let docs: Vec<_> = (i..end).map(|n| mongo_doc(d, n, &mut rng)).collect();
                coll.insert_many(docs, None).await?;
                i = end;
            }
        }
        Kind::Scylla => {
            let s = scylla_session(env).await?;
            s.query_unpaged(format!("CREATE KEYSPACE {db} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"), ()).await?;
            let (create, cols) = match d.shape {
                Shape::Narrow => ("id bigint PRIMARY KEY, name text, amount double, created timestamp, flag boolean".to_string(), 5),
                Shape::Wide => {
                    let c: Vec<String> = (1..100).map(|i| format!("c{i} {}", ["int", "text", "double"][i % 3])).collect();
                    (format!("id bigint PRIMARY KEY, {}", c.join(", ")), 100)
                }
                Shape::Large => ("id bigint PRIMARY KEY, payload text".to_string(), 2),
                _ => unreachable!(),
            };
            s.query_unpaged(format!("CREATE TABLE {db}.{} ({create})", d.shape.name()), ()).await?;
            let p = s.prepare(format!("INSERT INTO {db}.{} JSON ?", d.shape.name())).await?;
            let _ = cols;
            let mut rng = Rng(42);
            let docs: Vec<String> = (1..=d.rows)
                .map(|i| {
                    let doc = mongo_doc(d, i, &mut rng);
                    let mut j = serde_json::Map::new();
                    for (k, v) in doc {
                        let key = if k == "_id" { "id".to_string() } else { k };
                        let val = match v {
                            mongodb::bson::Bson::DateTime(t) => serde_json::Value::from(t.timestamp_millis()),
                            other => other.into_relaxed_extjson(),
                        };
                        j.insert(key, val);
                    }
                    serde_json::Value::Object(j).to_string()
                })
                .collect();
            stream::iter(docs).map(|j| s.execute_unpaged(&p, (j,))).buffer_unordered(256).try_collect::<Vec<_>>().await?;
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ running rustygrator

/// Peak RSS (MiB) and CPU seconds of a child process, sampled from `/proc` (Linux) without
/// unsafe code: `VmHWM` while it runs, `utime + stime` from the zombie before it is reaped.
struct Monitor {
    pid: u32,
    peak_kb: u64,
}

impl Monitor {
    fn sample(&mut self) -> Option<f64> {
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{}/status", self.pid)) {
            for l in status.lines() {
                if let Some(v) = l.strip_prefix("VmHWM:") {
                    let kb: u64 = v.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
                    self.peak_kb = self.peak_kb.max(kb);
                }
            }
        }
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", self.pid)).ok()?;
        let after = stat.rsplit_once(')')?.1;
        let f: Vec<&str> = after.split_whitespace().collect();
        // fields after the command: state(3) ... utime(14) stime(15)
        if f.first() == Some(&"Z") {
            let ticks: f64 = f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?;
            return Some(ticks / 100.0);
        }
        None
    }
}

/// Runs a child to completion: (wall seconds, cpu seconds, peak RSS MiB).
async fn run_child(cmd: &mut Command) -> R<(f64, f64, f64)> {
    let started = Instant::now();
    let mut child = cmd.spawn()?;
    let mut m = Monitor { pid: child.id(), peak_kb: 0 };
    let cpu = loop {
        if let Some(cpu) = m.sample() {
            break cpu;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let status = child.wait()?;
    let wall = started.elapsed().as_secs_f64();
    if !status.success() {
        return Err(format!("rustygrator failed: {}", status).into());
    }
    Ok((wall, cpu, m.peak_kb as f64 / 1024.0))
}

fn dir_size(dir: &Path, ext: &str) -> u64 {
    std::fs::read_dir(dir)
        .map(|it| it.flatten().filter(|e| e.path().extension().is_some_and(|x| x == ext)).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

async fn run_migration(args: &Args, env: &Env, p: &Path_, d: &Dataset) -> R<RunResult> {
    generate(env, p.from, d).await?;
    let target = "dmb_dst";
    reset(env, p.to, target).await?;
    let temp = env.work.join("tmp");
    let _ = std::fs::remove_dir_all(&temp);
    let mut cmd = Command::new(&args.bin);
    cmd.args(["-f", &env.url(p.from, &d.db_name()), "-t", &env.url(p.to, target), "--temp-dir", temp.to_str().unwrap(), "--transfer", &args.transfer, "--keep-temp"])
        .env("RUST_LOG", "warn")
        // fewer glibc malloc arenas: peak memory is measured without per-thread arena noise
        .env("MALLOC_ARENA_MAX", "2")
        .env_remove("DM_DBC_KEY");
    if d.shape == Shape::Fk {
        cmd.args(["--strategy", "denormalized"]);
    }
    let (seconds, cpu_s, peak_rss_mb) = run_child(&mut cmd).await?;
    let dbc_bytes = dir_size(&temp, "dbc");
    let _ = std::fs::remove_dir_all(&temp);
    let rows = d.total_rows();
    Ok(RunResult {
        path: p.id.to_string(),
        dataset: d.label(),
        rows,
        seconds,
        rows_per_s: rows as f64 / seconds,
        mb_per_s: (rows * d.shape.row_bytes()) as f64 / seconds / (1 << 20) as f64,
        peak_rss_mb,
        cpu_s,
        cpu_percent: cpu_s / seconds * 100.0,
        dbc_bytes,
    })
}

// ------------------------------------------------------------------ live benchmark (PERF-4)

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

async fn run_live(args: &Args, env: &Env, source: Kind, rate: u64, seconds: u64) -> R<LiveResult> {
    let src_db = "dmb_live_src";
    let dst_db = "dmb_live_dst";
    reset(env, source, src_db).await?;
    reset(env, Kind::Pg, dst_db).await?;
    let create = "CREATE TABLE live_events (id bigint PRIMARY KEY, payload text)";
    let sqlite_path = env.work.join(format!("{}.db", src_db));
    match source {
        Kind::Pg => pg(&env.url(Kind::Pg, src_db)).await?.batch_execute(&format!("{create}; INSERT INTO live_events VALUES (0, 'seed');")).await?,
        Kind::Sqlite => rusqlite::Connection::open(&sqlite_path)?.execute_batch(&format!("{create}; INSERT INTO live_events VALUES (0, 'seed');"))?,
        _ => return Err("live benchmark sources: PostgreSQL, SQLite".into()),
    }
    let temp = env.work.join("live-tmp");
    let _ = std::fs::remove_dir_all(&temp);
    let mut cmd = Command::new(&args.bin);
    cmd.args([
        "-f", &env.url(source, src_db), "-t", &env.url(Kind::Pg, dst_db), "--temp-dir", temp.to_str().unwrap(), "--live",
        "--batch-wait-ms", "50", "--slot-name", "dmb_slot", "--publication", "dmb_pub", "--stats-interval-secs", "5",
    ])
    .env("RUST_LOG", "warn")
    .env("MALLOC_ARENA_MAX", "2");
    let mut child = cmd.spawn()?;
    let mut monitor = Monitor { pid: child.id(), peak_kb: 0 };
    let target = pg(&env.url(Kind::Pg, dst_db)).await?;
    // snapshot done when the seed row arrived
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(r) = target.query_one("SELECT count(*) FROM live_events", &[]).await {
            if r.get::<_, i64>(0) >= 1 {
                break;
            }
        }
        if Instant::now() > deadline {
            return Err("live: initial snapshot did not finish".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let total = rate * seconds;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(i64, Instant)>();
    let src_url = env.url(source, src_db);
    let writer = tokio::spawn(async move {
        let started = Instant::now();
        let pg_client = if source == Kind::Pg { Some(pg(&src_url).await?) } else { None };
        let sqlite = if source == Kind::Sqlite { Some(std::sync::Arc::new(std::sync::Mutex::new(rusqlite::Connection::open(&sqlite_path)?))) } else { None };
        let mut next = 1i64;
        while (next as u64) <= total {
            // constant load: catch up to `rate` rows per second, in 10 ms ticks
            let due = ((started.elapsed().as_secs_f64() * rate as f64) as i64).min(total as i64);
            if due >= next {
                let ids: Vec<i64> = (next..=due).collect();
                if let Some(c) = &pg_client {
                    c.execute("INSERT INTO live_events SELECT g, 'event ' || g FROM generate_series($1::bigint, $2::bigint) g", &[&next, &due]).await?;
                } else if let Some(c) = &sqlite {
                    let c = c.clone();
                    let (a, b) = (next, due);
                    tokio::task::spawn_blocking(move || {
                        c.lock().unwrap().execute("WITH RECURSIVE g(n) AS (SELECT ?1 UNION ALL SELECT n + 1 FROM g WHERE n < ?2) INSERT INTO live_events SELECT n, 'event ' || n FROM g", [a, b])
                    })
                    .await??;
                }
                let at = Instant::now();
                for id in ids {
                    let _ = tx.send((id, at));
                }
                next = due + 1;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    let mut inserted: HashMap<i64, Instant> = HashMap::with_capacity(total as usize);
    let mut lags: Vec<f64> = Vec::with_capacity(total as usize);
    let mut last_seen = 0i64;
    let mut first_insert = None;
    let mut last_arrival = Instant::now();
    let end_deadline = Instant::now() + Duration::from_secs(seconds + 120);
    while (lags.len() as u64) < total && Instant::now() < end_deadline {
        while let Ok((id, at)) = rx.try_recv() {
            first_insert.get_or_insert(at);
            inserted.insert(id, at);
        }
        let rows = target.query("SELECT id FROM live_events WHERE id > $1 ORDER BY id", &[&last_seen]).await?;
        let now = Instant::now();
        for r in rows {
            let id: i64 = r.get(0);
            last_seen = last_seen.max(id);
            if let Some(at) = inserted.remove(&id) {
                lags.push(now.duration_since(at).as_secs_f64() * 1000.0);
                last_arrival = now;
            }
        }
        monitor.sample();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    writer.await??;
    // graceful stop (SIGTERM): rustygrator flushes the batch and saves the checkpoint
    Command::new("sh").arg("-c").arg(format!("kill -TERM {}", monitor.pid)).status()?;
    while monitor.sample().is_none() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    child.wait()?;
    if source == Kind::Pg {
        let _ = pg(&env.url(Kind::Pg, src_db)).await?.batch_execute("SELECT pg_drop_replication_slot('dmb_slot')").await;
    }
    lags.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let span = last_arrival.duration_since(first_insert.unwrap_or(last_arrival)).as_secs_f64().max(1e-3);
    Ok(LiveResult {
        path: format!("LIVE {:?}->PG", source),
        events: lags.len() as u64,
        events_per_s: lags.len() as f64 / span,
        lag_p50_ms: percentile(&lags, 0.50),
        lag_p95_ms: percentile(&lags, 0.95),
        lag_p99_ms: percentile(&lags, 0.99),
        peak_rss_mb: monitor.peak_kb as f64 / 1024.0,
    })
}

// ------------------------------------------------------------------ scenarios

fn scenarios(profile: Profile) -> Vec<(&'static str, Dataset)> {
    let ds = |shape, rows| Dataset { shape, rows };
    let mut v = Vec::new();
    let sizes: &[u64] = match profile {
        Profile::Quick => &[10_000],
        Profile::Full => &[10_000, 1_000_000, 10_000_000],
    };
    for p in PATHS {
        for &n in sizes {
            v.push((p.id, ds(Shape::Narrow, n)));
        }
    }
    let n = if profile == Profile::Quick { 10_000 } else { 1_000_000 };
    let large = if profile == Profile::Quick { 5 } else { 20 };
    for id in ["PG-PG", "MIG-3", "MIG-2", "MIG-8"] {
        v.push((id, ds(Shape::Wide, n)));
    }
    for id in ["PG-PG", "MIG-2", "MIG-8", "MIG-3"] {
        v.push((id, ds(Shape::Large, large)));
    }
    for id in ["MIG-1", "MIG-5", "MIG-7", "MIG-9"] {
        v.push((id, ds(Shape::Fk, n)));
    }
    for id in ["MIG-2", "MIG-11"] {
        v.push((id, ds(Shape::Nested, n)));
    }
    v
}

fn print_table(report: &Report) {
    println!("\n{:<14} {:<14} {:>10} {:>9} {:>11} {:>8} {:>9} {:>8} {:>6} {:>11}", "path", "dataset", "rows", "seconds", "rows/s", "MB/s", "peak MB", "cpu s", "cpu%", "dbc bytes");
    for r in &report.runs {
        println!(
            "{:<14} {:<14} {:>10} {:>9.2} {:>11.0} {:>8.1} {:>9.1} {:>8.2} {:>6.0} {:>11}",
            r.path, r.dataset, r.rows, r.seconds, r.rows_per_s, r.mb_per_s, r.peak_rss_mb, r.cpu_s, r.cpu_percent, r.dbc_bytes
        );
    }
    for l in &report.live {
        println!(
            "{:<20} {} events, {:.0} events/s, lag p50 {:.0} ms, p95 {:.0} ms, p99 {:.0} ms, peak {:.1} MB",
            l.path, l.events, l.events_per_s, l.lag_p50_ms, l.lag_p95_ms, l.lag_p99_ms, l.peak_rss_mb
        );
    }
    if let Some(m) = &report.memory_check {
        println!("{}", m);
    }
}

/// Differences below these are measurement noise (tiny runs take a few ms).
const NOISE_SECONDS: f64 = 0.05;
const NOISE_MB: f64 = 2.0;

/// Scenarios worse than the baseline by more than `tolerance` % (time or memory), ignoring
/// differences below the noise floor.
fn regressions(report: &Report, baseline: &Report, tolerance: f64) -> Vec<String> {
    let f = 1.0 + tolerance / 100.0;
    let base: HashMap<(String, String), &RunResult> = baseline.runs.iter().map(|r| ((r.path.clone(), r.dataset.clone()), r)).collect();
    let mut out = Vec::new();
    for r in &report.runs {
        if let Some(b) = base.get(&(r.path.clone(), r.dataset.clone())) {
            if r.seconds > b.seconds * f && r.seconds - b.seconds > NOISE_SECONDS {
                out.push(format!("{} {}: {:.2}s vs baseline {:.2}s", r.path, r.dataset, r.seconds, b.seconds));
            }
            if r.peak_rss_mb > b.peak_rss_mb * f && r.peak_rss_mb - b.peak_rss_mb > NOISE_MB {
                out.push(format!("{} {}: {:.1} MB vs baseline {:.1} MB", r.path, r.dataset, r.peak_rss_mb, b.peak_rss_mb));
            }
        }
    }
    let base_live: HashMap<&str, &LiveResult> = baseline.live.iter().map(|l| (l.path.as_str(), l)).collect();
    for l in &report.live {
        if let Some(b) = base_live.get(l.path.as_str()) {
            if l.events_per_s * f < b.events_per_s || l.lag_p95_ms > b.lag_p95_ms * f.max(1.0) + 50.0 {
                out.push(format!("{}: {:.0} events/s, p95 {:.0} ms vs baseline {:.0} events/s, p95 {:.0} ms", l.path, l.events_per_s, l.lag_p95_ms, b.events_per_s, b.lag_p95_ms));
            }
        }
    }
    out
}

#[tokio::main]
async fn main() -> R<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.work_dir)?;
    let env = Env {
        pg: std::env::var("BENCH_PG_URL").ok().filter(|s| !s.is_empty()),
        mongo: std::env::var("BENCH_MONGO_URL").ok().filter(|s| !s.is_empty()),
        scylla: std::env::var("BENCH_SCYLLA_URL").ok().filter(|s| !s.is_empty()),
        work: std::fs::canonicalize(&args.work_dir)?,
    };
    let wanted = |id: &str| args.paths.is_empty() || args.paths.iter().any(|p| p.eq_ignore_ascii_case(id));
    let mut report = Report::default();
    for (id, d) in scenarios(args.profile) {
        let Some(p) = PATHS.iter().find(|p| p.id == id) else { continue };
        if !wanted(id) {
            continue;
        }
        if !env.available(p.from) || !env.available(p.to) || (d.shape == Shape::Fk && !matches!(p.from, Kind::Pg | Kind::Sqlite)) {
            eprintln!("skip {} {} (database not configured)", id, d.label());
            continue;
        }
        let shape_ok = match d.shape {
            Shape::Nested => p.from == Kind::Mongo,
            Shape::Fk => matches!(p.from, Kind::Pg | Kind::Sqlite),
            _ => true,
        };
        if !shape_ok {
            continue;
        }
        eprintln!("run {} {}", id, d.label());
        let r = run_migration(&args, &env, p, &d).await?;
        eprintln!("  {:.2}s, {:.0} rows/s, peak {:.1} MB", r.seconds, r.rows_per_s, r.peak_rss_mb);
        report.runs.push(r);
    }

    // PERF-8: peak memory must not grow with the table size
    if wanted("MEMORY") && env.available(Kind::Pg) {
        let (small, big) = match args.profile {
            Profile::Quick => (1_000_000, 3_000_000),
            Profile::Full => (1_000_000, 10_000_000),
        };
        let p = PATHS.iter().find(|p| p.id == "PG-PG").unwrap();
        let a = run_migration(&args, &env, p, &Dataset { shape: Shape::Narrow, rows: small }).await?;
        let b = run_migration(&args, &env, p, &Dataset { shape: Shape::Narrow, rows: big }).await?;
        let ratio = b.peak_rss_mb / a.peak_rss_mb.max(1.0);
        let ok = ratio <= 1.25;
        report.memory_check = Some(format!(
            "memory check (PERF-8): {} rows {:.1} MB, {} rows {:.1} MB, ratio {:.2} -> {}",
            small, a.peak_rss_mb, big, b.peak_rss_mb, ratio, if ok { "constant (ok)" } else { "GROWS (fail)" }
        ));
        report.runs.push(a);
        report.runs.push(b);
        if !ok {
            print_table(&report);
            return Err("peak memory grows with the table size".into());
        }
    }

    if wanted("LIVE") {
        let (rate, secs) = match args.profile {
            Profile::Quick => (500, 15),
            Profile::Full => (2000, 60),
        };
        for source in [Kind::Pg, Kind::Sqlite] {
            if env.available(source) && env.available(Kind::Pg) {
                eprintln!("run live {:?} -> PG ({} rows/s for {} s)", source, rate, secs);
                report.live.push(run_live(&args, &env, source, rate, secs).await?);
            }
        }
    }

    print_table(&report);
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, serde_json::to_vec_pretty(&report)?)?;
    println!("\nresults: {}", args.out.display());

    if let Some(b) = &args.baseline {
        if args.save_baseline {
            std::fs::write(b, serde_json::to_vec_pretty(&report)?)?;
            println!("baseline saved: {}", b.display());
        } else if b.exists() {
            let baseline: Report = serde_json::from_slice(&std::fs::read(b)?)?;
            let bad = regressions(&report, &baseline, args.tolerance);
            if !bad.is_empty() {
                for r in &bad {
                    eprintln!("REGRESSION {}", r);
                }
                return Err(format!("{} benchmark(s) regressed by more than {}%", bad.len(), args.tolerance).into());
            }
            println!("no regression against {} (tolerance {}%)", b.display(), args.tolerance);
        } else {
            println!("no baseline at {} yet (run with --save-baseline)", b.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(path: &str, s: f64, mb: f64) -> RunResult {
        RunResult { path: path.into(), dataset: "narrow-10k".into(), rows: 1, seconds: s, rows_per_s: 0.0, mb_per_s: 0.0, peak_rss_mb: mb, cpu_s: 0.0, cpu_percent: 0.0, dbc_bytes: 0 }
    }

    #[test]
    fn regression_detection() {
        let base = Report { runs: vec![run("PG-PG", 1.0, 100.0), run("MIG-1", 2.0, 50.0), run("SQLITE-SQLITE", 0.01, 0.5)], ..Default::default() };
        let now = Report { runs: vec![run("PG-PG", 1.05, 109.0), run("MIG-1", 2.3, 50.0), run("SQLITE-SQLITE", 0.02, 1.0)], ..Default::default() };
        let r = regressions(&now, &base, 10.0);
        assert_eq!(r.len(), 1, "{r:?}");
        assert!(r[0].contains("MIG-1"));
        let now = Report { runs: vec![run("PG-PG", 1.0, 120.0)], ..Default::default() };
        assert_eq!(regressions(&now, &base, 10.0).len(), 1);
    }

    #[test]
    fn scenario_matrix_covers_every_path() {
        let q = scenarios(Profile::Quick);
        for p in PATHS {
            assert!(q.iter().any(|(id, d)| *id == p.id && d.shape == Shape::Narrow), "{}", p.id);
        }
        let f = scenarios(Profile::Full);
        assert!(f.iter().any(|(_, d)| d.rows == 10_000_000));
        assert_eq!(Dataset { shape: Shape::Narrow, rows: 1_000_000 }.label(), "narrow-1m");
    }
}
