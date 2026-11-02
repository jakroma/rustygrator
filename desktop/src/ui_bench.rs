//! UI benchmarks (APERF-1..APERF-5), part of the benchmark set of section 8.1:
//!
//! ```text
//! cargo build --release -p rustygrator-desktop
//! cargo test --release -p rustygrator-desktop -- --ignored aperf --nocapture
//! ```
//!
//! `DM_UI_BENCH_ROWS` sets the size of the big table (default 10 000 000). Results go to
//! `e2e/bench-results/ui.json`; with `e2e/bench-results/ui-baseline.json` present a run fails
//! when a number is more than 10 % worse (`DM_UI_BENCH_SAVE_BASELINE=1` writes the baseline).

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use iced::{mouse, Event, Point, Size};
use iced_test::{
    core::{clipboard, renderer::Headless},
    runtime::{user_interface, UserInterface},
};
use serde::{Deserialize, Serialize};

use rustygrator_core::{
    browse::{Query, QueryRunner},
    dbc::batch::ColumnBatch,
    migrator::{batching::BatchSizer, migrator_steps::DataProvider},
    models::migration_execution_settings::ContainerFilter,
};

use crate::{connections::{Profile, Tls}, pool::Pool, results::{ResultWindow, BATCH_ROWS}, App, Message, TabKind};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UiReport {
    pub rows: u64,
    /// APERF-1: process start to the first frame on screen (real window)
    pub startup_ms: Option<f64>,
    /// APERF-2: open table to first rows ready, small table and big table
    pub first_rows_small_ms: f64,
    pub first_rows_big_ms: f64,
    /// APERF-3: CPU time of one frame (view, layout, event handling, draw) while scrolling
    pub frame_p50_ms: f64,
    pub frame_p95_ms: f64,
    pub frame_max_ms: f64,
    pub frames: usize,
    /// first frame: empty layout and glyph caches (window just opened)
    #[serde(default)]
    pub first_frame_ms: f64,
    /// frames after the first one over the 120 Hz budget (dropped frames)
    #[serde(default)]
    pub slow_frames: usize,
    /// APERF-4: memory of the process with the big table open, after scrolling through all of it
    pub memory_mb: f64,
}

/// Targets of section 9.6.
const STARTUP_MS: f64 = 500.0;
const FIRST_ROWS_MS: f64 = 100.0;
/// one frame at 120 Hz
const FRAME_MS: f64 = 1000.0 / 120.0;
const MEMORY_MB: f64 = 200.0;

fn results_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../e2e/bench-results")
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

/// Resident memory of this process in MB (working set on Windows).
fn memory_mb() -> f64 {
    let pid = std::process::id().to_string();
    let out = if cfg!(windows) {
        Command::new("powershell").args(["-NoProfile", "-Command", &format!("(Get-Process -Id {pid}).WorkingSet64")]).output()
    } else {
        Command::new("ps").args(["-o", "rss=", "-p", &pid]).output()
    };
    let text = out.map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
    let v: f64 = text.parse().unwrap_or(f64::NAN);
    if cfg!(windows) {
        v / 1e6
    } else {
        v * 1024.0 / 1e6
    }
}

fn create_table(path: &Path, rows: u64) {
    if path.exists() {
        return;
    }
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let c = rusqlite::Connection::open(&tmp).unwrap();
    c.execute_batch("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; CREATE TABLE events (id INTEGER PRIMARY KEY, device TEXT, value REAL, note TEXT, created_at DATETIME);").unwrap();
    c.execute(
        "WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < ?1)
         INSERT INTO events SELECT n, 'device-' || (n % 997), n * 0.5, 'note ' || n, datetime('2024-01-01', '+' || (n % 100000) || ' seconds') FROM g",
        [rows],
    )
    .unwrap();
    drop(c);
    std::fs::rename(tmp, path).unwrap();
}

fn profile(path: &Path) -> Profile {
    Profile { id: "bench".into(), name: "bench".into(), url: format!("sqlite://{}", path.display().to_string().replace('\\', "/")), tls: Tls::default(), ssh: None }
}

/// Time from "open table" to the first batch of rows ready for the grid (the work the app does:
/// pooled connection, describe, first batch through the bounded channel).
fn first_rows_ms(rt: &tokio::runtime::Runtime, pool: &Arc<Pool>, table: &str) -> f64 {
    let mut times: Vec<f64> = (0..5)
        .map(|_| {
            rt.block_on(async {
                let def = pool.get().await.unwrap().get_definitions(&ContainerFilter::new(vec![table.into()], vec![])).await.unwrap().remove(0);
                let q = Query::Table { def: def.clone(), filter: None, sort: None };
                let started = Instant::now();
                let mut c = pool.get().await.unwrap();
                let def = c.describe(&q).await.unwrap();
                let (tx, mut rx) = tokio::sync::mpsc::channel::<ColumnBatch>(1);
                let fetch = c.fetch(&q, &def, Arc::new(BatchSizer::fixed(BATCH_ROWS)), tx);
                tokio::pin!(fetch);
                let first = tokio::select! {
                    b = rx.recv() => b,
                    _ = &mut fetch => None,
                };
                let ms = started.elapsed().as_secs_f64() * 1000.0;
                assert!(first.is_some_and(|b| b.rows > 0));
                drop(rx);
                ms
            })
        })
        .collect();
    times.sort_by(f64::total_cmp);
    times[times.len() / 2]
}

fn startup_ms() -> Option<f64> {
    let exe = std::env::current_exe().ok()?.parent()?.parent()?.join(if cfg!(windows) { "rustygrator-desktop.exe" } else { "rustygrator-desktop" });
    if !exe.exists() {
        eprintln!("APERF-1 skipped: build the app first ({} missing)", exe.display());
        return None;
    }
    let mut runs: Vec<f64> = (0..3)
        .filter_map(|_| {
            let started = Instant::now();
            let out = Command::new(&exe).env("DM_STARTUP_PROBE", "1").output().ok()?;
            let wall = started.elapsed().as_secs_f64() * 1000.0;
            let text = String::from_utf8_lossy(&out.stdout);
            text.lines().find_map(|l| l.strip_prefix("first-frame-ms ")).and_then(|v| v.trim().parse::<f64>().ok()).map(|_| wall)
        })
        .collect();
    runs.sort_by(f64::total_cmp);
    runs.get(runs.len() / 2).copied()
}

#[test]
#[ignore = "UI benchmark: cargo test --release -p rustygrator-desktop -- --ignored aperf --nocapture"]
fn aperf_ui_benchmarks() {
    let rows: u64 = std::env::var("DM_UI_BENCH_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000_000);
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/ui-bench");
    std::fs::create_dir_all(&dir).unwrap();
    let small = dir.join("small-10000.db");
    let big = dir.join(format!("big-{rows}.db"));
    create_table(&small, 10_000);
    create_table(&big, rows);

    let startup = startup_ms();
    let (mut app, _) = App::boot();
    let rt = app.rt.clone();
    let small_pool = Arc::new(Pool::new(&profile(&small), None).unwrap());
    let big_pool = Arc::new(Pool::new(&profile(&big), None).unwrap());
    let first_small = first_rows_ms(&rt, &small_pool, "events");
    let first_big = first_rows_ms(&rt, &big_pool, "events");

    // the big table in a tab, scrolled through with the wheel; batches are fetched when the grid
    // asks for them, exactly as GridView does in the app
    app.store.profiles = vec![profile(&big)];
    let def = Arc::new(rt.block_on(async { big_pool.get().await.unwrap().get_definitions(&ContainerFilter::default()).await.unwrap().remove(0) }));
    app.conns = vec![crate::Conn { id: "bench".into(), pool: Some(big_pool.clone()), status: None, expanded: true, defs: Some(vec![def.clone()]), error: None, open: Default::default() }];
    app.rebuild_sidebar();
    let id = app.new_tab("bench".into(), "events".into(), TabKind::Table);
    let q = Query::Table { def: (*def).clone(), filter: None, sort: None };
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ColumnBatch>(1);
    {
        let (pool, q, def) = (big_pool.clone(), q.clone(), def.clone());
        rt.spawn(async move {
            let mut c = pool.get().await.unwrap();
            let _ = c.fetch(&q, &def, Arc::new(BatchSizer::fixed(BATCH_ROWS)), tx).await;
        });
    }
    let tab = app.tab_mut(id).unwrap();
    tab.def = Some(def.clone());
    tab.results = Some(ResultWindow::new(def.clone()));
    app.main = Some(iced::window::Id::unique());
    let memory_before = memory_mb();

    let size = Size::new(1280.0, 800.0);
    let mut renderer = futures_block_on(iced::Renderer::new(iced::Font::DEFAULT, iced::Pixels(16.0), None)).expect("headless renderer");
    let theme = app.tokens().theme();
    let style = iced::advanced::renderer::Style { text_color: app.tokens().text };
    let cursor = mouse::Cursor::Available(Point::new(700.0, 400.0));
    let mut cache = user_interface::Cache::default();
    let mut frames = Vec::new();
    let mut pending: Vec<Message> = Vec::new();
    // 120 lines per wheel step: a fast scroll through the table
    let wheel = Event::Mouse(mouse::Event::WheelScrolled { delta: mouse::ScrollDelta::Lines { x: 0.0, y: -40.0 } });
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        // batches the grid asked for arrive from the database stream
        let wants = app.tab(id).and_then(|t| t.results.as_ref().map(|r| (r.wants_more(t.view.1), r.complete))).unwrap();
        if wants.0 {
            match rt.block_on(rx.recv()) {
                Some(b) => {
                    let tab = app.tab_mut(id).unwrap();
                    let keep = tab.view.0;
                    tab.results.as_mut().unwrap().push(b, keep);
                }
                None => app.tab_mut(id).unwrap().results.as_mut().unwrap().finish(),
            }
            continue;
        }
        let started = Instant::now();
        let mut ui = UserInterface::build(app.view(app.main.unwrap()), size, cache, &mut renderer);
        let mut messages = Vec::new();
        let _ = ui.update(std::slice::from_ref(&wheel), cursor, &mut renderer, &mut clipboard::Null, &mut messages);
        ui.draw(&mut renderer, &theme, &style, cursor);
        cache = ui.into_cache();
        frames.push(started.elapsed().as_secs_f64() * 1000.0);
        pending.extend(messages);
        for m in pending.drain(..) {
            if let Message::GridView(_, first, last) = m {
                let tab = app.tab_mut(id).unwrap();
                tab.view = (first, last);
            }
        }
        let (view_last, complete, known) = app.tab(id).map(|t| (t.view.1, t.results.as_ref().unwrap().complete, t.results.as_ref().unwrap().known_rows)).unwrap();
        if (complete && view_last + 1 >= known) || Instant::now() > deadline {
            break;
        }
    }
    let memory = memory_mb();
    let known = app.tab(id).unwrap().results.as_ref().unwrap().known_rows;
    let first_frame_ms = frames.first().copied().unwrap_or_default();
    let slow_frames = frames.iter().skip(1).filter(|f| **f > FRAME_MS).count();
    frames.sort_by(f64::total_cmp);
    let report = UiReport {
        rows: known,
        startup_ms: startup,
        first_rows_small_ms: first_small,
        first_rows_big_ms: first_big,
        frame_p50_ms: percentile(&frames, 0.5),
        frame_p95_ms: percentile(&frames, 0.95),
        frame_max_ms: *frames.last().unwrap(),
        frames: frames.len(),
        first_frame_ms,
        slow_frames,
        memory_mb: memory,
    };
    println!("{report:#?}\nmemory before opening the table: {memory_before:.1} MB");
    std::fs::create_dir_all(results_dir()).unwrap();
    std::fs::write(results_dir().join("ui.json"), serde_json::to_vec_pretty(&report).unwrap()).unwrap();

    let mut failures = Vec::new();
    if known < rows {
        failures.push(format!("scrolled through {known} of {rows} rows"));
    }
    if let Some(s) = report.startup_ms.filter(|s| *s > STARTUP_MS) {
        failures.push(format!("APERF-1 start {s:.0} ms > {STARTUP_MS} ms"));
    }
    if report.first_rows_big_ms > FIRST_ROWS_MS {
        failures.push(format!("APERF-2 first rows {:.1} ms > {FIRST_ROWS_MS} ms", report.first_rows_big_ms));
    }
    if report.slow_frames > 0 {
        failures.push(format!("APERF-3 {} frames over {FRAME_MS:.2} ms after the first one", report.slow_frames));
    }
    if report.frame_p95_ms > FRAME_MS {
        failures.push(format!("APERF-3 frame p95 {:.2} ms > {FRAME_MS:.2} ms", report.frame_p95_ms));
    }
    if report.memory_mb > MEMORY_MB {
        failures.push(format!("APERF-4 memory {:.0} MB > {MEMORY_MB} MB", report.memory_mb));
    }
    // APERF-5: regressions against the baseline
    let baseline_path = results_dir().join("ui-baseline.json");
    if std::env::var("DM_UI_BENCH_SAVE_BASELINE").is_ok() {
        std::fs::write(&baseline_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    } else if let Ok(b) = std::fs::read(&baseline_path) {
        let base: UiReport = serde_json::from_slice(&b).unwrap();
        failures.extend(regressions(&report, &base));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Numbers more than 10 % worse than the baseline (differences below 1 ms / 5 MB are noise).
pub fn regressions(r: &UiReport, base: &UiReport) -> Vec<String> {
    let mut out = Vec::new();
    let mut check = |name: &str, now: f64, then: f64, noise: f64| {
        if now > then * 1.10 && now - then > noise {
            out.push(format!("{name}: {now:.2} vs baseline {then:.2}"));
        }
    };
    if let (Some(n), Some(b)) = (r.startup_ms, base.startup_ms) {
        check("startup ms", n, b, 1.0);
    }
    check("first rows (big) ms", r.first_rows_big_ms, base.first_rows_big_ms, 1.0);
    check("frame p95 ms", r.frame_p95_ms, base.frame_p95_ms, 1.0);
    check("memory MB", r.memory_mb, base.memory_mb, 5.0);
    out
}

fn futures_block_on<F: std::future::Future>(f: F) -> F::Output {
    iced_test::futures::futures::executor::block_on(f)
}

#[test]
fn aperf5_regression_check() {
    let base = UiReport { startup_ms: Some(200.0), first_rows_big_ms: 10.0, frame_p95_ms: 3.0, memory_mb: 100.0, ..UiReport::default() };
    assert!(regressions(&base, &base).is_empty());
    let worse = UiReport { startup_ms: Some(260.0), memory_mb: 103.0, frame_p95_ms: 3.5, ..base };
    let r = regressions(&worse, &UiReport { startup_ms: Some(200.0), first_rows_big_ms: 10.0, frame_p95_ms: 3.0, memory_mb: 100.0, ..UiReport::default() });
    assert_eq!(r.len(), 1, "{r:?}");
    assert!(r[0].starts_with("startup"));
}
