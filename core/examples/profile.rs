//! PERF-9: profiles one migration and writes a flamegraph.
//!
//! cargo run --release -p rustygrator-core --example profile -- <from url> <to url> [out.svg]
//!
//! Prints the hottest functions (self time) and writes `target/flamegraph.svg` by default.

#[cfg(unix)]
use std::collections::HashMap;

#[cfg(unix)]
use rustygrator_core::{migrator::migrator::Migrator, models::{connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings}};

#[cfg(not(unix))]
fn main() {
    eprintln!("the profile example needs a Unix system (pprof)");
}

#[cfg(unix)]
#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: profile <from url> <to url> [flamegraph.svg]");
        std::process::exit(2);
    }
    let out = args.get(3).cloned().unwrap_or_else(|| "target/flamegraph.svg".into());
    let mut settings = MigrationExecutionSettings::new(4, 4096);
    settings.temp_dir = std::env::temp_dir().join("dm-profile");
    let guard = pprof::ProfilerGuardBuilder::default().frequency(999).blocklist(&["libc", "libgcc", "pthread", "vdso"]).build().unwrap();
    let mut m = Migrator::connect(Some(MigrationConnection::new(&args[1]).unwrap()), Some(MigrationConnection::new(&args[2]).unwrap()), settings)
        .await
        .unwrap();
    let summary = m.run().await.unwrap();
    let report = guard.report().build().unwrap();
    report.flamegraph(std::fs::File::create(&out).unwrap()).unwrap();

    // self time per function (leaf frame of each sample)
    let mut self_time: HashMap<String, isize> = HashMap::new();
    let mut total = 0isize;
    for (frames, count) in &report.data {
        total += *count;
        if let Some(leaf) = frames.frames.first().and_then(|f| f.first()) {
            *self_time.entry(leaf.name()).or_default() += *count;
        }
    }
    let mut top: Vec<_> = self_time.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1));
    println!("{:?}; {} samples; flamegraph: {}", summary, total, out);
    for (name, n) in top.into_iter().take(25) {
        println!("{:6.2}%  {}", n as f64 * 100.0 / total.max(1) as f64, name);
    }
}
