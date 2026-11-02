//! Live data transfer loop (section 5.2): poll -> apply -> acknowledge -> checkpoint.

use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use chrono::Utc;
use log::{error, info, warn};
use tokio::sync::watch;

use crate::{
    dbc::format::DbcOptions,
    migrator::migrator_steps::{ChangeReceiver, ChangeSource, MigrationStats},
    models::{
        change::{ChangeEvent, ChangeOp, LiveState},
        data_definitions::DataDefinition,
        migration_execution_settings::MigrationExecutionSettings,
    },
    DMError, DMResult,
};

const BACKOFF_START: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Live statistics (LIVE-14).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiveSummary {
    pub events: u64,
    pub batches: u64,
    pub errors: u64,
    /// Lag between source commit time and apply time of the last batch, in milliseconds.
    pub last_lag_ms: Option<i64>,
}

struct Stats {
    summary: LiveSummary,
    window_start: Instant,
    window_events: u64,
    interval: Duration,
}

impl Stats {
    fn maybe_log(&mut self, force: bool) {
        let elapsed = self.window_start.elapsed();
        if !force && elapsed < self.interval {
            return;
        }
        let rate = self.window_events as f64 / elapsed.as_secs_f64().max(0.001);
        info!(
            "[Live][Stats] {:.1} events/s, {} events total, {} batches, lag {} ms, {} errors",
            rate,
            self.summary.events,
            self.summary.batches,
            self.summary.last_lag_ms.map(|l| l.to_string()).unwrap_or_else(|| "-".into()),
            self.summary.errors
        );
        self.window_start = Instant::now();
        self.window_events = 0;
    }
}

/// Sleeps with exponential backoff; returns `false` when shutdown was requested meanwhile.
async fn backoff(attempt: u32, shutdown: &mut watch::Receiver<bool>) -> bool {
    let delay = BACKOFF_START.saturating_mul(1u32 << attempt.min(10)).min(BACKOFF_MAX);
    tokio::select! {
        _ = tokio::time::sleep(delay) => !*shutdown.borrow(),
        _ = shutdown.changed() => false,
    }
}

fn give_up(what: &str, attempt: u32, max: u32, e: &DMError) -> bool {
    error!("[Live] {} failed (attempt {}/{}): {}", what, attempt, max, e);
    attempt >= max
}

/// Splits a batch where an upsert follows a delete. Receivers apply a slice as "all upserts,
/// then all deletes" (parents / children ordered for foreign keys), which keeps the source order
/// only inside such a slice: a delete followed by an insert of the same unique value (e.g. a
/// changed primary key) must not be turned around.
fn segments(events: &[ChangeEvent]) -> Vec<&[ChangeEvent]> {
    let mut out = Vec::new();
    let mut start = 0;
    for i in 1..events.len() {
        if events[i].op == ChangeOp::Upsert && events[i - 1].op == ChangeOp::Delete {
            out.push(&events[start..i]);
            start = i;
        }
    }
    if start < events.len() {
        out.push(&events[start..]);
    }
    out
}

/// What a live run reads its configuration from and reports its counters to.
pub struct LiveContext<'a> {
    pub settings: &'a MigrationExecutionSettings,
    pub dbc: &'a DbcOptions,
    pub counters: &'a MigrationStats,
}

pub async fn run_live<S, R>(
    source: &mut S,
    receiver: &mut R,
    defs: &[DataDefinition],
    ctx: LiveContext<'_>,
    mut state: LiveState,
    mut shutdown: watch::Receiver<bool>,
) -> DMResult<LiveSummary>
where
    S: ChangeSource + ?Sized,
    R: ChangeReceiver + ?Sized,
{
    let LiveContext { settings, dbc, counters } = ctx;
    let live = &settings.live;
    let checkpoint_file = settings.checkpoint_file();
    let mut stats = Stats {
        summary: LiveSummary::default(),
        window_start: Instant::now(),
        window_events: 0,
        interval: live.stats_interval,
    };
    info!("[Live] Streaming changes (batch {} events / {} ms)", live.batch_size, live.batch_wait.as_millis());

    'main: while !*shutdown.borrow() {
        // poll (LIVE-10, LIVE-12)
        let mut attempt = 0;
        let events = loop {
            match source.poll_changes(live.batch_size, live.batch_wait, &shutdown).await {
                Ok(ev) => break ev,
                Err(e) => {
                    stats.summary.errors += 1;
                    counters.live_errors.fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    if give_up("reading changes", attempt, live.max_retries, &e) {
                        return Err(e);
                    }
                    if !backoff(attempt, &mut shutdown).await {
                        break 'main;
                    }
                    if let Err(e) = source.reconnect().await {
                        warn!("[Live] reconnecting to the source failed: {}", e);
                    }
                }
            }
        };

        // apply the whole batch, retrying the same events (LIVE-7, LIVE-12)
        if !events.is_empty() {
            let mut attempt = 0;
            loop {
                let mut applied = Ok(());
                for segment in segments(&events) {
                    applied = receiver.apply_changes(defs, segment).await;
                    if applied.is_err() {
                        break;
                    }
                }
                match applied {
                    Ok(()) => break,
                    Err(e) => {
                        stats.summary.errors += 1;
                        counters.live_errors.fetch_add(1, Ordering::Relaxed);
                        attempt += 1;
                        if give_up("applying changes", attempt, live.max_retries, &e) {
                            return Err(e);
                        }
                        if !backoff(attempt, &mut shutdown).await {
                            // not acknowledged: the batch is delivered again after a restart
                            break 'main;
                        }
                        if let Err(e) = receiver.reconnect().await {
                            warn!("[Live] reconnecting to the receiver failed: {}", e);
                        }
                    }
                }
            }
            stats.summary.events += events.len() as u64;
            counters.live_events.fetch_add(events.len() as u64, Ordering::Relaxed);
            stats.summary.batches += 1;
            stats.window_events += events.len() as u64;
            if let Some(newest) = events.iter().filter_map(|e| e.source_time).max() {
                let lag = (Utc::now() - newest).num_milliseconds().max(0);
                stats.summary.last_lag_ms = Some(lag);
                counters.live_lag_ms.store(lag, Ordering::Relaxed);
            }
        }

        // acknowledge + checkpoint (LIVE-6)
        let mut attempt = 0;
        let checkpoint = loop {
            match source.acknowledge().await {
                Ok(c) => break c,
                Err(e) => {
                    stats.summary.errors += 1;
                    counters.live_errors.fetch_add(1, Ordering::Relaxed);
                    attempt += 1;
                    if give_up("saving source position", attempt, live.max_retries, &e) {
                        return Err(e);
                    }
                    if !backoff(attempt, &mut shutdown).await {
                        break 'main;
                    }
                    let _ = source.reconnect().await;
                }
            }
        };
        if checkpoint.is_some() && checkpoint != state.checkpoint {
            *counters.live_checkpoint.lock().unwrap_or_else(|e| e.into_inner()) = checkpoint.as_ref().map(|c| format!("{c:?}"));
            state.checkpoint = checkpoint;
            state.save(&checkpoint_file, dbc)?;
        }
        stats.maybe_log(false);
    }

    // graceful shutdown (LIVE-13): the last batch was applied and acknowledged above
    state.save(&checkpoint_file, dbc)?;
    stats.maybe_log(true);
    info!("[Live] Stopped, checkpoint saved to {}", checkpoint_file.display());
    Ok(stats.summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(op: ChangeOp, id: i64) -> ChangeEvent {
        ChangeEvent { container: "t".into(), op, row: vec![("id".into(), crate::models::value::Value::I64(id))], source_time: None, complete: true }
    }

    #[test]
    fn live8_delete_then_insert_is_not_reordered() {
        use ChangeOp::{Delete as D, Upsert as U};
        let events: Vec<ChangeEvent> = [(U, 1), (U, 2), (D, 3), (D, 6), (U, 60), (D, 4), (U, 5)].iter().map(|(o, i)| ev(*o, *i)).collect();
        let lens: Vec<usize> = segments(&events).iter().map(|s| s.len()).collect();
        assert_eq!(lens, [4, 2, 1]);
        assert!(segments(&[]).is_empty());
    }
}
