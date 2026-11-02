//! Migration and live transfer tab (MAPP-1..MAPP-3): the job form, the running migrator and the
//! numbers shown while it runs (migration order, progress per table, rows/s, MB/s, live events
//! per second, lag chart, last checkpoint, errors).

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use iced::{
    mouse,
    widget::canvas::{self, Frame, Geometry, Path, Stroke},
    Point, Rectangle, Renderer, Theme,
};
use tokio::sync::watch;

use rustygrator_core::migrator::migrator_steps::{MigrationStats, TableProgress};

use crate::{jobs::Job, theme::Tokens};

/// Samples kept for the live chart (one per tick).
pub const SAMPLES: usize = 240;
pub const TICK: Duration = Duration::from_millis(500);

pub struct Running {
    pub stats: Arc<MigrationStats>,
    pub shutdown: watch::Sender<bool>,
    pub started: Instant,
    pub stopping: bool,
}

#[derive(Default)]
pub struct Migration {
    pub job: Job,
    pub running: Option<Running>,
    pub result: Option<Result<String, String>>,
    /// (rows, bytes, live events) at the previous tick
    last: Option<(Instant, u64, u64, u64)>,
    pub rows_per_s: f64,
    pub mb_per_s: f64,
    pub events_per_s: f64,
    /// (lag ms, events/s) per tick
    pub samples: VecDeque<(f64, f64)>,
    pub order: Vec<String>,
    pub tables: Vec<(String, TableProgress)>,
    pub live_events: u64,
    pub live_errors: u64,
    pub lag_ms: Option<i64>,
    pub checkpoint: Option<String>,
}

impl Migration {
    pub fn new(job: Job) -> Self {
        Migration { job, ..Default::default() }
    }

    /// Reads the counters of the running migrator (every [`TICK`]).
    pub fn tick(&mut self, now: Instant) {
        let Some(run) = &self.running else { return };
        let s = &run.stats;
        use std::sync::atomic::Ordering::Relaxed;
        self.order = s.order.lock().map(|o| o.clone()).unwrap_or_default();
        let tables = s.tables.lock().map(|t| t.clone()).unwrap_or_default();
        self.tables = self.order.iter().map(|n| (n.clone(), tables.get(n).cloned().unwrap_or_default())).collect();
        let rows: u64 = tables.values().map(|t| t.rows).sum();
        let bytes: u64 = tables.values().map(|t| t.bytes).sum();
        self.live_events = s.live_events.load(Relaxed);
        self.live_errors = s.live_errors.load(Relaxed);
        self.lag_ms = Some(s.live_lag_ms.load(Relaxed)).filter(|l| *l >= 0);
        self.checkpoint = s.live_checkpoint.lock().ok().and_then(|c| c.clone());
        if let Some((t, r, b, e)) = self.last {
            let dt = (now - t).as_secs_f64().max(1e-3);
            self.rows_per_s = rows.saturating_sub(r) as f64 / dt;
            self.mb_per_s = bytes.saturating_sub(b) as f64 / dt / 1e6;
            self.events_per_s = self.live_events.saturating_sub(e) as f64 / dt;
        }
        self.last = Some((now, rows, bytes, self.live_events));
        if self.job.live {
            self.samples.push_back((self.lag_ms.unwrap_or(0) as f64, self.events_per_s));
            while self.samples.len() > SAMPLES {
                self.samples.pop_front();
            }
        }
    }

    pub fn finished(&mut self, result: Result<String, String>) {
        self.running = None;
        self.last = None;
        self.result = Some(result);
    }
}

/// Line chart of the live lag (MAPP-3).
pub struct LagChart<'a> {
    pub samples: &'a VecDeque<(f64, f64)>,
    pub t: Tokens,
}

impl<Message> canvas::Program<Message> for LagChart<'_> {
    type State = ();

    fn draw(&self, _state: &(), renderer: &Renderer, _theme: &Theme, bounds: Rectangle, _cursor: mouse::Cursor) -> Vec<Geometry> {
        let mut frame = Frame::new(renderer, bounds.size());
        frame.fill_rectangle(Point::ORIGIN, bounds.size(), self.t.content);
        let max = self.samples.iter().map(|s| s.0).fold(100.0, f64::max);
        let w = bounds.width;
        let h = bounds.height;
        for k in 1..4 {
            let y = h * k as f32 / 4.0;
            frame.stroke(&Path::line(Point::new(0.0, y), Point::new(w, y)), Stroke::default().with_color(self.t.separator).with_width(1.0));
        }
        if self.samples.len() > 1 {
            let step = w / (SAMPLES - 1) as f32;
            let x0 = w - step * (self.samples.len() - 1) as f32;
            let path = Path::new(|p| {
                for (i, (lag, _)) in self.samples.iter().enumerate() {
                    let pt = Point::new(x0 + i as f32 * step, h - (*lag / max) as f32 * (h - 4.0) - 2.0);
                    if i == 0 {
                        p.move_to(pt);
                    } else {
                        p.line_to(pt);
                    }
                }
            });
            frame.stroke(&path, Stroke::default().with_color(self.t.accent).with_width(2.0));
        }
        frame.fill_text(canvas::Text {
            content: format!("max {:.0} ms", max),
            position: Point::new(6.0, 4.0),
            color: self.t.text_secondary,
            size: iced::Pixels(crate::theme::SMALL),
            ..canvas::Text::default()
        });
        vec![frame.into_geometry()]
    }
}

pub fn format_rate(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.1} M", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.1} k", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapp2_tick_reads_order_progress_and_rates() {
        let stats = Arc::new(MigrationStats::new());
        stats.set_order(vec!["a".into(), "b".into()]);
        stats.table_started("a", "transfer");
        let (tx, _rx) = watch::channel(false);
        let mut m = Migration::new(Job { live: true, ..Job::default() });
        m.running = Some(Running { stats: stats.clone(), shutdown: tx, started: Instant::now(), stopping: false });
        let t0 = Instant::now();
        m.tick(t0);
        stats.table_batch("a", 1000, 2_000_000);
        stats.live_events.fetch_add(50, std::sync::atomic::Ordering::Relaxed);
        stats.live_lag_ms.store(120, std::sync::atomic::Ordering::Relaxed);
        m.tick(t0 + Duration::from_secs(1));
        assert_eq!(m.order, ["a", "b"]);
        assert_eq!(m.tables[0].1.rows, 1000);
        assert!(!m.tables[1].1.done);
        assert!((m.rows_per_s - 1000.0).abs() < 1.0);
        assert!((m.mb_per_s - 2.0).abs() < 0.01);
        assert!((m.events_per_s - 50.0).abs() < 0.1);
        assert_eq!(m.lag_ms, Some(120));
        assert_eq!(m.samples.back(), Some(&(120.0, m.events_per_s)));
        m.finished(Ok("done".into()));
        assert!(m.running.is_none());
        assert_eq!(format_rate(12_345.0), "12.3 k");
    }
}
