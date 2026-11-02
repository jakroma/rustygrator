//! Adaptive batch size (PERF-16): the size is tuned from the measured throughput (hill
//! climbing) and capped by the memory of a batch, within the configured limits.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use crate::models::migration_execution_settings::BatchSettings;

const WINDOW_BATCHES: usize = 8;
const WINDOW_TIME: Duration = Duration::from_millis(300);
const STEP: f64 = 1.5;

struct Tuning {
    window_rows: usize,
    window_batches: usize,
    window_start: Instant,
    last_rate: f64,
    grow: bool,
}

/// Shared by the provider (asks for the size of the next batch) and the transfer stage that
/// observes how fast batches are taken by the receiver.
pub struct BatchSizer {
    current: AtomicUsize,
    min: usize,
    max: usize,
    max_bytes: usize,
    adaptive: bool,
    tuning: Mutex<Tuning>,
}

impl BatchSizer {
    pub fn new(initial: usize, settings: &BatchSettings) -> Self {
        let min = settings.min_rows.max(1);
        let max = settings.max_rows.max(min);
        BatchSizer {
            current: AtomicUsize::new(initial.clamp(min, max)),
            min,
            max,
            max_bytes: settings.max_bytes.max(1),
            adaptive: settings.adaptive,
            tuning: Mutex::new(Tuning { window_rows: 0, window_batches: 0, window_start: Instant::now(), last_rate: 0.0, grow: true }),
        }
    }

    /// Fixed size, no tuning.
    pub fn fixed(size: usize) -> Self {
        let s = BatchSettings { adaptive: false, min_rows: size.max(1), max_rows: size.max(1), max_bytes: usize::MAX };
        Self::new(size, &s)
    }

    /// Rows for the next batch.
    pub fn next(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Called for every batch handed to the receiver.
    pub fn observe(&self, rows: usize, bytes: usize) {
        if !self.adaptive || rows == 0 {
            return;
        }
        let current = self.next();
        if bytes > self.max_bytes {
            let shrunk = ((current as u128 * self.max_bytes as u128) / bytes as u128) as usize;
            self.current.store(shrunk.clamp(self.min, self.max), Ordering::Relaxed);
            return;
        }
        let Ok(mut t) = self.tuning.lock() else { return };
        t.window_rows += rows;
        t.window_batches += 1;
        let elapsed = t.window_start.elapsed();
        if t.window_batches < WINDOW_BATCHES && elapsed < WINDOW_TIME {
            return;
        }
        let rate = t.window_rows as f64 / elapsed.as_secs_f64().max(1e-6);
        t.window_rows = 0;
        t.window_batches = 0;
        t.window_start = Instant::now();
        let bytes_per_row = bytes / rows;
        drop(t);
        self.adjust(rate, bytes_per_row);
    }

    /// One hill-climbing step: keep the direction while throughput improves, reverse otherwise.
    fn adjust(&self, rate: f64, bytes_per_row: usize) {
        let Ok(mut t) = self.tuning.lock() else { return };
        if t.last_rate > 0.0 && rate < t.last_rate * 0.97 {
            t.grow = !t.grow;
        }
        t.last_rate = rate;
        let current = self.next() as f64;
        let mut next = if t.grow { current * STEP } else { current / STEP } as usize;
        if bytes_per_row > 0 {
            next = next.min(self.max_bytes / bytes_per_row);
        }
        self.current.store(next.clamp(self.min, self.max), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizer() -> BatchSizer {
        BatchSizer::new(1000, &BatchSettings { adaptive: true, min_rows: 100, max_rows: 10_000, max_bytes: 1 << 20 })
    }

    #[test]
    fn grows_while_throughput_improves_and_reverses_when_it_drops() {
        let s = sizer();
        s.adjust(100.0, 10);
        assert_eq!(s.next(), 1500);
        s.adjust(200.0, 10);
        assert_eq!(s.next(), 2250);
        s.adjust(150.0, 10); // worse -> shrink
        assert_eq!(s.next(), 1500);
        s.adjust(160.0, 10); // better -> keep shrinking
        assert_eq!(s.next(), 1000);
    }

    #[test]
    fn respects_limits_and_memory_cap() {
        let s = sizer();
        for _ in 0..20 {
            s.adjust(1e9, 10);
        }
        assert_eq!(s.next(), 10_000);
        // 1 KiB per row: at most 1024 rows fit into 1 MiB
        s.adjust(2e9, 1024);
        assert!(s.next() <= 1024);
        s.observe(1000, 4 << 20);
        assert!(s.next() >= 100 && s.next() <= 256, "{}", s.next());
        let f = BatchSizer::fixed(77);
        f.observe(77, 1 << 30);
        assert_eq!(f.next(), 77);
    }
}
