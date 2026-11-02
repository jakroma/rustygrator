//! Rows of an open table or query result (VIEW-2, VIEW-3): a window of column batches around
//! the visible rows. Batches arrive in order from the database stream; rows far above the view
//! are dropped, and scrolling back to them restarts the stream and skips to them.

use std::{collections::VecDeque, sync::Arc};

use rustygrator_core::{dbc::batch::ColumnBatch, models::data_definitions::DataDefinition};

/// Rows fetched ahead of the last visible row.
pub const PREFETCH_ROWS: u64 = 2_000;
/// Rows kept in memory around the view.
pub const WINDOW_ROWS: u64 = 50_000;
/// Rows per fetched batch.
pub const BATCH_ROWS: usize = 1_000;

#[derive(Debug)]
pub struct ResultWindow {
    pub def: Arc<DataDefinition>,
    /// Contiguous batches, `starts[i]` is the absolute index of the first row of `batches[i]`.
    batches: VecDeque<Arc<ColumnBatch>>,
    starts: VecDeque<u64>,
    /// Rows seen in the current stream (also the rows skipped after a restart).
    streamed: u64,
    /// Highest row count ever reached; stays when the stream restarts.
    pub known_rows: u64,
    /// The stream ended: `known_rows` is the total.
    pub complete: bool,
    /// After a restart, rows before this index are skipped.
    skip_until: u64,
    max_rows: u64,
}

impl ResultWindow {
    pub fn new(def: Arc<DataDefinition>) -> Self {
        Self::with_limit(def, WINDOW_ROWS)
    }

    pub fn with_limit(def: Arc<DataDefinition>, max_rows: u64) -> Self {
        ResultWindow { def, batches: VecDeque::new(), starts: VecDeque::new(), streamed: 0, known_rows: 0, complete: false, skip_until: 0, max_rows }
    }

    pub fn first_row(&self) -> u64 {
        self.starts.front().copied().unwrap_or(self.streamed)
    }

    pub fn end_row(&self) -> u64 {
        self.streamed
    }

    pub fn memory_rows(&self) -> u64 {
        self.batches.iter().map(|b| b.rows as u64).sum()
    }

    /// Adds the next batch of the stream; `keep_from` is the first visible row (rows from there
    /// on are not dropped).
    pub fn push(&mut self, batch: ColumnBatch, keep_from: u64) {
        let start = self.streamed;
        self.streamed += batch.rows as u64;
        self.known_rows = self.known_rows.max(self.streamed);
        if self.streamed <= self.skip_until {
            return;
        }
        self.starts.push_back(start);
        self.batches.push_back(Arc::new(batch));
        while self.memory_rows() > self.max_rows && self.starts.get(1).is_some_and(|&next| next <= keep_from) {
            self.starts.pop_front();
            self.batches.pop_front();
        }
    }

    pub fn finish(&mut self) {
        self.complete = true;
    }

    /// Batch and row inside it for an absolute row index.
    pub fn row(&self, index: u64) -> Option<(&ColumnBatch, usize)> {
        if index < self.first_row() || index >= self.streamed {
            return None;
        }
        let i = self.starts.partition_point(|&s| s <= index) - 1;
        let b = &self.batches[i];
        let r = (index - self.starts[i]) as usize;
        (r < b.rows).then_some((b.as_ref(), r))
    }

    /// The stream must deliver more rows for the view `first..=last`.
    pub fn wants_more(&self, last_visible: u64) -> bool {
        !self.complete && last_visible + PREFETCH_ROWS >= self.streamed
    }

    /// Rows above the view were dropped: the stream has to start again.
    pub fn needs_restart(&self, first_visible: u64) -> bool {
        first_visible < self.first_row()
    }

    /// Prepares for a new stream that skips to `first_visible` (minus some rows above it).
    pub fn restart(&mut self, first_visible: u64) {
        self.batches.clear();
        self.starts.clear();
        self.streamed = 0;
        self.complete = false;
        self.skip_until = first_visible.saturating_sub(PREFETCH_ROWS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustygrator_core::models::{data_definitions::DataPropertyInfo, value::{LogicalType, Value}};

    fn def() -> Arc<DataDefinition> {
        Arc::new(DataDefinition::new(
            "t".into(),
            vec![DataPropertyInfo {
                property_name: "id".into(),
                data_type: LogicalType::Int64,
                source_type: None,
                is_nullable: false,
                is_identifier: true,
                reference_container_name: None,
                reference_property_name: None,
                shape: None,
            }],
        ))
    }

    fn batch(from: i64, rows: i64) -> ColumnBatch {
        let mut b = ColumnBatch::new(&[LogicalType::Int64], rows as usize);
        for i in from..from + rows {
            b.push_row(vec![Value::I64(i)]).unwrap();
        }
        b
    }

    fn id(w: &ResultWindow, row: u64) -> Option<String> {
        w.row(row).map(|(b, r)| b.cell(r, 0).to_text().into_owned())
    }

    #[test]
    fn view3_memory_stays_bounded_while_scrolling_down() {
        let mut w = ResultWindow::with_limit(def(), 3_000);
        for k in 0..20 {
            w.push(batch(k * 1000, 1000), (k as u64) * 1000);
            assert!(w.memory_rows() <= 4_000, "window grows: {}", w.memory_rows());
        }
        assert_eq!(w.known_rows, 20_000);
        assert_eq!(id(&w, 19_999).as_deref(), Some("19999"));
        assert_eq!(id(&w, 0), None);
        assert!(w.needs_restart(5));
    }

    #[test]
    fn view2_fetches_ahead_of_the_view_only() {
        let mut w = ResultWindow::new(def());
        assert!(w.wants_more(0));
        for k in 0..3 {
            w.push(batch(k * 1000, 1000), 0);
        }
        assert!(!w.wants_more(500), "3000 rows loaded, view at 500");
        assert!(w.wants_more(1500));
        w.finish();
        assert!(!w.wants_more(2999));
        assert_eq!(id(&w, 1234).as_deref(), Some("1234"));
    }

    #[test]
    fn view3_restart_skips_to_the_view_and_keeps_the_total() {
        let mut w = ResultWindow::with_limit(def(), 3_000);
        for k in 0..10 {
            w.push(batch(k * 1000, 1000), k as u64 * 1000);
        }
        w.finish();
        w.restart(5_500);
        assert_eq!(w.known_rows, 10_000);
        for k in 0..10 {
            w.push(batch(k * 1000, 1000), 5_500);
        }
        assert_eq!(id(&w, 5_500).as_deref(), Some("5500"));
        assert_eq!(id(&w, 1_000), None, "rows far above the view are skipped");
        assert!(w.memory_rows() <= 6_000);
    }
}
