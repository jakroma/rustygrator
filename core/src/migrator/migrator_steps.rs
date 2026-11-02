use std::sync::Arc;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use crate::{
    dbc::batch::ColumnBatch,
    migrator::batching::BatchSizer,
    models::{
        change::{ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::DataDefinition,
        migration_execution_settings::ContainerFilter,
        value::Value,
    },
    DMResult,
};

/// Progress of one container in the current step (MAPP-2).
#[derive(Default, Debug, Clone, PartialEq)]
pub struct TableProgress {
    /// "transfer", "write file" or "load file"
    pub phase: &'static str,
    pub rows: u64,
    pub bytes: u64,
    pub done: bool,
}

/// Counters of a migration, readable while it runs (MAPP-2, MAPP-3).
#[derive(Default, Debug)]
pub struct MigrationStats {
    pub containers: AtomicU64,
    pub rows: AtomicU64,
    /// Containers in migration order (FN-5), set when the definitions are read.
    pub order: Mutex<Vec<String>>,
    pub tables: Mutex<HashMap<String, TableProgress>>,
    pub live_events: AtomicU64,
    pub live_errors: AtomicU64,
    /// Lag of the last applied batch in milliseconds, -1 before the first one.
    pub live_lag_ms: AtomicI64,
    pub live_checkpoint: Mutex<Option<String>>,
}

impl MigrationStats {
    pub fn new() -> Self {
        MigrationStats { live_lag_ms: AtomicI64::new(-1), ..Default::default() }
    }

    pub fn add_container(&self, rows: u64) {
        self.containers.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (self.containers.load(Ordering::Relaxed), self.rows.load(Ordering::Relaxed))
    }

    pub fn set_order(&self, names: Vec<String>) {
        *self.order.lock().unwrap_or_else(|e| e.into_inner()) = names;
    }

    pub fn table_started(&self, name: &str, phase: &'static str) {
        self.tables.lock().unwrap_or_else(|e| e.into_inner()).insert(name.to_string(), TableProgress { phase, ..Default::default() });
    }

    pub fn table_batch(&self, name: &str, rows: usize, bytes: usize) {
        if let Some(t) = self.tables.lock().unwrap_or_else(|e| e.into_inner()).get_mut(name) {
            t.rows += rows as u64;
            t.bytes += bytes as u64;
        }
    }

    pub fn table_done(&self, name: &str) {
        if let Some(t) = self.tables.lock().unwrap_or_else(|e| e.into_inner()).get_mut(name) {
            t.done = true;
        }
    }
}

/// Reads definitions and data from the source database.
#[async_trait]
pub trait DataProvider: Send {
    /// Definitions of all containers accepted by the filter (NoSQL sources analyze data first).
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>>;

    /// Streams all records of one container as column batches; every batch has
    /// `batch.next()` rows (asked again for each batch, PERF-16). Returns the number of rows.
    async fn read_data(
        &mut self,
        def: &DataDefinition,
        sizer: Arc<BatchSizer>,
        tx: mpsc::Sender<ColumnBatch>,
    ) -> DMResult<u64>;
}

/// Imports definitions and data into the target database.
#[async_trait]
pub trait DataReceiver: Send {
    /// Creates the target containers (without constraints that would slow the load down).
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()>;

    /// Writes all batches received from `rx` into the container. Re-running it with the same
    /// data must not create duplicates for containers with an identifier (idempotent load).
    async fn write_data(&mut self, def: &DataDefinition, rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64>;

    /// Foreign keys, indexes and (SQL -> NoSQL) denormalization after all data is loaded.
    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()>;
}

/// Change data capture on the source (section 5.1).
#[async_trait]
pub trait ChangeSource: Send {
    /// Starts capturing changes. Called *before* the initial snapshot so no change made during the
    /// snapshot is lost (LIVE-4). With `resume` it continues from a saved checkpoint (LIVE-6).
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()>;

    /// Returns at most about `max` events, waiting up to `wait` for the batch to fill (LIVE-10).
    /// Events are returned in source commit order (LIVE-8). Events stay unacknowledged until
    /// [`ChangeSource::acknowledge`] is called (at-least-once, LIVE-7).
    async fn poll_changes(
        &mut self,
        max: usize,
        wait: Duration,
        shutdown: &watch::Receiver<bool>,
    ) -> DMResult<Vec<ChangeEvent>>;

    /// Marks everything returned so far as applied; returns the checkpoint to persist.
    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>>;

    /// Re-establishes the connection after a failure (LIVE-12).
    async fn reconnect(&mut self) -> DMResult<()>;
}

/// Applies change events to the target database (LIVE-5).
#[async_trait]
pub trait ChangeReceiver: Send {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()>;

    async fn reconnect(&mut self) -> DMResult<()>;
}

/// Changes of one container after coalescing a batch.
pub struct ContainerChanges<'a> {
    pub def: &'a DataDefinition,
    /// Rows aligned with the definition; `None` = column not present in the change (keep the
    /// current target value).
    pub upserts: Vec<Vec<Option<Value>>>,
    /// Identifier values (aligned with `def.identifier_indexes()`) of deleted records.
    pub deletes: Vec<Vec<Value>>,
}

impl ContainerChanges<'_> {
    pub fn is_partial(row: &[Option<Value>]) -> bool {
        row.iter().any(|v| v.is_none())
    }
}

fn key_string(values: &[&Value]) -> String {
    let mut s = String::new();
    for v in values {
        s.push_str(&v.to_text());
        s.push('\u{1f}');
    }
    s
}

/// Coalesces a batch of events: the last change per record wins (so per-record order is kept,
/// LIVE-8, and applying it twice gives the same result, LIVE-7). Containers are returned in the
/// order of `defs`, which is the dependency order of the migration.
pub fn coalesce_changes<'a>(defs: &'a [DataDefinition], events: &[ChangeEvent]) -> DMResult<Vec<ContainerChanges<'a>>> {
    enum Pending {
        Upsert(Vec<Option<Value>>),
        Delete(Vec<Value>),
    }
    let index: HashMap<&str, usize> = defs.iter().enumerate().map(|(i, d)| (d.container_name.as_str(), i)).collect();
    // per container: key -> position in `order`, plus insertion ordered entries
    let mut per: Vec<(HashMap<String, usize>, Vec<Pending>)> = (0..defs.len()).map(|_| (HashMap::new(), Vec::new())).collect();

    for ev in events {
        let Some(&di) = index.get(ev.container.as_str()) else { continue };
        let def = &defs[di];
        let id_idx = def.identifier_indexes();
        if id_idx.is_empty() {
            continue;
        }
        let mut aligned: Vec<Option<Value>> = vec![None; def.properties_info.len()];
        for (name, value) in &ev.row {
            if let Some(ci) = def.column_index(name) {
                aligned[ci] = Some(value.clone().coerce(def.properties_info[ci].data_type)?);
            }
        }
        let key_vals: Vec<&Value> = match id_idx.iter().map(|&i| aligned[i].as_ref()).collect::<Option<Vec<_>>>() {
            Some(k) => k,
            None => continue, // event without a complete key cannot be applied
        };
        let key = key_string(&key_vals);
        let (keys, entries) = &mut per[di];
        let new_entry = match ev.op {
            ChangeOp::Delete => Pending::Delete(id_idx.iter().map(|&i| aligned[i].clone().unwrap_or(Value::Null)).collect()),
            ChangeOp::Upsert if ev.complete => Pending::Upsert(aligned.into_iter().map(|v| Some(v.unwrap_or(Value::Null))).collect()),
            ChangeOp::Upsert => Pending::Upsert(aligned),
        };
        match keys.get(&key) {
            Some(&pos) => {
                let merged = match (&mut entries[pos], new_entry) {
                    (Pending::Upsert(old), Pending::Upsert(new)) => {
                        for (o, n) in old.iter_mut().zip(new) {
                            if n.is_some() {
                                *o = n;
                            }
                        }
                        None
                    }
                    (_, other) => Some(other),
                };
                if let Some(m) = merged {
                    entries[pos] = m;
                }
            }
            None => {
                keys.insert(key, entries.len());
                entries.push(new_entry);
            }
        }
    }

    let mut out = Vec::new();
    for (di, (_, entries)) in per.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let mut changes = ContainerChanges { def: &defs[di], upserts: Vec::new(), deletes: Vec::new() };
        for e in entries {
            match e {
                Pending::Upsert(r) => changes.upserts.push(r),
                Pending::Delete(k) => changes.deletes.push(k),
            }
        }
        out.push(changes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{data_definitions::DataPropertyInfo, value::LogicalType};

    fn def() -> Vec<DataDefinition> {
        let p = |n: &str, t: LogicalType, id: bool| DataPropertyInfo {
            property_name: n.into(),
            data_type: t,
            source_type: None,
            is_nullable: !id,
            is_identifier: id,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,

        };
        vec![DataDefinition::new("t".into(), vec![p("id", LogicalType::Int32, true), p("name", LogicalType::String, false)])]
    }

    fn ev(op: ChangeOp, id: i64, name: Option<&str>) -> ChangeEvent {
        let mut row = vec![("id".to_string(), Value::I64(id))];
        if let Some(n) = name {
            row.push(("name".into(), Value::String(n.into())));
        }
        ChangeEvent { container: "t".into(), op, row, source_time: None, complete: false }
    }

    #[test]
    fn last_change_per_record_wins() {
        let defs = def();
        let events = vec![
            ev(ChangeOp::Upsert, 1, Some("a")),
            ev(ChangeOp::Upsert, 2, Some("b")),
            ev(ChangeOp::Upsert, 1, Some("c")),
            ev(ChangeOp::Delete, 2, None),
            ev(ChangeOp::Upsert, 3, None),
        ];
        let c = coalesce_changes(&defs, &events).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].upserts.len(), 2);
        assert_eq!(c[0].upserts[0], vec![Some(Value::I32(1)), Some(Value::String("c".into()))]);
        assert_eq!(c[0].upserts[1], vec![Some(Value::I32(3)), None]);
        assert_eq!(c[0].deletes, vec![vec![Value::I32(2)]]);
    }

    #[test]
    fn complete_rows_fill_missing_columns_with_null() {
        let defs = def();
        let mut e = ev(ChangeOp::Upsert, 1, None);
        e.complete = true;
        let c = coalesce_changes(&defs, &[e]).unwrap();
        assert_eq!(c[0].upserts[0], vec![Some(Value::I32(1)), Some(Value::Null)]);
        assert!(!ContainerChanges::is_partial(&c[0].upserts[0]));
    }

    #[test]
    fn delete_then_insert_becomes_upsert() {
        let defs = def();
        let events = vec![ev(ChangeOp::Delete, 1, None), ev(ChangeOp::Upsert, 1, Some("x"))];
        let c = coalesce_changes(&defs, &events).unwrap();
        assert!(c[0].deletes.is_empty());
        assert_eq!(c[0].upserts.len(), 1);
    }
}
