//! ScyllaDB change source (LIVE-15): ScyllaDB CDC (`WITH cdc = {'enabled': true}`).
//!
//! CDC is enabled before the snapshot (LIVE-4). Every poll reads the `<table>_scylla_cdc_log`
//! partitions of all CDC streams (of every generation) for the time window between the saved
//! watermark and `now - confidence window`, orders the deltas by `cdc$time` /
//! `cdc$batch_seq_no` (LIVE-8) and turns them into upserts and deletes. The watermark is the
//! checkpoint (LIVE-6) and moves only after the receiver applied the window (LIVE-7).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{stream, StreamExt, TryStreamExt};
use log::{info, warn};
use scylla::value::{CqlTimestamp, CqlValue, Row};
use tokio::{sync::watch, time::Instant};

use crate::{
    db::scylladb::{
        scylladb_data_types::{cql_scalar_value, cql_to_json, quote},
        scylladb_provider::CDC_LOG_SUFFIX,
    },
    migrator::migrator_steps::ChangeSource,
    models::{
        change::{ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::DataDefinition,
        value::{LogicalType, Value},
    },
    DMError, DMResult,
};

use super::scylladb::{sc, Scylla};

/// Deltas younger than this may still be in flight on other replicas.
const CONFIDENCE_WINDOW_MS: i64 = 2_000;
const STREAM_CONCURRENCY: usize = 64;

const OP_UPDATE: i8 = 1;
const OP_INSERT: i8 = 2;
const OP_ROW_DELETE: i8 = 3;
const OP_PARTITION_DELETE: i8 = 4;

struct CdcTable {
    def: DataDefinition,
    /// non-key columns (their `cdc$deleted_<col>` flags are selected too)
    regular: Vec<String>,
    keys: Vec<String>,
    partition_only_key: bool,
}

pub(crate) struct ScyllaCdcState {
    tables: Vec<CdcTable>,
    streams: Vec<Vec<u8>>,
    watermark: i64,
    pending: Option<i64>,
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// 100 ns ticks of a version 1 (time) UUID, for ordering.
pub fn timeuuid_ticks(u: &uuid::Uuid) -> u64 {
    let b = u.as_bytes();
    let low = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as u64;
    let mid = u16::from_be_bytes([b[4], b[5]]) as u64;
    let hi = (u16::from_be_bytes([b[6], b[7]]) & 0x0fff) as u64;
    (hi << 48) | (mid << 32) | low
}

pub fn timeuuid_millis(u: &uuid::Uuid) -> i64 {
    ((timeuuid_ticks(u) as i64) - 0x01B2_1DD2_1381_4000) / 10_000
}

/// `system_distributed` is replicated with its own factor (3): CDC metadata is read with ONE, like
/// Scylla's own CDC clients, so it also works on clusters with fewer nodes than that.
fn system_query(cql: &str) -> scylla::statement::Statement {
    let mut s = scylla::statement::Statement::new(cql);
    s.set_consistency(scylla::statement::Consistency::One);
    s
}

impl Scylla {
    fn cdc(&mut self) -> DMResult<&mut ScyllaCdcState> {
        self.cdc.as_mut().ok_or_else(|| DMError::Error("change capture not started".to_string()))
    }

    async fn cdc_streams(&self) -> DMResult<Vec<Vec<u8>>> {
        let gens = self
            .session
            .query_unpaged(system_query("SELECT time FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps'"), ())
            .await
            .map_err(sc)?
            .into_rows_result()
            .map_err(sc)?;
        let times: Vec<CqlTimestamp> = gens.rows::<(CqlTimestamp,)>().map_err(sc)?.map(|r| r.map(|t| t.0)).collect::<Result<_, _>>().map_err(sc)?;
        let mut streams = Vec::new();
        for t in times {
            let rows = self
                .session
                .query_unpaged(system_query("SELECT streams FROM system_distributed.cdc_streams_descriptions_v2 WHERE time = ?"), (t,))
                .await
                .map_err(sc)?
                .into_rows_result()
                .map_err(sc)?;
            for r in rows.rows::<(Vec<Vec<u8>>,)>().map_err(sc)? {
                streams.extend(r.map_err(sc)?.0);
            }
        }
        streams.sort();
        streams.dedup();
        if streams.is_empty() {
            return Err(DMError::Error("ScyllaDB CDC: no CDC streams (is the keyspace using tablets? CDC needs vnodes)".to_string()));
        }
        Ok(streams)
    }

    async fn read_window(&self, from: i64, upto: i64) -> DMResult<Vec<ChangeEvent>> {
        let state = self.cdc.as_ref().ok_or_else(|| DMError::Error("change capture not started".to_string()))?;
        let mut all: Vec<(u64, i32, ChangeEvent)> = Vec::new();
        for t in &state.tables {
            let mut cols = vec!["\"cdc$time\"".to_string(), "\"cdc$batch_seq_no\"".to_string(), "\"cdc$operation\"".to_string()];
            cols.extend(t.keys.iter().map(|k| quote(k)));
            cols.extend(t.regular.iter().map(|c| quote(c)));
            cols.extend(t.regular.iter().map(|c| quote(&format!("cdc$deleted_{}", c))));
            let mut prepared = self
                .session
                .prepare(format!(
                    "SELECT {} FROM {}.{} WHERE \"cdc$stream_id\" = ? AND \"cdc$time\" > maxTimeuuid(?) AND \"cdc$time\" <= maxTimeuuid(?)",
                    cols.join(", "),
                    quote(&self.keyspace),
                    quote(&format!("{}{}", t.def.container_name, CDC_LOG_SUFFIX))
                ))
                .await
                .map_err(sc)?;
            prepared.set_consistency(scylla::statement::Consistency::LocalQuorum);
            type RowsFuture = std::pin::Pin<Box<dyn std::future::Future<Output = DMResult<Vec<Row>>> + Send>>;
            let mut futures: Vec<RowsFuture> = Vec::with_capacity(state.streams.len());
            for s in &state.streams {
                let (p, session, s) = (prepared.clone(), self.session.clone(), s.clone());
                futures.push(Box::pin(async move {
                    let r = session
                        .execute_unpaged(&p, (s, CqlTimestamp(from), CqlTimestamp(upto)))
                        .await
                        .map_err(sc)?
                        .into_rows_result()
                        .map_err(sc)?;
                    r.rows::<Row>().map_err(sc)?.collect::<Result<Vec<Row>, _>>().map_err(sc)
                }));
            }
            let rows: Vec<Row> = stream::iter(futures).buffer_unordered(STREAM_CONCURRENCY).try_concat().await?;
            let types = |name: &str| t.def.column_index(name).map(|i| t.def.properties_info[i].data_type).unwrap_or(LogicalType::String);
            for row in rows {
                let mut it = row.columns.into_iter();
                let time = match it.next().flatten() {
                    Some(CqlValue::Timeuuid(u)) => *u.as_ref(),
                    _ => continue,
                };
                let seq = match it.next().flatten() {
                    Some(CqlValue::Int(i)) => i,
                    _ => 0,
                };
                let op = match it.next().flatten() {
                    Some(CqlValue::TinyInt(o)) => o,
                    _ => continue,
                };
                let convert = |name: &str, v: CqlValue| -> Value {
                    if types(name) == LogicalType::Json {
                        Value::Json(cql_to_json(&v))
                    } else {
                        cql_scalar_value(&v)
                    }
                };
                let mut row_values: Vec<(String, Value)> = Vec::new();
                for k in &t.keys {
                    row_values.push((k.clone(), it.next().flatten().map(|v| convert(k, v)).unwrap_or(Value::Null)));
                }
                let regular: Vec<Option<CqlValue>> = t.regular.iter().map(|_| it.next().flatten()).collect();
                let deleted: Vec<bool> = t.regular.iter().map(|_| matches!(it.next().flatten(), Some(CqlValue::Boolean(true)))).collect();
                let source_time = DateTime::from_timestamp_millis(timeuuid_millis(&time));
                let event = match op {
                    OP_UPDATE | OP_INSERT => {
                        for ((name, v), del) in t.regular.iter().zip(regular).zip(deleted) {
                            match (v, del) {
                                (Some(v), _) => row_values.push((name.clone(), convert(name, v))),
                                (None, true) => row_values.push((name.clone(), Value::Null)),
                                (None, false) => {}
                            }
                        }
                        ChangeEvent { container: t.def.container_name.clone(), op: ChangeOp::Upsert, row: row_values, source_time, complete: false }
                    }
                    OP_ROW_DELETE => ChangeEvent { container: t.def.container_name.clone(), op: ChangeOp::Delete, row: row_values, source_time, complete: false },
                    OP_PARTITION_DELETE if t.partition_only_key => {
                        row_values.retain(|(k, _)| t.def.partition_key_names().contains(&k.as_str()));
                        ChangeEvent { container: t.def.container_name.clone(), op: ChangeOp::Delete, row: row_values, source_time, complete: false }
                    }
                    OP_PARTITION_DELETE => {
                        warn!("[Live][{}] partition delete with clustering rows is not replicated", t.def.container_name);
                        continue;
                    }
                    // pre / post images and range deletes
                    _ => continue,
                };
                all.push((timeuuid_ticks(&time), seq, event));
            }
        }
        all.sort_by_key(|(t, s, _)| (*t, *s));
        Ok(all.into_iter().map(|(_, _, e)| e).collect())
    }
}

#[async_trait]
impl ChangeSource for Scylla {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        let mut tables = Vec::new();
        for def in defs {
            if def.identifier_indexes().is_empty() {
                continue;
            }
            info!("[Live][{}] enabling ScyllaDB CDC", def.container_name);
            self.session
                .query_unpaged(format!("ALTER TABLE {} WITH cdc = {{'enabled': true}}", self.table(&def.container_name)), ())
                .await
                .map_err(sc)?;
            let keys: Vec<String> = def.identifier_names().iter().map(|s| s.to_string()).collect();
            let regular = def.properties_info.iter().filter(|p| !p.is_identifier).map(|p| p.property_name.clone()).collect();
            let partition_only_key = def.partition_key_names().len() == keys.len();
            tables.push(CdcTable { def: def.clone(), regular, keys, partition_only_key });
        }
        let watermark = match resume {
            Some(Checkpoint::Scylla { watermark_ms }) => *watermark_ms,
            _ => now_ms(),
        };
        let streams = self.cdc_streams().await?;
        info!("[Live] ScyllaDB CDC: {} streams, from {}", streams.len(), watermark);
        self.cdc = Some(ScyllaCdcState { tables, streams, watermark, pending: None });
        Ok(())
    }

    async fn poll_changes(&mut self, max: usize, wait: Duration, shutdown: &watch::Receiver<bool>) -> DMResult<Vec<ChangeEvent>> {
        let deadline = Instant::now() + wait;
        loop {
            let from = self.cdc()?.pending.unwrap_or(self.cdc()?.watermark);
            let upto = now_ms() - CONFIDENCE_WINDOW_MS;
            if upto > from {
                let events = self.read_window(from, upto).await?;
                self.cdc()?.pending = Some(upto);
                if !events.is_empty() || events.len() >= max {
                    return Ok(events);
                }
            }
            if Instant::now() >= deadline || *shutdown.borrow() {
                return Ok(Vec::new());
            }
            tokio::time::sleep(deadline.saturating_duration_since(Instant::now()).min(Duration::from_millis(250))).await;
        }
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        let s = self.cdc()?;
        if let Some(p) = s.pending.take() {
            s.watermark = p;
        }
        Ok(Some(Checkpoint::Scylla { watermark_ms: s.watermark }))
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Scylla::reconnect(self).await?;
        // re-read the window that was not acknowledged
        self.cdc()?.pending = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeuuid_time() {
        // 2024-01-02T03:04:05.678Z as a version 1 uuid
        let ms = 1_704_164_645_678i64;
        let ticks = (ms * 10_000 + 0x01B2_1DD2_1381_4000) as u64;
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&(ticks as u32).to_be_bytes());
        b[4..6].copy_from_slice(&((ticks >> 32) as u16).to_be_bytes());
        b[6..8].copy_from_slice(&((((ticks >> 48) as u16) & 0x0fff) | 0x1000).to_be_bytes());
        let u = uuid::Uuid::from_bytes(b);
        assert_eq!(timeuuid_millis(&u), ms);
        assert_eq!(timeuuid_ticks(&u), ticks);
    }
}
