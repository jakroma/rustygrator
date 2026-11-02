//! SQLite change source (LIVE-3): AFTER INSERT / UPDATE / DELETE triggers write every change to
//! a changelog table which rustygrator polls. Acknowledged entries are removed.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{info, warn};
use tokio::{sync::watch, time::Instant};

use crate::{
    constants::sqlite::{CHANGELOG_TABLE, INTERNAL_PREFIX},
    migrator::migrator_steps::ChangeSource,
    models::{
        change::{ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::DataDefinition,
        value::{from_hex, LogicalType, Value},
    },
    DMError, DMResult,
};

use super::sqlite::{quote_ident, quote_literal, Sqlite};

const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) struct SqliteCdcState {
    /// container -> bytes columns (stored hex encoded in the changelog)
    blob_columns: std::collections::HashMap<String, Vec<String>>,
    last_read: i64,
    acked: i64,
}

fn json_object(prefix: &str, def: &DataDefinition, only_ids: bool) -> String {
    let mut parts = Vec::new();
    for p in &def.properties_info {
        if only_ids && !p.is_identifier {
            continue;
        }
        let col = format!("{}.{}", prefix, quote_ident(&p.property_name));
        let expr = if p.data_type == LogicalType::Bytes { format!("hex({})", col) } else { col };
        parts.push(format!("{}, {}", quote_literal(&p.property_name), expr));
    }
    format!("json_object({})", parts.join(", "))
}

/// SQL creating the changelog table and the triggers for all tables with a primary key.
pub(crate) fn capture_ddl(defs: &[DataDefinition]) -> Vec<String> {
    let mut ddl = vec![format!(
        "CREATE TABLE IF NOT EXISTS {CHANGELOG_TABLE} (\
         id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, op TEXT NOT NULL, data TEXT NOT NULL, \
         ts INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)))"
    )];
    for def in defs {
        if def.identifier_indexes().is_empty() {
            warn!("[Live][{}] table has no primary key and is not streamed", def.container_name);
            continue;
        }
        let table = def.container_name.as_str();
        let q = quote_ident(table);
        let lit = quote_literal(table);
        let trig = |suffix: &str| quote_ident(&format!("{}{}_{}", INTERNAL_PREFIX, table, suffix));
        let key_changed: Vec<String> = def
            .identifier_names()
            .iter()
            .map(|c| format!("OLD.{0} IS NOT NEW.{0}", quote_ident(c)))
            .collect();
        ddl.push(format!(
            "CREATE TRIGGER IF NOT EXISTS {} AFTER INSERT ON {} BEGIN \
             INSERT INTO {CHANGELOG_TABLE} (tbl, op, data) VALUES ({lit}, 'U', {}); END",
            trig("ai"),
            q,
            json_object("NEW", def, false)
        ));
        ddl.push(format!(
            "CREATE TRIGGER IF NOT EXISTS {} AFTER UPDATE ON {} BEGIN \
             INSERT INTO {CHANGELOG_TABLE} (tbl, op, data) SELECT {lit}, 'D', {} WHERE {}; \
             INSERT INTO {CHANGELOG_TABLE} (tbl, op, data) VALUES ({lit}, 'U', {}); END",
            trig("au"),
            q,
            json_object("OLD", def, true),
            key_changed.join(" OR "),
            json_object("NEW", def, false)
        ));
        ddl.push(format!(
            "CREATE TRIGGER IF NOT EXISTS {} AFTER DELETE ON {} BEGIN \
             INSERT INTO {CHANGELOG_TABLE} (tbl, op, data) VALUES ({lit}, 'D', {}); END",
            trig("ad"),
            q,
            json_object("OLD", def, true)
        ));
    }
    ddl
}

fn json_to_value(v: serde_json::Value, is_blob: bool) -> Value {
    use serde_json::Value as J;
    match v {
        J::Null => Value::Null,
        J::Bool(b) => Value::Bool(b),
        J::Number(n) => n.as_i64().map(Value::I64).unwrap_or_else(|| Value::F64(n.as_f64().unwrap_or_default())),
        J::String(s) if is_blob => from_hex(&s).map(Value::Bytes).unwrap_or(Value::String(s)),
        J::String(s) => Value::String(s),
        other => Value::Json(other),
    }
}

impl Sqlite {
    fn cdc(&mut self) -> DMResult<&mut SqliteCdcState> {
        self.cdc.as_mut().ok_or_else(|| DMError::Error("change capture not started".to_string()))
    }

    /// Removes triggers and the changelog table created for live transfer.
    pub async fn drop_live_objects(&self) -> DMResult<()> {
        self.run(|conn| {
            let names: Vec<String> = conn
                .prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE '\\_data\\_mover\\_%' ESCAPE '\\'")?
                .query_map([], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            for n in names {
                conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {}", quote_ident(&n)))?;
            }
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {CHANGELOG_TABLE}"))?;
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl ChangeSource for Sqlite {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        let ddl = capture_ddl(defs);
        info!("[Live] Installing SQLite change triggers");
        let start = self
            .run(move |conn| {
                let t = conn.transaction()?;
                for s in &ddl {
                    t.execute_batch(s)?;
                }
                let max: i64 = t.query_row(&format!("SELECT COALESCE(MAX(id), 0) FROM {CHANGELOG_TABLE}"), [], |r| r.get(0))?;
                t.commit()?;
                Ok(max)
            })
            .await?;
        // fresh start: changes logged by an earlier run are older than the coming snapshot
        let last_read = match resume {
            Some(Checkpoint::Sqlite { id }) => *id,
            _ => start,
        };
        let blob_columns = defs
            .iter()
            .map(|d| {
                (
                    d.container_name.clone(),
                    d.properties_info
                        .iter()
                        .filter(|p| p.data_type == LogicalType::Bytes)
                        .map(|p| p.property_name.clone())
                        .collect(),
                )
            })
            .collect();
        self.cdc = Some(SqliteCdcState { blob_columns, last_read, acked: last_read });
        Ok(())
    }

    async fn poll_changes(&mut self, max: usize, wait: Duration, shutdown: &watch::Receiver<bool>) -> DMResult<Vec<ChangeEvent>> {
        let deadline = Instant::now() + wait;
        let mut events = Vec::new();
        loop {
            let from = self.cdc()?.last_read;
            let limit = (max.saturating_sub(events.len())).max(1) as i64;
            let rows: Vec<(i64, String, String, String, i64)> = self
                .run(move |conn| {
                    let mut s = conn.prepare_cached(&format!(
                        "SELECT id, tbl, op, data, ts FROM {CHANGELOG_TABLE} WHERE id > ?1 ORDER BY id LIMIT ?2"
                    ))?;
                    let rows = s
                        .query_map(rusqlite::params![from, limit], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(rows)
                })
                .await?;
            let state = self.cdc()?;
            for (id, tbl, op, data, ts) in rows {
                state.last_read = id;
                let blobs = state.blob_columns.get(&tbl);
                let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&data)?;
                let row = obj
                    .into_iter()
                    .map(|(k, v)| {
                        let is_blob = blobs.is_some_and(|b| b.contains(&k));
                        (k, json_to_value(v, is_blob))
                    })
                    .collect();
                events.push(ChangeEvent {
                    container: tbl,
                    op: if op == "D" { ChangeOp::Delete } else { ChangeOp::Upsert },
                    row,
                    source_time: DateTime::<Utc>::from_timestamp_millis(ts),
                    complete: op != "D",
                });
            }
            if events.len() >= max || Instant::now() >= deadline || *shutdown.borrow() {
                return Ok(events);
            }
            tokio::time::sleep(deadline.saturating_duration_since(Instant::now()).min(POLL_INTERVAL)).await;
        }
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        let (last, acked) = {
            let s = self.cdc()?;
            (s.last_read, s.acked)
        };
        if last > acked {
            self.run(move |conn| {
                conn.execute(&format!("DELETE FROM {CHANGELOG_TABLE} WHERE id <= ?1"), [last])?;
                Ok(())
            })
            .await?;
            self.cdc()?.acked = last;
        }
        Ok(Some(Checkpoint::Sqlite { id: last }))
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Sqlite::reconnect(self).await
    }
}
