//! MySQL change source (LIVE-16): the row-based binary log, read like a replica
//! (`COM_BINLOG_DUMP`). Needs `binlog_format=ROW`, `binlog_row_image=FULL` and the
//! `REPLICATION SLAVE` / `REPLICATION CLIENT` privileges. The checkpoint is the binlog file and
//! the position after the last committed transaction that was handed to the receiver.

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chrono::DateTime;
use futures_util::StreamExt;
use log::{info, warn};
use mysql_async::{
    binlog::{
        events::{EventData, RowsEventData},
        row::BinlogRow,
        value::BinlogValue,
    },
    prelude::Queryable,
    BinlogStream, BinlogStreamRequest, Row, Value as My,
};
use tokio::{sync::watch, time::Instant};

use crate::{
    dbc::{batch::Column, crypto::random_bytes},
    migrator::migrator_steps::ChangeSource,
    models::{
        change::{ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::DataDefinition,
        value::{LogicalType, Value},
    },
    DMError, DMResult,
};

use super::{
    mysql_connection::{connect, Mysql},
    mysql_data_types::{enum_labels, push_value, timestamp_from_text},
};

/// How a table's binlog rows are turned into events (columns in table order).
pub(crate) struct TableInfo {
    container: String,
    names: Vec<String>,
    kinds: Vec<LogicalType>,
    /// `enum` / `set` labels: the binlog sends the index / bit mask.
    labels: Vec<Option<(Vec<String>, bool)>>,
    keys: Vec<usize>,
}

pub(crate) struct MysqlCdcState {
    stream: Option<BinlogStream>,
    tables: HashMap<(String, String), TableInfo>,
    server_id: u32,
    file: String,
    /// Position after the last commit read (events up to here were handed out).
    committed: (String, u64),
    acked: (String, u64),
}

/// Converts one binlog value to the column's logical type.
pub(crate) fn binlog_value(v: Option<&BinlogValue<'_>>, kind: LogicalType, labels: Option<&(Vec<String>, bool)>) -> DMResult<Value> {
    let v = match v {
        None => return Ok(Value::Null),
        Some(BinlogValue::Jsonb(j)) => {
            return serde_json::Value::try_from(j.clone()).map(Value::Json).map_err(|e| DMError::Error(format!("binlog JSON: {e}")));
        }
        Some(BinlogValue::JsonDiff(_)) => {
            return Err(DMError::Error("partial JSON updates in the binlog are not supported (set binlog_row_value_options='')".into()));
        }
        Some(BinlogValue::Value(v)) => v,
    };
    match (v, labels) {
        // enum: 1-based index; set: bit mask
        (My::Int(i), Some((labels, false))) => Ok(Value::String(labels.get((*i as usize).wrapping_sub(1)).cloned().unwrap_or_default())),
        (My::Int(i), Some((labels, true))) => {
            Ok(Value::String(labels.iter().enumerate().filter(|(b, _)| i >> b & 1 == 1).map(|(_, l)| l.as_str()).collect::<Vec<_>>().join(",")))
        }
        // TIMESTAMP2 arrives as seconds since the epoch, the old TIMESTAMP as an integer
        (My::Bytes(b), _) if kind == LogicalType::DateTimeTz => {
            let text = std::str::from_utf8(b).unwrap_or_default();
            timestamp_from_text(text).map(Value::DateTimeTz).ok_or_else(|| DMError::Error(format!("binlog timestamp {text}")))
        }
        (My::Int(i), _) if kind == LogicalType::DateTimeTz => Ok(DateTime::from_timestamp(*i, 0).map(Value::DateTimeTz).unwrap_or(Value::Null)),
        (v, _) => {
            let mut c = Column::new(kind, 1);
            push_value(&mut c, v)?;
            Ok(c.cell(0).to_value())
        }
    }
}

fn row_values(t: &TableInfo, row: &BinlogRow, only_keys: bool) -> DMResult<Vec<(String, Value)>> {
    let columns: Box<dyn Iterator<Item = usize>> = if only_keys { Box::new(t.keys.iter().copied()) } else { Box::new(0..t.names.len()) };
    columns.map(|i| Ok((t.names[i].clone(), binlog_value(row.as_ref(i), t.kinds[i], t.labels[i].as_ref())?))).collect()
}

fn keys_differ(t: &TableInfo, before: &BinlogRow, after: &BinlogRow) -> bool {
    t.keys.iter().any(|&i| before.as_ref(i) != after.as_ref(i))
}

impl Mysql {
    fn cdc(&mut self) -> DMResult<&mut MysqlCdcState> {
        self.cdc.as_mut().ok_or_else(|| DMError::Error("change capture not started".to_string()))
    }

    /// Current end of the binary log (`SHOW BINARY LOG STATUS` on MySQL 8.4+, `SHOW MASTER
    /// STATUS` before and on MariaDB).
    async fn binlog_position(&mut self) -> DMResult<(String, u64)> {
        let row: Option<Row> = match self.conn.query_first("SHOW BINARY LOG STATUS").await {
            Ok(r) => r,
            Err(_) => self.conn.query_first("SHOW MASTER STATUS").await?,
        };
        let row = row.ok_or_else(|| DMError::Error("MySQL binary logging is off (start the server with log_bin)".to_string()))?;
        Ok((row.get::<String, _>(0).unwrap_or_default(), row.get::<u64, _>(1).unwrap_or(4)))
    }

    async fn open_stream(&mut self) -> DMResult<()> {
        let conn = connect(&self.connection).await?;
        let state = self.cdc()?;
        let (file, pos) = state.acked.clone();
        info!("[Live] Reading the MySQL binlog from {file}:{pos}");
        let stream = conn.get_binlog_stream(BinlogStreamRequest::new(state.server_id).with_filename(file.as_bytes()).with_pos(pos)).await?;
        state.file = file.clone();
        state.committed = (file, pos);
        state.stream = Some(stream);
        Ok(())
    }
}

#[async_trait]
impl ChangeSource for Mysql {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        let (format, image): (String, String) = self
            .conn
            .query_first("SELECT @@binlog_format, @@binlog_row_image")
            .await?
            .ok_or_else(|| DMError::Error("cannot read the binlog settings".to_string()))?;
        if !format.eq_ignore_ascii_case("ROW") || !image.eq_ignore_ascii_case("FULL") {
            return Err(DMError::Error(format!("live mode needs binlog_format=ROW and binlog_row_image=FULL (server: {format}, {image})")));
        }
        let db = self.database().to_string();
        let column_types: Vec<(String, String, String)> =
            self.conn.exec("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?", (db.as_str(),)).await?;
        let column_types: HashMap<(String, String), String> = column_types.into_iter().map(|(t, c, ty)| ((t, c), ty.to_ascii_lowercase())).collect();
        let mut tables = HashMap::new();
        for def in defs {
            let keys = def.identifier_indexes();
            if keys.is_empty() {
                warn!("[Live][{}] table has no primary key and is not streamed", def.container_name);
                continue;
            }
            let (db, table) = def.container_name.split_once('.').unwrap_or((db.as_str(), &def.container_name));
            let labels = def
                .properties_info
                .iter()
                .map(|p| {
                    let ty = column_types.get(&(table.to_string(), p.property_name.clone()))?;
                    enum_labels(ty).map(|l| (l, ty.starts_with("set(")))
                })
                .collect();
            tables.insert(
                (db.to_string(), table.to_string()),
                TableInfo {
                    container: def.container_name.clone(),
                    names: def.properties_info.iter().map(|p| p.property_name.clone()).collect(),
                    kinds: def.properties_info.iter().map(|p| p.data_type).collect(),
                    labels,
                    keys,
                },
            );
        }
        // the position is taken before the snapshot, so changes made meanwhile are replayed (LIVE-4)
        let start = match resume {
            Some(Checkpoint::Mysql { file, pos }) => (file.clone(), *pos),
            _ => self.binlog_position().await?,
        };
        // a replica id of its own; 1..=2^16 are left to real replicas
        let server_id = (u32::from_le_bytes(random_bytes::<4>()?) | 0x0001_0000).max(0x0001_0000);
        self.cdc = Some(MysqlCdcState { stream: None, tables, server_id, file: start.0.clone(), committed: start.clone(), acked: start });
        self.open_stream().await
    }

    async fn poll_changes(&mut self, max: usize, wait: Duration, shutdown: &watch::Receiver<bool>) -> DMResult<Vec<ChangeEvent>> {
        let deadline = Instant::now() + wait;
        let mut shutdown = shutdown.clone();
        let mut out = Vec::new();
        let MysqlCdcState { stream, tables, file, committed, .. } = self.cdc()?;
        let stream = stream.as_mut().ok_or_else(|| DMError::Error("binlog stream is closed".to_string()))?;
        while out.len() < max {
            let event = tokio::select! {
                e = tokio::time::timeout_at(deadline, stream.next()) => match e {
                    Err(_) => break,
                    Ok(None) => return Err(DMError::Error("the MySQL binlog stream ended".to_string())),
                    Ok(Some(e)) => e?,
                },
                _ = shutdown.changed() => break,
            };
            let header = event.header();
            let source_time = DateTime::from_timestamp(header.timestamp() as i64, 0);
            match event.read_data()? {
                Some(EventData::RotateEvent(r)) => {
                    *file = r.name().into_owned();
                    *committed = (file.clone(), r.position());
                }
                Some(EventData::XidEvent(_)) => *committed = (file.clone(), header.log_pos() as u64),
                // COMMIT of non-transactional tables and DDL end a transaction too
                Some(EventData::QueryEvent(q)) if !q.query().eq_ignore_ascii_case("BEGIN") => *committed = (file.clone(), header.log_pos() as u64),
                Some(EventData::RowsEvent(re)) => {
                    let Some(tme) = stream.get_tme(re.table_id()) else { continue };
                    let Some(t) = tables.get(&(tme.database_name().into_owned(), tme.table_name().into_owned())) else { continue };
                    let upsert = |row: &BinlogRow| -> DMResult<ChangeEvent> {
                        Ok(ChangeEvent { container: t.container.clone(), op: ChangeOp::Upsert, row: row_values(t, row, false)?, source_time, complete: true })
                    };
                    let delete = |row: &BinlogRow| -> DMResult<ChangeEvent> {
                        Ok(ChangeEvent { container: t.container.clone(), op: ChangeOp::Delete, row: row_values(t, row, true)?, source_time, complete: true })
                    };
                    for row in re.rows(tme) {
                        let (before, after) = row.map_err(|e| DMError::Error(format!("binlog row: {e}")))?;
                        match (&re, before, after) {
                            (RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_), Some(b), _) => out.push(delete(&b)?),
                            (_, Some(b), Some(a)) => {
                                // a changed key is a delete of the old record and an insert of the new one
                                if keys_differ(t, &b, &a) {
                                    out.push(delete(&b)?);
                                }
                                out.push(upsert(&a)?);
                            }
                            (_, None, Some(a)) => out.push(upsert(&a)?),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        let state = self.cdc()?;
        state.acked = state.committed.clone();
        let (file, pos) = state.acked.clone();
        Ok(Some(Checkpoint::Mysql { file, pos }))
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Mysql::reconnect(self).await?;
        // a new stream from the acknowledged position: unacknowledged events are read again
        if let Some(stream) = self.cdc()?.stream.take() {
            let _ = stream.close().await;
        }
        self.open_stream().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live16_binlog_values() {
        let labels = (vec!["new".to_string(), "paid".to_string(), "sent".to_string()], false);
        let set = (vec!["a".to_string(), "b".to_string(), "c".to_string()], true);
        assert_eq!(binlog_value(Some(&BinlogValue::Value(My::Int(2))), LogicalType::String, Some(&labels)).unwrap(), Value::String("paid".into()));
        assert_eq!(binlog_value(Some(&BinlogValue::Value(My::Int(0b101))), LogicalType::String, Some(&set)).unwrap(), Value::String("a,c".into()));
        let ts = binlog_value(Some(&BinlogValue::Value(My::Bytes(b"1700000000.25".to_vec()))), LogicalType::DateTimeTz, None).unwrap();
        assert_eq!(ts, Value::DateTimeTz("2023-11-14T22:13:20.25Z".parse().unwrap()));
        assert_eq!(binlog_value(Some(&BinlogValue::Value(My::Bytes(b"12.50".to_vec()))), LogicalType::Decimal, None).unwrap(), Value::Decimal("12.50".into()));
        assert_eq!(binlog_value(None, LogicalType::Int32, None).unwrap(), Value::Null);
        assert_eq!(binlog_value(Some(&BinlogValue::Value(My::Int(7))), LogicalType::Int32, None).unwrap(), Value::I32(7));
    }
}
