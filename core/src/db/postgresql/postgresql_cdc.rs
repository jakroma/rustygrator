//! PostgreSQL change source (LIVE-2): logical replication slot with the built-in `pgoutput`
//! plugin, read through `pg_logical_slot_peek_binary_changes`.
//!
//! Changes are *peeked* (not consumed) and the slot is advanced with
//! `pg_replication_slot_advance` only after the receiver applied them, which gives at-least-once
//! delivery (LIVE-7). The slot is created before the initial snapshot, so changes made while the
//! snapshot runs are replayed afterwards (LIVE-4).

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use log::{info, warn};
use tokio::{sync::watch, time::Instant};

use crate::{
    migrator::migrator_steps::ChangeSource,
    models::{
        change::{format_lsn, parse_lsn, ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::DataDefinition,
        value::{LogicalType, Value},
    },
    DMError, DMResult,
};

use super::postgresql::{quote_ident, Postgresql};

const PEEK_INTERVAL: Duration = Duration::from_millis(200);

struct Relation {
    container: String,
    columns: Vec<String>,
}

pub(crate) struct PgCdcState {
    slot: String,
    publication: String,
    /// container -> (column -> logical type, identifier columns)
    types: HashMap<String, (HashMap<String, LogicalType>, Vec<String>)>,
    relations: HashMap<u32, Relation>,
    /// Commit LSN of the newest transaction already returned by `poll_changes`.
    last_returned_commit: u64,
    /// End LSN to advance the slot to on the next acknowledge.
    pending_ack: Option<u64>,
    acked: u64,
}

/// Decoded pgoutput message (only the parts rustygrator needs).
#[derive(Debug, PartialEq)]
pub(crate) enum PgOutput {
    Begin { final_lsn: u64, commit_time: i64 },
    Commit { commit_lsn: u64, end_lsn: u64 },
    Relation { id: u32, namespace: String, name: String, columns: Vec<String> },
    Insert { relation: u32, new: Vec<TupleValue> },
    Update { relation: u32, key: Option<Vec<TupleValue>>, new: Vec<TupleValue> },
    Delete { relation: u32, key: Vec<TupleValue> },
    Truncate,
    Other,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum TupleValue {
    Null,
    UnchangedToast,
    Text(String),
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn err() -> DMError {
        DMError::Error("malformed pgoutput message".to_string())
    }
    fn take(&mut self, n: usize) -> DMResult<&'a [u8]> {
        let s = self.data.get(self.pos..self.pos + n).ok_or_else(Self::err)?;
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> DMResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> DMResult<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().map_err(|_| Self::err())?))
    }
    fn u32(&mut self) -> DMResult<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| Self::err())?))
    }
    fn u64(&mut self) -> DMResult<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().map_err(|_| Self::err())?))
    }
    fn cstr(&mut self) -> DMResult<String> {
        let rest = &self.data[self.pos..];
        let end = rest.iter().position(|b| *b == 0).ok_or_else(Self::err)?;
        let s = String::from_utf8_lossy(&rest[..end]).into_owned();
        self.pos += end + 1;
        Ok(s)
    }
    fn tuple(&mut self) -> DMResult<Vec<TupleValue>> {
        let n = self.i16()?.max(0) as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => TupleValue::Null,
                b'u' => TupleValue::UnchangedToast,
                b't' | b'b' => {
                    let len = self.u32()? as usize;
                    TupleValue::Text(String::from_utf8_lossy(self.take(len)?).into_owned())
                }
                _ => return Err(Self::err()),
            });
        }
        Ok(out)
    }
}

pub(crate) fn decode_pgoutput(data: &[u8]) -> DMResult<PgOutput> {
    let mut r = Reader { data, pos: 0 };
    Ok(match r.u8()? {
        b'B' => {
            let final_lsn = r.u64()?;
            let commit_time = r.u64()? as i64;
            PgOutput::Begin { final_lsn, commit_time }
        }
        b'C' => {
            r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            PgOutput::Commit { commit_lsn, end_lsn }
        }
        b'R' => {
            let id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            r.u8()?; // replica identity
            let n = r.i16()?.max(0) as usize;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                r.u8()?; // flags
                columns.push(r.cstr()?);
                r.u32()?; // type oid
                r.u32()?; // typmod
            }
            PgOutput::Relation { id, namespace, name, columns }
        }
        b'I' => {
            let relation = r.u32()?;
            r.u8()?; // 'N'
            PgOutput::Insert { relation, new: r.tuple()? }
        }
        b'U' => {
            let relation = r.u32()?;
            let mut key = None;
            let mut marker = r.u8()?;
            if marker == b'K' || marker == b'O' {
                key = Some(r.tuple()?);
                marker = r.u8()?;
            }
            if marker != b'N' {
                return Err(Reader::err());
            }
            PgOutput::Update { relation, key, new: r.tuple()? }
        }
        b'D' => {
            let relation = r.u32()?;
            r.u8()?; // 'K' or 'O'
            PgOutput::Delete { relation, key: r.tuple()? }
        }
        b'T' => PgOutput::Truncate,
        _ => PgOutput::Other,
    })
}

/// pgoutput timestamps are microseconds since 2000-01-01.
fn pg_time(micros: i64) -> Option<DateTime<Utc>> {
    let epoch = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).single()?;
    epoch.checked_add_signed(chrono::Duration::microseconds(micros))
}

impl PgCdcState {
    fn named_row(
        &self,
        container: &str,
        columns: &[String],
        tuple: &[TupleValue],
        only: Option<&[String]>,
    ) -> DMResult<Vec<(String, Value)>> {
        let (types, _) = &self.types[container];
        let mut row = Vec::with_capacity(tuple.len());
        for (name, v) in columns.iter().zip(tuple) {
            if let Some(only) = only {
                if !only.contains(name) {
                    continue;
                }
            }
            let Some(t) = types.get(name) else { continue };
            match v {
                TupleValue::Null => row.push((name.clone(), Value::Null)),
                TupleValue::UnchangedToast => {}
                TupleValue::Text(s) => row.push((name.clone(), Value::parse_text(s, *t)?)),
            }
        }
        Ok(row)
    }
}

impl Postgresql {
    fn cdc(&mut self) -> DMResult<&mut PgCdcState> {
        self.cdc
            .as_mut()
            .ok_or_else(|| DMError::Error("change capture not started".to_string()))
    }

    async fn ensure_publication(&self, publication: &str, defs: &[DataDefinition]) -> DMResult<()> {
        let tables: Vec<String> = defs
            .iter()
            .filter(|d| !d.identifier_indexes().is_empty())
            .filter_map(|d| d.container_name.split_once('.').map(|(s, t)| format!("{}.{}", quote_ident(s), quote_ident(t))))
            .collect();
        for d in defs.iter().filter(|d| d.identifier_indexes().is_empty()) {
            warn!("[Live][{}] table has no primary key and is not streamed", d.container_name);
        }
        if tables.is_empty() {
            return Err(DMError::Error("no table with a primary key to stream".to_string()));
        }
        let exists = self
            .client
            .query_opt("SELECT 1 FROM pg_publication WHERE pubname = $1", &[&publication])
            .await?
            .is_some();
        let sql = if exists {
            format!("ALTER PUBLICATION {} SET TABLE {}", quote_ident(publication), tables.join(", "))
        } else {
            format!("CREATE PUBLICATION {} FOR TABLE {}", quote_ident(publication), tables.join(", "))
        };
        self.client.batch_execute(&sql).await?;
        Ok(())
    }
}

#[async_trait]
impl ChangeSource for Postgresql {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        let slot = self.settings.live.slot_name.clone();
        let publication = self.settings.live.publication_name.clone();
        self.ensure_publication(&publication, defs).await?;

        let existing = self
            .client
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await?;
        let confirmed = match existing {
            Some(row) => {
                info!("[Live] Using existing replication slot {}", slot);
                row.get::<_, Option<String>>(0)
            }
            None => {
                info!("[Live] Creating replication slot {}", slot);
                let row = self
                    .client
                    .query_one(
                        "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                        &[&slot],
                    )
                    .await?;
                Some(row.get::<_, String>(0))
            }
        };
        let mut start = confirmed.as_deref().and_then(parse_lsn).unwrap_or(0);
        if let Some(Checkpoint::Postgresql { lsn }) = resume {
            start = start.max(parse_lsn(lsn).unwrap_or(0));
        }

        let types = defs
            .iter()
            .map(|d| {
                (
                    d.container_name.clone(),
                    (
                        d.properties_info.iter().map(|p| (p.property_name.clone(), p.data_type)).collect(),
                        d.identifier_names().into_iter().map(String::from).collect(),
                    ),
                )
            })
            .collect();
        self.cdc = Some(PgCdcState {
            slot,
            publication,
            types,
            relations: HashMap::new(),
            last_returned_commit: start,
            pending_ack: None,
            acked: start,
        });
        Ok(())
    }

    async fn poll_changes(
        &mut self,
        max: usize,
        wait: Duration,
        shutdown: &watch::Receiver<bool>,
    ) -> DMResult<Vec<ChangeEvent>> {
        let deadline = Instant::now() + wait;
        let mut events: Vec<ChangeEvent> = Vec::new();
        loop {
            let (slot, publication) = {
                let s = self.cdc()?;
                (s.slot.clone(), s.publication.clone())
            };
            let current: String = self.client.query_one("SELECT pg_current_wal_lsn()::text", &[]).await?.get(0);
            let rows = self
                .client
                .query(
                    "SELECT data FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, \
                     'proto_version', '1', 'publication_names', $3)",
                    &[&slot, &(max.max(1) as i32), &publication],
                )
                .await?;
            let had_rows = !rows.is_empty();
            let state = self.cdc()?;
            state.relations.clear();
            let mut skipping = false;
            let mut commit_time = None;
            let mut tx_events: Vec<ChangeEvent> = Vec::new();
            for row in rows {
                let data: &[u8] = row.get(0);
                match decode_pgoutput(data)? {
                    PgOutput::Begin { final_lsn, commit_time: ts } => {
                        skipping = final_lsn <= state.last_returned_commit;
                        commit_time = pg_time(ts);
                        tx_events.clear();
                    }
                    PgOutput::Commit { commit_lsn, end_lsn } => {
                        if !skipping {
                            state.last_returned_commit = commit_lsn;
                            state.pending_ack = Some(end_lsn);
                            events.append(&mut tx_events);
                        }
                        skipping = false;
                    }
                    PgOutput::Relation { id, namespace, name, columns } => {
                        state.relations.insert(id, Relation { container: format!("{}.{}", namespace, name), columns });
                    }
                    PgOutput::Truncate if !skipping => {
                        warn!("[Live] TRUNCATE is not replicated");
                    }
                    msg @ (PgOutput::Insert { .. } | PgOutput::Update { .. } | PgOutput::Delete { .. }) if !skipping => {
                        let rel_id = match &msg {
                            PgOutput::Insert { relation, .. } | PgOutput::Update { relation, .. } | PgOutput::Delete { relation, .. } => *relation,
                            _ => unreachable!(),
                        };
                        let Some(rel) = state.relations.get(&rel_id) else { continue };
                        if !state.types.contains_key(&rel.container) {
                            continue;
                        }
                        let container = rel.container.clone();
                        let ids = state.types[&container].1.clone();
                        match msg {
                            PgOutput::Insert { new, .. } => tx_events.push(ChangeEvent {
                                complete: !new.contains(&TupleValue::UnchangedToast),
                                row: state.named_row(&container, &rel.columns, &new, None)?,
                                container,
                                op: ChangeOp::Upsert,
                                source_time: commit_time,
                            }),
                            PgOutput::Update { key, mut new, .. } => {
                                if let Some(old) = key {
                                    // primary key changed (or REPLICA IDENTITY FULL): fill unchanged
                                    // TOAST values from the old tuple, delete the old key if it differs
                                    for (n, o) in new.iter_mut().zip(old.iter()) {
                                        if *n == TupleValue::UnchangedToast && *o != TupleValue::UnchangedToast {
                                            *n = o.clone();
                                        }
                                    }
                                    let old_key = state.named_row(&container, &rel.columns, &old, Some(&ids))?;
                                    let new_key = state.named_row(&container, &rel.columns, &new, Some(&ids))?;
                                    if old_key != new_key && old_key.len() == ids.len() {
                                        tx_events.push(ChangeEvent {
                                            container: container.clone(),
                                            op: ChangeOp::Delete,
                                            row: old_key,
                                            source_time: commit_time,
                                            complete: false,
                                        });
                                    }
                                }
                                tx_events.push(ChangeEvent {
                                    complete: !new.contains(&TupleValue::UnchangedToast),
                                    row: state.named_row(&container, &rel.columns, &new, None)?,
                                    container,
                                    op: ChangeOp::Upsert,
                                    source_time: commit_time,
                                });
                            }
                            PgOutput::Delete { key, .. } => tx_events.push(ChangeEvent {
                                row: state.named_row(&container, &rel.columns, &key, Some(&ids))?,
                                container,
                                op: ChangeOp::Delete,
                                source_time: commit_time,
                                complete: false,
                            }),
                            _ => unreachable!(),
                        }
                    }
                    _ => {}
                }
            }
            if !had_rows && events.is_empty() {
                // nothing to decode up to `current`: the slot may move there, which keeps the
                // amount of WAL re-decoded on every peek small
                if let Some(lsn) = parse_lsn(&current) {
                    if lsn > state.acked {
                        state.pending_ack = Some(lsn);
                        state.last_returned_commit = state.last_returned_commit.max(lsn);
                    }
                }
            }
            if events.len() >= max || Instant::now() >= deadline || *shutdown.borrow() {
                return Ok(events);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(remaining.min(PEEK_INTERVAL)).await;
        }
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        let (slot, pending, acked) = {
            let s = self.cdc()?;
            (s.slot.clone(), s.pending_ack.take(), s.acked)
        };
        if let Some(lsn) = pending.filter(|l| *l > acked) {
            let text = format_lsn(lsn);
            self.client
                .query("SELECT pg_replication_slot_advance($1, $2::text::pg_lsn)", &[&slot, &text])
                .await?;
            self.cdc()?.acked = lsn;
        }
        let acked = self.cdc()?.acked;
        Ok(Some(Checkpoint::Postgresql { lsn: format_lsn(acked) }))
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Postgresql::reconnect(self).await
    }
}

/// Drops the replication slot and publication created for live transfer.
pub async fn drop_live_objects(pg: &Postgresql) -> DMResult<()> {
    let slot = &pg.settings.live.slot_name;
    pg.client
        .query(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[slot],
        )
        .await?;
    pg.client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pg.settings.live.publication_name)))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(values: &[Option<&str>]) -> Vec<u8> {
        let mut b = (values.len() as i16).to_be_bytes().to_vec();
        for v in values {
            match v {
                None => b.push(b'n'),
                Some(s) => {
                    b.push(b't');
                    b.extend((s.len() as u32).to_be_bytes());
                    b.extend(s.as_bytes());
                }
            }
        }
        b
    }

    #[test]
    fn decodes_begin_commit_relation_and_dml() {
        let mut begin = vec![b'B'];
        begin.extend(0x10u64.to_be_bytes());
        begin.extend(5u64.to_be_bytes());
        begin.extend(7u32.to_be_bytes());
        assert_eq!(decode_pgoutput(&begin).unwrap(), PgOutput::Begin { final_lsn: 0x10, commit_time: 5 });

        let mut commit = vec![b'C', 0];
        commit.extend(0x10u64.to_be_bytes());
        commit.extend(0x18u64.to_be_bytes());
        commit.extend(5u64.to_be_bytes());
        assert_eq!(decode_pgoutput(&commit).unwrap(), PgOutput::Commit { commit_lsn: 0x10, end_lsn: 0x18 });

        let mut rel = vec![b'R'];
        rel.extend(42u32.to_be_bytes());
        rel.extend(b"public\0users\0d");
        rel.extend(2i16.to_be_bytes());
        for c in ["id", "name"] {
            rel.push(1);
            rel.extend(c.as_bytes());
            rel.push(0);
            rel.extend(23u32.to_be_bytes());
            rel.extend((-1i32).to_be_bytes());
        }
        assert_eq!(
            decode_pgoutput(&rel).unwrap(),
            PgOutput::Relation { id: 42, namespace: "public".into(), name: "users".into(), columns: vec!["id".into(), "name".into()] }
        );

        let mut ins = vec![b'I'];
        ins.extend(42u32.to_be_bytes());
        ins.push(b'N');
        ins.extend(tuple(&[Some("1"), None]));
        assert_eq!(
            decode_pgoutput(&ins).unwrap(),
            PgOutput::Insert { relation: 42, new: vec![TupleValue::Text("1".into()), TupleValue::Null] }
        );

        let mut upd = vec![b'U'];
        upd.extend(42u32.to_be_bytes());
        upd.push(b'K');
        upd.extend(tuple(&[Some("1"), None]));
        upd.push(b'N');
        upd.extend(tuple(&[Some("2"), Some("x")]));
        match decode_pgoutput(&upd).unwrap() {
            PgOutput::Update { key: Some(k), new, .. } => {
                assert_eq!(k[0], TupleValue::Text("1".into()));
                assert_eq!(new[1], TupleValue::Text("x".into()));
            }
            other => panic!("{other:?}"),
        }

        let mut del = vec![b'D'];
        del.extend(42u32.to_be_bytes());
        del.push(b'K');
        del.extend(tuple(&[Some("1"), None]));
        assert!(matches!(decode_pgoutput(&del).unwrap(), PgOutput::Delete { relation: 42, .. }));
        assert!(decode_pgoutput(&[b'I', 0]).is_err());
    }
}
