//! MongoDB change source (LIVE-1): database change stream (needs a replica set or a sharded
//! cluster). The cluster time is captured before the initial snapshot and the stream starts at
//! that time (LIVE-4); afterwards it resumes from the last acknowledged resume token (LIVE-6).

use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use chrono::DateTime;
use log::{info, warn};
use mongodb::{
    bson::{self, doc, Bson, Document, Timestamp},
    change_stream::{
        event::{ChangeStreamEvent, OperationType, ResumeToken},
        ChangeStream,
    },
    options::{ChangeStreamOptions, FullDocumentType},
};
use tokio::{sync::watch, time::Instant};

use crate::{
    db::mongodb::mongodb_data_types::bson_to_value,
    migrator::migrator_steps::ChangeSource,
    models::{
        change::{ChangeEvent, ChangeOp, Checkpoint},
        data_definitions::DataDefinition,
        value::Value,
    },
    DMError, DMResult,
};

use super::mongodb::Mongodb;

pub(crate) struct MongoCdcState {
    defs: Vec<DataDefinition>,
    collections: HashSet<String>,
    start_time: Option<Timestamp>,
    // Mutex only makes the state Sync (it is always accessed through &mut, never locked)
    stream: std::sync::Mutex<Option<ChangeStream<ChangeStreamEvent<Document>>>>,
    read_token: Option<ResumeToken>,
    acked_token: Option<ResumeToken>,
}

fn token_to_json(t: &ResumeToken) -> DMResult<serde_json::Value> {
    Ok(bson::to_bson(t)?.into_canonical_extjson())
}

fn token_from_json(j: &serde_json::Value) -> DMResult<ResumeToken> {
    let b = Bson::try_from(j.clone()).map_err(|e| DMError::Error(format!("invalid resume token: {}", e)))?;
    Ok(bson::from_bson(b)?)
}

impl Mongodb {
    fn cdc(&mut self) -> DMResult<&mut MongoCdcState> {
        self.cdc.as_mut().ok_or_else(|| DMError::Error("change capture not started".to_string()))
    }

    async fn cluster_time(&self) -> DMResult<Timestamp> {
        let reply = self.db.run_command(doc! {"ping": 1}, None).await?;
        if let Ok(t) = reply.get_timestamp("operationTime") {
            return Ok(t);
        }
        reply
            .get_document("$clusterTime")
            .ok()
            .and_then(|c| c.get_timestamp("clusterTime").ok())
            .ok_or_else(|| DMError::Error("MongoDB change streams need a replica set or sharded cluster".to_string()))
    }

    async fn open_stream(&mut self) -> DMResult<()> {
        let (collections, token, start) = {
            let s = self.cdc()?;
            (s.collections.iter().cloned().collect::<Vec<_>>(), s.acked_token.clone(), s.start_time)
        };
        let mut options = ChangeStreamOptions::builder()
            .full_document(Some(FullDocumentType::UpdateLookup))
            .max_await_time(Some(Duration::from_millis(200)))
            .build();
        match token {
            Some(t) => options.resume_after = Some(t),
            None => options.start_at_operation_time = start,
        }
        let pipeline = [doc! {"$match": {"ns.coll": {"$in": collections}}}];
        let stream = self.db.watch(pipeline, options).await?;
        *self.cdc()?.stream.get_mut().map_err(|_| DMError::Error("poisoned".into()))? = Some(stream);
        Ok(())
    }
}

fn event_row(def: &DataDefinition, d: &Document) -> Vec<(String, Value)> {
    def.properties_info
        .iter()
        .filter_map(|p| d.get(&p.property_name).map(|b| (p.property_name.clone(), bson_to_value(b))))
        .collect()
}

#[async_trait]
impl ChangeSource for Mongodb {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        let (token, time) = match resume {
            Some(Checkpoint::Mongodb { resume_token, operation_time }) => (
                resume_token.as_ref().map(token_from_json).transpose()?,
                operation_time.map(|(t, i)| Timestamp { time: t, increment: i }),
            ),
            _ => (None, None),
        };
        let start_time = match (&token, time) {
            (Some(_), t) => t,
            (None, Some(t)) => Some(t),
            (None, None) => Some(self.cluster_time().await?),
        };
        info!("[Live] MongoDB change stream starts at {:?}", start_time);
        self.cdc = Some(MongoCdcState {
            defs: defs.to_vec(),
            collections: defs.iter().map(|d| d.container_name.clone()).collect(),
            start_time,
            stream: std::sync::Mutex::new(None),
            read_token: token.clone(),
            acked_token: token,
        });
        self.open_stream().await
    }

    async fn poll_changes(&mut self, max: usize, wait: Duration, shutdown: &watch::Receiver<bool>) -> DMResult<Vec<ChangeEvent>> {
        if self.cdc()?.stream.get_mut().map(|s| s.is_none()).unwrap_or(true) {
            self.open_stream().await?;
        }
        let deadline = Instant::now() + wait;
        let mut events = Vec::new();
        let MongoCdcState { defs, stream, read_token, .. } = self.cdc()?;
        let stream = stream
            .get_mut()
            .ok()
            .and_then(|s| s.as_mut())
            .ok_or_else(|| DMError::Error("change stream not open".into()))?;
        loop {
            match stream.next_if_any().await? {
                Some(ev) => {
                    let token = ev.id.clone();
                    let coll = ev.ns.as_ref().and_then(|n| n.coll.clone()).unwrap_or_default();
                    let def = defs.iter().find(|d| d.container_name == coll);
                    let source_time = ev.cluster_time.and_then(|t| DateTime::from_timestamp(t.time as i64, 0));
                    match (def, ev.operation_type.clone()) {
                        (Some(def), OperationType::Insert | OperationType::Update | OperationType::Replace) => {
                            // a missing full document means it was deleted later; the delete
                            // event follows
                            if let Some(d) = ev.full_document {
                                events.push(ChangeEvent { container: coll, op: ChangeOp::Upsert, row: event_row(def, &d), source_time, complete: true });
                            }
                        }
                        (Some(def), OperationType::Delete) => {
                            if let Some(k) = ev.document_key {
                                events.push(ChangeEvent { container: coll, op: ChangeOp::Delete, row: event_row(def, &k), source_time, complete: false });
                            }
                        }
                        (_, OperationType::Invalidate | OperationType::Drop | OperationType::DropDatabase | OperationType::Rename) => {
                            warn!("[Live] change stream event {:?} on {} is not replicated", ev.operation_type, coll);
                        }
                        _ => {}
                    }
                    *read_token = Some(token);
                }
                None => {
                    if let Some(t) = stream.resume_token() {
                        *read_token = Some(t);
                    }
                }
            }
            if events.len() >= max || Instant::now() >= deadline || *shutdown.borrow() {
                return Ok(events);
            }
        }
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        let state = self.cdc()?;
        state.acked_token = state.read_token.clone();
        Ok(Some(Checkpoint::Mongodb {
            resume_token: state.acked_token.as_ref().map(token_to_json).transpose()?,
            operation_time: state.start_time.map(|t| (t.time, t.increment)),
        }))
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Mongodb::reconnect(self).await?;
        // reopen from the last acknowledged position so nothing unapplied is skipped
        let state = self.cdc()?;
        *state.stream.get_mut().map_err(|_| DMError::Error("poisoned".into()))? = None;
        state.read_token = state.acked_token.clone();
        Ok(())
    }
}
