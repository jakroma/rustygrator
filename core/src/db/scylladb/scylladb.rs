//! ScyllaDB connection (SCY-1): shard-aware CQL driver, keyspace from the connection string.

use std::{collections::HashMap, sync::Arc};

use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    statement::Consistency,
};

use crate::{
    models::{connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings},
    DMError, DMResult,
};

use super::{scylladb_cdc::ScyllaCdcState, scylladb_data_types::quote};

pub struct Scylla {
    pub session: Arc<Session>,
    pub keyspace: String,
    pub settings: MigrationExecutionSettings,
    pub(crate) connection: MigrationConnection,
    pub(crate) consistency: Consistency,
    pub(crate) cdc: Option<ScyllaCdcState>,
    /// Embedded (denormalized) parent rows already looked up: table -> key -> UDT value.
    pub(crate) parent_cache: tokio::sync::Mutex<HashMap<String, HashMap<String, Option<scylla::value::CqlValue>>>>,
}

/// Maps any driver error into a [`DMError`].
pub fn sc<E: std::fmt::Display>(e: E) -> DMError {
    DMError::Error(format!("ScyllaDB: {}", e))
}

pub fn parse_consistency(s: &str) -> DMResult<Consistency> {
    Ok(match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "any" => Consistency::Any,
        "one" => Consistency::One,
        "two" => Consistency::Two,
        "three" => Consistency::Three,
        "quorum" => Consistency::Quorum,
        "all" => Consistency::All,
        "local_quorum" => Consistency::LocalQuorum,
        "each_quorum" => Consistency::EachQuorum,
        "local_one" => Consistency::LocalOne,
        other => return Err(DMError::Error(format!("unknown consistency level '{}'", other))),
    })
}

pub(crate) async fn open_session(c: &MigrationConnection) -> DMResult<Session> {
    let mut b = SessionBuilder::new().known_nodes(c.hosts(9042));
    if !c.user.is_empty() {
        b = b.user(c.user.clone(), c.password.clone());
    }
    if let Some(tls) = &c.tls {
        b = b.tls_context(Some(Arc::new(tls.client_config()?)));
    }
    b.build().await.map_err(sc)
}

impl Scylla {
    pub async fn new(connection: MigrationConnection, settings: MigrationExecutionSettings) -> DMResult<Self> {
        let consistency = parse_consistency(&settings.scylla.consistency)?;
        let session = open_session(&connection).await?;
        // default keyspace for queries typed in the app; a receiver creates a missing one later
        if !connection.database.is_empty() {
            let _ = session.use_keyspace(&connection.database, true).await;
        }
        Ok(Scylla {
            session: Arc::new(session),
            keyspace: connection.database.clone(),
            settings,
            connection,
            consistency,
            cdc: None,
            parent_cache: Default::default(),
        })
    }

    pub fn table(&self, name: &str) -> String {
        format!("{}.{}", quote(&self.keyspace), quote(name))
    }

    pub async fn reconnect(&mut self) -> DMResult<()> {
        self.session = Arc::new(open_session(&self.connection).await?);
        Ok(())
    }
}
