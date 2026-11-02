use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::{
    models::{
        connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings,
    },
    DMError, DMResult,
};

use super::sqlite_cdc::SqliteCdcState;

pub struct Sqlite {
    pub(crate) conn: Arc<Mutex<Connection>>,
    pub path: String,
    pub settings: MigrationExecutionSettings,
    pub(crate) cdc: Option<SqliteCdcState>,
    /// journal_mode / synchronous before the bulk load (PERF-14), restored in finalize
    pub(crate) saved_pragmas: Option<(String, i64)>,
    /// interrupts a running statement from another thread (VIEW-6 cancel)
    pub(crate) interrupt: Arc<rusqlite::InterruptHandle>,
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn open(path: &str) -> DMResult<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(conn)
}

impl Sqlite {
    pub async fn new(connection_model: MigrationConnection, settings: MigrationExecutionSettings) -> DMResult<Self> {
        let path = connection_model.database.clone();
        let p = path.clone();
        let conn = tokio::task::spawn_blocking(move || open(&p)).await??;
        let interrupt = Arc::new(conn.get_interrupt_handle());
        Ok(Sqlite { conn: Arc::new(Mutex::new(conn)), path, settings, cdc: None, saved_pragmas: None, interrupt })
    }

    /// Runs blocking SQLite work on the blocking thread pool.
    pub(crate) async fn run<F, R>(&self, f: F) -> DMResult<R>
    where
        F: FnOnce(&mut Connection) -> DMResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.lock().map_err(|_| DMError::Error("sqlite connection poisoned".to_string()))?;
            f(&mut guard)
        })
        .await?
    }

    pub async fn reconnect(&mut self) -> DMResult<()> {
        let p = self.path.clone();
        let conn = tokio::task::spawn_blocking(move || open(&p)).await??;
        self.interrupt = Arc::new(conn.get_interrupt_handle());
        self.conn = Arc::new(Mutex::new(conn));
        Ok(())
    }
}
