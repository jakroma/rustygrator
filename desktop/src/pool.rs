//! Connection pool per saved connection (CONN-4): browsing, queries, migrations and live
//! transfers take connections from here. A connection that failed is not returned; the next
//! request opens a new one, and the status tells the UI whether the database is reachable.

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use rustygrator_core::{
    db::database::{connect, DatabaseConnectionType},
    models::{connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings},
};

use crate::connections::{host_port, open_tunnel, through_tunnel, with_password, with_tls, Profile, Tunnel};

#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Connecting,
    Connected,
    Failed(String),
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").field("status", &self.status()).finish_non_exhaustive()
    }
}

pub struct Pool {
    /// Connection string with password, TLS parameters and tunnel applied.
    pub url: String,
    pub settings: MigrationExecutionSettings,
    idle: Mutex<Vec<DatabaseConnectionType>>,
    status: Mutex<Status>,
    max_idle: usize,
    _tunnel: Option<Tunnel>,
}

/// Documents sampled to describe a MongoDB collection while browsing.
const BROWSE_SCHEMA_SAMPLE: u32 = 1000;

impl Pool {
    /// Builds the effective connection string and opens the SSH tunnel if there is one
    /// (blocking: call on the blocking pool).
    pub fn new(profile: &Profile, password: Option<&str>) -> Result<Pool, String> {
        let mut url = with_tls(&with_password(&profile.url, password), &profile.tls);
        let tunnel = match (&profile.ssh, host_port(&url)) {
            (Some(ssh), Some((host, port))) => {
                let t = open_tunnel(ssh, &host, port)?;
                url = through_tunnel(&url, t.local_port);
                Some(t)
            }
            _ => None,
        };
        MigrationConnection::new(&url).map_err(|e| e.to_string())?;
        let mut settings = MigrationExecutionSettings::new(4, 1000);
        settings.schema_sample = BROWSE_SCHEMA_SAMPLE;
        Ok(Pool { url, settings, idle: Mutex::new(Vec::new()), status: Mutex::new(Status::Connecting), max_idle: 4, _tunnel: tunnel })
    }

    pub fn status(&self) -> Status {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn set_status(&self, s: Status) {
        *self.status.lock().unwrap_or_else(|e| e.into_inner()) = s;
    }

    pub fn connection(&self) -> MigrationConnection {
        MigrationConnection::new(&self.url).expect("checked in Pool::new")
    }

    /// An idle connection, or a new one.
    pub async fn get(self: &Arc<Self>) -> Result<Pooled, String> {
        let idle = self.idle.lock().unwrap_or_else(|e| e.into_inner()).pop();
        let conn = match idle {
            Some(c) => c,
            None => match connect(self.connection(), self.settings.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    self.set_status(Status::Failed(e.to_string()));
                    return Err(e.to_string());
                }
            },
        };
        self.set_status(Status::Connected);
        Ok(Pooled { conn: Some(conn), pool: self.clone(), broken: false })
    }

    /// A new connection that is not returned to the pool (migrations keep it to the end).
    pub async fn detached(self: &Arc<Self>) -> Result<DatabaseConnectionType, String> {
        let mut p = self.get().await?;
        p.broken = true;
        Ok(p.conn.take().expect("set in get"))
    }

    #[cfg(test)]
    pub fn idle_count(&self) -> usize {
        self.idle.lock().unwrap().len()
    }
}

pub struct Pooled {
    conn: Option<DatabaseConnectionType>,
    pool: Arc<Pool>,
    /// Set after an error: the connection is dropped instead of reused.
    pub broken: bool,
}

impl Deref for Pooled {
    type Target = DatabaseConnectionType;
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("taken only on drop")
    }
}

impl DerefMut for Pooled {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("taken only on drop")
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        if let Some(c) = self.conn.take() {
            let mut idle = self.pool.idle.lock().unwrap_or_else(|e| e.into_inner());
            if !self.broken && idle.len() < self.pool.max_idle {
                idle.push(c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::Tls;

    fn profile(url: &str) -> Profile {
        Profile { id: "p".into(), name: "p".into(), url: url.into(), tls: Tls::default(), ssh: None }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conn4_connections_are_reused_and_broken_ones_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("a.db").display());
        let pool = Arc::new(Pool::new(&profile(&url), None).unwrap());
        assert_eq!(pool.status(), Status::Connecting);
        {
            let _a = pool.get().await.unwrap();
            let _b = pool.get().await.unwrap();
        }
        assert_eq!(pool.status(), Status::Connected);
        assert_eq!(pool.idle_count(), 2);
        {
            let mut c = pool.get().await.unwrap();
            c.broken = true;
        }
        assert_eq!(pool.idle_count(), 1);
        let _kept = pool.detached().await.unwrap();
        assert_eq!(pool.idle_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conn4_failed_connection_is_reported() {
        let pool = Arc::new(Pool::new(&profile("postgresql://u@127.0.0.1:1/db"), Some("pw")).unwrap());
        assert!(pool.get().await.is_err());
        assert!(matches!(pool.status(), Status::Failed(_)));
        assert!(Pool::new(&profile("ftp://nothing"), None).is_err());
    }
}
