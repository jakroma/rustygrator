//! Oracle connection (ORA-1): `oracle://user:password@host:1521/service_name` (Easy Connect),
//! TLS (TCPS) from the same `sslmode` / `sslrootcert` / `sslcert` parameters as PostgreSQL. The
//! `oracle` crate is blocking, so every call runs on tokio's blocking pool.

use std::sync::Arc;

use crate::{
    db::tls::SslMode,
    models::{connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings},
    DMError, DMResult,
};

use super::oracle_cdc::OracleCdcState;

/// UTC session time zone (TIMESTAMP WITH LOCAL TIME ZONE values arrive in UTC) and fixed NLS
/// formats, so numbers and dates bound as text and LogMiner's SQL_REDO literals parse the same
/// way on every server.
const SESSION_INIT: &str = "ALTER SESSION SET TIME_ZONE = '+00:00' NLS_NUMERIC_CHARACTERS = '.,' \
     NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9' \
     NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'";

/// Oracle identifiers are at most 128 bytes (12.2+).
pub const MAX_IDENT: usize = 128;

pub struct Oracle {
    pub conn: Arc<oracle::Connection>,
    pub settings: MigrationExecutionSettings,
    pub(crate) connection: MigrationConnection,
    /// Schema read from / written into (`?schema=`, default: the user in upper case).
    pub(crate) schema: String,
    /// ORA-6: `?case=upper` folds target identifiers to upper case.
    pub(crate) upper: bool,
    /// Server major version (BOOLEAN columns from 23).
    pub(crate) major: u32,
    /// ORA-2: a `SET TRANSACTION READ ONLY` transaction is open; every table read on this
    /// connection sees the data of its first read (one SCN).
    pub(crate) read_tx: bool,
    pub(crate) cdc: Option<OracleCdcState>,
    /// ORA-5: columns already reported for empty strings (container, column)
    pub(crate) empty_warned: std::collections::HashSet<(String, String)>,
}

/// Quotes an identifier (`"name"`, embedded quotes doubled).
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `"SCHEMA"."TABLE"` of a container name (`schema.table`, or a short name in `default_schema`).
pub fn qualified(default_schema: &str, container: &str) -> String {
    let (schema, table) = container.split_once('.').unwrap_or((default_schema, container));
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

/// ORA-4 / ORA-6: a target identifier: optionally upper case, names over 128 bytes shortened
/// with a hash of the full name so different long names stay different.
pub fn target_ident(name: &str, upper: bool) -> String {
    let name = if upper { name.to_uppercase() } else { name.to_string() };
    if name.len() <= MAX_IDENT {
        return name;
    }
    // FNV-1a: stable between runs and platforms
    let hash = name.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ b as u64).wrapping_mul(0x0100_0000_01b3));
    let mut end = MAX_IDENT - 9;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}_{:08x}", &name[..end], hash as u32)
}

/// Easy Connect string of a connection: `host:port/service`, or with TLS
/// `tcps://host:port/service?ssl_server_dn_match=..&wallet_location=..`. Oracle clients read
/// certificates from a wallet directory, so the folder of `sslrootcert` (or `sslcert`) is used
/// as the wallet location (it must hold `ewallet.pem` or `cwallet.sso`).
pub(crate) fn connect_string(c: &MigrationConnection) -> String {
    let address = format!("{}:{}/{}", c.host, c.port, c.database);
    let Some(tls) = &c.tls else { return address };
    let mut s = format!("tcps://{address}?ssl_server_dn_match={}", if tls.mode == SslMode::VerifyFull { "on" } else { "off" });
    if let Some(dir) = tls.root_cert.as_ref().or(tls.client_cert.as_ref()).and_then(|p| p.parent()).filter(|d| !d.as_os_str().is_empty()) {
        s.push_str("&wallet_location=");
        s.push_str(&dir.display().to_string());
    }
    s
}

/// `?case=upper` in the connection string (ORA-6).
fn fold_upper(c: &MigrationConnection) -> bool {
    let query = c.full_url.split_once('?').map(|(_, q)| q).unwrap_or("");
    url::form_urlencoded::parse(query.as_bytes()).any(|(k, v)| k == "case" && v.eq_ignore_ascii_case("upper"))
}

pub(crate) fn open(c: &MigrationConnection) -> DMResult<oracle::Connection> {
    let conn = oracle::Connector::new(c.user.clone(), c.password.clone(), connect_string(c))
        .stmt_cache_size(64)
        .connect()
        .map_err(|e| match e.dpi_code() {
            Some(1047) => DMError::Error(format!(
                "Oracle Instant Client not found: install it and add its folder to PATH (Windows) or LD_LIBRARY_PATH (Linux) ({e})"
            )),
            _ => DMError::from(e),
        })?;
    conn.execute(SESSION_INIT, &[])?;
    Ok(conn)
}

impl Oracle {
    pub async fn new(connection: MigrationConnection, settings: MigrationExecutionSettings) -> DMResult<Oracle> {
        let c = connection.clone();
        let (conn, major) = tokio::task::spawn_blocking(move || -> DMResult<_> {
            let conn = open(&c)?;
            let major = conn.server_version()?.0.major() as u32;
            Ok((conn, major))
        })
        .await??;
        let schema = connection.schema.clone().unwrap_or_else(|| connection.user.to_uppercase());
        Ok(Oracle { conn: Arc::new(conn), settings, upper: fold_upper(&connection), connection, schema, major, read_tx: false, cdc: None, empty_warned: Default::default() })
    }

    /// Schema the receiver writes into.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Runs blocking driver code on tokio's blocking pool.
    pub(crate) async fn blocking<T, F>(&self, f: F) -> DMResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&oracle::Connection) -> DMResult<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || f(&conn)).await?
    }

    /// Ends the read-only snapshot transaction of the provider (before writes or LogMiner calls).
    pub(crate) async fn end_read_tx(&mut self) -> DMResult<()> {
        if self.read_tx {
            self.blocking(|c| Ok(c.commit()?)).await?;
            self.read_tx = false;
        }
        Ok(())
    }

    pub async fn reconnect(&mut self) -> DMResult<()> {
        let c = self.connection.clone();
        self.conn = Arc::new(tokio::task::spawn_blocking(move || open(&c)).await??);
        self.read_tx = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ora1_connect_strings() {
        let c = MigrationConnection::new(&"oracle://dm:p%40ss@db.local/FREEPDB1".to_string()).unwrap();
        assert_eq!((c.user.as_str(), c.password.as_str(), c.port), ("dm", "p@ss", 1521));
        assert_eq!(connect_string(&c), "db.local:1521/FREEPDB1");
        let c = MigrationConnection::new(&"oracle://dm:pw@db:2484/svc?sslmode=verify-full&sslrootcert=/etc/wallet/ewallet.pem".to_string()).unwrap();
        assert_eq!(connect_string(&c), "tcps://db:2484/svc?ssl_server_dn_match=on&wallet_location=/etc/wallet");
        let c = MigrationConnection::new(&"oracle://dm:pw@db/svc?sslmode=require&schema=SHOP&case=upper".to_string()).unwrap();
        assert_eq!(connect_string(&c), "tcps://db:1521/svc?ssl_server_dn_match=off");
        assert_eq!(c.schema.as_deref(), Some("SHOP"));
        assert!(fold_upper(&c));
    }

    #[test]
    fn ora6_identifiers() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(qualified("DM", "orders"), "\"DM\".\"orders\"");
        assert_eq!(qualified("DM", "SHOP.orders"), "\"SHOP\".\"orders\"");
        assert_eq!(target_ident("Orders", true), "ORDERS");
        assert_eq!(target_ident("Orders", false), "Orders");
        let long = "x".repeat(200);
        let short = target_ident(&long, false);
        assert_eq!(short.len(), MAX_IDENT);
        assert_ne!(short, target_ident(&format!("{long}y"), false));
        assert_eq!(short, target_ident(&long, false));
    }
}
