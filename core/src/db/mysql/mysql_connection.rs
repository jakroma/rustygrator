//! MySQL / MariaDB connection (MY-1): `mysql://user:password@host:3306/database`, TLS with the
//! same `sslmode` / `sslrootcert` / `sslcert` / `sslkey` parameters as PostgreSQL (CONN-3).

use mysql_async::{prelude::Queryable, ClientIdentity, Conn, Opts, OptsBuilder, SslOpts};

use crate::{
    db::tls::SslMode,
    models::{connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings},
    DMResult,
};

use super::mysql_cdc::MysqlCdcState;

pub struct Mysql {
    pub conn: Conn,
    pub settings: MigrationExecutionSettings,
    pub(crate) connection: MigrationConnection,
    pub(crate) cdc: Option<MysqlCdcState>,
}

/// Quotes an identifier (`` `name` ``, embedded backticks doubled).
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `database.table` of a container name (`db.table`, or a short name in `default_db`).
pub fn qualified(default_db: &str, container: &str) -> String {
    let (db, table) = container.split_once('.').unwrap_or((default_db, container));
    format!("{}.{}", quote_ident(db), quote_ident(table))
}

pub(crate) fn opts(c: &MigrationConnection) -> DMResult<Opts> {
    let mut b = OptsBuilder::default()
        .ip_or_hostname(c.host.clone())
        .tcp_port(c.port)
        .user(Some(c.user.clone()))
        .pass(Some(c.password.clone()).filter(|p| !p.is_empty()))
        // no default database: the receiver creates it (connect() selects it when it exists)
        .prefer_socket(false)
        // UTC for TIMESTAMP values, utf8mb4 for all text
        .init(vec!["SET time_zone = '+00:00', NAMES utf8mb4".to_string()]);
    if let Some(tls) = &c.tls {
        let mut ssl = SslOpts::default()
            .with_danger_accept_invalid_certs(tls.mode == SslMode::Require)
            .with_danger_skip_domain_validation(tls.mode != SslMode::VerifyFull);
        if let Some(ca) = &tls.root_cert {
            ssl = ssl.with_root_certs(vec![ca.clone().into()]);
        }
        if let (Some(cert), Some(key)) = (&tls.client_cert, &tls.client_key) {
            ssl = ssl.with_client_identity(Some(ClientIdentity::new(cert.clone().into(), key.clone().into())));
        }
        b = b.ssl_opts(ssl);
    }
    Ok(b.into())
}

pub(crate) async fn connect(c: &MigrationConnection) -> DMResult<Conn> {
    let mut conn = Conn::new(opts(c)?).await?;
    // default database for queries typed in the app; a receiver creates a missing one later
    let _ = conn.query_drop(format!("USE {}", quote_ident(&c.database))).await;
    Ok(conn)
}

impl Mysql {
    pub async fn new(connection: MigrationConnection, settings: MigrationExecutionSettings) -> DMResult<Mysql> {
        let conn = connect(&connection).await?;
        Ok(Mysql { conn, settings, connection, cdc: None })
    }

    /// Database the receiver writes into (the one in the connection string).
    pub fn database(&self) -> &str {
        &self.connection.database
    }

    pub async fn reconnect(&mut self) -> DMResult<()> {
        self.conn = connect(&self.connection).await?;
        Ok(())
    }

    /// Server version text, e.g. `8.4.2` or `11.4.3-MariaDB`.
    pub async fn version(&mut self) -> DMResult<String> {
        Ok(self.conn.query_first::<String, _>("SELECT VERSION()").await?.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn my1_identifiers_are_quoted() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(qualified("shop", "users"), "`shop`.`users`");
        assert_eq!(qualified("shop", "crm.users"), "`crm`.`users`");
    }

    #[test]
    fn my1_options_from_the_connection_string() {
        let c = MigrationConnection::new(&"mysql://root:p%40ss@db.local/shop?sslmode=verify-ca".to_string()).unwrap();
        let o = opts(&c).unwrap();
        assert_eq!((o.ip_or_hostname(), o.tcp_port(), o.user(), o.pass()), ("db.local", 3306, Some("root"), Some("p@ss")));
        assert_eq!(c.database, "shop");
        let ssl = o.ssl_opts().unwrap();
        assert!(ssl.skip_domain_validation() && !ssl.accept_invalid_certs());
    }
}
