use log::error;
use tokio_postgres::{Client, Config, NoTls};

use crate::{
    models::{
        connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings,
    },
    DMResult,
};

use super::postgresql_cdc::PgCdcState;

pub struct Postgresql {
    pub client: Client,
    pub schema: Option<String>,
    pub settings: MigrationExecutionSettings,
    pub(crate) connection: MigrationConnection,
    pub(crate) cdc: Option<PgCdcState>,
}

/// Quotes an identifier (`"name"`, embedded quotes doubled).
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quotes a string literal.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) async fn connect(connection_model: &MigrationConnection) -> DMResult<Client> {
    let mut config = Config::new();
    config
        .host(&connection_model.host)
        .port(connection_model.port)
        .user(&connection_model.user)
        .password(&connection_model.password)
        .dbname(&connection_model.database)
        .application_name("rustygrator");
    let client = match &connection_model.tls {
        Some(tls) => {
            config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let (client, connection) = config.connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls.client_config()?)).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("PostgreSQL connection error: {}", e);
                }
            });
            client
        }
        None => {
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("PostgreSQL connection error: {}", e);
                }
            });
            client
        }
    };
    Ok(client)
}

impl Postgresql {
    pub async fn new(
        connection_model: MigrationConnection,
        exec_settings: MigrationExecutionSettings,
    ) -> DMResult<Postgresql> {
        let client = connect(&connection_model).await?;
        Ok(Postgresql {
            client,
            settings: exec_settings,
            schema: connection_model.schema.clone(),
            connection: connection_model,
            cdc: None,
        })
    }

    /// Target schema of the receiver (`?schema=`, default `public`).
    pub fn target_schema(&self) -> &str {
        self.schema.as_deref().unwrap_or(crate::constants::postgresql::DEFAULT_SCHEMA)
    }

    pub fn qualified_target(&self, table: &str) -> String {
        format!("{}.{}", quote_ident(self.target_schema()), quote_ident(table))
    }

    pub async fn reconnect(&mut self) -> DMResult<()> {
        self.client = connect(&self.connection).await?;
        Ok(())
    }
}
