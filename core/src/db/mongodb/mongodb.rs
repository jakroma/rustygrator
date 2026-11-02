use mongodb::{options::ClientOptions, Client, Database};

use crate::{
    models::{
        connection::MigrationConnection, migration_execution_settings::MigrationExecutionSettings,
    },
    DMResult,
};

use super::mongodb_cdc::MongoCdcState;

pub struct Mongodb {
    pub client: Client,
    pub exec_settings: MigrationExecutionSettings,
    pub db: Database,
    pub(crate) connection: MigrationConnection,
    pub(crate) cdc: Option<MongoCdcState>,
}

impl Mongodb {
    pub async fn new(connection_model: MigrationConnection, settings: MigrationExecutionSettings) -> DMResult<Self> {
        let client_options = ClientOptions::parse(&connection_model.full_url).await?;
        let client = Client::with_options(client_options)?;
        let db = client.database(&connection_model.database);

        Ok(Mongodb { client, db, exec_settings: settings, connection: connection_model, cdc: None })
    }

    pub async fn reconnect(&mut self) -> DMResult<()> {
        // the driver keeps a pool and reconnects by itself; recreate the client to drop broken
        // connections after a longer outage
        let client_options = ClientOptions::parse(&self.connection.full_url).await?;
        self.client = Client::with_options(client_options)?;
        self.db = self.client.database(&self.connection.database);
        Ok(())
    }
}
