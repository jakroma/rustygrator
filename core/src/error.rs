use url::ParseError;

#[derive(thiserror::Error, Debug)]
pub enum DMError {
    #[error("IO error: {0}")]
    IoError(std::io::Error),
    #[error("PostgreSQL error: {}", pg_message(.0))]
    PostgreSQLError(tokio_postgres::Error),
    #[error("MongoDB error: {0}")]
    MongoDbError(mongodb::error::Error),
    #[error("SQLite error: {0}")]
    SqliteError(rusqlite::Error),
    #[error("Oracle error: {0}")]
    OracleError(oracle::Error),
    #[error("MySQL error: {0}")]
    MysqlError(mysql_async::Error),
    #[error("Not supported type")]
    NotSupportedType(),
    #[error("Not supported db")]
    NotSupportedDb(),
    #[error("Invalid in connection string: {0}")]
    InvalidConnectionString(String),
    #[error("url parser error: {0}")]
    UrlParseError(ParseError),
    #[error("Directory does not exist: {0}")]
    NotExistsDirectoryError(String),
    #[error("Json error")]
    JsonError(),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error("Error: {0}")]
    Error(String),
}

impl DMError {
    /// Connection level failures worth a reconnect + retry in live mode (LIVE-12).
    pub fn is_connection_error(&self) -> bool {
        match self {
            DMError::PostgreSQLError(e) => e.is_closed() || e.code().is_none(),
            DMError::MongoDbError(e) => matches!(
                *e.kind,
                mongodb::error::ErrorKind::Io(_)
                    | mongodb::error::ErrorKind::ServerSelection { .. }
                    | mongodb::error::ErrorKind::ConnectionPoolCleared { .. }
            ) || e.contains_label("RetryableWriteError") || e.contains_label("ResumableChangeStreamError"),
            DMError::IoError(_) => true,
            DMError::MysqlError(e) => matches!(e, mysql_async::Error::Io(_) | mysql_async::Error::Driver(_)),
            // ORA-03113/03114/03135 lost connection, ORA-12xxx network, ORA-01012/01089 session ended; DPI-1010/1080 closed
            DMError::OracleError(e) => matches!(e.oci_code(), Some(3113 | 3114 | 3135 | 1012 | 1089 | 12152 | 12170 | 12514 | 12528 | 12537 | 12541 | 12543 | 12547 | 12571 | 28547)) || matches!(e.dpi_code(), Some(1010 | 1080)),
            _ => false,
        }
    }
}

impl From<ParseError> for DMError {
    fn from(err: ParseError) -> DMError {
        DMError::UrlParseError(err)
    }
}

impl From<std::io::Error> for DMError {
    fn from(err: std::io::Error) -> DMError {
        DMError::IoError(err)
    }
}

impl From<tokio_postgres::Error> for DMError {
    fn from(err: tokio_postgres::Error) -> DMError {
        DMError::PostgreSQLError(err)
    }
}

impl From<mongodb::error::Error> for DMError {
    fn from(err: mongodb::error::Error) -> DMError {
        DMError::MongoDbError(err)
    }
}

impl From<mysql_async::Error> for DMError {
    fn from(err: mysql_async::Error) -> DMError {
        DMError::MysqlError(err)
    }
}

impl From<mysql_async::FromRowError> for DMError {
    fn from(err: mysql_async::FromRowError) -> DMError {
        DMError::Error(format!("MySQL row: {err}"))
    }
}

impl From<rusqlite::Error> for DMError {
    fn from(err: rusqlite::Error) -> DMError {
        DMError::SqliteError(err)
    }
}

impl From<mongodb::bson::ser::Error> for DMError {
    fn from(err: mongodb::bson::ser::Error) -> DMError {
        DMError::Error(format!("BSON serialization: {}", err))
    }
}

impl From<mongodb::bson::de::Error> for DMError {
    fn from(err: mongodb::bson::de::Error) -> DMError {
        DMError::Error(format!("BSON deserialization: {}", err))
    }
}

impl From<tokio::task::JoinError> for DMError {
    fn from(err: tokio::task::JoinError) -> DMError {
        DMError::Error(format!("task failed: {}", err))
    }
}

/// tokio-postgres prints only "db error" for server errors; the server's message is the useful part.
fn pg_message(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{} ({})", db.message(), db.code().code()),
        None => match std::error::Error::source(e) {
            Some(cause) => format!("{e}: {cause}"),
            None => e.to_string(),
        },
    }
}

impl From<oracle::Error> for DMError {
    fn from(err: oracle::Error) -> DMError {
        DMError::OracleError(err)
    }
}
