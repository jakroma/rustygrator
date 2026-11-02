use url::Url;

use crate::{
    db::database::{DatabaseConnectionType, DatabaseType},
    DMError, DMResult,
};

#[derive(Clone, Debug)]
pub struct MigrationConnection {
    pub full_url: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// Database name; for SQLite the path of the database file.
    pub database: String,
    pub db_type: DatabaseType,
    /// `?schema=` query parameter: source schema filter / target schema (FN-6).
    pub schema: Option<String>,
    /// PostgreSQL / ScyllaDB TLS from `sslmode` and friends (CONN-3).
    pub tls: Option<crate::db::tls::TlsOptions>,
}

/// User names and passwords are percent-encoded in a URL (`p%40ss` is `p@ss`).
pub fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s).decode_utf8_lossy().into_owned()
}

impl MigrationConnection {
    pub fn new(full_url: &String) -> DMResult<MigrationConnection> {
        let db_type = DatabaseConnectionType::new(full_url)?;
        match db_type {
            DatabaseType::Sqlite => return Self::new_sqlite(full_url),
            DatabaseType::Mongodb => return Self::new_mongodb(full_url),
            DatabaseType::Scylla => return Self::new_scylla(full_url),
            DatabaseType::Postgresql | DatabaseType::Mysql | DatabaseType::Oracle => {}
        }
        let url = Url::parse(full_url)?;

        let user = decode(url.username());
        let password = url.password().map(decode).unwrap_or_default();
        if user.is_empty() {
            return Err(DMError::InvalidConnectionString(String::from("username")));
        }

        let host = match url.host_str() {
            Some(host) => host.to_string(),
            None => return Err(DMError::InvalidConnectionString(String::from("host"))),
        };

        let port = url.port().unwrap_or(match db_type {
            DatabaseType::Postgresql => 5432,
            DatabaseType::Oracle => 1521,
            _ => 3306,
        });

        let database = match url.path_segments().and_then(|mut s| s.next()) {
            Some(x) if !x.is_empty() => x.to_string(),
            _ => return Err(DMError::InvalidConnectionString(String::from("database"))),
        };

        let mut schema = None;
        for (key, value) in url.query_pairs() {
            if key.eq_ignore_ascii_case("schema") {
                schema = Some(value.to_string());
            }
        }

        Ok(MigrationConnection {
            full_url: full_url.to_owned(),
            host,
            port,
            user,
            password,
            database,
            db_type,
            schema,
            tls: crate::db::tls::TlsOptions::from_query(url.query_pairs())?,
        })
    }

    /// MongoDB connection strings may list several hosts (`mongodb://a:1,b:2/db?replicaSet=rs`)
    /// or use `mongodb+srv://`, which `url::Url` cannot parse; the driver parses the full URL,
    /// here only the database name (required) and the first host are extracted.
    fn new_mongodb(full_url: &str) -> DMResult<MigrationConnection> {
        let rest = full_url.split_once("://").map(|(_, r)| r).unwrap_or("");
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let database = path.split('?').next().unwrap_or("");
        if database.is_empty() {
            return Err(DMError::InvalidConnectionString(String::from("database")));
        }
        let (credentials, hosts) = authority.rsplit_once('@').unwrap_or(("", authority));
        let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
        let first = hosts.split(',').next().unwrap_or("");
        let (host, port) = match first.rsplit_once(':') {
            Some((h, p)) => (h, p.parse().unwrap_or(27017)),
            None => (first, 27017),
        };
        if host.is_empty() {
            return Err(DMError::InvalidConnectionString(String::from("host")));
        }
        Ok(MigrationConnection {
            full_url: full_url.to_owned(),
            host: host.to_string(),
            port,
            user: decode(user),
            password: decode(password),
            database: database.to_string(),
            db_type: DatabaseType::Mongodb,
            schema: None,
            tls: None,
        })
    }

    /// `scylla://[user:password@]host[:port][,host2[:port]]/keyspace` (SCY-1); `cassandra://`
    /// works too. All hosts are passed to the driver as contact points.
    fn new_scylla(full_url: &str) -> DMResult<MigrationConnection> {
        let mut c = Self::new_mongodb(full_url)?;
        c.db_type = DatabaseType::Scylla;
        let query = full_url.split_once('?').map(|(_, q)| q).unwrap_or("");
        c.tls = crate::db::tls::TlsOptions::from_query(url::form_urlencoded::parse(query.as_bytes()))?;
        c.port = c.hosts(9042).first().and_then(|h| h.rsplit_once(':')).and_then(|(_, p)| p.parse().ok()).unwrap_or(9042);
        Ok(c)
    }

    /// Contact points `host:port` of a multi-host URL.
    pub fn hosts(&self, default_port: u16) -> Vec<String> {
        let rest = self.full_url.split_once("://").map(|(_, r)| r).unwrap_or("");
        let authority = rest.split('/').next().unwrap_or("");
        let hosts = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
        hosts
            .split(',')
            .filter(|h| !h.is_empty())
            .map(|h| if h.contains(':') { h.to_string() } else { format!("{}:{}", h, default_port) })
            .collect()
    }

    /// `sqlite://relative/or/absolute/path.db`, `sqlite:///abs/path.db`, `sqlite:path.db`.
    fn new_sqlite(full_url: &str) -> DMResult<MigrationConnection> {
        let rest = full_url
            .strip_prefix("sqlite://")
            .or_else(|| full_url.strip_prefix("sqlite:"))
            .unwrap_or("");
        let path = rest.split('?').next().unwrap_or("");
        if path.is_empty() {
            return Err(DMError::InvalidConnectionString(String::from("database file path")));
        }
        Ok(MigrationConnection {
            full_url: full_url.to_owned(),
            host: String::new(),
            port: 0,
            user: String::new(),
            password: String::new(),
            database: path.to_string(),
            db_type: DatabaseType::Sqlite,
            schema: None,
            tls: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_postgres_url_with_schema() {
        let c = MigrationConnection::new(&"postgresql://user:password@127.0.0.1:5433/test?schema=public".to_string()).unwrap();
        assert_eq!((c.host.as_str(), c.port, c.database.as_str()), ("127.0.0.1", 5433, "test"));
        assert_eq!(c.schema.as_deref(), Some("public"));
    }

    #[test]
    fn parses_mongo_url_without_credentials() {
        let c = MigrationConnection::new(&"mongodb://mongo:27017/dm?replicaSet=rs0".to_string()).unwrap();
        assert_eq!(c.database, "dm");
        assert!(matches!(c.db_type, DatabaseType::Mongodb));
    }

    #[test]
    fn parses_mongo_multi_host_and_srv() {
        let c = MigrationConnection::new(&"mongodb://root:ex@a:27018,b:27019/shop?replicaSet=rs0".to_string()).unwrap();
        assert_eq!((c.host.as_str(), c.port, c.database.as_str(), c.user.as_str()), ("a", 27018, "shop", "root"));
        let c = MigrationConnection::new(&"mongodb+srv://u:p@cluster.example.net/app".to_string()).unwrap();
        assert_eq!(c.database, "app");
        assert!(MigrationConnection::new(&"mongodb://localhost:27017/".to_string()).is_err());
    }

    #[test]
    fn parses_scylla_hosts_and_keyspace() {
        let c = MigrationConnection::new(&"scylla://cassandra:pw@a,b:9043/shop".to_string()).unwrap();
        assert!(matches!(c.db_type, DatabaseType::Scylla));
        assert_eq!((c.database.as_str(), c.user.as_str(), c.password.as_str()), ("shop", "cassandra", "pw"));
        assert_eq!(c.hosts(9042), vec!["a:9042".to_string(), "b:9043".to_string()]);
        assert!(MigrationConnection::new(&"cassandra://h/ks".to_string()).is_ok());
    }

    #[test]
    fn parses_sqlite_paths() {
        let c = MigrationConnection::new(&"sqlite://data/test.db".to_string()).unwrap();
        assert_eq!(c.database, "data/test.db");
        let c = MigrationConnection::new(&"sqlite:///tmp/x.db".to_string()).unwrap();
        assert_eq!(c.database, "/tmp/x.db");
        let c = MigrationConnection::new(&"sqlite://D:/data/x.db".to_string()).unwrap();
        assert_eq!(c.database, "D:/data/x.db");
    }

    #[test]
    fn fix1_credentials_are_percent_decoded() {
        let c = MigrationConnection::new(&"postgresql://us%3Aer:p%40ss%2Fw%25rd@h/db".to_string()).unwrap();
        assert_eq!((c.user.as_str(), c.password.as_str()), ("us:er", "p@ss/w%rd"));
        let c = MigrationConnection::new(&"scylla://u:p%40ss@h/ks".to_string()).unwrap();
        assert_eq!(c.password, "p@ss");
    }

    #[test]
    fn my1_parses_mysql_url() {
        let c = MigrationConnection::new(&"mysql://root:pw@db:3307/shop?sslmode=require".to_string()).unwrap();
        assert!(matches!(c.db_type, DatabaseType::Mysql));
        assert_eq!((c.host.as_str(), c.port, c.database.as_str()), ("db", 3307, "shop"));
        assert!(c.tls.is_some());
        assert_eq!(MigrationConnection::new(&"mariadb://u@db/x".to_string()).unwrap().port, 3306);
        assert!(MigrationConnection::new(&"mysql://db/x".to_string()).is_err());
    }

    #[test]
    fn rejects_unknown_db() {
        assert!(MigrationConnection::new(&"db2://a:b@h:1/d".to_string()).is_err());
    }
}
