use std::sync::Arc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use crate::{
    migrator::batching::BatchSizer,
    migrator::migrator_steps::{ChangeReceiver, ChangeSource, DataProvider, DataReceiver},
    models::{
        change::{ChangeEvent, Checkpoint},
        connection::MigrationConnection,
        data_definitions::DataDefinition,
        migration_execution_settings::{ContainerFilter, MigrationExecutionSettings},
    },
    dbc::batch::ColumnBatch,
    DMError, DMResult,
};

use super::{mongodb::mongodb::Mongodb, mysql::mysql_connection::Mysql, oracle::oracle_connection::Oracle, postgresql::postgresql::Postgresql, scylladb::scylladb::Scylla, sqlite::sqlite::Sqlite};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseType {
    Postgresql,
    Mongodb,
    Sqlite,
    Scylla,
    Mysql,
    Oracle,
}

impl DatabaseType {
    pub fn is_sql(self) -> bool {
        matches!(self, DatabaseType::Postgresql | DatabaseType::Sqlite | DatabaseType::Mysql | DatabaseType::Oracle)
    }
}

pub enum DatabaseConnectionType {
    Postgresql(Postgresql),
    Mongodb(Mongodb),
    Sqlite(Sqlite),
    Scylla(Scylla),
    Mysql(Mysql),
    Oracle(Oracle),
}

impl DatabaseConnectionType {
    pub fn new(s: &str) -> DMResult<DatabaseType> {
        if s.starts_with("postgresql") || s.starts_with("postgres:") {
            Ok(DatabaseType::Postgresql)
        } else if s.starts_with("mongodb") {
            Ok(DatabaseType::Mongodb)
        } else if s.starts_with("sqlite:") {
            Ok(DatabaseType::Sqlite)
        } else if s.starts_with("mysql://") || s.starts_with("mariadb://") {
            Ok(DatabaseType::Mysql)
        } else if s.starts_with("oracle://") {
            Ok(DatabaseType::Oracle)
        } else if s.starts_with("scylla://") || s.starts_with("cassandra://") {
            Ok(DatabaseType::Scylla)
        } else {
            Err(DMError::NotSupportedDb())
        }
    }

    pub fn db_type(&self) -> DatabaseType {
        match self {
            DatabaseConnectionType::Postgresql(_) => DatabaseType::Postgresql,
            DatabaseConnectionType::Mongodb(_) => DatabaseType::Mongodb,
            DatabaseConnectionType::Sqlite(_) => DatabaseType::Sqlite,
            DatabaseConnectionType::Scylla(_) => DatabaseType::Scylla,
            DatabaseConnectionType::Mysql(_) => DatabaseType::Mysql,
            DatabaseConnectionType::Oracle(_) => DatabaseType::Oracle,
        }
    }
}


pub async fn connect(connection_model: MigrationConnection, settings: MigrationExecutionSettings) -> DMResult<DatabaseConnectionType> {
    Ok(match connection_model.db_type {
        DatabaseType::Postgresql => DatabaseConnectionType::Postgresql(Postgresql::new(connection_model, settings).await?),
        DatabaseType::Mongodb => DatabaseConnectionType::Mongodb(Mongodb::new(connection_model, settings).await?),
        DatabaseType::Sqlite => DatabaseConnectionType::Sqlite(Sqlite::new(connection_model, settings).await?),
        DatabaseType::Scylla => DatabaseConnectionType::Scylla(Scylla::new(connection_model, settings).await?),
        DatabaseType::Mysql => DatabaseConnectionType::Mysql(Mysql::new(connection_model, settings).await?),
        DatabaseType::Oracle => DatabaseConnectionType::Oracle(Oracle::new(connection_model, settings).await?),
    })
}

macro_rules! dispatch {
    ($self:ident, $db:ident => $e:expr) => {
        match $self {
            DatabaseConnectionType::Postgresql($db) => $e,
            DatabaseConnectionType::Mongodb($db) => $e,
            DatabaseConnectionType::Sqlite($db) => $e,
            DatabaseConnectionType::Scylla($db) => $e,
            DatabaseConnectionType::Mysql($db) => $e,
            DatabaseConnectionType::Oracle($db) => $e,
        }
    };
}

#[async_trait]
impl DataProvider for DatabaseConnectionType {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        dispatch!(self, d => d.get_definitions(filter).await)
    }

    async fn read_data(&mut self, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        dispatch!(self, d => d.read_data(def, sizer, tx).await)
    }
}

#[async_trait]
impl DataReceiver for DatabaseConnectionType {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        dispatch!(self, d => d.prepare(defs).await)
    }

    async fn write_data(&mut self, def: &DataDefinition, rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        dispatch!(self, d => d.write_data(def, rx).await)
    }

    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        dispatch!(self, d => d.finalize(defs).await)
    }
}

#[async_trait]
impl ChangeSource for DatabaseConnectionType {
    async fn start_capture(&mut self, defs: &[DataDefinition], resume: Option<&Checkpoint>) -> DMResult<()> {
        dispatch!(self, d => d.start_capture(defs, resume).await)
    }

    async fn poll_changes(&mut self, max: usize, wait: Duration, shutdown: &watch::Receiver<bool>) -> DMResult<Vec<ChangeEvent>> {
        dispatch!(self, d => d.poll_changes(max, wait, shutdown).await)
    }

    async fn acknowledge(&mut self) -> DMResult<Option<Checkpoint>> {
        dispatch!(self, d => ChangeSource::acknowledge(d).await)
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        dispatch!(self, d => ChangeSource::reconnect(d).await)
    }
}

#[async_trait]
impl ChangeReceiver for DatabaseConnectionType {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()> {
        dispatch!(self, d => d.apply_changes(defs, events).await)
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        dispatch!(self, d => ChangeReceiver::reconnect(d).await)
    }
}

/// Topological order of containers (FN-5): every container comes after the containers it
/// references. Containers without dependencies are kept, self references are ignored,
/// references to containers that are not migrated are ignored, and the result is deterministic.
pub fn sort_tables_by_dependencies(order_hash: &HashMap<String, Vec<String>>) -> DMResult<Vec<String>> {
    let tables: BTreeSet<&String> = order_hash.keys().collect();
    let mut dependents: BTreeMap<&String, Vec<&String>> = BTreeMap::new();
    let mut in_degree: BTreeMap<&String, usize> = tables.iter().map(|t| (*t, 0)).collect();

    for (table, dependencies) in order_hash {
        let unique: BTreeSet<&String> = dependencies.iter().filter(|d| *d != table && tables.contains(d)).collect();
        for dependency in unique {
            dependents.entry(dependency).or_default().push(table);
            *in_degree.get_mut(table).expect("table is a key") += 1;
        }
    }

    // BTreeSet as a priority queue keeps the output stable between runs
    let mut ready: BTreeSet<&String> = in_degree.iter().filter(|(_, d)| **d == 0).map(|(t, _)| *t).collect();
    let mut sorted_tables: Vec<String> = Vec::with_capacity(tables.len());

    while let Some(table) = ready.pop_first() {
        sorted_tables.push(table.clone());
        if let Some(children) = dependents.get(table) {
            for child in children {
                let degree = in_degree.get_mut(child).expect("table is a key");
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(child);
                }
            }
        }
    }

    if sorted_tables.len() != tables.len() {
        return Err(DMError::Error("Cycle detected in table dependencies".to_string()));
    }

    Ok(sorted_tables)
}

/// Orders definitions with [`sort_tables_by_dependencies`].
pub fn order_definitions(defs: Vec<DataDefinition>) -> DMResult<Vec<DataDefinition>> {
    let order_hash: HashMap<String, Vec<String>> = defs.iter().map(|d| (d.container_name.clone(), d.dependencies())).collect();
    let order = sort_tables_by_dependencies(&order_hash)?;
    let mut by_name: HashMap<String, DataDefinition> = defs.into_iter().map(|d| (d.container_name.clone(), d)).collect();
    Ok(order.into_iter().filter_map(|n| by_name.remove(&n)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs.iter().map(|(t, d)| (t.to_string(), d.iter().map(|x| x.to_string()).collect())).collect()
    }

    #[test]
    fn keeps_tables_without_dependencies() {
        let order = sort_tables_by_dependencies(&h(&[("a", &[]), ("b", &[])])).unwrap();
        assert_eq!(order, vec!["a", "b"]);
    }

    #[test]
    fn parents_come_first_and_self_references_are_ignored() {
        let order = sort_tables_by_dependencies(&h(&[
            ("users", &["user_types"]),
            ("user_types", &[]),
            ("orders", &["users", "orders", "missing"]),
            ("logs", &[]),
        ]))
        .unwrap();
        let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
        assert_eq!(order.len(), 4);
        assert!(pos("user_types") < pos("users"));
        assert!(pos("users") < pos("orders"));
    }

    #[test]
    fn detects_cycles() {
        assert!(sort_tables_by_dependencies(&h(&[("a", &["b"]), ("b", &["a"])])).is_err());
    }
}
