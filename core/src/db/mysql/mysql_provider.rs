//! MySQL provider (MY-2): definitions from `information_schema` (three queries for the whole
//! database), data streamed with the binary protocol inside a consistent-snapshot transaction.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use log::info;
use mysql_async::{prelude::Queryable, IsolationLevel, Row, TxOpts};
use tokio::sync::mpsc;

use crate::{
    dbc::batch::ColumnBatch,
    migrator::{batching::BatchSizer, migrator_steps::DataProvider},
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo, ForeignKeyInfo, IndexInfo},
        migration_execution_settings::ContainerFilter,
    },
    DMError, DMResult,
};

use super::{
    mysql_connection::{qualified, quote_ident, Mysql},
    mysql_data_types::{logical_type_for, push_value},
};

const TABLES: &str = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
const COLUMNS: &str = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE = 'YES', COLUMN_KEY = 'PRI' \
     FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION";
const FOREIGN_KEYS: &str = "SELECT TABLE_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME \
     FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL \
     ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION";
const INDEXES: &str = "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE = 0, COLUMN_NAME FROM information_schema.STATISTICS \
     WHERE TABLE_SCHEMA = ? AND INDEX_NAME <> 'PRIMARY' ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX";

type ColumnRow = (String, String, String, String, bool, bool);
type ForeignKeyRow = (String, String, String, String, String, String);
type IndexRow = (String, String, bool, Option<String>);
/// (table, index) -> (unique, columns; `None` for an expression part)
type IndexColumns = BTreeMap<(String, String), (bool, Vec<Option<String>>)>;

/// `SELECT <columns> FROM db.table` for a definition.
pub(crate) fn select_query(db: &str, def: &DataDefinition) -> String {
    let cols: Vec<String> = def.properties_info.iter().map(|p| quote_ident(&p.property_name)).collect();
    format!("SELECT {} FROM {}", cols.join(", "), qualified(db, &def.container_name))
}

/// Streams the rows of `sql` (columns aligned with `def`) as column batches.
pub(crate) async fn stream_rows<Q: Queryable + Send>(q: &mut Q, sql: &str, def: &DataDefinition, sizer: &BatchSizer, tx: &mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
    let mut result = q.exec_iter(sql, ()).await?;
    let mut total = 0u64;
    let mut size = sizer.next();
    let mut batch = ColumnBatch::for_definition(def, size);
    if let Some(mut rows) = result.stream::<Row>().await? {
        while let Some(row) = rows.try_next().await? {
            for (i, col) in batch.columns.iter_mut().enumerate() {
                push_value(col, row.as_ref(i).unwrap_or(&mysql_async::Value::NULL))?;
            }
            batch.end_row();
            if batch.rows >= size {
                total += batch.rows as u64;
                size = sizer.next();
                tx.send(std::mem::replace(&mut batch, ColumnBatch::for_definition(def, size)))
                    .await
                    .map_err(|_| DMError::Error("receiver stopped".to_string()))?;
            }
        }
    }
    if !batch.is_empty() {
        total += batch.rows as u64;
        tx.send(batch).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
    }
    Ok(total)
}

impl Mysql {
    async fn read_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        let db = self.database().to_string();
        let exists: Option<String> = self.conn.exec_first("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", (db.as_str(),)).await?;
        if exists.is_none() {
            return Err(DMError::Error(format!("MySQL database {db} does not exist")));
        }
        let tables: Vec<String> = self.conn.exec(TABLES, (db.as_str(),)).await?;
        let columns: Vec<ColumnRow> = self.conn.exec(COLUMNS, (db.as_str(),)).await?;
        let fk_rows: Vec<ForeignKeyRow> = self.conn.exec(FOREIGN_KEYS, (db.as_str(),)).await?;
        let index_rows: Vec<IndexRow> = self.conn.exec(INDEXES, (db.as_str(),)).await?;

        let mut props: BTreeMap<String, Vec<DataPropertyInfo>> = BTreeMap::new();
        for (table, name, data_type, column_type, nullable, key) in columns {
            props.entry(table).or_default().push(DataPropertyInfo {
                property_name: name,
                data_type: logical_type_for(&data_type.to_ascii_lowercase(), &column_type.to_ascii_lowercase()),
                source_type: Some(column_type.to_ascii_lowercase()),
                is_nullable: nullable && !key,
                is_identifier: key,
                reference_container_name: None,
                reference_property_name: None,
                shape: None,
            });
        }
        let mut fks: BTreeMap<(String, String), ForeignKeyInfo> = BTreeMap::new();
        for (table, name, ref_db, ref_table, column, ref_column) in fk_rows {
            let fk = fks.entry((table, name.clone())).or_insert_with(|| ForeignKeyInfo {
                name,
                columns: Vec::new(),
                reference_container_name: format!("{ref_db}.{ref_table}"),
                reference_columns: Vec::new(),
            });
            fk.columns.push(column);
            fk.reference_columns.push(ref_column);
        }
        let mut indexes: IndexColumns = BTreeMap::new();
        for (table, name, unique, column) in index_rows {
            indexes.entry((table, name)).or_insert((unique, Vec::new())).1.push(column);
        }

        let mut defs = Vec::with_capacity(tables.len());
        for table in tables {
            let name = format!("{db}.{table}");
            if !filter.accepts(&name) {
                continue;
            }
            let mut def = DataDefinition::new(name, props.remove(&table).unwrap_or_default());
            def.foreign_keys = fks.iter().filter(|((t, _), _)| *t == table).map(|(_, fk)| fk.clone()).collect();
            def.indexes = indexes
                .iter()
                .filter(|((t, _), (_, cols))| *t == table && cols.iter().all(Option::is_some)) // functional indexes are skipped
                .map(|((_, n), (unique, cols))| IndexInfo { name: n.clone(), unique: *unique, columns: cols.iter().flatten().cloned().collect() })
                .collect();
            def.sync_property_references();
            defs.push(def);
        }
        Ok(defs)
    }
}

#[async_trait]
impl DataProvider for Mysql {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        info!("[Migration] Create table infos");
        self.read_definitions(filter).await
    }

    async fn read_data(&mut self, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let sql = select_query(self.database(), def);
        let mut opts = TxOpts::default();
        opts.with_isolation_level(IsolationLevel::RepeatableRead).with_consistent_snapshot(true).with_readonly(true);
        let mut t = self.conn.start_transaction(opts).await?;
        let total = stream_rows(&mut t, &sql, def, &sizer, &tx).await?;
        t.commit().await?;
        Ok(total)
    }
}
