use std::sync::Arc;
use async_trait::async_trait;
use futures_util::{pin_mut, TryStreamExt};
use log::info;
use tokio::sync::mpsc;
use tokio_postgres::{binary_copy::BinaryCopyOutStream, types::Type, IsolationLevel};

use crate::{
    constants::postgresql::{
        POSTGRESQL_COLUMNS_QUERY, POSTGRESQL_FOREIGN_KEYS_QUERY, POSTGRESQL_INDEXES_QUERY,
        POSTGRESQL_TABLES_QUERY,
    },
    db::postgresql::postgresql_data_types::{logical_type_for, push_copy_value, select_expression},
    dbc::batch::ColumnBatch,
    migrator::batching::BatchSizer,
    migrator::migrator_steps::DataProvider,
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo, ForeignKeyInfo, IndexInfo},
        migration_execution_settings::ContainerFilter,
    },
    DMError, DMResult,
};

use super::postgresql::{quote_ident, Postgresql};

#[async_trait]
impl DataProvider for Postgresql {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        info!("[Migration] Create table infos");
        self.create_table_info(filter).await
    }

    /// Reads the table with binary `COPY (SELECT ..) TO STDOUT` (PERF-12) inside a read-only
    /// REPEATABLE READ transaction and streams column batches.
    async fn read_data(
        &mut self,
        def: &DataDefinition,
        sizer: Arc<BatchSizer>,
        tx: mpsc::Sender<ColumnBatch>,
    ) -> DMResult<u64> {
        let query = select_query(def)?;
        let ranges = self.settings.parallel_ranges;
        let connection = self.connection.clone();
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let total = match split_ranges(&transaction, def, ranges).await? {
            Some(bounds) => {
                // PERF-11: one reader per primary key range, all on the same exported snapshot
                let snapshot: String = transaction.query_one("SELECT pg_export_snapshot()", &[]).await?.get(0);
                info!("[Migration][{}] reading {} key ranges in parallel", def.container_name, bounds.len());
                let mut tasks = tokio::task::JoinSet::new();
                for (lo, hi) in bounds {
                    let (connection, def, tx, sizer, snapshot) = (connection.clone(), def.clone(), tx.clone(), sizer.clone(), snapshot.clone());
                    let key = quote_ident(def.identifier_names()[0]);
                    let q = format!("{} WHERE {key} >= {lo} AND {key} {} {hi}", select_query(&def)?, if hi == i64::MAX { "<=" } else { "<" });
                    tasks.spawn(async move {
                        let mut client = super::postgresql::connect(&connection).await?;
                        let t = client.build_transaction().isolation_level(IsolationLevel::RepeatableRead).read_only(true).start().await?;
                        t.batch_execute(&format!("SET TRANSACTION SNAPSHOT '{}'", snapshot.replace('\'', ""))).await?;
                        let n = copy_out(&t, &def, &q, &sizer, &tx).await?;
                        t.commit().await?;
                        Ok::<u64, DMError>(n)
                    });
                }
                let mut total = 0;
                while let Some(r) = tasks.join_next().await {
                    total += r??;
                }
                total
            }
            None => copy_out(&transaction, def, &query, &sizer, &tx).await?,
        };
        transaction.commit().await?;
        Ok(total)
    }
}

/// Rows below which a table is read by one reader.
const PARALLEL_RANGE_MIN_ROWS: f32 = 100_000.0;

/// Splits a table with a single integer primary key into `parts` key ranges of similar size
/// (from the planner statistics); `None` when the table is small or has no such key.
async fn split_ranges(t: &tokio_postgres::Transaction<'_>, def: &DataDefinition, parts: usize) -> DMResult<Option<Vec<(i64, i64)>>> {
    let ids = def.identifier_indexes();
    if parts < 2 || ids.len() != 1 || !def.properties_info[ids[0]].data_type.is_integer() {
        return Ok(None);
    }
    let (schema, table) = def.container_name.split_once('.').unwrap_or(("public", &def.container_name));
    let estimate: Option<f32> = t
        .query_opt(
            "SELECT c.reltuples FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await?
        .map(|r| r.get(0));
    if estimate.unwrap_or(0.0) < PARALLEL_RANGE_MIN_ROWS {
        return Ok(None);
    }
    let key = quote_ident(def.identifier_names()[0]);
    // equal-count boundaries from the column histogram would need ANALYZE; min / max split is
    // cheap (index lookups) and good for the usual dense keys
    let row = t
        .query_one(
            &format!("SELECT min({key})::bigint, max({key})::bigint FROM {}.{}", quote_ident(schema), quote_ident(table)),
            &[],
        )
        .await?;
    let (Some(min), Some(max)) = (row.get::<_, Option<i64>>(0), row.get::<_, Option<i64>>(1)) else { return Ok(None) };
    let span = (max as i128 - min as i128 + 1) as u128;
    let step = span.div_ceil(parts as u128).max(1);
    let mut out = Vec::with_capacity(parts);
    let mut lo = min as i128;
    while lo <= max as i128 {
        let hi = lo + step as i128;
        if hi > max as i128 {
            out.push((lo as i64, i64::MAX));
            break;
        }
        out.push((lo as i64, hi as i64));
        lo = hi;
    }
    Ok(Some(out))
}

/// `SELECT <columns> FROM <table>` with per-type read expressions.
pub(crate) fn select_query(def: &DataDefinition) -> DMResult<String> {
    let columns: Vec<String> = def
        .properties_info
        .iter()
        .map(|p| select_expression(&quote_ident(&p.property_name), p.source_type.as_deref().unwrap_or("text")))
        .collect();
    let (schema, table) = def
        .container_name
        .split_once('.')
        .ok_or_else(|| DMError::Error(format!("invalid table name {}", def.container_name)))?;
    Ok(format!("SELECT {} FROM {}.{}", columns.join(", "), quote_ident(schema), quote_ident(table)))
}

/// Binary COPY of `query` into column batches.
pub(crate) async fn copy_out(
    client: &tokio_postgres::Transaction<'_>,
    def: &DataDefinition,
    query: &str,
    sizer: &BatchSizer,
    tx: &mpsc::Sender<ColumnBatch>,
) -> DMResult<u64> {
    let types: Vec<Type> = client
        .prepare(&format!("{} LIMIT 0", query))
        .await?
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect();
    let encoding: String = client.query_one("SHOW server_encoding", &[]).await?.get(0);
    let utf8 = encoding.eq_ignore_ascii_case("UTF8");
    let stream = client.copy_out(&format!("COPY ({}) TO STDOUT (FORMAT binary)", query)).await?;
    let rows = BinaryCopyOutStream::new(stream, &types);
    pin_mut!(rows);
    let mut total = 0u64;
    let mut size = sizer.next();
        let mut batch = ColumnBatch::for_definition(def, size);
    while let Some(row) = rows.try_next().await? {
        for (i, (col, ty)) in batch.columns.iter_mut().zip(&types).enumerate() {
            push_copy_value(&row, i, ty, col, utf8)?;
        }
        batch.end_row();
        if batch.rows >= size {
            total += batch.rows as u64;
            let full = { size = sizer.next(); std::mem::replace(&mut batch, ColumnBatch::for_definition(def, size)) };
            tx.send(full).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
        }
    }
    if !batch.is_empty() {
        total += batch.rows as u64;
        tx.send(batch).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
    }
    Ok(total)
}

impl Postgresql {
    pub async fn create_table_info(&self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        let schema: Option<&str> = self.schema.as_deref();
        let tables = self.client.query(POSTGRESQL_TABLES_QUERY, &[&schema]).await?;
        let mut result: Vec<DataDefinition> = Vec::with_capacity(tables.len());

        for t in tables {
            let schema: String = t.get(0);
            let table: String = t.get(1);
            let name = format!("{}.{}", schema, table);
            if !filter.accepts(&name) {
                continue;
            }
            let mut def = DataDefinition::new(name, self.create_column_info(&schema, &table).await?);
            def.foreign_keys = self.foreign_keys(&schema, &table).await?;
            def.indexes = self.indexes(&schema, &table).await?;
            def.sync_property_references();
            result.push(def);
        }

        Ok(result)
    }

    pub async fn create_column_info(&self, schema: &str, table: &str) -> DMResult<Vec<DataPropertyInfo>> {
        let rows = self.client.query(POSTGRESQL_COLUMNS_QUERY, &[&schema, &table]).await?;
        Ok(rows
            .iter()
            .map(|row| {
                let data_type: String = row.get(1);
                DataPropertyInfo {
                    property_name: row.get(0),
                    data_type: logical_type_for(&data_type),
                    source_type: Some(data_type),
                    is_nullable: row.get(2),
                    is_identifier: row.get(3),
                    reference_container_name: None,
                    reference_property_name: None,
                    shape: None,

                }
            })
            .collect())
    }

    async fn foreign_keys(&self, schema: &str, table: &str) -> DMResult<Vec<ForeignKeyInfo>> {
        let rows = self.client.query(POSTGRESQL_FOREIGN_KEYS_QUERY, &[&schema, &table]).await?;
        Ok(rows
            .iter()
            .map(|r| ForeignKeyInfo {
                name: r.get(0),
                reference_container_name: format!("{}.{}", r.get::<_, String>(1), r.get::<_, String>(2)),
                columns: r.get(3),
                reference_columns: r.get(4),
            })
            .collect())
    }

    async fn indexes(&self, schema: &str, table: &str) -> DMResult<Vec<IndexInfo>> {
        let rows = self.client.query(POSTGRESQL_INDEXES_QUERY, &[&schema, &table]).await?;
        Ok(rows
            .iter()
            .map(|r| IndexInfo { name: r.get(0), unique: r.get(1), columns: r.get(2) })
            .collect())
    }
}
