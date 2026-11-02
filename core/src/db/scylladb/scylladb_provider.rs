//! ScyllaDB provider (DB-10): schema from `system_schema` (SCY-2), data with parallel token
//! range scans and paging (SCY-3).

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use log::info;
use scylla::value::Row;
use tokio::sync::mpsc;

use crate::{
    db::scylladb::scylladb_data_types::{list_shape, logical_type_for, push_cql, quote},
    dbc::batch::ColumnBatch,
    migrator::{batching::BatchSizer, migrator_steps::DataProvider},
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo, IndexInfo, JsonShape},
        migration_execution_settings::ContainerFilter,
        value::LogicalType,
    },
    DMError, DMResult,
};

use super::scylladb::{sc, Scylla};

pub(crate) const CDC_LOG_SUFFIX: &str = "_scylla_cdc_log";

/// Splits the whole Murmur3 token ring into `parts` contiguous ranges.
pub fn token_ranges(parts: usize) -> Vec<(i64, i64)> {
    let parts = parts.max(1) as i128;
    let (min, max) = (i64::MIN as i128, i64::MAX as i128);
    let step = (max - min + 1) / parts;
    (0..parts)
        .map(|i| {
            let lo = min + i * step;
            let hi = if i == parts - 1 { max } else { lo + step - 1 };
            (lo as i64, hi as i64)
        })
        .collect()
}

impl Scylla {
    async fn udt_shapes(&self) -> DMResult<HashMap<String, Vec<(String, LogicalType)>>> {
        let rows = self
            .session
            .query_unpaged("SELECT type_name, field_names, field_types FROM system_schema.types WHERE keyspace_name = ?", (&self.keyspace,))
            .await
            .map_err(sc)?
            .into_rows_result()
            .map_err(sc)?;
        let mut out = HashMap::new();
        for r in rows.rows::<(String, Vec<String>, Vec<String>)>().map_err(sc)? {
            let (name, fields, types) = r.map_err(sc)?;
            out.insert(name, fields.into_iter().zip(types.iter().map(|t| logical_type_for(t))).collect());
        }
        Ok(out)
    }

    async fn table_definition(&self, table: &str, udts: &HashMap<String, Vec<(String, LogicalType)>>) -> DMResult<DataDefinition> {
        let rows = self
            .session
            .query_unpaged(
                "SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                (&self.keyspace, table),
            )
            .await
            .map_err(sc)?
            .into_rows_result()
            .map_err(sc)?;
        let mut cols: Vec<(String, String, i32, String)> = rows
            .rows::<(String, String, i32, String)>()
            .map_err(sc)?
            .collect::<Result<_, _>>()
            .map_err(sc)?;
        // partition key, clustering key, then the other columns by name
        let rank = |k: &str| match k {
            "partition_key" => 0,
            "clustering" => 1,
            "static" => 2,
            _ => 3,
        };
        cols.sort_by(|a, b| (rank(&a.1), a.2, &a.0).cmp(&(rank(&b.1), b.2, &b.0)));
        let props = cols
            .iter()
            .map(|(name, kind, _, ty)| {
                let key = kind == "partition_key" || kind == "clustering";
                let udt = ty.strip_prefix("frozen<").and_then(|t| t.strip_suffix('>')).unwrap_or(ty).trim_matches('"');
                let shape = list_shape(ty).or_else(|| udts.get(udt).map(|f| JsonShape::Object(f.clone())));
                DataPropertyInfo {
                    property_name: name.clone(),
                    data_type: logical_type_for(ty),
                    source_type: Some(ty.clone()),
                    is_nullable: !key,
                    is_identifier: key,
                    reference_container_name: None,
                    reference_property_name: None,
                    shape,
                }
            })
            .collect();
        let mut def = DataDefinition::new(table.to_string(), props);
        def.partition_key = cols.iter().filter(|c| c.1 == "partition_key").map(|c| c.0.clone()).collect();

        let idx = self
            .session
            .query_unpaged("SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?", (&self.keyspace, table))
            .await
            .map_err(sc)?
            .into_rows_result()
            .map_err(sc)?;
        for r in idx.rows::<(String, HashMap<String, String>)>().map_err(sc)? {
            let (name, options) = r.map_err(sc)?;
            let target = options.get("target").map(|t| t.trim_matches('"').to_string()).unwrap_or_default();
            if def.column_index(&target).is_some() {
                def.indexes.push(IndexInfo { name, columns: vec![target], unique: false });
            }
        }
        Ok(def)
    }
}

#[async_trait]
impl DataProvider for Scylla {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        let rows = self
            .session
            .query_unpaged("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", (&self.keyspace,))
            .await
            .map_err(sc)?
            .into_rows_result()
            .map_err(sc)?;
        let mut tables: Vec<String> = rows.rows::<(String,)>().map_err(sc)?.map(|r| r.map(|t| t.0)).collect::<Result<_, _>>().map_err(sc)?;
        tables.retain(|t| !t.ends_with(CDC_LOG_SUFFIX) && filter.accepts(t));
        tables.sort();
        let udts = self.udt_shapes().await?;
        let mut defs = Vec::with_capacity(tables.len());
        for t in tables {
            defs.push(self.table_definition(&t, &udts).await?);
        }
        Ok(defs)
    }

    /// Token range scans (SCY-3): `token_ranges` ranges, `concurrency` of them in flight, every
    /// range paged by the driver.
    async fn read_data(&mut self, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let cols: Vec<String> = def.properties_info.iter().map(|p| quote(&p.property_name)).collect();
        let pk: Vec<String> = def.partition_key_names().iter().map(|c| quote(c)).collect();
        if pk.is_empty() {
            return Err(DMError::Error(format!("{}: no partition key", def.container_name)));
        }
        let token = format!("token({})", pk.join(", "));
        let mut prepared = self
            .session
            .prepare(format!("SELECT {} FROM {} WHERE {token} >= ? AND {token} <= ?", cols.join(", "), self.table(&def.container_name)))
            .await
            .map_err(sc)?;
        prepared.set_page_size(sizer.next().clamp(100, 10_000) as i32);
        let ranges = token_ranges(self.settings.scylla.token_ranges);
        info!("[Migration][{}] {} token ranges", def.container_name, ranges.len());
        let concurrency = self.settings.scylla.concurrency.clamp(1, ranges.len());
        let mut pending = ranges.into_iter();
        let mut tasks = tokio::task::JoinSet::new();
        let mut total = 0;
        loop {
            while tasks.len() < concurrency {
                let Some((lo, hi)) = pending.next() else { break };
                let (session, prepared, def, sizer, tx) = (self.session.clone(), prepared.clone(), def.clone(), sizer.clone(), tx.clone());
                tasks.spawn(async move {
                    let mut rows = session.execute_iter(prepared, (lo, hi)).await.map_err(sc)?.rows_stream::<Row>().map_err(sc)?;
                    let mut size = sizer.next();
                    let mut batch = ColumnBatch::for_definition(&def, size);
                    let mut n = 0u64;
                    while let Some(row) = rows.try_next().await.map_err(sc)? {
                        for (col, v) in batch.columns.iter_mut().zip(row.columns.iter()) {
                            push_cql(col, v.as_ref())?;
                        }
                        batch.end_row();
                        if batch.rows >= size {
                            n += batch.rows as u64;
                            size = sizer.next();
                            tx.send(std::mem::replace(&mut batch, ColumnBatch::for_definition(&def, size)))
                                .await
                                .map_err(|_| DMError::Error("receiver stopped".to_string()))?;
                        }
                    }
                    if !batch.is_empty() {
                        n += batch.rows as u64;
                        tx.send(batch).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
                    }
                    Ok::<u64, DMError>(n)
                });
            }
            match tasks.join_next().await {
                Some(r) => total += r??,
                None => break,
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_ranges_cover_the_ring_without_gaps() {
        for n in [1usize, 2, 7, 32] {
            let r = token_ranges(n);
            assert_eq!(r.len(), n);
            assert_eq!(r[0].0, i64::MIN);
            assert_eq!(r[n - 1].1, i64::MAX);
            for w in r.windows(2) {
                assert_eq!(w[0].1 + 1, w[1].0);
            }
        }
    }
}
