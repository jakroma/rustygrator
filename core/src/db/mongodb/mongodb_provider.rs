//! MongoDB provider (DB-4): analyzes documents to build table-like definitions (NoSQL -> SQL).

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use log::info;
use mongodb::{
    bson::{doc, Document},
    options::{AggregateOptions, FindOptions},
    IndexModel,
};
use tokio::sync::mpsc;

use crate::{
    db::mongodb::mongodb_data_types::{push_bson, SchemaAnalyzer},
    dbc::batch::ColumnBatch,
    migrator::batching::BatchSizer,
    migrator::migrator_steps::DataProvider,
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo, IndexInfo},
        migration_execution_settings::ContainerFilter,
    },
    DMError, DMResult,
};

use super::mongodb::Mongodb;

impl Mongodb {
    async fn analyze_collection(&self, name: &str) -> DMResult<DataDefinition> {
        let coll = self.db.collection::<Document>(name);
        let mut analyzer = SchemaAnalyzer::new();
        let sample = self.exec_settings.schema_sample;
        // NOT NULL only when every document was seen and no new ones can arrive (live mode)
        let complete = sample == 0 && !self.exec_settings.live.enabled;
        if sample > 0 {
            let mut cursor = coll
                .aggregate([doc! {"$sample": {"size": sample as i64}}], AggregateOptions::builder().allow_disk_use(true).build())
                .await?;
            while let Some(d) = cursor.try_next().await? {
                analyzer.add(&d);
            }
        } else {
            let mut cursor = coll.find(None, FindOptions::builder().batch_size(self.exec_settings.pagination_limit.max(100)).build()).await?;
            while let Some(d) = cursor.try_next().await? {
                analyzer.add(&d);
            }
        }
        let properties = analyzer
            .finish()
            .into_iter()
            .map(|(name, t, nullable, shape)| DataPropertyInfo {
                is_identifier: name == "_id",
                is_nullable: (nullable || !complete) && name != "_id",
                property_name: name,
                data_type: t,
                source_type: None,
                reference_container_name: None,
                reference_property_name: None,
                shape,

            })
            .collect();
        let mut def = DataDefinition::new(name.to_string(), properties);

        let mut indexes = coll.list_indexes(None).await?;
        while let Some(IndexModel { keys, options, .. }) = indexes.try_next().await? {
            let idx_name = options.as_ref().and_then(|o| o.name.clone()).unwrap_or_default();
            if idx_name == "_id_" {
                continue;
            }
            // only plain ascending / descending indexes on top level fields map to SQL
            if keys.iter().any(|(k, v)| k.contains('.') || !(v.as_i32().is_some() || v.as_i64().is_some() || v.as_f64().is_some())) {
                continue;
            }
            def.indexes.push(IndexInfo {
                name: idx_name,
                columns: keys.keys().cloned().collect(),
                unique: options.and_then(|o| o.unique).unwrap_or(false),
            });
        }
        Ok(def)
    }
}

#[async_trait]
impl DataProvider for Mongodb {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        let mut names = self.db.list_collection_names(doc! {"type": "collection"}).await?;
        names.retain(|n| !n.starts_with("system.") && filter.accepts(n));
        names.sort();
        let mut defs = Vec::with_capacity(names.len());
        for n in names {
            info!("[Migration][{}] Analyze documents", n);
            defs.push(self.analyze_collection(&n).await?);
        }
        Ok(defs)
    }

    async fn read_data(&mut self, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let coll = self.db.collection::<Document>(&def.container_name);
        let ranges = self.exec_settings.parallel_ranges;
        match split_id_ranges(&coll, ranges).await? {
            Some(filters) => {
                // PERF-11: one cursor per `_id` range
                info!("[Migration][{}] reading {} _id ranges in parallel", def.container_name, filters.len());
                let mut tasks = tokio::task::JoinSet::new();
                for f in filters {
                    let (coll, def, sizer, tx) = (coll.clone(), def.clone(), sizer.clone(), tx.clone());
                    tasks.spawn(async move { read_range(&coll, &def, f, &sizer, &tx).await });
                }
                let mut total = 0;
                while let Some(r) = tasks.join_next().await {
                    total += r??;
                }
                Ok(total)
            }
            None => read_range(&coll, def, None, &sizer, &tx).await,
        }
    }
}

/// Documents below which a collection is read by one cursor.
const PARALLEL_RANGE_MIN_DOCS: u64 = 100_000;

/// `_id` boundaries at equal document counts (walks the `_id` index only).
async fn split_id_ranges(coll: &mongodb::Collection<Document>, parts: usize) -> DMResult<Option<Vec<Option<Document>>>> {
    if parts < 2 {
        return Ok(None);
    }
    let count = coll.estimated_document_count(None).await?;
    if count < PARALLEL_RANGE_MIN_DOCS {
        return Ok(None);
    }
    let mut bounds = Vec::with_capacity(parts - 1);
    for k in 1..parts as u64 {
        let opts = mongodb::options::FindOneOptions::builder()
            .sort(doc! {"_id": 1})
            .skip(k * count / parts as u64)
            .projection(doc! {"_id": 1})
            .build();
        if let Some(d) = coll.find_one(None, opts).await? {
            if let Some(id) = d.get("_id") {
                bounds.push(id.clone());
            }
        }
    }
    bounds.dedup();
    if bounds.is_empty() {
        return Ok(None);
    }
    let mut filters = Vec::with_capacity(bounds.len() + 1);
    filters.push(Some(doc! {"_id": {"$lt": bounds[0].clone()}}));
    for w in bounds.windows(2) {
        filters.push(Some(doc! {"_id": {"$gte": w[0].clone(), "$lt": w[1].clone()}}));
    }
    filters.push(Some(doc! {"_id": {"$gte": bounds[bounds.len() - 1].clone()}}));
    Ok(Some(filters))
}

async fn read_range(
    coll: &mongodb::Collection<Document>,
    def: &DataDefinition,
    filter: Option<Document>,
    sizer: &BatchSizer,
    tx: &mpsc::Sender<ColumnBatch>,
) -> DMResult<u64> {
    let mut cursor = coll
        .find(filter, FindOptions::builder().batch_size(sizer.next().min(u32::MAX as usize) as u32).build())
        .await?;
    let mut total = 0u64;
    let mut size = sizer.next();
    let mut batch = ColumnBatch::for_definition(def, size);
    while let Some(d) = cursor.try_next().await? {
        for (p, col) in def.properties_info.iter().zip(batch.columns.iter_mut()) {
            push_bson(col, d.get(&p.property_name))?;
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
    if !batch.is_empty() {
        total += batch.rows as u64;
        tx.send(batch).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
    }
    Ok(total)
}
