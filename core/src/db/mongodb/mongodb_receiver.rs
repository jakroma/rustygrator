//! MongoDB receiver (DB-5) and SQL -> NoSQL strategies (section 4).
//!
//! * primary key -> `_id` (composite keys become an `_id` sub-document),
//! * collections get a `$jsonSchema` validator generated from the definition,
//! * `Normalized`: foreign keys stay plain (indexed) reference fields,
//! * `Denormalized`: every row referenced by a foreign key is embedded into the referencing
//!   document. Embedding runs inside MongoDB (`$lookup` + `$merge`), in dependency order, so
//!   nested references are embedded recursively without loading collections into memory.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use futures_util::{stream, StreamExt, TryStreamExt};
use log::{info, warn};
use mongodb::{
    bson::{self, doc, Bson, Document, RawDocumentBuf},
    error::ErrorKind,
    options::{CreateCollectionOptions, InsertManyOptions, ReplaceOptions, ValidationLevel},
    Collection, IndexModel,
};
use tokio::sync::mpsc;

use crate::{
    dbc::batch::ColumnBatch,
    db::mongodb::mongodb_data_types::{append_cell, get_bson_type, value_to_bson},
    migrator::migrator_steps::{coalesce_changes, ChangeReceiver, ContainerChanges, DataReceiver},
    models::{
        change::ChangeEvent,
        data_definitions::{short_name, DataDefinition, DataPropertyInfo, ForeignKeyInfo},
        migration_execution_settings::SqlToNoSqlStrategy,
        value::Value,
    },
    DMError, DMResult,
};

use super::mongodb::Mongodb;

const DUPLICATE_KEY: i32 = 11000;
const WRITE_CONCURRENCY: usize = 16;
/// `insert_many` calls in flight per collection during the initial load.
const INSERT_CONCURRENCY: usize = 4;
const MAX_PROPAGATION_DEPTH: usize = 4;

/// Path of a column inside the target document.
pub(crate) fn field_path(def: &DataDefinition, column: &str) -> String {
    let ids = def.identifier_names();
    if ids.contains(&column) {
        if ids.len() == 1 {
            "_id".to_string()
        } else {
            format!("_id.{}", column)
        }
    } else {
        column.to_string()
    }
}

pub(crate) fn id_bson(def: &DataDefinition, row: &[Value]) -> Option<Bson> {
    let ids = def.identifier_indexes();
    match ids.len() {
        0 => None,
        1 => Some(value_to_bson(&row[ids[0]])),
        _ => {
            let mut d = Document::new();
            for &i in &ids {
                d.insert(def.properties_info[i].property_name.clone(), value_to_bson(&row[i]));
            }
            Some(Bson::Document(d))
        }
    }
}

pub(crate) fn row_to_document(def: &DataDefinition, row: &[Value]) -> Document {
    let mut d = Document::new();
    if let Some(id) = id_bson(def, row) {
        d.insert("_id", id);
    }
    for (p, v) in def.properties_info.iter().zip(row) {
        if !p.is_identifier {
            d.insert(p.property_name.clone(), value_to_bson(v));
        }
    }
    d
}

/// Name of the field holding the embedded referenced document.
pub(crate) fn embed_field_name(def: &DataDefinition, fk: &ForeignKeyInfo) -> String {
    let base = if fk.columns.len() == 1 {
        let c = &fk.columns[0];
        c.strip_suffix("_id")
            .or_else(|| c.strip_suffix("Id"))
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}_ref", c))
    } else {
        short_name(&fk.reference_container_name).to_string()
    };
    if def.column_index(&base).is_some() || base == "_id" {
        format!("{}_doc", base)
    } else {
        base
    }
}

pub fn generate_validation_schema(properties: &[DataPropertyInfo]) -> Document {
    let mut required_fields = Vec::new();
    let mut properties_doc = doc! {};

    for prop in properties.iter().filter(|p| !p.is_identifier) {
        let mut property_doc = Document::new();
        if let Some(bson_type) = get_bson_type(prop.data_type) {
            if prop.is_nullable {
                property_doc.insert("bsonType", vec![Bson::String(bson_type.into()), Bson::String("null".into())]);
            } else {
                property_doc.insert("bsonType", bson_type);
            }
            property_doc.insert(
                "description",
                format!("must be a {}{}", bson_type, if prop.is_nullable { "" } else { " and is required" }),
            );
        }
        if !prop.is_nullable {
            required_fields.push(Bson::String(prop.property_name.clone()));
        }
        properties_doc.insert(prop.property_name.clone(), property_doc);
    }

    let mut schema = doc! { "bsonType": "object", "properties": properties_doc };
    if !required_fields.is_empty() {
        schema.insert("required", required_fields);
    }
    schema
}

/// `$lookup` + `$set` + `$merge` pipeline embedding the documents referenced by `def`'s foreign
/// keys. `None` when nothing can be embedded.
fn denormalize_pipeline(defs: &HashMap<&str, &DataDefinition>, def: &DataDefinition, filter: Option<Document>) -> Option<Vec<Document>> {
    let mut pipeline = Vec::new();
    if let Some(f) = filter {
        pipeline.push(doc! {"$match": f});
    }
    let mut set = Document::new();
    let mut tmp_fields = Vec::new();
    for (n, fk) in def.foreign_keys.iter().enumerate() {
        let Some(parent) = defs.get(fk.reference_container_name.as_str()) else { continue };
        let mut lets = Document::new();
        let mut conds = Vec::new();
        for (i, (col, ref_col)) in fk.columns.iter().zip(&fk.reference_columns).enumerate() {
            let var = format!("k{}", i);
            lets.insert(var.clone(), format!("${}", field_path(def, col)));
            conds.push(Bson::Document(doc! {"$eq": [format!("${}", field_path(parent, ref_col)), format!("$${}", var)]}));
        }
        let tmp = format!("__data_mover_tmp_{}", n);
        pipeline.push(doc! {
            "$lookup": {
                "from": parent.target_name(),
                "let": lets,
                "pipeline": [ {"$match": {"$expr": {"$and": conds}}}, {"$limit": 1} ],
                "as": tmp.clone(),
            }
        });
        set.insert(embed_field_name(def, fk), doc! {"$ifNull": [{"$arrayElemAt": [format!("${}", tmp), 0]}, Bson::Null]});
        tmp_fields.push(Bson::String(tmp));
    }
    if tmp_fields.is_empty() {
        return None;
    }
    pipeline.push(doc! {"$set": set});
    pipeline.push(doc! {"$unset": tmp_fields});
    pipeline.push(doc! {
        "$merge": {"into": def.target_name(), "on": "_id", "whenMatched": "replace", "whenNotMatched": "discard"}
    });
    Some(pipeline)
}

impl Mongodb {
    fn strategy(&self) -> SqlToNoSqlStrategy {
        self.exec_settings.strategy
    }

    async fn run_pipeline(&self, def: &DataDefinition, pipeline: Vec<Document>) -> DMResult<()> {
        let coll = self.db.collection::<Document>(def.target_name());
        let mut cursor = coll.aggregate(pipeline, None).await?;
        while cursor.try_next().await?.is_some() {}
        Ok(())
    }

    async fn denormalize(&self, defs: &[DataDefinition]) -> DMResult<()> {
        let by_name: HashMap<&str, &DataDefinition> = defs.iter().map(|d| (d.container_name.as_str(), d)).collect();
        // dependency order: referenced collections are already denormalized when they get embedded
        for def in defs {
            if let Some(p) = denormalize_pipeline(&by_name, def, None) {
                info!("[Migration][{}] Embed referenced documents", def.container_name);
                self.run_pipeline(def, p).await?;
            }
        }
        Ok(())
    }

    /// Re-embeds after live changes: the changed documents themselves and, recursively, the
    /// documents that embed them.
    async fn refresh_embedded(&self, defs: &[DataDefinition], changed: HashMap<String, Vec<Bson>>) -> DMResult<()> {
        let by_name: HashMap<&str, &DataDefinition> = defs.iter().map(|d| (d.container_name.as_str(), d)).collect();
        for (container, ids) in &changed {
            let Some(def) = by_name.get(container.as_str()) else { continue };
            if let Some(p) = denormalize_pipeline(&by_name, def, Some(doc! {"_id": {"$in": ids.clone()}})) {
                self.run_pipeline(def, p).await?;
            }
        }
        let mut frontier = changed;
        let mut visited: HashSet<String> = HashSet::new();
        for _ in 0..MAX_PROPAGATION_DEPTH {
            let mut next: HashMap<String, Vec<Bson>> = HashMap::new();
            for def in defs {
                let mut ors = Vec::new();
                for fk in &def.foreign_keys {
                    let Some(ids) = frontier.get(&fk.reference_container_name) else { continue };
                    let Some(parent) = by_name.get(fk.reference_container_name.as_str()) else { continue };
                    let parent_ids = parent.identifier_names();
                    if fk.columns.len() == 1 && parent_ids.len() == 1 && fk.reference_columns[0] == parent_ids[0] {
                        ors.push(doc! {field_path(def, &fk.columns[0]): {"$in": ids.clone()}});
                    } else {
                        ors.clear();
                        ors.push(doc! {});
                        break;
                    }
                }
                if ors.is_empty() || !visited.insert(format!("{}#{:?}", def.container_name, ors)) {
                    continue;
                }
                let filter = if ors.len() == 1 { ors.pop().unwrap() } else { doc! {"$or": ors} };
                let coll = self.db.collection::<Document>(def.target_name());
                let ids = coll.distinct("_id", filter, None).await?;
                if ids.is_empty() {
                    continue;
                }
                if let Some(p) = denormalize_pipeline(&by_name, def, Some(doc! {"_id": {"$in": ids.clone()}})) {
                    self.run_pipeline(def, p).await?;
                }
                next.entry(def.container_name.clone()).or_default().extend(ids);
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok(())
    }

    async fn upsert_documents(&self, def: &DataDefinition, docs: Vec<Document>) -> DMResult<()> {
        let coll = self.db.collection::<Document>(def.target_name());
        stream::iter(docs)
            .map(|d| {
                let coll = coll.clone();
                async move {
                    match d.get("_id").cloned() {
                        Some(id) => coll
                            .replace_one(doc! {"_id": id}, d, ReplaceOptions::builder().upsert(true).build())
                            .await
                            .map(|_| ()),
                        None => coll.insert_one(d, None).await.map(|_| ()),
                    }
                }
            })
            .buffer_unordered(WRITE_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// `insert_many` (fast path); documents rejected as duplicates are upserted instead, which
    /// makes re-running a migration idempotent.
    async fn insert_raw(coll: Collection<RawDocumentBuf>, docs: Vec<RawDocumentBuf>) -> DMResult<()> {
        match coll.insert_many(&docs, InsertManyOptions::builder().ordered(false).build()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let ErrorKind::BulkWrite(failure) = e.kind.as_ref() else { return Err(e.into()) };
                let errors = failure.write_errors.as_deref().unwrap_or_default();
                if failure.write_concern_error.is_some() || errors.iter().any(|w| w.code != DUPLICATE_KEY) {
                    return Err(e.into());
                }
                for w in errors {
                    let Some(d) = docs.get(w.index) else { continue };
                    let id = d
                        .get("_id")
                        .map_err(|e| DMError::Error(e.to_string()))?
                        .map(|r| Bson::try_from(r.to_raw_bson()))
                        .transpose()
                        .map_err(|e| DMError::Error(e.to_string()))?
                        .unwrap_or(Bson::Null);
                    coll.replace_one(doc! {"_id": id}, d, ReplaceOptions::builder().upsert(true).build()).await?;
                }
                Ok(())
            }
        }
    }

    async fn ensure_indexes(&self, def: &DataDefinition) -> DMResult<()> {
        let coll = self.db.collection::<Document>(def.target_name());
        let mut models = Vec::new();
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        for idx in &def.indexes {
            if idx.columns.is_empty() || idx.columns.iter().any(|c| def.column_index(c).is_none()) {
                continue;
            }
            let paths: Vec<String> = idx.columns.iter().map(|c| field_path(def, c)).collect();
            if paths == ["_id"] || !seen.insert(paths.clone()) {
                continue;
            }
            let mut keys = Document::new();
            for p in &paths {
                keys.insert(p.clone(), 1);
            }
            models.push(
                IndexModel::builder()
                    .keys(keys)
                    .options(mongodb::options::IndexOptions::builder().unique(idx.unique).build())
                    .build(),
            );
        }
        // reference fields are indexed so lookups (and live re-embedding) stay fast
        for fk in &def.foreign_keys {
            let paths: Vec<String> = fk.columns.iter().map(|c| field_path(def, c)).collect();
            if paths.iter().all(|p| p.starts_with("_id")) || !seen.insert(paths.clone()) {
                continue;
            }
            let mut keys = Document::new();
            for p in &paths {
                keys.insert(p.clone(), 1);
            }
            models.push(IndexModel::builder().keys(keys).build());
        }
        if !models.is_empty() {
            if let Err(e) = coll.create_indexes(models, None).await {
                warn!("[Migration][{}] indexes not created: {}", def.container_name, e);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DataReceiver for Mongodb {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let existing: HashSet<String> = self.db.list_collection_names(None).await?.into_iter().collect();
        for def in defs {
            if existing.contains(def.target_name()) {
                continue;
            }
            let validation_schema = generate_validation_schema(&def.properties_info);
            info!("[Migration][{}] validation schema created {:?}", def.container_name, validation_schema);
            let options = CreateCollectionOptions::builder()
                .validator(doc! {"$jsonSchema": validation_schema})
                .validation_level(ValidationLevel::Strict)
                .build();
            self.db.create_collection(def.target_name(), options).await?;
        }
        Ok(())
    }

    /// Raw BSON is built straight from the column batches and several `insert_many` calls run at
    /// once (PERF-15).
    async fn write_data(&mut self, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        let coll = self.db.collection::<RawDocumentBuf>(def.target_name());
        let id_idx = def.identifier_indexes();
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let mut in_flight = tokio::task::JoinSet::new();
        let mut total = 0u64;
        while let Some(batch) = rx.recv().await {
            let mut docs = Vec::with_capacity(batch.rows);
            for r in 0..batch.rows {
                let mut d = RawDocumentBuf::new();
                match id_idx.len() {
                    0 => {}
                    1 => append_cell(&mut d, "_id", batch.cell(r, id_idx[0])),
                    _ => {
                        let mut id = RawDocumentBuf::new();
                        for &i in &id_idx {
                            append_cell(&mut id, names[i], batch.cell(r, i));
                        }
                        d.append("_id", id);
                    }
                }
                for (i, name) in names.iter().enumerate() {
                    if !def.properties_info[i].is_identifier {
                        append_cell(&mut d, name, batch.cell(r, i));
                    }
                }
                docs.push(d);
            }
            total += docs.len() as u64;
            if in_flight.len() >= INSERT_CONCURRENCY {
                in_flight.join_next().await.expect("non empty")??;
            }
            in_flight.spawn(Self::insert_raw(coll.clone(), docs));
        }
        while let Some(r) = in_flight.join_next().await {
            r??;
        }
        Ok(total)
    }

    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        for def in defs {
            self.ensure_indexes(def).await?;
        }
        if self.strategy() == SqlToNoSqlStrategy::Denormalized {
            self.denormalize(defs).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeReceiver for Mongodb {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()> {
        let changes = coalesce_changes(defs, events)?;
        let mut changed: HashMap<String, Vec<Bson>> = HashMap::new();
        for c in &changes {
            let def = c.def;
            let mut full = Vec::new();
            let coll = self.db.collection::<Document>(def.target_name());
            for r in &c.upserts {
                if ContainerChanges::is_partial(r) {
                    let id_row: Vec<Value> = r.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect();
                    let Some(id) = id_bson(def, &id_row) else { continue };
                    let mut set = Document::new();
                    for (p, v) in def.properties_info.iter().zip(r) {
                        if let (Some(v), false) = (v, p.is_identifier) {
                            set.insert(p.property_name.clone(), value_to_bson(v));
                        }
                    }
                    changed.entry(def.container_name.clone()).or_default().push(id.clone());
                    if !set.is_empty() {
                        coll.update_one(doc! {"_id": id}, doc! {"$set": set}, None).await?;
                    }
                } else {
                    let row: Vec<Value> = r.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect();
                    let d = row_to_document(def, &row);
                    if let Some(id) = d.get("_id") {
                        changed.entry(def.container_name.clone()).or_default().push(id.clone());
                    }
                    full.push(d);
                }
            }
            self.upsert_documents(def, full).await?;
        }
        for c in changes.iter().rev() {
            if c.deletes.is_empty() {
                continue;
            }
            let def = c.def;
            let id_idx = def.identifier_indexes();
            let ids: Vec<Bson> = c
                .deletes
                .iter()
                .filter_map(|k| {
                    let mut row = vec![Value::Null; def.properties_info.len()];
                    for (&i, v) in id_idx.iter().zip(k) {
                        row[i] = v.clone();
                    }
                    id_bson(def, &row)
                })
                .collect();
            changed.entry(def.container_name.clone()).or_default().extend(ids.iter().cloned());
            self.db
                .collection::<Document>(def.target_name())
                .delete_many(doc! {"_id": {"$in": ids}}, None)
                .await?;
        }
        if self.strategy() == SqlToNoSqlStrategy::Denormalized && defs.iter().any(|d| !d.foreign_keys.is_empty()) {
            self.refresh_embedded(defs, changed).await?;
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Mongodb::reconnect(self).await
    }
}

/// Converts a document read back from MongoDB into named values (used by tests and tools).
pub fn document_values(d: &Document) -> DMResult<serde_json::Value> {
    bson::to_bson(d)
        .map(|b| b.into_relaxed_extjson())
        .map_err(|e| DMError::Error(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::value::LogicalType;

    fn prop(n: &str, t: LogicalType, id: bool, nullable: bool) -> DataPropertyInfo {
        DataPropertyInfo {
            property_name: n.into(),
            data_type: t,
            source_type: None,
            is_nullable: nullable,
            is_identifier: id,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,

        }
    }

    fn users() -> DataDefinition {
        let mut d = DataDefinition::new(
            "public.users".into(),
            vec![
                prop("id", LogicalType::Int32, true, false),
                prop("username", LogicalType::String, false, false),
                prop("user_type_id", LogicalType::Int32, false, false),
                prop("note", LogicalType::String, false, true),
            ],
        );
        d.foreign_keys.push(ForeignKeyInfo {
            name: "fk".into(),
            columns: vec!["user_type_id".into()],
            reference_container_name: "public.user_types".into(),
            reference_columns: vec!["id".into()],
        });
        d
    }

    #[test]
    fn primary_key_becomes_id_and_validator_allows_nulls() {
        let def = users();
        let d = row_to_document(&def, &[Value::I32(7), Value::String("a".into()), Value::I32(1), Value::Null]);
        assert_eq!(d.get("_id"), Some(&Bson::Int32(7)));
        assert!(!d.contains_key("id"));
        let schema = generate_validation_schema(&def.properties_info);
        let required = schema.get_array("required").unwrap();
        assert!(required.contains(&Bson::String("username".into())));
        assert!(!required.contains(&Bson::String("id".into())));
        let note = schema.get_document("properties").unwrap().get_document("note").unwrap();
        assert_eq!(note.get_array("bsonType").unwrap().len(), 2);
    }

    #[test]
    fn denormalize_pipeline_embeds_parent() {
        let users = users();
        let types = DataDefinition::new(
            "public.user_types".into(),
            vec![prop("id", LogicalType::Int32, true, false), prop("type_name", LogicalType::String, false, false)],
        );
        let map: HashMap<&str, &DataDefinition> = [("public.users", &users), ("public.user_types", &types)].into_iter().collect();
        let p = denormalize_pipeline(&map, &users, None).unwrap();
        let lookup = p[0].get_document("$lookup").unwrap();
        assert_eq!(lookup.get_str("from").unwrap(), "user_types");
        assert_eq!(lookup.get_document("let").unwrap().get_str("k0").unwrap(), "$user_type_id");
        assert!(p[1].get_document("$set").unwrap().contains_key("user_type"));
        assert!(denormalize_pipeline(&map, &types, None).is_none());
        assert_eq!(embed_field_name(&users, &users.foreign_keys[0]), "user_type");
    }
}
