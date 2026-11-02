//! ScyllaDB receiver (DB-11, SCY-5..SCY-10, SN-5..SN-8).
//!
//! * keyspace and tables are created when missing (replication configurable),
//! * key: source primary key -> partition key; composite -> first column partition key, the
//!   rest clustering key; `--scylla-key` overrides it per table (SCY-8); tables without a key
//!   get a generated `data_mover_row_id uuid` partition key,
//! * json columns with a stable shape become `list<..>` or a frozen UDT (SCY-10),
//! * writes: prepared, token aware, rows of the same partition in unlogged batches, many
//!   requests in flight, configurable consistency (SCY-6, SCY-7),
//! * denormalized (section 4): every row referenced by a foreign key is stored in a frozen UDT
//!   column (`dm_<parent>`), looked up by primary key in the already loaded parent table;
//!   normalized: foreign key columns stay plain. Foreign key columns and source indexes get
//!   secondary indexes (SCY-9).

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use futures_util::{stream, StreamExt, TryStreamExt};
use log::{info, warn};
use scylla::{
    statement::{
        batch::{Batch, BatchType},
        prepared::PreparedStatement,
    },
    value::{CqlValue, Row},
};
use tokio::sync::mpsc;

use crate::{
    db::{
        mongodb::mongodb_receiver::embed_field_name,
        scylladb::scylladb_data_types::{cell_to_cql, cql_scalar, cql_to_json, logical_type_for, quote, CqlTarget},
    },
    dbc::batch::{Cell, ColumnBatch},
    migrator::migrator_steps::{coalesce_changes, ChangeReceiver, ContainerChanges, DataReceiver},
    models::{
        change::ChangeEvent,
        data_definitions::{short_name, DataDefinition, ForeignKeyInfo, JsonShape},
        migration_execution_settings::SqlToNoSqlStrategy,
        value::{LogicalType, Value},
    },
    DMError, DMResult,
};

use super::scylladb::{sc, Scylla};

pub const ROW_ID: &str = "data_mover_row_id";
const MAX_BATCH_ROWS: usize = 50;

/// Column of the target table as found in `system_schema`.
#[derive(Debug, Clone)]
struct TargetColumn {
    name: String,
    kind: String,
    position: i32,
    cql: String,
}

/// Denormalized parent embedded into a column.
struct Embed {
    column: String,
    parent: String,
    fk_columns: Vec<usize>,
    select: PreparedStatement,
    udt_name: String,
    udt_fields: Vec<(String, String)>,
}

/// Everything needed to write one table.
struct WritePlan {
    targets: Vec<CqlTarget>,
    embeds: Vec<Embed>,
    row_id: bool,
    insert: PreparedStatement,
    key_columns: Vec<String>,
}

fn udt_name(table: &str, column: &str) -> String {
    format!("{}_{}", table, column)
}

fn parent_udt(parent: &str) -> String {
    format!("dm_{}", short_name(parent))
}

/// Shape -> type of a column in a new table.
fn column_target(keyspace: &str, def: &DataDefinition, i: usize) -> CqlTarget {
    let p = &def.properties_info[i];
    match (&p.shape, p.data_type) {
        (Some(JsonShape::List(t)), LogicalType::Json) => CqlTarget::List(*t),
        (Some(JsonShape::Object(fields)), LogicalType::Json) => CqlTarget::Udt {
            keyspace: keyspace.to_string(),
            name: udt_name(def.target_name(), &p.property_name),
            fields: fields.clone(),
        },
        (_, t) => CqlTarget::Scalar(t),
    }
}

impl Scylla {
    fn strategy(&self) -> SqlToNoSqlStrategy {
        self.settings.strategy
    }

    /// (partition key, clustering key) of a target table (SCY-8).
    fn key_for(&self, def: &DataDefinition) -> (Vec<String>, Vec<String>) {
        if let Some((p, c)) = self.settings.scylla.key_overrides.get(def.target_name()) {
            return (p.clone(), c.clone());
        }
        let ids: Vec<String> = def.identifier_names().iter().map(|s| s.to_string()).collect();
        if ids.is_empty() {
            return (vec![ROW_ID.to_string()], Vec::new());
        }
        let explicit: Vec<String> = def.partition_key.iter().filter(|c| ids.contains(c)).cloned().collect();
        let partition = if explicit.is_empty() { vec![ids[0].clone()] } else { explicit };
        let clustering = ids.into_iter().filter(|c| !partition.contains(c)).collect();
        (partition, clustering)
    }

    /// Foreign keys that can be embedded: the referenced columns are the parent's key.
    fn embeddable<'a>(&self, def: &'a DataDefinition, defs: &HashMap<&str, &DataDefinition>) -> Vec<&'a ForeignKeyInfo> {
        if self.strategy() != SqlToNoSqlStrategy::Denormalized {
            return Vec::new();
        }
        def.foreign_keys
            .iter()
            .filter(|fk| {
                defs.get(fk.reference_container_name.as_str()).is_some_and(|p| {
                    let ids = p.identifier_names();
                    !ids.is_empty() && ids.len() == fk.reference_columns.len() && fk.reference_columns.iter().all(|c| ids.contains(&c.as_str()))
                })
            })
            .collect()
    }

    async fn exec(&self, cql: String) -> DMResult<()> {
        self.session.query_unpaged(cql, ()).await.map_err(sc)?;
        Ok(())
    }

    async fn target_columns(&self, table: &str) -> DMResult<Vec<TargetColumn>> {
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
        let mut cols: Vec<TargetColumn> = rows
            .rows::<(String, String, i32, String)>()
            .map_err(sc)?
            .map(|r| r.map(|(name, kind, position, cql)| TargetColumn { name, kind, position, cql }))
            .collect::<Result<_, _>>()
            .map_err(sc)?;
        let rank = |k: &str| match k {
            "partition_key" => 0,
            "clustering" => 1,
            _ => 2,
        };
        cols.sort_by(|a, b| (rank(&a.kind), a.position, &a.name).cmp(&(rank(&b.kind), b.position, &b.name)));
        if cols.is_empty() {
            return Err(DMError::Error(format!("table {}.{} does not exist", self.keyspace, table)));
        }
        Ok(cols)
    }

    async fn udt_fields(&self, name: &str) -> DMResult<Vec<(String, String)>> {
        let rows = self
            .session
            .query_unpaged("SELECT field_names, field_types FROM system_schema.types WHERE keyspace_name = ? AND type_name = ?", (&self.keyspace, name))
            .await
            .map_err(sc)?
            .into_rows_result()
            .map_err(sc)?;
        let (names, types) = rows
            .maybe_first_row::<(Vec<String>, Vec<String>)>()
            .map_err(sc)?
            .ok_or_else(|| DMError::Error(format!("type {} does not exist", name)))?;
        Ok(names.into_iter().zip(types).collect())
    }

    /// Column target from the target schema (works in any process / worker connection).
    async fn target_for(&self, cql: &str) -> DMResult<CqlTarget> {
        let t = cql.trim();
        if let Some(inner) = t.strip_prefix("list<").and_then(|r| r.strip_suffix('>')) {
            return Ok(CqlTarget::List(logical_type_for(inner)));
        }
        if let Some(name) = t.strip_prefix("frozen<").and_then(|r| r.strip_suffix('>')) {
            let name = name.trim_matches('"');
            let fields = self.udt_fields(name).await?.into_iter().map(|(f, ty)| (f, logical_type_for(&ty))).collect();
            return Ok(CqlTarget::Udt { keyspace: self.keyspace.clone(), name: name.to_string(), fields });
        }
        Ok(CqlTarget::Scalar(logical_type_for(t)))
    }

    async fn write_plan(&self, def: &DataDefinition) -> DMResult<WritePlan> {
        let table = def.target_name();
        let schema = self.target_columns(table).await?;
        let mut targets = Vec::with_capacity(def.properties_info.len());
        for p in &def.properties_info {
            let col = schema
                .iter()
                .find(|c| c.name == p.property_name)
                .ok_or_else(|| DMError::Error(format!("{}: column {} missing in the target", table, p.property_name)))?;
            targets.push(match self.target_for(&col.cql).await? {
                // json text in a text column, typed scalars as declared by the definition
                CqlTarget::Scalar(LogicalType::String) => CqlTarget::Scalar(if p.data_type == LogicalType::Json { LogicalType::Json } else { LogicalType::String }),
                other => other,
            });
        }
        let mut embeds = Vec::new();
        let mut row_id = false;
        let mut names: Vec<String> = def.properties_info.iter().map(|p| quote(&p.property_name)).collect();
        for c in schema.iter().filter(|c| def.column_index(&c.name).is_none()) {
            if c.name == ROW_ID {
                row_id = true;
                names.push(quote(ROW_ID));
                continue;
            }
            let Some(fk) = def.foreign_keys.iter().find(|fk| embed_field_name(def, fk) == c.name) else { continue };
            let udt = parent_udt(&fk.reference_container_name);
            let udt_fields = self.udt_fields(&udt).await?;
            let select_cols: Vec<String> = udt_fields.iter().map(|(f, _)| quote(f)).collect();
            let wheres: Vec<String> = fk.reference_columns.iter().map(|r| format!("{} = ?", quote(r))).collect();
            let mut select = self
                .session
                .prepare(format!("SELECT {} FROM {} WHERE {}", select_cols.join(", "), self.table(short_name(&fk.reference_container_name)), wheres.join(" AND ")))
                .await
                .map_err(sc)?;
            select.set_consistency(self.consistency);
            embeds.push(Embed {
                column: c.name.clone(),
                parent: fk.reference_container_name.clone(),
                fk_columns: fk.columns.iter().filter_map(|n| def.column_index(n)).collect(),
                select,
                udt_name: udt,
                udt_fields,
            });
            names.push(quote(&c.name));
        }
        let placeholders = vec!["?"; names.len()].join(", ");
        let mut insert = self
            .session
            .prepare(format!("INSERT INTO {} ({}) VALUES ({})", self.table(table), names.join(", "), placeholders))
            .await
            .map_err(sc)?;
        insert.set_consistency(self.consistency);
        insert.set_is_idempotent(true);
        let key_columns = schema.iter().filter(|c| c.kind == "partition_key" || c.kind == "clustering").map(|c| c.name.clone()).collect();
        Ok(WritePlan { targets, embeds, row_id, insert, key_columns })
    }

    /// Parent row as the embedded UDT value (cached per run).
    async fn parent_value(&self, e: &Embed, key: Vec<Option<CqlValue>>) -> DMResult<Option<CqlValue>> {
        if key.iter().any(|k| k.is_none()) {
            return Ok(None);
        }
        let cache_key = format!("{:?}", key);
        if let Some(v) = self.parent_cache.lock().await.get(&e.parent).and_then(|m| m.get(&cache_key)) {
            return Ok(v.clone());
        }
        let rows = self.session.execute_unpaged(&e.select, key).await.map_err(sc)?.into_rows_result().map_err(sc)?;
        let value = match rows.maybe_first_row::<Row>().map_err(sc)? {
            None => None,
            Some(row) => {
                let fields = e
                    .udt_fields
                    .iter()
                    .zip(row.columns)
                    .map(|((name, ty), v)| {
                        // collections / UDT columns of the parent are kept as json text
                        let v = match (ty.as_str(), v) {
                            ("text", Some(v @ (CqlValue::List(_) | CqlValue::Set(_) | CqlValue::Map(_) | CqlValue::UserDefinedType { .. } | CqlValue::Tuple(_)))) => {
                                Some(CqlValue::Text(cql_to_json(&v).to_string()))
                            }
                            (_, v) => v,
                        };
                        (name.clone(), v)
                    })
                    .collect();
                Some(CqlValue::UserDefinedType { keyspace: self.keyspace.clone(), name: e.udt_name.clone(), fields })
            }
        };
        self.parent_cache.lock().await.entry(e.parent.clone()).or_default().insert(cache_key, value.clone());
        Ok(value)
    }

    async fn row_values(&self, plan: &WritePlan, batch: &ColumnBatch, r: usize) -> DMResult<Vec<Option<CqlValue>>> {
        let mut values = Vec::with_capacity(plan.targets.len() + plan.embeds.len() + 1);
        for (c, t) in batch.columns.iter().zip(&plan.targets) {
            values.push(cell_to_cql(c.cell(r), t)?);
        }
        // columns follow the schema order used in `write_plan`: row id and embeds after the data
        let mut extra: Vec<(usize, Option<CqlValue>)> = Vec::new();
        for (i, e) in plan.embeds.iter().enumerate() {
            let key: Vec<Option<CqlValue>> = e.fk_columns.iter().map(|&ci| values[ci].clone()).collect();
            extra.push((i, self.parent_value(e, key).await?));
        }
        if plan.row_id {
            values.push(Some(CqlValue::Uuid(uuid::Uuid::from_bytes(crate::dbc::crypto::random_bytes::<16>()?))));
        }
        values.extend(extra.into_iter().map(|(_, v)| v));
        Ok(values)
    }

    /// Writes rows grouped by partition token: one unlogged batch per partition, single rows as
    /// plain prepared requests; `concurrency` requests in flight.
    async fn write_rows(&self, insert: &PreparedStatement, rows: Vec<Vec<Option<CqlValue>>>) -> DMResult<()> {
        let mut groups: HashMap<i64, Vec<Vec<Option<CqlValue>>>> = HashMap::new();
        let mut singles = Vec::new();
        for r in rows {
            match insert.calculate_token(&r).map_err(sc)? {
                Some(t) => groups.entry(t.value()).or_default().push(r),
                None => singles.push(r),
            }
        }
        let mut requests: Vec<Vec<Vec<Option<CqlValue>>>> = singles.into_iter().map(|r| vec![r]).collect();
        for (_, g) in groups {
            let mut it = g.into_iter().peekable();
            while it.peek().is_some() {
                requests.push(it.by_ref().take(MAX_BATCH_ROWS).collect());
            }
        }
        type WriteFuture = std::pin::Pin<Box<dyn std::future::Future<Output = DMResult<()>> + Send>>;
        let mut futures: Vec<WriteFuture> = Vec::with_capacity(requests.len());
        for rows in requests {
            let (session, insert, consistency) = (self.session.clone(), insert.clone(), self.consistency);
            futures.push(Box::pin(async move {
                if rows.len() == 1 {
                    session.execute_unpaged(&insert, &rows[0]).await.map_err(sc)?;
                } else {
                    let mut b = Batch::new(BatchType::Unlogged);
                    for _ in 0..rows.len() {
                        b.append_statement(insert.clone());
                    }
                    b.set_consistency(consistency);
                    session.batch(&b, rows).await.map_err(sc)?;
                }
                Ok(())
            }));
        }
        stream::iter(futures).buffer_unordered(self.settings.scylla.concurrency.max(1)).try_collect::<Vec<_>>().await?;
        Ok(())
    }

    async fn write_batch(&self, plan: &WritePlan, batch: &ColumnBatch) -> DMResult<()> {
        let mut rows = Vec::with_capacity(batch.rows);
        for r in 0..batch.rows {
            rows.push(self.row_values(plan, batch, r).await?);
        }
        self.write_rows(&plan.insert, rows).await
    }
}

#[async_trait]
impl DataReceiver for Scylla {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        self.exec(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {}", quote(&self.keyspace), self.settings.scylla.replication))
            .await?;
        let by_name: HashMap<&str, &DataDefinition> = defs.iter().map(|d| (d.container_name.as_str(), d)).collect();
        let referenced: HashSet<&str> = defs
            .iter()
            .flat_map(|d| self.embeddable(d, &by_name))
            .map(|fk| fk.reference_container_name.as_str())
            .collect();
        for def in defs {
            let table = def.target_name();
            let mut columns = Vec::new();
            for (i, p) in def.properties_info.iter().enumerate() {
                let target = column_target(&self.keyspace, def, i);
                if let CqlTarget::Udt { name, fields, .. } = &target {
                    let f: Vec<String> = fields.iter().map(|(n, t)| format!("{} {}", quote(n), cql_scalar(*t))).collect();
                    self.exec(format!("CREATE TYPE IF NOT EXISTS {}.{} ({})", quote(&self.keyspace), quote(name), f.join(", "))).await?;
                }
                columns.push(format!("{} {}", quote(&p.property_name), target.cql_type()));
            }
            let (partition, clustering) = self.key_for(def);
            if partition == [ROW_ID] {
                warn!("[Migration][{}] no primary key: a generated {} is the partition key", def.container_name, ROW_ID);
                columns.push(format!("{} uuid", quote(ROW_ID)));
            }
            let mut embed_types = Vec::new();
            for fk in self.embeddable(def, &by_name) {
                let col = embed_field_name(def, fk);
                let udt = parent_udt(&fk.reference_container_name);
                columns.push(format!("{} frozen<{}>", quote(&col), quote(&udt)));
                embed_types.push((col, udt));
            }
            let pk = format!(
                "PRIMARY KEY (({}){}{})",
                partition.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", "),
                if clustering.is_empty() { "" } else { ", " },
                clustering.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ")
            );
            info!("[Migration][{}] Create table {}", def.container_name, self.table(table));
            self.exec(format!("CREATE TABLE IF NOT EXISTS {} ({}, {})", self.table(table), columns.join(", "), pk)).await?;

            // UDT used by children that embed this table: its columns (collections / UDTs as
            // json text) and its own embedded parents (nested denormalization)
            if referenced.contains(def.container_name.as_str()) {
                let mut fields: Vec<String> = def
                    .properties_info
                    .iter()
                    .map(|p| format!("{} {}", quote(&p.property_name), cql_scalar(p.data_type)))
                    .collect();
                fields.extend(embed_types.iter().map(|(c, u)| format!("{} frozen<{}>", quote(c), quote(u))));
                self.exec(format!("CREATE TYPE IF NOT EXISTS {}.{} ({})", quote(&self.keyspace), quote(&parent_udt(&def.container_name)), fields.join(", ")))
                    .await?;
            }
        }
        Ok(())
    }

    async fn write_data(&mut self, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        let plan = self.write_plan(def).await?;
        let mut total = 0;
        while let Some(batch) = rx.recv().await {
            total += batch.rows as u64;
            self.write_batch(&plan, &batch).await?;
        }
        Ok(total)
    }

    /// Secondary indexes for source indexes and foreign key columns (SCY-9).
    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        for def in defs {
            let table = def.target_name();
            let (partition, clustering) = self.key_for(def);
            let mut cols: Vec<&str> = def.indexes.iter().filter_map(|i| i.columns.first().map(|c| c.as_str())).collect();
            cols.extend(def.foreign_keys.iter().filter(|fk| fk.columns.len() == 1).map(|fk| fk.columns[0].as_str()));
            let mut seen = HashSet::new();
            for c in cols {
                if def.column_index(c).is_none() || partition.iter().any(|p| p == c) || clustering.iter().any(|p| p == c) || !seen.insert(c) {
                    continue;
                }
                let name = format!("{}_{}_idx", table, c);
                if let Err(e) = self.exec(format!("CREATE INDEX IF NOT EXISTS {} ON {} ({})", quote(&name), self.table(table), quote(c))).await {
                    warn!("[Migration][{}] index {} not created: {}", def.container_name, name, e);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeReceiver for Scylla {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()> {
        let changes = coalesce_changes(defs, events)?;
        let mut changed_parents: HashMap<String, Vec<Vec<Value>>> = HashMap::new();
        for c in &changes {
            let def = c.def;
            let plan = self.write_plan(def).await?;
            // parents may have changed: forget cached embedded values
            self.parent_cache.lock().await.remove(&def.container_name);
            let full: Vec<&Vec<Option<Value>>> = c.upserts.iter().filter(|r| !ContainerChanges::is_partial(r)).collect();
            if !full.is_empty() {
                let mut batch = ColumnBatch::for_definition(def, full.len());
                for r in &full {
                    batch.push_row(r.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect())?;
                }
                self.write_batch(&plan, &batch).await?;
            }
            for r in c.upserts.iter().filter(|r| ContainerChanges::is_partial(r)) {
                let mut sets = Vec::new();
                let mut values = Vec::new();
                let mut wheres = Vec::new();
                let mut keys = Vec::new();
                for (i, v) in r.iter().enumerate() {
                    let Some(v) = v else { continue };
                    let name = &def.properties_info[i].property_name;
                    let cql = cell_to_cql(Cell::from_value(v), &plan.targets[i])?;
                    if plan.key_columns.contains(name) {
                        wheres.push(format!("{} = ?", quote(name)));
                        keys.push(cql);
                    } else {
                        sets.push(format!("{} = ?", quote(name)));
                        values.push(cql);
                    }
                }
                if sets.is_empty() || wheres.len() != plan.key_columns.len() {
                    continue;
                }
                values.extend(keys);
                self.session
                    .query_unpaged(format!("UPDATE {} SET {} WHERE {}", self.table(def.target_name()), sets.join(", "), wheres.join(" AND ")), values)
                    .await
                    .map_err(sc)?;
            }
            let ids = def.identifier_indexes();
            let entry = changed_parents.entry(def.container_name.clone()).or_default();
            for r in &c.upserts {
                entry.push(ids.iter().map(|&i| r[i].clone().unwrap_or(Value::Null)).collect());
            }
            entry.extend(c.deletes.iter().cloned());
        }
        for c in changes.iter().rev() {
            if c.deletes.is_empty() {
                continue;
            }
            let def = c.def;
            let ids = def.identifier_indexes();
            let wheres: Vec<String> = ids.iter().map(|&i| format!("{} = ?", quote(&def.properties_info[i].property_name))).collect();
            let mut del = self
                .session
                .prepare(format!("DELETE FROM {} WHERE {}", self.table(def.target_name()), wheres.join(" AND ")))
                .await
                .map_err(sc)?;
            del.set_consistency(self.consistency);
            for k in &c.deletes {
                let values: Vec<Option<CqlValue>> = k
                    .iter()
                    .zip(&ids)
                    .map(|(v, &i)| cell_to_cql(Cell::from_value(v), &CqlTarget::Scalar(def.properties_info[i].data_type)))
                    .collect::<DMResult<_>>()?;
                self.session.execute_unpaged(&del, values).await.map_err(sc)?;
            }
        }
        if self.strategy() == SqlToNoSqlStrategy::Denormalized {
            self.refresh_children(defs, &changed_parents).await?;
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Scylla::reconnect(self).await
    }
}

impl Scylla {
    /// Live (LIVE-11): children embedding a changed parent get the new embedded value, found
    /// through the secondary index on the foreign key column (one level).
    async fn refresh_children(&self, defs: &[DataDefinition], changed: &HashMap<String, Vec<Vec<Value>>>) -> DMResult<()> {
        for child in defs {
            for fk in &child.foreign_keys {
                let Some(keys) = changed.get(&fk.reference_container_name) else { continue };
                if fk.columns.len() != 1 {
                    continue;
                }
                let column = embed_field_name(child, fk);
                let schema = self.target_columns(child.target_name()).await?;
                if !schema.iter().any(|c| c.name == column) {
                    continue;
                }
                let plan = self.write_plan(child).await?;
                let Some(embed) = plan.embeds.iter().find(|e| e.column == column) else { continue };
                let key_cols: Vec<&TargetColumn> = schema.iter().filter(|c| c.kind == "partition_key" || c.kind == "clustering").collect();
                let select = format!(
                    "SELECT {} FROM {} WHERE {} = ?",
                    key_cols.iter().map(|c| quote(&c.name)).collect::<Vec<_>>().join(", "),
                    self.table(child.target_name()),
                    quote(&fk.columns[0])
                );
                let update = format!(
                    "UPDATE {} SET {} = ? WHERE {}",
                    self.table(child.target_name()),
                    quote(&column),
                    key_cols.iter().map(|c| format!("{} = ?", quote(&c.name))).collect::<Vec<_>>().join(" AND ")
                );
                let fk_type = child.column_index(&fk.columns[0]).map(|i| child.properties_info[i].data_type).unwrap_or(LogicalType::String);
                for k in keys {
                    let Some(v) = k.first() else { continue };
                    let key = cell_to_cql(Cell::from_value(v), &CqlTarget::Scalar(fk_type))?;
                    let rows = self.session.query_unpaged(select.clone(), (key.clone(),)).await.map_err(sc)?.into_rows_result().map_err(sc)?;
                    let embedded = self.parent_value(embed, vec![key]).await?;
                    for r in rows.rows::<Row>().map_err(sc)? {
                        let mut values = vec![embedded.clone()];
                        values.extend(r.map_err(sc)?.columns);
                        self.session.query_unpaged(update.clone(), values).await.map_err(sc)?;
                    }
                }
            }
        }
        Ok(())
    }
}
