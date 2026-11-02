//! PostgreSQL receiver (DB-2).
//!
//! Data is loaded with binary `COPY` (the fastest bulk path) into a temporary staging table and
//! merged with `INSERT .. ON CONFLICT DO UPDATE`, so loads are idempotent for tables with a
//! primary key. Foreign keys and indexes are created after the data is loaded.

use std::collections::HashSet;

use async_trait::async_trait;
use futures_util::pin_mut;
use log::{info, warn};
use tokio::sync::mpsc;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type, Transaction};

use crate::{
    dbc::batch::{Cell, ColumnBatch},
    db::postgresql::postgresql_data_types::{pg_type_for, PgParam, PgValue},
    migrator::migrator_steps::{coalesce_changes, ChangeReceiver, ContainerChanges, DataReceiver},
    models::{
        change::ChangeEvent,
        data_definitions::{short_name, DataDefinition},
        value::Value,
    },
    DMResult,
};

use super::postgresql::{quote_ident, Postgresql};

/// Rows copied into the staging table before they are merged into the target.
const STAGE_CHUNK_ROWS: usize = 50_000;
const STAGE_TABLE: &str = "data_mover_stage";
const KEYS_TABLE: &str = "data_mover_keys";

fn truncate_ident(name: String) -> String {
    if name.len() <= 63 {
        return name;
    }
    let mut end = 63;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_string()
}

fn column_list(names: &[&str]) -> String {
    names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ")
}

fn merge_statement(target: &str, columns: &[&str], ids: &[&str]) -> String {
    let cols = column_list(columns);
    let updates: Vec<String> = columns
        .iter()
        .filter(|c| !ids.contains(c))
        .map(|c| format!("{0} = EXCLUDED.{0}", quote_ident(c)))
        .collect();
    let action = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    format!(
        "INSERT INTO {target} ({cols}) SELECT {cols} FROM {STAGE_TABLE} ON CONFLICT ({}) {action}",
        column_list(ids)
    )
}

/// Binary COPY of `rows` into `table`.
async fn copy_rows(tx: &Transaction<'_>, table: &str, columns: &str, types: &[Type], batch: &ColumnBatch) -> DMResult<u64> {
    let sink = tx
        .copy_in(&format!("COPY {} ({}) FROM STDIN (FORMAT binary)", table, columns))
        .await?;
    let writer = BinaryCopyInWriter::new(sink, types);
    pin_mut!(writer);
    let mut params: Vec<PgParam<'_>> = Vec::with_capacity(types.len());
    for r in 0..batch.rows {
        params.clear();
        params.extend(batch.columns.iter().map(|c| match c.text_bytes(r) {
            Some(b) if c.is_text() => PgParam::Text(c.kind, b),
            _ => PgParam::Cell(c.cell(r)),
        }));
        writer.as_mut().write_raw(params.iter()).await?;
    }
    Ok(writer.finish().await?)
}

impl Postgresql {
    async fn target_types(&self, target: &str, columns: &str) -> DMResult<Vec<Type>> {
        let probe = self.client.prepare(&format!("SELECT {} FROM {} LIMIT 0", columns, target)).await?;
        Ok(probe.columns().iter().map(|c| c.type_().clone()).collect())
    }

    /// COPY + merge of one chunk of full rows in its own transaction. With `direct` (target was
    /// empty when the load started) rows are copied straight into the table.
    async fn write_chunk(&mut self, def: &DataDefinition, types: &[Type], rows: &ColumnBatch, direct: bool) -> DMResult<u64> {
        let target = self.qualified_target(def.target_name());
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let ids = def.identifier_names();
        let cols = column_list(&names);
        let tx = self.client.transaction().await?;
        let written = if ids.is_empty() || direct {
            copy_rows(&tx, &target, &cols, types, rows).await?
        } else {
            tx.batch_execute(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {STAGE_TABLE} AS SELECT {cols} FROM {target} WITH NO DATA"
            ))
            .await?;
            copy_rows(&tx, STAGE_TABLE, &cols, types, rows).await?;
            tx.execute(merge_statement(&target, &names, &ids).as_str(), &[]).await?;
            tx.batch_execute(&format!("DROP TABLE {STAGE_TABLE}")).await?;
            rows.rows as u64
        };
        tx.commit().await?;
        Ok(written)
    }

    async fn create_table(&self, def: &DataDefinition) -> DMResult<()> {
        let mut columns: Vec<String> = def
            .properties_info
            .iter()
            .map(|p| {
                format!(
                    "{} {}{}",
                    quote_ident(&p.property_name),
                    pg_type_for(p.data_type),
                    if p.is_nullable && !p.is_identifier { "" } else { " NOT NULL" }
                )
            })
            .collect();
        let ids = def.identifier_names();
        if !ids.is_empty() {
            columns.push(format!("PRIMARY KEY ({})", column_list(&ids)));
        }
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            self.qualified_target(def.target_name()),
            columns.join(", ")
        );
        self.client.batch_execute(&sql).await?;
        Ok(())
    }

    async fn apply_container(&mut self, c: &ContainerChanges<'_>) -> DMResult<()> {
        let def = c.def;
        let target = self.qualified_target(def.target_name());
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let id_idx = def.identifier_indexes();

        let (full, partial): (Vec<&Vec<Option<Value>>>, Vec<&Vec<Option<Value>>>) =
            c.upserts.iter().partition(|r| !ContainerChanges::is_partial(r));

        if !full.is_empty() {
            let mut rows = ColumnBatch::for_definition(def, full.len());
            for r in &full {
                rows.push_row(r.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect())?;
            }
            let types = self.target_types(&target, &column_list(&names)).await?;
            self.write_chunk(def, &types, &rows, false).await?;
        }

        // rows with only some columns known (unchanged TOAST values, CDC deltas): insert or
        // update just those columns
        for row in partial {
            let mut cols = Vec::new();
            let mut params: Vec<PgValue> = Vec::new();
            for (i, v) in row.iter().enumerate() {
                if let Some(v) = v {
                    params.push(PgValue(Cell::from_value(v)));
                    cols.push(names[i]);
                }
            }
            if id_idx.iter().any(|&i| row[i].is_none()) {
                continue;
            }
            let placeholders: Vec<String> = (1..=params.len()).map(|n| format!("${}", n)).collect();
            let ids = def.identifier_names();
            let updates: Vec<String> = cols.iter().filter(|c| !ids.contains(c)).map(|c| format!("{0} = EXCLUDED.{0}", quote_ident(c))).collect();
            let action = if updates.is_empty() { "DO NOTHING".to_string() } else { format!("DO UPDATE SET {}", updates.join(", ")) };
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}",
                target,
                column_list(&cols),
                placeholders.join(", "),
                column_list(&ids),
                action
            );
            let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params.iter().map(|p| p as _).collect();
            self.client.execute(sql.as_str(), &refs).await?;
        }
        Ok(())
    }

    async fn delete_container(&mut self, c: &ContainerChanges<'_>) -> DMResult<()> {
        if c.deletes.is_empty() {
            return Ok(());
        }
        let target = self.qualified_target(c.def.target_name());
        let ids = c.def.identifier_names();
        let cols = column_list(&ids);
        let types = self.target_types(&target, &cols).await?;
        let tx = self.client.transaction().await?;
        tx.batch_execute(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS {KEYS_TABLE} AS SELECT {cols} FROM {target} WITH NO DATA"
        ))
        .await?;
        let kinds: Vec<_> = c.def.identifier_indexes().iter().map(|&i| c.def.properties_info[i].data_type).collect();
        let mut keys = ColumnBatch::new(&kinds, c.deletes.len());
        for k in &c.deletes {
            keys.push_row(k.clone())?;
        }
        copy_rows(&tx, KEYS_TABLE, &cols, &types, &keys).await?;
        let cond: Vec<String> = ids.iter().map(|i| format!("t.{0} = k.{0}", quote_ident(i))).collect();
        tx.execute(&format!("DELETE FROM {} t USING {KEYS_TABLE} k WHERE {}", target, cond.join(" AND ")), &[])
            .await?;
        tx.batch_execute(&format!("DROP TABLE {KEYS_TABLE}")).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl DataReceiver for Postgresql {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        self.client
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(self.target_schema())))
            .await?;
        for def in defs {
            info!("[Migration][{}] Create table {}", def.container_name, self.qualified_target(def.target_name()));
            self.create_table(def).await?;
        }
        Ok(())
    }

    async fn write_data(&mut self, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        let target = self.qualified_target(def.target_name());
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let types = self.target_types(&target, &column_list(&names)).await?;
        // an empty target needs no merge: plain binary COPY is the fastest load; a re-run (or a
        // resumed load) finds rows and merges on the primary key instead
        let direct = self
            .client
            .query_one(&format!("SELECT NOT EXISTS (SELECT 1 FROM {})", target), &[])
            .await?
            .get::<_, bool>(0);

        let mut total = 0u64;
        let mut chunk: Option<ColumnBatch> = None;
        while let Some(batch) = rx.recv().await {
            match &mut chunk {
                Some(c) => c.append(&batch),
                None => chunk = Some(batch),
            }
            if chunk.as_ref().is_some_and(|c| c.rows >= STAGE_CHUNK_ROWS) {
                total += self.write_chunk(def, &types, chunk.as_ref().unwrap(), direct).await?;
                chunk = None;
            }
        }
        if let Some(c) = chunk.filter(|c| !c.is_empty()) {
            total += self.write_chunk(def, &types, &c, direct).await?;
        }
        Ok(total)
    }

    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let migrated: HashSet<&str> = defs.iter().map(|d| d.container_name.as_str()).collect();
        for def in defs {
            let target = self.qualified_target(def.target_name());
            for idx in &def.indexes {
                if idx.columns.is_empty() || idx.columns.iter().any(|c| def.column_index(c).is_none()) {
                    continue;
                }
                let cols: Vec<&str> = idx.columns.iter().map(|s| s.as_str()).collect();
                let name = truncate_ident(format!("{}_{}", def.target_name(), idx.name));
                let sql = format!(
                    "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
                    if idx.unique { "UNIQUE " } else { "" },
                    quote_ident(&name),
                    target,
                    column_list(&cols)
                );
                if let Err(e) = self.client.batch_execute(&sql).await {
                    warn!("[Migration][{}] index {} not created: {}", def.container_name, name, e);
                }
            }
        }
        for def in defs {
            let target = self.qualified_target(def.target_name());
            for fk in &def.foreign_keys {
                if !migrated.contains(fk.reference_container_name.as_str()) {
                    continue;
                }
                let cols: Vec<&str> = fk.columns.iter().map(|s| s.as_str()).collect();
                let ref_cols: Vec<&str> = fk.reference_columns.iter().map(|s| s.as_str()).collect();
                let name = truncate_ident(format!("{}_{}", def.target_name(), fk.name));
                let sql = format!(
                    "DO $$ BEGIN ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}); \
                     EXCEPTION WHEN duplicate_object THEN NULL; END $$",
                    target,
                    quote_ident(&name),
                    column_list(&cols),
                    self.qualified_target(short_name(&fk.reference_container_name)),
                    column_list(&ref_cols)
                );
                if let Err(e) = self.client.batch_execute(&sql).await {
                    warn!("[Migration][{}] foreign key {} not created: {}", def.container_name, name, e);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeReceiver for Postgresql {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()> {
        let changes = coalesce_changes(defs, events)?;
        // parents before children for upserts, children before parents for deletes
        for c in &changes {
            if !c.upserts.is_empty() {
                self.apply_container(c).await?;
            }
        }
        for c in changes.iter().rev() {
            self.delete_container(c).await?;
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Postgresql::reconnect(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_statement_updates_non_key_columns() {
        let s = merge_statement("\"public\".\"t\"", &["id", "name"], &["id"]);
        assert!(s.contains("ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""), "{s}");
        let s = merge_statement("t", &["a", "b"], &["a", "b"]);
        assert!(s.ends_with("DO NOTHING"));
    }
}
