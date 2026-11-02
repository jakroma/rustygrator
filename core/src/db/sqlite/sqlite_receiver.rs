//! SQLite receiver (DB-8).

use std::collections::HashSet;

use async_trait::async_trait;
use log::{info, warn};
use tokio::sync::mpsc;

use crate::{
    dbc::batch::{Cell, ColumnBatch},
    db::sqlite::sqlite_data_types::{sqlite_type_for, SqlValue},
    migrator::migrator_steps::{coalesce_changes, ChangeReceiver, ContainerChanges, DataReceiver},
    models::{
        change::ChangeEvent,
        data_definitions::{short_name, DataDefinition},
        value::Value,
    },
    DMResult,
};

use super::sqlite::{quote_ident, Sqlite};

/// Rows written per transaction during the initial load.
const WRITE_CHUNK_ROWS: usize = 20_000;

fn columns(names: &[&str]) -> String {
    names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ")
}

pub(crate) fn upsert_statement(def: &DataDefinition) -> String {
    let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
    let ids = def.identifier_names();
    let placeholders = vec!["?"; names.len()].join(", ");
    let mut sql = format!("INSERT INTO {} ({}) VALUES ({})", quote_ident(def.target_name()), columns(&names), placeholders);
    if !ids.is_empty() {
        let updates: Vec<String> = names
            .iter()
            .filter(|n| !ids.contains(n))
            .map(|n| format!("{0} = excluded.{0}", quote_ident(n)))
            .collect();
        if updates.is_empty() {
            sql.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", columns(&ids)));
        } else {
            sql.push_str(&format!(" ON CONFLICT ({}) DO UPDATE SET {}", columns(&ids), updates.join(", ")));
        }
    }
    sql
}

fn create_table_statement(def: &DataDefinition, migrated: &HashSet<&str>) -> String {
    let mut parts: Vec<String> = def
        .properties_info
        .iter()
        .map(|p| {
            format!(
                "{} {}{}",
                quote_ident(&p.property_name),
                sqlite_type_for(p.data_type),
                if p.is_nullable && !p.is_identifier { "" } else { " NOT NULL" }
            )
        })
        .collect();
    let ids = def.identifier_names();
    if !ids.is_empty() {
        parts.push(format!("PRIMARY KEY ({})", columns(&ids)));
    }
    for fk in &def.foreign_keys {
        if !migrated.contains(fk.reference_container_name.as_str()) {
            continue;
        }
        let cols: Vec<&str> = fk.columns.iter().map(|s| s.as_str()).collect();
        let refs: Vec<&str> = fk.reference_columns.iter().map(|s| s.as_str()).collect();
        parts.push(format!(
            "FOREIGN KEY ({}) REFERENCES {} ({})",
            columns(&cols),
            quote_ident(short_name(&fk.reference_container_name)),
            columns(&refs)
        ));
    }
    format!("CREATE TABLE IF NOT EXISTS {} ({})", quote_ident(def.target_name()), parts.join(", "))
}

fn write_rows(conn: &mut rusqlite::Connection, sql: &str, batch: &ColumnBatch) -> DMResult<u64> {
    let t = conn.transaction()?;
    {
        let mut stmt = t.prepare_cached(sql)?;
        for r in 0..batch.rows {
            stmt.execute(rusqlite::params_from_iter(batch.columns.iter().map(|c| match c.text_bytes(r) {
                // text is bound as bytes: no UTF-8 re-check (PERF-9)
                Some(b) if c.is_text() => SqlValue::Text(b),
                _ => SqlValue::Cell(c.cell(r)),
            })))?;
        }
    }
    t.commit()?;
    Ok(batch.rows as u64)
}

/// Owned statements of one live batch, executed in a single transaction.
enum Op {
    Rows(String, Vec<Vec<Value>>),
}

#[async_trait]
impl DataReceiver for Sqlite {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let migrated: HashSet<&str> = defs.iter().map(|d| d.container_name.as_str()).collect();
        let statements: Vec<String> = defs.iter().map(|d| create_table_statement(d, &migrated)).collect();
        for d in defs {
            info!("[Migration][{}] Create table {}", d.container_name, d.target_name());
        }
        let saved = self
            .run(move |conn| {
                for s in statements {
                    conn.execute_batch(&s)?;
                }
                // bulk load settings (PERF-14): no rollback journal, no fsync, 256 MiB page cache
                let journal: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
                let sync: i64 = conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
                conn.query_row("PRAGMA journal_mode = OFF", [], |_| Ok(()))?;
                conn.execute_batch("PRAGMA synchronous = OFF; PRAGMA cache_size = -262144;")?;
                Ok((journal, sync))
            })
            .await?;
        if self.saved_pragmas.is_none() {
            self.saved_pragmas = Some(saved);
        }
        Ok(())
    }

    async fn write_data(&mut self, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        let sql = upsert_statement(def);
        let mut total = 0u64;
        let mut chunk: Option<ColumnBatch> = None;
        loop {
            let next = rx.recv().await;
            let done = next.is_none();
            if let Some(b) = next {
                match &mut chunk {
                    Some(c) => c.append(&b),
                    None => chunk = Some(b),
                }
            }
            let rows_ready = chunk.as_ref().map(|c| c.rows).unwrap_or(0);
            if (done && rows_ready > 0) || rows_ready >= WRITE_CHUNK_ROWS {
                let rows = chunk.take().unwrap();
                let s = sql.clone();
                total += self.run(move |conn| write_rows(conn, &s, &rows)).await?;
            }
            if done {
                return Ok(total);
            }
        }
    }

    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let mut statements = Vec::new();
        for def in defs {
            for idx in &def.indexes {
                if idx.columns.is_empty() || idx.columns.iter().any(|c| def.column_index(c).is_none()) {
                    continue;
                }
                let cols: Vec<&str> = idx.columns.iter().map(|s| s.as_str()).collect();
                statements.push(format!(
                    "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
                    if idx.unique { "UNIQUE " } else { "" },
                    quote_ident(&format!("{}_{}", def.target_name(), idx.name)),
                    quote_ident(def.target_name()),
                    columns(&cols)
                ));
            }
        }
        let saved = self.saved_pragmas.take();
        self.run(move |conn| {
            // indexes after the load, then the normal durability settings come back
            for s in statements {
                if let Err(e) = conn.execute_batch(&s) {
                    warn!("[Migration] index not created ({}): {}", s, e);
                }
            }
            if let Some((journal, sync)) = saved {
                let journal = if journal.eq_ignore_ascii_case("off") { "delete".to_string() } else { journal };
                conn.query_row(&format!("PRAGMA journal_mode = {}", journal), [], |_| Ok(()))?;
                conn.execute_batch(&format!("PRAGMA synchronous = {}; PRAGMA cache_size = -2000;", sync))?;
            }
            Ok(())
        })
        .await
    }
}

fn plan(changes: &[ContainerChanges<'_>]) -> Vec<Op> {
    let mut ops = Vec::new();
    for c in changes {
        let def = c.def;
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let id_idx = def.identifier_indexes();
        let full: Vec<Vec<Value>> = c
            .upserts
            .iter()
            .filter(|r| !ContainerChanges::is_partial(r))
            .map(|r| r.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect())
            .collect();
        if !full.is_empty() {
            ops.push(Op::Rows(upsert_statement(def), full));
        }
        // only some columns known: insert or update just those columns
        for r in c.upserts.iter().filter(|r| ContainerChanges::is_partial(r)) {
            if id_idx.iter().any(|&i| r[i].is_none()) {
                continue;
            }
            let present: Vec<usize> = (0..r.len()).filter(|&i| r[i].is_some()).collect();
            let cols: Vec<&str> = present.iter().map(|&i| names[i]).collect();
            let ids = def.identifier_names();
            let updates: Vec<String> = cols.iter().filter(|c| !ids.contains(c)).map(|c| format!("{0} = excluded.{0}", quote_ident(c))).collect();
            let action = if updates.is_empty() { "DO NOTHING".to_string() } else { format!("DO UPDATE SET {}", updates.join(", ")) };
            ops.push(Op::Rows(
                format!(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}",
                    quote_ident(def.target_name()),
                    columns(&cols),
                    vec!["?"; cols.len()].join(", "),
                    columns(&ids),
                    action
                ),
                vec![present.iter().map(|&i| r[i].clone().unwrap_or(Value::Null)).collect()],
            ));
        }
    }
    for c in changes.iter().rev() {
        if c.deletes.is_empty() {
            continue;
        }
        let ids = c.def.identifier_names();
        let wheres: Vec<String> = ids.iter().map(|i| format!("{} = ?", quote_ident(i))).collect();
        ops.push(Op::Rows(
            format!("DELETE FROM {} WHERE {}", quote_ident(c.def.target_name()), wheres.join(" AND ")),
            c.deletes.clone(),
        ));
    }
    ops
}

#[async_trait]
impl ChangeReceiver for Sqlite {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()> {
        let ops = plan(&coalesce_changes(defs, events)?);
        if ops.is_empty() {
            return Ok(());
        }
        self.run(move |conn| {
            let t = conn.transaction()?;
            for Op::Rows(sql, rows) in &ops {
                let mut stmt = t.prepare_cached(sql)?;
                for row in rows {
                    stmt.execute(rusqlite::params_from_iter(row.iter().map(|v| SqlValue::Cell(Cell::from_value(v)))))?;
                }
            }
            t.commit()?;
            Ok(())
        })
        .await
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Sqlite::reconnect(self).await
    }
}
