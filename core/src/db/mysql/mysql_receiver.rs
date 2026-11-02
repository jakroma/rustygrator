//! MySQL receiver (MY-3): tables created from the definitions, rows written with multi-row
//! prepared `INSERT`s (one transaction per batch); a target that already has rows is merged
//! with `ON DUPLICATE KEY UPDATE`, so re-runs and live changes are idempotent. Indexes and
//! foreign keys are created after the data is loaded.

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;
use log::{info, warn};
use mysql_async::{prelude::Queryable, Params, TxOpts, Value as My};
use tokio::sync::mpsc;

use crate::{
    dbc::batch::{Cell, ColumnBatch},
    migrator::migrator_steps::{coalesce_changes, ChangeReceiver, ContainerChanges, DataReceiver},
    models::{
        change::ChangeEvent,
        data_definitions::{short_name, DataDefinition},
    },
    DMResult,
};

use super::{
    mysql_connection::{quote_ident, Mysql},
    mysql_data_types::{mysql_type_for, param},
};

/// MySQL allows 65 535 placeholders per statement.
const MAX_PARAMS: usize = 65_535;
/// Rows per INSERT statement (larger statements stop getting faster).
const MAX_ROWS_PER_INSERT: usize = 1_000;
/// Index names are limited to 64 characters.
const MAX_IDENT: usize = 64;

fn column_list(names: &[&str]) -> String {
    names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ")
}

fn truncate_ident(mut name: String) -> String {
    if name.len() > MAX_IDENT {
        let mut end = MAX_IDENT;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }
    name
}

/// Rows per statement for `columns` columns.
pub(crate) fn rows_per_insert(columns: usize) -> usize {
    (MAX_PARAMS / columns.max(1)).clamp(1, MAX_ROWS_PER_INSERT)
}

/// `INSERT INTO t (cols) VALUES (?, ..), .. [ON DUPLICATE KEY UPDATE c = VALUES(c), ..]`.
/// `VALUES(c)` works on MySQL 5.7 / 8.x and MariaDB alike.
pub(crate) fn insert_statement(target: &str, columns: &[&str], ids: &[&str], rows: usize, upsert: bool) -> String {
    let one = format!("({})", vec!["?"; columns.len()].join(", "));
    let mut sql = format!("INSERT INTO {target} ({}) VALUES {}", column_list(columns), vec![one; rows].join(", "));
    if upsert {
        let mut updates: Vec<String> = columns.iter().filter(|c| !ids.contains(c)).map(|c| format!("{0} = VALUES({0})", quote_ident(c))).collect();
        if updates.is_empty() {
            // every column is part of the key: nothing to update, keep the row
            updates.push(format!("{0} = {0}", quote_ident(ids[0])));
        }
        sql.push_str(" ON DUPLICATE KEY UPDATE ");
        sql.push_str(&updates.join(", "));
    }
    sql
}

/// Columns that need a bounded type: keys, indexed columns and foreign key columns.
fn keyed_columns(def: &DataDefinition) -> HashSet<&str> {
    let mut keyed: HashSet<&str> = def.identifier_names().into_iter().collect();
    keyed.extend(def.indexes.iter().flat_map(|i| i.columns.iter().map(String::as_str)));
    keyed.extend(def.foreign_keys.iter().flat_map(|f| f.columns.iter().map(String::as_str)));
    keyed
}

pub(crate) fn create_table_statement(target: &str, def: &DataDefinition) -> String {
    let keyed = keyed_columns(def);
    let mut columns: Vec<String> = def
        .properties_info
        .iter()
        .map(|p| {
            format!(
                "{} {}{}",
                quote_ident(&p.property_name),
                mysql_type_for(p, keyed.contains(p.property_name.as_str())),
                if p.is_nullable && !p.is_identifier { " NULL" } else { " NOT NULL" }
            )
        })
        .collect();
    let ids = def.identifier_names();
    if !ids.is_empty() {
        columns.push(format!("PRIMARY KEY ({})", column_list(&ids)));
    }
    format!("CREATE TABLE IF NOT EXISTS {target} ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", columns.join(", "))
}

impl Mysql {
    fn target(&self, def: &DataDefinition) -> String {
        format!("{}.{}", quote_ident(self.database()), quote_ident(def.target_name()))
    }

    /// Writes `rows` (all columns, full statements of `rows_per_insert` rows) in one transaction.
    async fn write_rows(&mut self, def: &DataDefinition, rows: &ColumnBatch, upsert: bool) -> DMResult<u64> {
        let target = self.target(def);
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let ids = def.identifier_names();
        let upsert = upsert && !ids.is_empty();
        let per = rows_per_insert(names.len());
        let full = insert_statement(&target, &names, &ids, per, upsert);
        let mut t = self.conn.start_transaction(TxOpts::default()).await?;
        let mut start = 0;
        while start < rows.rows {
            let end = (start + per).min(rows.rows);
            let mut params: Vec<My> = Vec::with_capacity((end - start) * names.len());
            for r in start..end {
                params.extend(rows.columns.iter().map(|c| param(c.cell(r))));
            }
            // statements are cached by text: the full-size one is prepared once per table
            let sql = if end - start == per { full.clone() } else { insert_statement(&target, &names, &ids, end - start, upsert) };
            t.exec_drop(sql, Params::Positional(params)).await?;
            start = end;
        }
        t.commit().await?;
        Ok(rows.rows as u64)
    }

    async fn apply_container(&mut self, c: &ContainerChanges<'_>) -> DMResult<()> {
        let def = c.def;
        let target = self.target(def);
        let names: Vec<&str> = def.properties_info.iter().map(|p| p.property_name.as_str()).collect();
        let ids = def.identifier_names();
        let id_idx = def.identifier_indexes();
        let (full, partial): (Vec<_>, Vec<_>) = c.upserts.iter().partition(|r| !ContainerChanges::is_partial(r));
        if !full.is_empty() {
            let mut rows = ColumnBatch::for_definition(def, full.len());
            for r in &full {
                rows.push_row(r.iter().map(|v| v.clone().unwrap_or(crate::models::value::Value::Null)).collect())?;
            }
            self.write_rows(def, &rows, true).await?;
        }
        // only some columns known (CDC deltas): insert or update just those
        for row in partial {
            if id_idx.iter().any(|&i| row[i].is_none()) {
                continue;
            }
            let (cols, params): (Vec<&str>, Vec<My>) = row.iter().enumerate().filter_map(|(i, v)| v.as_ref().map(|v| (names[i], param(Cell::from_value(v))))).unzip();
            self.conn.exec_drop(insert_statement(&target, &cols, &ids, 1, true), Params::Positional(params)).await?;
        }
        Ok(())
    }

    async fn delete_container(&mut self, c: &ContainerChanges<'_>) -> DMResult<()> {
        if c.deletes.is_empty() {
            return Ok(());
        }
        let target = self.target(c.def);
        let ids = c.def.identifier_names();
        let tuple = format!("({})", vec!["?"; ids.len()].join(", "));
        for chunk in c.deletes.chunks(rows_per_insert(ids.len())) {
            let sql = format!("DELETE FROM {target} WHERE ({}) IN ({})", column_list(&ids), vec![tuple.as_str(); chunk.len()].join(", "));
            let params: Vec<My> = chunk.iter().flat_map(|k| k.iter().map(|v| param(Cell::from_value(v)))).collect();
            self.conn.exec_drop(sql, Params::Positional(params)).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DataReceiver for Mysql {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let db = quote_ident(self.database());
        self.conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {db} DEFAULT CHARACTER SET utf8mb4")).await?;
        self.conn.query_drop(format!("USE {db}")).await?;
        for def in defs {
            info!("[Migration][{}] Create table {}", def.container_name, self.target(def));
            let sql = create_table_statement(&self.target(def), def);
            self.conn.query_drop(sql).await?;
        }
        Ok(())
    }

    async fn write_data(&mut self, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        // an empty target needs no merge; a re-run merges on the primary key
        let has_rows: Option<i64> = self.conn.query_first(format!("SELECT 1 FROM {} LIMIT 1", self.target(def))).await?;
        let mut total = 0;
        while let Some(batch) = rx.recv().await {
            total += self.write_rows(def, &batch, has_rows.is_some()).await?;
        }
        Ok(total)
    }

    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let migrated: HashSet<&str> = defs.iter().map(|d| d.container_name.as_str()).collect();
        let mut statements: VecDeque<(String, String)> = VecDeque::new();
        for def in defs {
            let target = self.target(def);
            for idx in def.indexes.iter().filter(|i| !i.columns.is_empty() && i.columns.iter().all(|c| def.column_index(c).is_some())) {
                let cols: Vec<&str> = idx.columns.iter().map(String::as_str).collect();
                let name = truncate_ident(format!("{}_{}", def.target_name(), idx.name));
                statements.push_back((name.clone(), format!("CREATE {}INDEX {} ON {target} ({})", if idx.unique { "UNIQUE " } else { "" }, quote_ident(&name), column_list(&cols))));
            }
        }
        for def in defs {
            let target = self.target(def);
            for fk in def.foreign_keys.iter().filter(|f| migrated.contains(f.reference_container_name.as_str())) {
                let cols: Vec<&str> = fk.columns.iter().map(String::as_str).collect();
                let refs: Vec<&str> = fk.reference_columns.iter().map(String::as_str).collect();
                let name = truncate_ident(format!("{}_{}", def.target_name(), fk.name));
                let parent = format!("{}.{}", quote_ident(self.database()), quote_ident(short_name(&fk.reference_container_name)));
                statements.push_back((
                    name.clone(),
                    format!("ALTER TABLE {target} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {parent} ({})", quote_ident(&name), column_list(&cols), column_list(&refs)),
                ));
            }
        }
        // MySQL has no IF NOT EXISTS for indexes / constraints: an existing one (re-run) is reported and skipped
        for (name, sql) in statements {
            if let Err(e) = self.conn.query_drop(sql).await {
                warn!("[Migration] {name} not created: {e}");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeReceiver for Mysql {
    async fn apply_changes(&mut self, defs: &[DataDefinition], events: &[ChangeEvent]) -> DMResult<()> {
        let changes = coalesce_changes(defs, events)?;
        // parents before children for upserts, children before parents for deletes
        for c in changes.iter().filter(|c| !c.upserts.is_empty()) {
            self.apply_container(c).await?;
        }
        for c in changes.iter().rev() {
            self.delete_container(c).await?;
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> DMResult<()> {
        Mysql::reconnect(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{data_definitions::DataPropertyInfo, value::LogicalType};

    #[test]
    fn my3_insert_statements() {
        assert_eq!(insert_statement("`d`.`t`", &["id", "name"], &["id"], 2, false), "INSERT INTO `d`.`t` (`id`, `name`) VALUES (?, ?), (?, ?)");
        assert!(insert_statement("t", &["id", "name"], &["id"], 1, true).ends_with("ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"));
        assert!(insert_statement("t", &["a", "b"], &["a", "b"], 1, true).ends_with("ON DUPLICATE KEY UPDATE `a` = `a`"));
        assert_eq!(rows_per_insert(3), 1_000);
        assert_eq!(rows_per_insert(200), 327);
        assert_eq!(truncate_ident("x".repeat(70)).len(), 64);
    }

    #[test]
    fn my3_keys_get_bounded_types() {
        let p = |n: &str, t: LogicalType, id: bool| DataPropertyInfo {
            property_name: n.into(),
            data_type: t,
            source_type: None,
            is_nullable: !id,
            is_identifier: id,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,
        };
        let def = DataDefinition::new("s.t".into(), vec![p("code", LogicalType::String, true), p("note", LogicalType::String, false)]);
        let sql = create_table_statement("`d`.`t`", &def);
        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS `d`.`t` (`code` VARCHAR(255) NOT NULL, `note` LONGTEXT NULL, PRIMARY KEY (`code`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }
}
