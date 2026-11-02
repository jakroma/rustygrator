//! Oracle receiver (ORA-3): tables created from the definitions, rows written with array DML
//! (one execution and one transaction per batch); a target that already has rows is merged with
//! `MERGE INTO .. USING (SELECT :1 .. FROM dual)`, so re-runs and live changes are idempotent.
//! Indexes and foreign keys are created after the data is loaded.

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::Timelike;
use log::{info, warn};
use oracle::sql_type::{OracleType, ToSql};
use tokio::sync::mpsc;

use crate::{
    dbc::batch::{Cell, ColumnBatch},
    migrator::migrator_steps::{coalesce_changes, ChangeReceiver, ContainerChanges, DataReceiver},
    models::{
        change::ChangeEvent,
        data_definitions::{short_name, DataDefinition, DataPropertyInfo},
        value::{to_hex, LogicalType, Value},
    },
    DMResult,
};

use super::{
    oracle_connection::{quote_ident, target_ident, Oracle},
    oracle_data_types::oracle_type_for,
};

/// Longest text / bytes bound as VARCHAR2 / RAW in SQL; longer values go through LOBs.
const MAX_VARCHAR: usize = 4000;
const MAX_RAW: usize = 2000;

/// Target names of the columns of a definition.
fn column_names(def: &DataDefinition, upper: bool) -> Vec<String> {
    def.properties_info.iter().map(|p| target_ident(&p.property_name, upper)).collect()
}

fn column_list(names: &[&str]) -> String {
    names.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(", ")
}

/// `INSERT INTO t (cols) VALUES (:1, ..)`.
pub(crate) fn insert_statement(target: &str, columns: &[&str]) -> String {
    let binds: Vec<String> = (1..=columns.len()).map(|i| format!(":{i}")).collect();
    format!("INSERT INTO {target} ({}) VALUES ({})", column_list(columns), binds.join(", "))
}

/// `MERGE INTO t USING (SELECT :1 "A", .. FROM dual) s ON (t."ID" = s."ID") WHEN MATCHED THEN
/// UPDATE SET .. WHEN NOT MATCHED THEN INSERT ..`; without non-key columns only the insert part.
pub(crate) fn merge_statement(target: &str, columns: &[&str], ids: &[&str]) -> String {
    let source: Vec<String> = columns.iter().enumerate().map(|(i, c)| format!(":{} {}", i + 1, quote_ident(c))).collect();
    let on: Vec<String> = ids.iter().map(|c| format!("t.{0} = s.{0}", quote_ident(c))).collect();
    let updates: Vec<String> = columns.iter().filter(|c| !ids.contains(c)).map(|c| format!("t.{0} = s.{0}", quote_ident(c))).collect();
    let values: Vec<String> = columns.iter().map(|c| format!("s.{}", quote_ident(c))).collect();
    let mut sql = format!("MERGE INTO {target} t USING (SELECT {} FROM dual) s ON ({})", source.join(", "), on.join(" AND "));
    if !updates.is_empty() {
        sql.push_str(&format!(" WHEN MATCHED THEN UPDATE SET {}", updates.join(", ")));
    }
    sql.push_str(&format!(" WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})", column_list(columns), values.join(", ")));
    sql
}

/// Columns that need an indexable type: keys, indexed columns and foreign key columns.
fn keyed_columns(def: &DataDefinition) -> HashSet<&str> {
    let mut keyed: HashSet<&str> = def.identifier_names().into_iter().collect();
    keyed.extend(def.indexes.iter().flat_map(|i| i.columns.iter().map(String::as_str)));
    keyed.extend(def.foreign_keys.iter().flat_map(|f| f.columns.iter().map(String::as_str)));
    keyed
}

/// ORA-5: Oracle stores `''` as NULL, so text from other databases may arrive as NULL in a
/// column that is NOT NULL in the source. Only key columns keep NOT NULL there (an empty key
/// fails loudly instead of being changed).
fn keeps_not_null(p: &DataPropertyInfo) -> bool {
    let oracle_text = p.source_type.as_deref().is_some_and(|s| ["varchar2", "nvarchar2", "char", "nchar", "clob", "nclob"].iter().any(|t| s.starts_with(t)));
    p.is_identifier || !matches!(p.data_type, LogicalType::String | LogicalType::Json) || oracle_text
}

pub(crate) fn create_table_statement(target: &str, def: &DataDefinition, upper: bool, major: u32) -> String {
    let keyed = keyed_columns(def);
    let names = column_names(def, upper);
    let mut columns: Vec<String> = def
        .properties_info
        .iter()
        .zip(&names)
        .map(|(p, name)| {
            let not_null = (!p.is_nullable || p.is_identifier) && keeps_not_null(p);
            format!("{} {}{}", quote_ident(name), oracle_type_for(p, keyed.contains(p.property_name.as_str()), major, name), if not_null { " NOT NULL" } else { "" })
        })
        .collect();
    let ids: Vec<&str> = def.identifier_indexes().into_iter().map(|i| names[i].as_str()).collect();
    if !ids.is_empty() {
        columns.push(format!("PRIMARY KEY ({})", column_list(&ids)));
    }
    format!("CREATE TABLE {target} ({})", columns.join(", "))
}

/// Bind type of a column for one batch: text and bytes are sized to the longest value of the
/// batch (small bind buffers), LOBs only when a value does not fit VARCHAR2 / RAW.
fn bind_type(rows: &ColumnBatch, ci: usize) -> OracleType {
    let col = &rows.columns[ci];
    let max = |f: &dyn Fn(Cell<'_>) -> usize| (0..rows.rows).map(|r| f(col.cell(r))).max().unwrap_or(0).max(1);
    match col.kind {
        LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 | LogicalType::Bool => OracleType::Int64,
        LogicalType::Float32 => OracleType::BinaryFloat,
        LogicalType::Float64 => OracleType::BinaryDouble,
        LogicalType::Date => OracleType::Date,
        LogicalType::DateTime => OracleType::Timestamp(9),
        LogicalType::DateTimeTz => OracleType::TimestampTZ(9),
        LogicalType::Time => OracleType::Varchar2(18),
        LogicalType::ObjectId => OracleType::Varchar2(24),
        LogicalType::Uuid => OracleType::Raw(16),
        LogicalType::Bytes => match max(&|c| if let Cell::Bytes(b) = c { b.len() } else { 0 }) {
            n if n <= MAX_RAW => OracleType::Raw(n as u32),
            _ => OracleType::BLOB,
        },
        LogicalType::Decimal | LogicalType::String | LogicalType::Json => match max(&|c| match c {
            Cell::Null => 0,
            Cell::Decimal(s) | Cell::Str(s) | Cell::Json(s) => s.len(),
            other => other.to_text().len(),
        }) {
            n if n <= MAX_VARCHAR => OracleType::Varchar2(n as u32),
            _ => OracleType::CLOB,
        },
    }
}

/// A cell as a bind value.
fn param(c: Cell<'_>) -> Box<dyn ToSql> {
    match c {
        Cell::Null => Box::new(None::<i64>),
        Cell::Bool(b) => Box::new(b as i64),
        Cell::I16(i) => Box::new(i as i64),
        Cell::I32(i) => Box::new(i as i64),
        Cell::I64(i) => Box::new(i),
        Cell::F32(f) => Box::new(f),
        Cell::F64(f) => Box::new(f),
        Cell::Decimal(s) | Cell::Str(s) | Cell::Json(s) => Box::new(s.to_string()),
        Cell::JsonValue(j) => Box::new(j.to_string()),
        Cell::Bytes(b) => Box::new(b.to_vec()),
        Cell::Date(d) => Box::new(d),
        Cell::Time(t) => Box::new(format!("{}.{:06}", t.format("%H:%M:%S"), t.nanosecond() / 1000)),
        Cell::DateTime(t) => Box::new(t),
        Cell::DateTimeTz(t) => Box::new(t),
        Cell::Uuid(u) => Box::new(u.as_bytes().to_vec()),
        Cell::ObjectId(o) => Box::new(to_hex(&o)),
    }
}

/// Binds one cell at `pos` (1-based) without allocating for the common types.
fn bind(batch: &mut oracle::Batch<'_>, pos: usize, c: Cell<'_>) -> oracle::Result<()> {
    match c {
        Cell::Null => batch.set(pos, &None::<i64>),
        Cell::Bool(b) => batch.set(pos, &(b as i64)),
        Cell::I16(i) => batch.set(pos, &(i as i64)),
        Cell::I32(i) => batch.set(pos, &(i as i64)),
        Cell::I64(i) => batch.set(pos, &i),
        Cell::F32(f) => batch.set(pos, &f),
        Cell::F64(f) => batch.set(pos, &f),
        Cell::Decimal(s) | Cell::Str(s) | Cell::Json(s) => batch.set(pos, &s),
        Cell::Bytes(b) => batch.set(pos, &b),
        Cell::Date(d) => batch.set(pos, &d),
        Cell::DateTime(t) => batch.set(pos, &t),
        Cell::DateTimeTz(t) => batch.set(pos, &t),
        Cell::Uuid(u) => batch.set(pos, &&u.as_bytes()[..]),
        other => batch.set(pos, param(other).as_ref()),
    }
}

/// Writes all rows of `rows` with one array DML execution and commits.
fn write_batch(c: &oracle::Connection, sql: &str, rows: &ColumnBatch) -> DMResult<()> {
    let mut batch = c.batch(sql, rows.rows.max(1)).build()?;
    for ci in 0..rows.columns.len() {
        batch.set_type(ci + 1, &bind_type(rows, ci))?;
    }
    for r in 0..rows.rows {
        for (ci, col) in rows.columns.iter().enumerate() {
            bind(&mut batch, ci + 1, col.cell(r))?;
        }
        batch.append_row(&[])?;
    }
    batch.execute()?;
    c.commit()?;
    Ok(())
}

impl Oracle {
    fn target(&self, def: &DataDefinition) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&target_ident(def.target_name(), self.upper)))
    }

    /// ORA-5: reports once per column that empty strings become NULL.
    fn warn_empty_strings(&mut self, def: &DataDefinition, rows: &ColumnBatch) {
        for (p, col) in def.properties_info.iter().zip(&rows.columns) {
            if !matches!(col.kind, LogicalType::String | LogicalType::Json) || self.empty_warned.contains(&(def.container_name.clone(), p.property_name.clone())) {
                continue;
            }
            if (0..rows.rows).any(|r| matches!(col.cell(r), Cell::Str("") | Cell::Json(""))) {
                warn!("[Migration][{}] empty strings in {} are stored as NULL (Oracle has no empty string)", def.container_name, p.property_name);
                self.empty_warned.insert((def.container_name.clone(), p.property_name.clone()));
            }
        }
    }

    async fn write_rows(&mut self, def: &DataDefinition, rows: ColumnBatch, upsert: bool) -> DMResult<u64> {
        self.warn_empty_strings(def, &rows);
        let names = column_names(def, self.upper);
        let cols: Vec<&str> = names.iter().map(String::as_str).collect();
        let ids: Vec<&str> = def.identifier_indexes().into_iter().map(|i| cols[i]).collect();
        let sql = if upsert && !ids.is_empty() { merge_statement(&self.target(def), &cols, &ids) } else { insert_statement(&self.target(def), &cols) };
        let n = rows.rows as u64;
        self.blocking(move |c| write_batch(c, &sql, &rows)).await?;
        Ok(n)
    }

    async fn apply_container(&mut self, c: &ContainerChanges<'_>) -> DMResult<()> {
        let def = c.def;
        let (full, partial): (Vec<_>, Vec<_>) = c.upserts.iter().partition(|r| !ContainerChanges::is_partial(r));
        if !full.is_empty() {
            let mut rows = ColumnBatch::for_definition(def, full.len());
            for r in &full {
                rows.push_row(r.iter().map(|v| v.clone().unwrap_or(Value::Null)).collect())?;
            }
            self.write_rows(def, rows, true).await?;
        }
        // only some columns known (updates without full supplemental logging): merge just those
        let names = column_names(def, self.upper);
        let id_idx = def.identifier_indexes();
        let target = self.target(def);
        let mut statements: Vec<(String, Vec<Value>)> = Vec::new();
        for row in partial {
            if id_idx.iter().any(|&i| row[i].is_none()) {
                continue;
            }
            let (cols, values): (Vec<&str>, Vec<Value>) = row.iter().enumerate().filter_map(|(i, v)| v.as_ref().map(|v| (names[i].as_str(), v.clone()))).unzip();
            let ids: Vec<&str> = id_idx.iter().map(|&i| names[i].as_str()).collect();
            statements.push((merge_statement(&target, &cols, &ids), values));
        }
        if !statements.is_empty() {
            self.blocking(move |c| {
                for (sql, values) in statements {
                    let params: Vec<Box<dyn ToSql>> = values.iter().map(|v| param(Cell::from_value(v))).collect();
                    let refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
                    c.execute(&sql, &refs)?;
                }
                Ok(c.commit()?)
            })
            .await?;
        }
        Ok(())
    }

    async fn delete_container(&mut self, c: &ContainerChanges<'_>) -> DMResult<()> {
        if c.deletes.is_empty() {
            return Ok(());
        }
        let names = column_names(c.def, self.upper);
        let id_idx = c.def.identifier_indexes();
        let cond: Vec<String> = id_idx.iter().enumerate().map(|(n, &i)| format!("{} = :{}", quote_ident(&names[i]), n + 1)).collect();
        let sql = format!("DELETE FROM {} WHERE {}", self.target(c.def), cond.join(" AND "));
        let kinds: Vec<LogicalType> = id_idx.iter().map(|&i| c.def.properties_info[i].data_type).collect();
        let mut keys = ColumnBatch::new(&kinds, c.deletes.len());
        for k in &c.deletes {
            keys.push_row(k.clone())?;
        }
        self.blocking(move |c| write_batch(c, &sql, &keys)).await
    }
}

#[async_trait]
impl DataReceiver for Oracle {
    async fn prepare(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        self.end_read_tx().await?;
        let schema = self.schema.clone();
        let tables: Vec<(String, String, String)> = defs
            .iter()
            .map(|d| (d.container_name.clone(), target_ident(d.target_name(), self.upper), create_table_statement(&self.target(d), d, self.upper, self.major)))
            .collect();
        self.blocking(move |c| {
            for (container, table, sql) in tables {
                // CREATE TABLE IF NOT EXISTS needs 23ai
                let exists = c.query_row_as::<i64>("SELECT count(*) FROM all_tables WHERE owner = :1 AND table_name = :2", &[&schema, &table])?;
                if exists == 0 {
                    info!("[Migration][{container}] Create table {schema}.{table}");
                    c.execute(&sql, &[])?;
                }
            }
            Ok(())
        })
        .await
    }

    async fn write_data(&mut self, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>) -> DMResult<u64> {
        // an empty target needs no merge; a re-run merges on the primary key
        let probe = format!("SELECT count(*) FROM {} WHERE ROWNUM = 1", self.target(def));
        let has_rows = self.blocking(move |c| Ok(c.query_row_as::<i64>(&probe, &[])? > 0)).await?;
        let mut total = 0;
        while let Some(batch) = rx.recv().await {
            total += self.write_rows(def, batch, has_rows).await?;
        }
        Ok(total)
    }

    async fn finalize(&mut self, defs: &[DataDefinition]) -> DMResult<()> {
        let migrated: HashSet<&str> = defs.iter().map(|d| d.container_name.as_str()).collect();
        let mut statements: Vec<(String, String)> = Vec::new();
        for def in defs {
            let target = self.target(def);
            let names = column_names(def, self.upper);
            let col = |n: &String| def.column_index(n).map(|i| names[i].as_str());
            let table = target_ident(def.target_name(), self.upper);
            for idx in def.indexes.iter().filter(|i| !i.columns.is_empty()) {
                let Some(cols) = idx.columns.iter().map(col).collect::<Option<Vec<&str>>>() else { continue };
                let name = target_ident(&format!("{table}_{}", idx.name), self.upper);
                statements.push((name.clone(), format!("CREATE {}INDEX {} ON {target} ({})", if idx.unique { "UNIQUE " } else { "" }, quote_ident(&name), column_list(&cols))));
            }
        }
        for def in defs {
            let target = self.target(def);
            let names = column_names(def, self.upper);
            let table = target_ident(def.target_name(), self.upper);
            for fk in def.foreign_keys.iter().filter(|f| migrated.contains(f.reference_container_name.as_str())) {
                let Some(cols) = fk.columns.iter().map(|n| def.column_index(n).map(|i| names[i].as_str())).collect::<Option<Vec<&str>>>() else { continue };
                let refs: Vec<String> = fk.reference_columns.iter().map(|c| target_ident(c, self.upper)).collect();
                let refs: Vec<&str> = refs.iter().map(String::as_str).collect();
                let name = target_ident(&format!("{table}_{}", fk.name), self.upper);
                let parent = format!("{}.{}", quote_ident(&self.schema), quote_ident(&target_ident(short_name(&fk.reference_container_name), self.upper)));
                statements.push((
                    name.clone(),
                    format!("ALTER TABLE {target} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {parent} ({})", quote_ident(&name), column_list(&cols), column_list(&refs)),
                ));
            }
        }
        // an index / constraint that exists already (re-run) is reported and skipped
        self.blocking(move |c| {
            for (name, sql) in statements {
                if let Err(e) = c.execute(&sql, &[]) {
                    warn!("[Migration] {name} not created: {e}");
                }
            }
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl ChangeReceiver for Oracle {
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
        Oracle::reconnect(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::data_definitions::IndexInfo;

    fn p(n: &str, t: LogicalType, id: bool, nullable: bool, source: Option<&str>) -> DataPropertyInfo {
        DataPropertyInfo {
            property_name: n.into(),
            data_type: t,
            source_type: source.map(str::to_string),
            is_nullable: nullable,
            is_identifier: id,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,
        }
    }

    #[test]
    fn ora3_statements() {
        assert_eq!(insert_statement("\"DM\".\"t\"", &["id", "name"]), "INSERT INTO \"DM\".\"t\" (\"id\", \"name\") VALUES (:1, :2)");
        assert_eq!(
            merge_statement("T", &["id", "name"], &["id"]),
            "MERGE INTO T t USING (SELECT :1 \"id\", :2 \"name\" FROM dual) s ON (t.\"id\" = s.\"id\") \
             WHEN MATCHED THEN UPDATE SET t.\"name\" = s.\"name\" WHEN NOT MATCHED THEN INSERT (\"id\", \"name\") VALUES (s.\"id\", s.\"name\")"
        );
        // all columns are the key: nothing to update
        assert!(!merge_statement("T", &["a", "b"], &["a", "b"]).contains("WHEN MATCHED"));
    }

    #[test]
    fn ora3_ora5_create_table() {
        let mut def = DataDefinition::new(
            "shop.customers".into(),
            vec![
                p("id", LogicalType::Int32, true, false, None),
                p("email", LogicalType::String, false, false, Some("varchar(255)")),
                p("name", LogicalType::String, false, false, Some("text")),
                p("code", LogicalType::String, false, false, Some("varchar2(10)")),
                p("vip", LogicalType::Bool, false, false, None),
            ],
        );
        def.indexes.push(IndexInfo { name: "email_key".into(), unique: true, columns: vec!["email".into()] });
        assert_eq!(
            create_table_statement("\"DM\".\"customers\"", &def, false, 19),
            "CREATE TABLE \"DM\".\"customers\" (\"id\" NUMBER(10) NOT NULL, \"email\" VARCHAR2(255 CHAR), \"name\" CLOB, \
             \"code\" VARCHAR2(10 CHAR) NOT NULL, \"vip\" NUMBER(1) CHECK (\"vip\" IN (0, 1)) NOT NULL, PRIMARY KEY (\"id\"))"
        );
        assert!(create_table_statement("T", &def, true, 23).contains("\"VIP\" BOOLEAN NOT NULL, PRIMARY KEY (\"ID\")"));
    }

    #[test]
    fn ora3_bind_types_follow_the_batch() {
        let mut b = ColumnBatch::new(&[LogicalType::String, LogicalType::Bytes, LogicalType::Decimal], 2);
        b.push_row(vec![Value::String("abc".into()), Value::Bytes(vec![1; 3000]), Value::Decimal("12.50".into())]).unwrap();
        b.push_row(vec![Value::String("x".repeat(5000)), Value::Null, Value::Null]).unwrap();
        assert!(matches!(bind_type(&b, 0), OracleType::CLOB));
        assert!(matches!(bind_type(&b, 1), OracleType::BLOB));
        assert!(matches!(bind_type(&b, 2), OracleType::Varchar2(5)));
        let mut b = ColumnBatch::new(&[LogicalType::String], 1);
        b.push_row(vec![Value::Null]).unwrap();
        assert!(matches!(bind_type(&b, 0), OracleType::Varchar2(1)));
    }
}
