//! Oracle provider (ORA-2): definitions from the `ALL_*` dictionary views (one query each for
//! the whole schema, no query per table), rows streamed with array fetch inside one
//! `SET TRANSACTION READ ONLY` transaction, so every table of a connection is read at one SCN.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use log::info;
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
    oracle_connection::{qualified, Oracle},
    oracle_data_types::{is_bool_check, is_json_check, logical_type_for, select_expression, source_type, Reader},
};

const TABLES: &str = "SELECT table_name FROM all_tables WHERE owner = :1 AND nested = 'NO' AND secondary = 'N' \
     AND temporary = 'N' AND dropped = 'NO' AND (iot_type IS NULL OR iot_type = 'IOT') ORDER BY table_name";
const COLUMNS: &str = "SELECT table_name, column_name, data_type, data_length, char_length, data_precision, data_scale, nullable \
     FROM all_tab_columns WHERE owner = :1 ORDER BY table_name, column_id";
/// Primary keys, foreign keys (with the referenced columns) and single column checks.
const CONSTRAINTS: &str = "SELECT c.table_name, c.constraint_name, c.constraint_type, cc.column_name, r.owner, r.table_name, rc.column_name, c.search_condition_vc \
     FROM all_constraints c \
     JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name AND cc.table_name = c.table_name \
     LEFT JOIN all_constraints r ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name \
     LEFT JOIN all_cons_columns rc ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name AND rc.position = cc.position \
     WHERE c.owner = :1 AND c.constraint_type IN ('P', 'R', 'C') AND c.status = 'ENABLED' \
     ORDER BY c.table_name, c.constraint_name, cc.position";
/// Indexes other than the ones behind primary keys; LOB / IOT indexes left out.
const INDEXES: &str = "SELECT i.table_name, i.index_name, i.uniqueness, ic.column_name FROM all_indexes i \
     JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name \
     WHERE i.table_owner = :1 AND i.index_type IN ('NORMAL', 'NORMAL/REV', 'BITMAP', 'FUNCTION-BASED NORMAL') \
     AND NOT EXISTS (SELECT 1 FROM all_constraints p WHERE p.owner = i.table_owner AND p.index_name = i.index_name AND p.constraint_type = 'P') \
     ORDER BY i.table_name, i.index_name, ic.column_position";

type ColumnRow = (String, String, String, Option<u32>, Option<u32>, Option<u32>, Option<i32>, String);
type ConstraintRow = (String, String, String, String, Option<String>, Option<String>, Option<String>, Option<String>);
type IndexRow = (String, String, String, String);

struct Dictionary {
    tables: Vec<String>,
    columns: Vec<ColumnRow>,
    constraints: Vec<ConstraintRow>,
    indexes: Vec<IndexRow>,
}

fn read_dictionary(c: &oracle::Connection, schema: &str) -> DMResult<Dictionary> {
    let mut tables = Vec::new();
    for r in c.query_as::<String>(TABLES, &[&schema])? {
        tables.push(r?);
    }
    let mut columns = Vec::new();
    for r in c.query_as::<ColumnRow>(COLUMNS, &[&schema])? {
        columns.push(r?);
    }
    let mut constraints = Vec::new();
    for r in c.query_as::<ConstraintRow>(CONSTRAINTS, &[&schema])? {
        constraints.push(r?);
    }
    let mut indexes = Vec::new();
    for r in c.query_as::<IndexRow>(INDEXES, &[&schema])? {
        indexes.push(r?);
    }
    Ok(Dictionary { tables, columns, constraints, indexes })
}

/// Builds the definitions of `schema` from the dictionary rows.
fn definitions(schema: &str, d: Dictionary, filter: &ContainerFilter) -> Vec<DataDefinition> {
    let mut keys: HashMap<(String, String), ()> = HashMap::new();
    let mut bool01: HashMap<(String, String), ()> = HashMap::new();
    let mut json: HashMap<(String, String), ()> = HashMap::new();
    let mut fks: BTreeMap<(String, String), ForeignKeyInfo> = BTreeMap::new();
    // checks on several columns are not type hints: count columns per check first
    let mut check_columns: HashMap<(String, String), usize> = HashMap::new();
    for (table, name, kind, ..) in &d.constraints {
        if kind == "C" {
            *check_columns.entry((table.clone(), name.clone())).or_default() += 1;
        }
    }
    for (table, name, kind, column, ref_owner, ref_table, ref_column, condition) in d.constraints {
        match kind.as_str() {
            "P" => {
                keys.insert((table, column), ());
            }
            "R" => {
                let (Some(ref_owner), Some(ref_table), Some(ref_column)) = (ref_owner, ref_table, ref_column) else { continue };
                let fk = fks.entry((table, name.clone())).or_insert_with(|| ForeignKeyInfo {
                    name,
                    columns: Vec::new(),
                    reference_container_name: format!("{ref_owner}.{ref_table}"),
                    reference_columns: Vec::new(),
                });
                fk.columns.push(column);
                fk.reference_columns.push(ref_column);
            }
            _ => {
                let condition = condition.unwrap_or_default();
                if check_columns.get(&(table.clone(), name)) != Some(&1) {
                    continue;
                }
                if is_bool_check(&column, &condition) {
                    bool01.insert((table, column), ());
                } else if is_json_check(&column, &condition) {
                    json.insert((table, column), ());
                }
            }
        }
    }

    let mut props: BTreeMap<String, Vec<DataPropertyInfo>> = BTreeMap::new();
    for (table, name, data_type, length, char_length, precision, scale, nullable) in d.columns {
        let k = (table, name);
        let key = keys.contains_key(&k);
        let (table, name) = k;
        let data_type_ = logical_type_for(
            &data_type,
            length,
            precision,
            scale,
            bool01.contains_key(&(table.clone(), name.clone())),
            json.contains_key(&(table.clone(), name.clone())),
        );
        props.entry(table).or_default().push(DataPropertyInfo {
            property_name: name,
            data_type: data_type_,
            source_type: Some(source_type(&data_type, char_length, precision, scale)),
            is_nullable: nullable == "Y" && !key,
            is_identifier: key,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,
        });
    }

    let mut indexes: BTreeMap<(String, String), (bool, Vec<String>)> = BTreeMap::new();
    for (table, name, uniqueness, column) in d.indexes {
        indexes.entry((table, name)).or_insert((uniqueness == "UNIQUE", Vec::new())).1.push(column);
    }

    let mut defs = Vec::with_capacity(d.tables.len());
    for table in d.tables {
        let name = format!("{schema}.{table}");
        if !filter.accepts(&name) {
            continue;
        }
        let mut def = DataDefinition::new(name, props.remove(&table).unwrap_or_default());
        def.foreign_keys = fks.iter().filter(|((t, _), _)| *t == table).map(|(_, fk)| fk.clone()).collect();
        def.indexes = indexes
            .iter()
            // function-based index parts are hidden SYS_NC columns, not table columns
            .filter(|((t, _), (_, cols))| *t == table && cols.iter().all(|c| def.column_index(c).is_some()))
            .map(|((_, n), (unique, cols))| IndexInfo { name: n.clone(), unique: *unique, columns: cols.clone() })
            .collect();
        def.sync_property_references();
        defs.push(def);
    }
    defs
}

/// `SELECT <columns> FROM "SCHEMA"."TABLE"` for a definition.
pub(crate) fn select_query(schema: &str, def: &DataDefinition) -> String {
    let cols: Vec<String> = def.properties_info.iter().map(select_expression).collect();
    format!("SELECT {} FROM {}", cols.join(", "), qualified(schema, &def.container_name))
}

/// Streams the rows of `sql` (columns aligned with `def`) as column batches, on a blocking
/// thread; `blocking_send` keeps the reader in step with the receiver.
pub(crate) fn stream_rows(c: &oracle::Connection, sql: &str, def: &DataDefinition, sizer: &BatchSizer, tx: &mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
    let mut size = sizer.next();
    let fetch = size.clamp(100, 10_000) as u32;
    let mut stmt = c.statement(sql).fetch_array_size(fetch).prefetch_rows(fetch).build()?;
    let rows = stmt.query(&[])?;
    let readers: Vec<Reader> = rows.column_info().iter().zip(&def.properties_info).map(|(ci, p)| Reader::new(p.data_type, ci.oracle_type())).collect();
    let mut batch = ColumnBatch::for_definition(def, size);
    let mut total = 0u64;
    for row in rows {
        let row = row?;
        for (i, (col, reader)) in batch.columns.iter_mut().zip(&readers).enumerate() {
            reader.push(col, &row, i)?;
        }
        batch.end_row();
        if batch.rows >= size {
            total += batch.rows as u64;
            size = sizer.next();
            tx.blocking_send(std::mem::replace(&mut batch, ColumnBatch::for_definition(def, size))).map_err(|_| DMError::Error("receiver stopped".to_string()))?;
        }
    }
    if !batch.is_empty() {
        total += batch.rows as u64;
        tx.blocking_send(batch).map_err(|_| DMError::Error("receiver stopped".to_string()))?;
    }
    Ok(total)
}

/// [`stream_rows`] inside an open `READ ONLY` transaction. ORA-01466 (raised before the first
/// row) means the table's DDL is newer than the snapshot: Oracle maps SCNs to DDL times in
/// steps of a few seconds, so a table created just before the snapshot is read from a new
/// read-only transaction a moment later.
pub(crate) fn stream_read_only(c: &oracle::Connection, sql: &str, def: &DataDefinition, sizer: &BatchSizer, tx: &mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
    let mut attempt = 0;
    loop {
        match stream_rows(c, sql, def, sizer, tx) {
            Err(DMError::OracleError(e)) if e.oci_code() == Some(1466) && attempt < 10 => {
                attempt += 1;
                c.commit()?;
                std::thread::sleep(std::time::Duration::from_secs(1));
                c.execute("SET TRANSACTION READ ONLY", &[])?;
            }
            other => return other,
        }
    }
}

#[async_trait]
impl DataProvider for Oracle {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        info!("[Migration] Create table infos");
        let schema = self.schema.clone();
        let filter = filter.clone();
        self.blocking(move |c| {
            let exists = c.query_row_as::<i64>("SELECT count(*) FROM all_users WHERE username = :1", &[&schema])?;
            if exists == 0 {
                return Err(DMError::Error(format!("Oracle schema {schema} does not exist")));
            }
            Ok(definitions(&schema, read_dictionary(c, &schema)?, &filter))
        })
        .await
    }

    async fn read_data(&mut self, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let start = !self.read_tx;
        self.read_tx = true;
        let sql = select_query(&self.schema, def);
        let def = def.clone();
        self.blocking(move |c| {
            if start {
                // must be the first statement of a transaction
                c.commit()?;
                c.execute("SET TRANSACTION READ ONLY", &[])?;
            }
            stream_read_only(c, &sql, &def, &sizer, &tx)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::value::LogicalType;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn ora2_definitions_from_dictionary_rows() {
        let d = Dictionary {
            tables: vec![s("CUSTOMERS"), s("ORDERS")],
            columns: vec![
                (s("CUSTOMERS"), s("ID"), s("NUMBER"), Some(22), Some(0), Some(10), Some(0), s("N")),
                (s("CUSTOMERS"), s("VIP"), s("NUMBER"), Some(22), Some(0), Some(1), Some(0), s("N")),
                (s("CUSTOMERS"), s("TAGS"), s("CLOB"), Some(4000), Some(0), None, None, s("Y")),
                (s("ORDERS"), s("ID"), s("NUMBER"), Some(22), Some(0), Some(19), Some(0), s("N")),
                (s("ORDERS"), s("CUSTOMER_ID"), s("NUMBER"), Some(22), Some(0), Some(10), Some(0), s("N")),
                (s("ORDERS"), s("STATUS"), s("VARCHAR2"), Some(40), Some(10), None, None, s("N")),
            ],
            constraints: vec![
                (s("CUSTOMERS"), s("PK_C"), s("P"), s("ID"), None, None, None, None),
                (s("CUSTOMERS"), s("CK_VIP"), s("C"), s("VIP"), None, None, None, Some(s("\"VIP\" IN (0,1)"))),
                (s("CUSTOMERS"), s("CK_NN"), s("C"), s("VIP"), None, None, None, Some(s("\"VIP\" IS NOT NULL"))),
                (s("CUSTOMERS"), s("CK_TAGS"), s("C"), s("TAGS"), None, None, None, Some(s("\"TAGS\" IS JSON"))),
                (s("ORDERS"), s("PK_O"), s("P"), s("ID"), None, None, None, None),
                (s("ORDERS"), s("FK_C"), s("R"), s("CUSTOMER_ID"), Some(s("DM")), Some(s("CUSTOMERS")), Some(s("ID")), None),
            ],
            indexes: vec![(s("ORDERS"), s("ORDERS_STATUS"), s("NONUNIQUE"), s("STATUS")), (s("ORDERS"), s("FN_IDX"), s("NONUNIQUE"), s("SYS_NC00005$"))],
        };
        let defs = definitions("DM", d, &ContainerFilter::default());
        assert_eq!(defs.iter().map(|d| d.container_name.as_str()).collect::<Vec<_>>(), ["DM.CUSTOMERS", "DM.ORDERS"]);
        let c = &defs[0];
        assert_eq!(c.identifier_names(), ["ID"]);
        assert_eq!(c.properties_info.iter().map(|p| p.data_type).collect::<Vec<_>>(), [LogicalType::Int64, LogicalType::Bool, LogicalType::Json]);
        let o = &defs[1];
        assert_eq!(o.properties_info[0].data_type, LogicalType::Decimal);
        assert_eq!(o.properties_info[2].source_type.as_deref(), Some("varchar2(10)"));
        assert_eq!(o.foreign_keys.len(), 1);
        assert_eq!(o.foreign_keys[0].reference_container_name, "DM.CUSTOMERS");
        assert_eq!(o.indexes.len(), 1);
        assert_eq!(o.indexes[0].columns, ["STATUS"]);
        assert_eq!(select_query("DM", o), "SELECT \"ID\", \"CUSTOMER_ID\", \"STATUS\" FROM \"DM\".\"ORDERS\"");
    }
}
