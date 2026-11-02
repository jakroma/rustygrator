//! SQLite provider (DB-7).

use std::sync::Arc;

use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::{
    browse::sqlite_stream,
    constants::sqlite::INTERNAL_PREFIX,
    db::sqlite::sqlite_data_types::logical_type_for,
    dbc::batch::ColumnBatch,
    migrator::batching::BatchSizer,
    migrator::migrator_steps::DataProvider,
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo, ForeignKeyInfo, IndexInfo},
        migration_execution_settings::ContainerFilter,
    },
    DMResult,
};

use super::sqlite::{quote_ident, Sqlite};

pub(crate) fn read_definitions(conn: &rusqlite::Connection, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
    )?;
    let tables: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;

    let mut defs = Vec::new();
    for table in tables.into_iter().filter(|t| !t.starts_with(INTERNAL_PREFIX) && filter.accepts(t)) {
        let q = quote_ident(&table);
        let mut props = Vec::new();
        let mut pk_order: Vec<(i64, String)> = Vec::new();
        {
            let mut s = conn.prepare(&format!("PRAGMA table_info({})", q))?;
            let mut rows = s.query([])?;
            while let Some(r) = rows.next()? {
                let name: String = r.get(1)?;
                let declared: String = r.get::<_, Option<String>>(2)?.unwrap_or_default();
                let not_null: bool = r.get::<_, i64>(3)? != 0;
                let pk: i64 = r.get(5)?;
                if pk > 0 {
                    pk_order.push((pk, name.clone()));
                }
                props.push(DataPropertyInfo {
                    property_name: name,
                    data_type: logical_type_for(&declared),
                    source_type: Some(declared),
                    is_nullable: !not_null && pk == 0,
                    is_identifier: pk > 0,
                    reference_container_name: None,
                    reference_property_name: None,
                    shape: None,

                });
            }
        }
        let mut def = DataDefinition::new(table.clone(), props);

        // foreign keys, grouped by constraint id
        let mut fks: BTreeMap<i64, ForeignKeyInfo> = BTreeMap::new();
        {
            let mut s = conn.prepare(&format!("PRAGMA foreign_key_list({})", q))?;
            let mut rows = s.query([])?;
            while let Some(r) = rows.next()? {
                let id: i64 = r.get(0)?;
                let ref_table: String = r.get(2)?;
                let from: String = r.get(3)?;
                let to: Option<String> = r.get(4)?;
                let fk = fks.entry(id).or_insert_with(|| ForeignKeyInfo {
                    name: format!("fk_{}_{}", table, id),
                    columns: Vec::new(),
                    reference_container_name: ref_table,
                    reference_columns: Vec::new(),
                });
                fk.columns.push(from);
                fk.reference_columns.push(to.unwrap_or_default());
            }
        }
        for fk in fks.values_mut() {
            if fk.reference_columns.iter().any(|c| c.is_empty()) {
                // REFERENCES parent without column list -> parent's primary key
                let mut s = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(&fk.reference_container_name)))?;
                let mut pk: Vec<(i64, String)> = s
                    .query_map([], |r| Ok((r.get::<_, i64>(5)?, r.get::<_, String>(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|(p, _)| *p > 0)
                    .collect();
                pk.sort();
                fk.reference_columns = pk.into_iter().map(|(_, n)| n).collect();
            }
        }
        def.foreign_keys = fks.into_values().collect();

        {
            let mut s = conn.prepare(&format!("PRAGMA index_list({})", q))?;
            let list: Vec<(String, bool, String, bool)> = s
                .query_map([], |r| Ok((r.get(1)?, r.get::<_, i64>(2)? != 0, r.get(3)?, r.get::<_, i64>(4)? != 0)))?
                .collect::<Result<_, _>>()?;
            for (name, unique, origin, partial) in list {
                if origin == "pk" || partial {
                    continue;
                }
                let mut s = conn.prepare(&format!("PRAGMA index_info({})", quote_ident(&name)))?;
                let cols: Vec<Option<String>> = s.query_map([], |r| r.get(2))?.collect::<Result<_, _>>()?;
                if cols.iter().any(|c| c.is_none()) {
                    continue; // expression index
                }
                def.indexes.push(IndexInfo { name, unique, columns: cols.into_iter().flatten().collect() });
            }
        }
        def.sync_property_references();
        defs.push(def);
    }
    Ok(defs)
}

#[async_trait]
impl DataProvider for Sqlite {
    async fn get_definitions(&mut self, filter: &ContainerFilter) -> DMResult<Vec<DataDefinition>> {
        let filter = filter.clone();
        self.run(move |conn| read_definitions(conn, &filter)).await
    }

    async fn read_data(&mut self, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let def = def.clone();
        self.run(move |conn| {
            let cols: Vec<String> = def.properties_info.iter().map(|p| quote_ident(&p.property_name)).collect();
            // a read transaction gives a consistent snapshot of the table
            let t = conn.transaction()?;
            let total = sqlite_stream(&t, &format!("SELECT {} FROM {}", cols.join(", "), quote_ident(&def.container_name)), &def, &sizer, &tx)?;
            t.commit()?;
            Ok(total)
        })
        .await
    }
}
