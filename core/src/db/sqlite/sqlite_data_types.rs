//! SQLite <-> logical type mapping.

use rusqlite::types::{ToSql, ToSqlOutput, ValueRef};

use crate::{
    dbc::batch::{Cell, Column},
    models::value::{LogicalType, Value},
    DMError, DMResult,
};

/// Logical type for a declared SQLite column type (follows SQLite affinity rules, with the
/// common date/time/json/uuid/bool names recognised).
pub fn logical_type_for(declared: &str) -> LogicalType {
    let t = declared.to_ascii_uppercase();
    if t.contains("TIMESTAMPTZ") || t.contains("TIMESTAMP WITH TIME ZONE") {
        LogicalType::DateTimeTz
    } else if t.contains("DATETIME") || t.contains("TIMESTAMP") {
        LogicalType::DateTime
    } else if t.contains("DATE") {
        LogicalType::Date
    } else if t.contains("TIME") {
        LogicalType::Time
    } else if t.contains("BOOL") {
        LogicalType::Bool
    } else if t.contains("SMALLINT") || t.contains("INT2") {
        LogicalType::Int16
    } else if t.contains("INT") {
        LogicalType::Int64
    } else if t.contains("DECIMAL") || t.contains("NUMERIC") {
        LogicalType::Decimal
    } else if t.contains("UUID") {
        LogicalType::Uuid
    } else if t.contains("JSON") {
        LogicalType::Json
    } else if t.contains("OBJECTID") {
        LogicalType::ObjectId
    } else if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        LogicalType::String
    } else if t.contains("BLOB") {
        LogicalType::Bytes
    } else if t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") {
        LogicalType::Float64
    } else {
        // no declared type: values keep their dynamic type, read as text
        LogicalType::String
    }
}

/// Declared type used when creating a table. Every name maps back to the same logical type in
/// [`logical_type_for`]; decimals use TEXT affinity so no precision is lost.
pub fn sqlite_type_for(t: LogicalType) -> &'static str {
    match t {
        LogicalType::Int16 => "SMALLINT",
        LogicalType::Int32 | LogicalType::Int64 => "INTEGER",
        LogicalType::Float32 | LogicalType::Float64 => "REAL",
        LogicalType::Decimal => "DECIMAL TEXT",
        LogicalType::String => "TEXT",
        LogicalType::Bool => "BOOLEAN",
        LogicalType::Date => "DATE",
        LogicalType::Time => "TIME",
        LogicalType::DateTime => "DATETIME",
        LogicalType::DateTimeTz => "TIMESTAMPTZ",
        LogicalType::Uuid => "UUID TEXT",
        LogicalType::Json => "JSON TEXT",
        LogicalType::Bytes => "BLOB",
        LogicalType::ObjectId => "OBJECTID TEXT",
    }
}

/// Appends a stored SQLite value to a column (converted to the column's logical type).
pub fn push_value(col: &mut Column, v: ValueRef<'_>) -> DMResult<()> {
    let cell = match v {
        ValueRef::Null => Cell::Null,
        ValueRef::Integer(i) => Cell::I64(i),
        ValueRef::Real(f) => Cell::F64(f),
        ValueRef::Text(s) => Cell::Str(std::str::from_utf8(s).map_err(|e| DMError::Error(e.to_string()))?),
        ValueRef::Blob(b) => Cell::Bytes(b),
    };
    col.push_cell(cell)
}

/// Converts a stored SQLite value to the column's logical type.
pub fn read_value(v: ValueRef<'_>, t: LogicalType) -> DMResult<Value> {
    let mut c = Column::new(t, 1);
    push_value(&mut c, v)?;
    Ok(c.cell(0).to_value())
}

/// Parameter wrapper used for inserts.
pub enum SqlValue<'a> {
    Cell(Cell<'a>),
    /// UTF-8 bytes of a text column, bound as TEXT without a re-check (PERF-9).
    Text(&'a [u8]),
}

impl ToSql for SqlValue<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        use rusqlite::types::Value as S;
        let cell = match self {
            SqlValue::Text(b) => return Ok(ToSqlOutput::Borrowed(ValueRef::Text(b))),
            SqlValue::Cell(c) => *c,
        };
        Ok(match cell {
            Cell::Null => ToSqlOutput::Owned(S::Null),
            Cell::Bool(b) => ToSqlOutput::Owned(S::Integer(b as i64)),
            Cell::I16(i) => ToSqlOutput::Owned(S::Integer(i as i64)),
            Cell::I32(i) => ToSqlOutput::Owned(S::Integer(i as i64)),
            Cell::I64(i) => ToSqlOutput::Owned(S::Integer(i)),
            Cell::F32(f) => ToSqlOutput::Owned(S::Real(f as f64)),
            Cell::F64(f) => ToSqlOutput::Owned(S::Real(f)),
            Cell::Decimal(s) | Cell::Str(s) | Cell::Json(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
            Cell::Bytes(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b)),
            Cell::DateTimeTz(t) => ToSqlOutput::Owned(S::Text(t.format("%Y-%m-%dT%H:%M:%S%.f+00:00").to_string())),
            other => ToSqlOutput::Owned(S::Text(other.to_text().into_owned())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_types_round_trip() {
        use LogicalType::*;
        for t in [Int16, Int64, Float64, Decimal, String, Bool, Date, Time, DateTime, DateTimeTz, Uuid, Json, Bytes, ObjectId] {
            assert_eq!(logical_type_for(sqlite_type_for(t)), t, "{t:?}");
        }
        assert_eq!(logical_type_for("varchar(255)"), String);
        assert_eq!(logical_type_for(""), String);
    }
}
