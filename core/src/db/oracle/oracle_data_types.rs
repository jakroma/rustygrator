//! Oracle <-> logical type mapping (ORA-4).

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use oracle::{sql_type::OracleType, Row};

use crate::{
    dbc::batch::{Cell, Column},
    models::{
        data_definitions::DataPropertyInfo,
        value::{LogicalType, Value},
    },
    DMResult,
};

/// Logical type of a table column from `ALL_TAB_COLUMNS`. `bool01`: `NUMBER(1)` with a
/// `CHECK (col IN (0, 1))`; `is_json`: text with a `CHECK (col IS JSON)`.
pub fn logical_type_for(data_type: &str, length: Option<u32>, precision: Option<u32>, scale: Option<i32>, bool01: bool, is_json: bool) -> LogicalType {
    match data_type {
        "NUMBER" => match (precision, scale) {
            (Some(1), Some(0)) if bool01 => LogicalType::Bool,
            (Some(p), Some(0)) if p <= 4 => LogicalType::Int16,
            (Some(p), Some(0)) if p <= 9 => LogicalType::Int32,
            (Some(p), Some(0)) if p <= 18 => LogicalType::Int64,
            _ => LogicalType::Decimal,
        },
        "FLOAT" | "BINARY_DOUBLE" => LogicalType::Float64,
        "BINARY_FLOAT" => LogicalType::Float32,
        "DATE" => LogicalType::DateTime,
        t if t.starts_with("TIMESTAMP") && t.ends_with("TIME ZONE") => LogicalType::DateTimeTz,
        t if t.starts_with("TIMESTAMP") => LogicalType::DateTime,
        "RAW" if length == Some(16) => LogicalType::Uuid,
        "RAW" | "LONG RAW" | "BLOB" => LogicalType::Bytes,
        "JSON" => LogicalType::Json,
        "BOOLEAN" => LogicalType::Bool,
        _ if is_json => LogicalType::Json,
        // VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG, ROWID, UROWID, INTERVAL ..., XMLTYPE
        _ => LogicalType::String,
    }
}

/// `source_type` of a column: the lower case Oracle type with its size, e.g. `number(10,2)`,
/// `varchar2(50)`.
pub fn source_type(data_type: &str, char_length: Option<u32>, precision: Option<u32>, scale: Option<i32>) -> String {
    let t = data_type.to_ascii_lowercase();
    match (t.as_str(), precision, scale, char_length) {
        ("number", Some(p), Some(s), _) => format!("number({p},{s})"),
        ("number", None, Some(0), _) => "number(38,0)".to_string(),
        ("varchar2" | "nvarchar2" | "char" | "nchar", _, _, Some(n)) if n > 0 => format!("{t}({n})"),
        _ => t,
    }
}

/// `CHECK` condition that marks a `NUMBER(1)` column as boolean: `"FLAG" IN (0,1)`.
pub fn is_bool_check(column: &str, condition: &str) -> bool {
    let norm: String = condition.chars().filter(|c| !c.is_whitespace() && *c != '"').collect::<String>().to_uppercase();
    let col = column.to_uppercase();
    [format!("{col}IN(0,1)"), format!("{col}IN(1,0)"), format!("{col}BETWEEN0AND1")].contains(&norm)
}

/// `CHECK` condition `"DOC" IS JSON` (optionally with options).
pub fn is_json_check(column: &str, condition: &str) -> bool {
    let norm: String = condition.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
    let col = column.to_uppercase();
    norm.starts_with(&format!("\"{col}\" IS JSON")) || norm.starts_with(&format!("{col} IS JSON"))
}

/// Logical type of a query result column (VIEW-6).
pub fn logical_type_for_result(t: &OracleType) -> LogicalType {
    match t {
        // NUMBER without precision is reported as (0, -127)
        OracleType::Number(p, 0) if (1..=4).contains(p) => LogicalType::Int16,
        OracleType::Number(p, 0) if (5..=9).contains(p) => LogicalType::Int32,
        OracleType::Number(p, 0) if (10..=18).contains(p) => LogicalType::Int64,
        OracleType::Number(..) | OracleType::UInt64 => LogicalType::Decimal,
        OracleType::Int64 => LogicalType::Int64,
        OracleType::Float(_) | OracleType::BinaryDouble => LogicalType::Float64,
        OracleType::BinaryFloat => LogicalType::Float32,
        OracleType::Date | OracleType::Timestamp(_) => LogicalType::DateTime,
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => LogicalType::DateTimeTz,
        OracleType::Raw(16) => LogicalType::Uuid,
        OracleType::Raw(_) | OracleType::BLOB | OracleType::LongRaw => LogicalType::Bytes,
        OracleType::Boolean => LogicalType::Bool,
        OracleType::Json => LogicalType::Json,
        _ => LogicalType::String,
    }
}

/// `(p,s)` of a decimal source type (`decimal(10,2)`, `numeric(10,2)`, `number(10,2)`) when it
/// fits Oracle's NUMBER (precision 1..38).
fn decimal_size(source: Option<&str>) -> Option<String> {
    let s = source?;
    let inner = ["decimal(", "numeric(", "number("].iter().find_map(|p| s.strip_prefix(p))?.split(')').next()?;
    let p: u32 = inner.split(',').next()?.trim().parse().ok()?;
    (1..=38).contains(&p).then(|| inner.replace(' ', ""))
}

/// Length of a bounded text source type (`varchar2(50)`, `varchar(50)`, `character varying(50)`).
fn text_length(source: Option<&str>) -> Option<u32> {
    let s = source?;
    let inner = ["varchar2(", "nvarchar2(", "varchar(", "character varying(", "char(", "nchar(", "character("]
        .iter()
        .find_map(|p| s.strip_prefix(p))?
        .split(')')
        .next()?;
    inner.trim().parse().ok().filter(|n| (1..=4000).contains(n))
}

/// Target column type (without NOT NULL). Key, index and foreign key columns need a type that
/// can be indexed, so text / bytes keys become `VARCHAR2` / `RAW`; `check` gets the inline
/// constraint text for booleans before 23ai and JSON text.
pub fn oracle_type_for(p: &DataPropertyInfo, keyed: bool, major: u32, column: &str) -> String {
    let source = p.source_type.as_deref();
    let col = super::oracle_connection::quote_ident(column);
    match p.data_type {
        LogicalType::Int16 => "NUMBER(5)".into(),
        LogicalType::Int32 => "NUMBER(10)".into(),
        LogicalType::Int64 => "NUMBER(19)".into(),
        LogicalType::Float32 => "BINARY_FLOAT".into(),
        LogicalType::Float64 => "BINARY_DOUBLE".into(),
        LogicalType::Decimal => decimal_size(source).map(|ps| format!("NUMBER({ps})")).unwrap_or_else(|| "NUMBER".into()),
        LogicalType::String => match text_length(source) {
            Some(n) => format!("VARCHAR2({n} CHAR)"),
            None if keyed => "VARCHAR2(1000 CHAR)".into(),
            None => "CLOB".into(),
        },
        LogicalType::Bool if major >= 23 => "BOOLEAN".into(),
        LogicalType::Bool => format!("NUMBER(1) CHECK ({col} IN (0, 1))"),
        LogicalType::Date => "DATE".into(),
        // no TIME type in Oracle: `HH:MI:SS.ffffff` text
        LogicalType::Time => "VARCHAR2(18)".into(),
        LogicalType::DateTime if source == Some("date") => "DATE".into(),
        LogicalType::DateTime => "TIMESTAMP(6)".into(),
        LogicalType::DateTimeTz => "TIMESTAMP(6) WITH TIME ZONE".into(),
        LogicalType::Uuid => "RAW(16)".into(),
        LogicalType::Json => format!("CLOB CHECK ({col} IS JSON)"),
        LogicalType::Bytes if keyed => "RAW(2000)".into(),
        LogicalType::Bytes => "BLOB".into(),
        LogicalType::ObjectId => "VARCHAR2(24)".into(),
    }
}

/// Select expression of a column: native JSON / XML are serialized to text in the database.
pub fn select_expression(p: &DataPropertyInfo) -> String {
    let col = super::oracle_connection::quote_ident(&p.property_name);
    match p.source_type.as_deref() {
        Some("json") => format!("JSON_SERIALIZE({col} RETURNING CLOB) AS {col}"),
        Some("xmltype") => format!("XMLSERIALIZE(CONTENT {col} AS CLOB) AS {col}"),
        _ => col,
    }
}

/// ODPI formats numbers below 1 as `.5`.
fn number_text(mut s: String) -> String {
    if s.starts_with('.') {
        s.insert(0, '0');
    } else if s.starts_with("-.") {
        s.insert(1, '0');
    }
    s
}

/// How a column is read from a row (chosen once per column, not per value).
#[derive(Clone, Copy, Debug)]
pub enum Reader {
    Int,
    F32,
    F64,
    Decimal,
    Text,
    Json,
    Bytes,
    NativeBool,
    NumberBool,
    Date,
    DateTime,
    DateTimeTz,
    Parsed,
}

impl Reader {
    pub fn new(kind: LogicalType, oracle_type: &OracleType) -> Reader {
        match kind {
            LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 => Reader::Int,
            LogicalType::Float32 => Reader::F32,
            LogicalType::Float64 => Reader::F64,
            LogicalType::Decimal => Reader::Decimal,
            LogicalType::String => Reader::Text,
            LogicalType::Json => Reader::Json,
            LogicalType::Bytes | LogicalType::Uuid => Reader::Bytes,
            LogicalType::Bool if matches!(oracle_type, OracleType::Boolean) => Reader::NativeBool,
            LogicalType::Bool => Reader::NumberBool,
            LogicalType::Date => Reader::Date,
            LogicalType::DateTime => Reader::DateTime,
            LogicalType::DateTimeTz => Reader::DateTimeTz,
            LogicalType::Time | LogicalType::ObjectId => Reader::Parsed,
        }
    }

    /// Appends column `i` of `row` to `col`.
    pub fn push(self, col: &mut Column, row: &Row, i: usize) -> DMResult<()> {
        macro_rules! get {
            ($t:ty) => {
                match row.get::<usize, Option<$t>>(i)? {
                    Some(v) => v,
                    None => {
                        col.push_null();
                        return Ok(());
                    }
                }
            };
        }
        let cell = match self {
            Reader::Int => Cell::I64(get!(i64)),
            Reader::F32 => Cell::F32(get!(f32)),
            Reader::F64 => Cell::F64(get!(f64)),
            Reader::Decimal => return col.push(Value::Decimal(number_text(get!(String)))),
            Reader::Text => return col.push_str(&get!(String)),
            Reader::Json => return col.push_cell(Cell::Json(&get!(String))),
            Reader::Bytes => {
                let b = get!(Vec<u8>);
                return match col.kind {
                    LogicalType::Uuid => col.push(Value::Bytes(b).coerce(LogicalType::Uuid)?),
                    _ => {
                        col.push_bytes(&b);
                        Ok(())
                    }
                };
            }
            Reader::NativeBool => Cell::Bool(get!(bool)),
            Reader::NumberBool => Cell::Bool(get!(i64) != 0),
            Reader::Date => Cell::Date(get!(NaiveDate)),
            Reader::DateTime => Cell::DateTime(get!(NaiveDateTime)),
            Reader::DateTimeTz => Cell::DateTimeTz(get!(DateTime<FixedOffset>).with_timezone(&Utc)),
            Reader::Parsed => return col.push(Value::parse_text(&get!(String), col.kind)?),
        };
        col.push_cell(cell)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prop(t: LogicalType, source: Option<&str>) -> DataPropertyInfo {
        DataPropertyInfo {
            property_name: "c".into(),
            data_type: t,
            source_type: source.map(str::to_string),
            is_nullable: true,
            is_identifier: false,
            reference_container_name: None,
            reference_property_name: None,
            shape: None,
        }
    }

    #[test]
    fn ora4_declared_types() {
        use LogicalType::*;
        let t = |dt: &str, len: Option<u32>, p: Option<u32>, s: Option<i32>| logical_type_for(dt, len, p, s, false, false);
        assert_eq!(t("NUMBER", None, Some(4), Some(0)), Int16);
        assert_eq!(t("NUMBER", None, Some(9), Some(0)), Int32);
        assert_eq!(t("NUMBER", None, Some(18), Some(0)), Int64);
        assert_eq!(t("NUMBER", None, Some(19), Some(0)), Decimal);
        assert_eq!(t("NUMBER", None, Some(10), Some(2)), Decimal);
        assert_eq!(t("NUMBER", None, None, None), Decimal);
        assert_eq!(t("NUMBER", None, Some(1), Some(0)), Int16);
        assert_eq!(logical_type_for("NUMBER", None, Some(1), Some(0), true, false), Bool);
        assert_eq!(t("BINARY_FLOAT", None, None, None), Float32);
        assert_eq!(t("FLOAT", None, Some(126), None), Float64);
        assert_eq!(t("DATE", None, None, None), DateTime);
        assert_eq!(t("TIMESTAMP(6)", None, None, Some(6)), DateTime);
        assert_eq!(t("TIMESTAMP(6) WITH TIME ZONE", None, None, Some(6)), DateTimeTz);
        assert_eq!(t("TIMESTAMP(6) WITH LOCAL TIME ZONE", None, None, Some(6)), DateTimeTz);
        assert_eq!(t("RAW", Some(16), None, None), Uuid);
        assert_eq!(t("RAW", Some(2000), None, None), Bytes);
        assert_eq!(t("BLOB", None, None, None), Bytes);
        assert_eq!(t("JSON", None, None, None), Json);
        assert_eq!(logical_type_for("CLOB", None, None, None, false, true), Json);
        assert_eq!(t("BOOLEAN", None, None, None), Bool);
        assert_eq!(t("INTERVAL DAY(2) TO SECOND(6)", None, None, None), String);
        assert_eq!(t("NVARCHAR2", Some(10), None, None), String);
        assert_eq!(t("ROWID", None, None, None), String);
    }

    #[test]
    fn ora4_checks_and_source_types() {
        assert!(is_bool_check("VIP", "\"VIP\" IN (0, 1)"));
        assert!(is_bool_check("vip", "vip in (1,0)"));
        assert!(!is_bool_check("VIP", "VIP IN (0, 1, 2)"));
        assert!(is_json_check("TAGS", "\"TAGS\" IS JSON"));
        assert!(is_json_check("TAGS", "tags is json (strict)"));
        assert!(!is_json_check("TAGS", "\"OTHER\" IS JSON"));
        assert_eq!(source_type("NUMBER", None, Some(10), Some(2)), "number(10,2)");
        assert_eq!(source_type("VARCHAR2", Some(50), None, None), "varchar2(50)");
        assert_eq!(source_type("CLOB", Some(0), None, None), "clob");
    }

    #[test]
    fn ora4_target_types() {
        use LogicalType::*;
        assert_eq!(oracle_type_for(&prop(Decimal, Some("decimal(10,2)")), false, 19, "c"), "NUMBER(10,2)");
        assert_eq!(oracle_type_for(&prop(Decimal, Some("numeric(1000,2)")), false, 19, "c"), "NUMBER");
        assert_eq!(oracle_type_for(&prop(String, Some("varchar(255)")), false, 19, "c"), "VARCHAR2(255 CHAR)");
        assert_eq!(oracle_type_for(&prop(String, Some("text")), false, 19, "c"), "CLOB");
        assert_eq!(oracle_type_for(&prop(String, None), true, 19, "c"), "VARCHAR2(1000 CHAR)");
        assert_eq!(oracle_type_for(&prop(Bool, None), false, 19, "c"), "NUMBER(1) CHECK (\"c\" IN (0, 1))");
        assert_eq!(oracle_type_for(&prop(Bool, None), false, 23, "c"), "BOOLEAN");
        assert_eq!(oracle_type_for(&prop(Uuid, None), true, 23, "c"), "RAW(16)");
        assert_eq!(oracle_type_for(&prop(Json, None), false, 23, "c"), "CLOB CHECK (\"c\" IS JSON)");
        assert_eq!(oracle_type_for(&prop(DateTime, Some("date")), false, 23, "c"), "DATE");
        assert_eq!(oracle_type_for(&prop(Bytes, None), true, 23, "c"), "RAW(2000)");
        // NUMBER(5) / (10) / (19) hold every value of the type, so they read back one step wider
        assert_eq!(oracle_type_for(&prop(Int64, None), false, 23, "c"), "NUMBER(19)");
        assert_eq!(logical_type_for("NUMBER", None, Some(19), Some(0), false, false), Decimal);
        assert_eq!(select_expression(&prop(Json, Some("json"))), "JSON_SERIALIZE(\"c\" RETURNING CLOB) AS \"c\"");
        assert_eq!(number_text(".5".into()), "0.5");
        assert_eq!(number_text("-.25".into()), "-0.25");
    }

    #[test]
    fn ora4_result_types() {
        assert_eq!(logical_type_for_result(&OracleType::Number(0, -127)), LogicalType::Decimal);
        assert_eq!(logical_type_for_result(&OracleType::Number(10, 0)), LogicalType::Int64);
        assert_eq!(logical_type_for_result(&OracleType::TimestampTZ(6)), LogicalType::DateTimeTz);
        assert_eq!(logical_type_for_result(&OracleType::Raw(16)), LogicalType::Uuid);
        assert_eq!(logical_type_for_result(&OracleType::Varchar2(10)), LogicalType::String);
    }
}
