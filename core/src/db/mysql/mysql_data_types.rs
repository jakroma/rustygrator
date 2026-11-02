//! MySQL / MariaDB <-> logical type mapping (MY-4).

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use mysql_async::{consts::ColumnFlags, consts::ColumnType, Value as My};

use crate::{
    dbc::batch::{Cell, Column},
    models::{
        data_definitions::DataPropertyInfo,
        value::{to_hex, LogicalType, Value},
    },
    DMError, DMResult,
};

/// Logical type of a column from `information_schema.COLUMNS` (`DATA_TYPE`, `COLUMN_TYPE`).
pub fn logical_type_for(data_type: &str, column_type: &str) -> LogicalType {
    let unsigned = column_type.contains("unsigned");
    match data_type {
        "tinyint" if column_type.starts_with("tinyint(1)") => LogicalType::Bool,
        "bit" if column_type == "bit(1)" => LogicalType::Bool,
        "tinyint" | "year" => LogicalType::Int16,
        "smallint" if !unsigned => LogicalType::Int16,
        "smallint" | "mediumint" => LogicalType::Int32,
        "int" | "integer" if !unsigned => LogicalType::Int32,
        "int" | "integer" => LogicalType::Int64,
        "bigint" if !unsigned => LogicalType::Int64,
        // unsigned bigint does not fit in i64
        "bigint" | "decimal" | "numeric" => LogicalType::Decimal,
        "float" => LogicalType::Float32,
        "double" | "real" => LogicalType::Float64,
        "date" => LogicalType::Date,
        "time" => LogicalType::Time,
        "datetime" => LogicalType::DateTime,
        "timestamp" => LogicalType::DateTimeTz,
        "json" => LogicalType::Json,
        // MariaDB 10.7+
        "uuid" => LogicalType::Uuid,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" | "geometry" | "point" | "linestring"
        | "polygon" | "multipoint" | "multilinestring" | "multipolygon" | "geometrycollection" | "geomcollection" => LogicalType::Bytes,
        // char, varchar, *text, enum, set, inet6, ...
        _ => LogicalType::String,
    }
}

/// Logical type of a result column of a query (VIEW-6).
pub fn logical_type_for_result(t: ColumnType, flags: ColumnFlags, length: u32) -> LogicalType {
    use ColumnType::*;
    let unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
    let binary = flags.contains(ColumnFlags::BINARY_FLAG);
    match t {
        MYSQL_TYPE_TINY if length == 1 => LogicalType::Bool,
        MYSQL_TYPE_TINY | MYSQL_TYPE_YEAR => LogicalType::Int16,
        MYSQL_TYPE_SHORT if !unsigned => LogicalType::Int16,
        MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 => LogicalType::Int32,
        MYSQL_TYPE_LONG if !unsigned => LogicalType::Int32,
        MYSQL_TYPE_LONG => LogicalType::Int64,
        MYSQL_TYPE_LONGLONG if !unsigned => LogicalType::Int64,
        MYSQL_TYPE_LONGLONG | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => LogicalType::Decimal,
        MYSQL_TYPE_FLOAT => LogicalType::Float32,
        MYSQL_TYPE_DOUBLE => LogicalType::Float64,
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => LogicalType::Date,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => LogicalType::Time,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => LogicalType::DateTime,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => LogicalType::DateTimeTz,
        MYSQL_TYPE_JSON => LogicalType::Json,
        MYSQL_TYPE_BIT | MYSQL_TYPE_GEOMETRY => LogicalType::Bytes,
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
        | MYSQL_TYPE_VARCHAR
            if binary =>
        {
            LogicalType::Bytes
        }
        _ => LogicalType::String,
    }
}

/// Target column type. Key, index and foreign key columns need a bounded length in MySQL, so
/// text / bytes keys become `VARCHAR(255)` / `VARBINARY(255)`; decimals keep the source
/// precision when it is known.
pub fn mysql_type_for(p: &DataPropertyInfo, keyed: bool) -> String {
    let decimal = || {
        p.source_type
            .as_deref()
            .and_then(|s| s.strip_prefix("decimal(").or_else(|| s.strip_prefix("numeric(")))
            .and_then(|s| s.split(')').next())
            .map(|ps| format!("DECIMAL({ps})"))
            .unwrap_or_else(|| "DECIMAL(65,30)".to_string())
    };
    match p.data_type {
        LogicalType::Int16 => "SMALLINT".into(),
        LogicalType::Int32 => "INT".into(),
        LogicalType::Int64 => "BIGINT".into(),
        LogicalType::Float32 => "FLOAT".into(),
        LogicalType::Float64 => "DOUBLE".into(),
        LogicalType::Decimal => decimal(),
        LogicalType::String if keyed => "VARCHAR(255)".into(),
        LogicalType::String => "LONGTEXT".into(),
        LogicalType::Bool => "BOOLEAN".into(),
        LogicalType::Date => "DATE".into(),
        LogicalType::Time => "TIME(6)".into(),
        LogicalType::DateTime => "DATETIME(6)".into(),
        // TIMESTAMP only covers 1970-2038; values are stored as UTC
        LogicalType::DateTimeTz => "DATETIME(6)".into(),
        LogicalType::Uuid => "CHAR(36)".into(),
        LogicalType::Json => "JSON".into(),
        LogicalType::Bytes if keyed => "VARBINARY(255)".into(),
        LogicalType::Bytes => "LONGBLOB".into(),
        LogicalType::ObjectId => "CHAR(24)".into(),
    }
}

fn date(y: u16, m: u8, d: u8) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32)
}

fn datetime(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> Option<NaiveDateTime> {
    date(y, mo, d)?.and_hms_micro_opt(h as u32, mi as u32, s as u32, us)
}

/// Appends a MySQL value to a column of the given logical type. Zero dates (`0000-00-00`)
/// become null, like `zeroDateTimeBehavior=convertToNull` in other clients.
pub fn push_value(col: &mut Column, v: &My) -> DMResult<()> {
    let kind = col.kind;
    let cell = match v {
        My::NULL => Cell::Null,
        My::Int(i) if kind == LogicalType::Bool => Cell::Bool(*i != 0),
        My::Int(i) => Cell::I64(*i),
        My::UInt(u) => match i64::try_from(*u) {
            Ok(i) if kind != LogicalType::Decimal => Cell::I64(i),
            _ => return col.push(Value::Decimal(u.to_string())),
        },
        My::Float(f) => Cell::F32(*f),
        My::Double(f) => Cell::F64(*f),
        My::Bytes(b) => match kind {
            LogicalType::Bytes => Cell::Bytes(b),
            LogicalType::Bool => Cell::Bool(b.iter().any(|x| *x != 0)),
            _ => {
                let s = std::str::from_utf8(b).map_err(|e| DMError::Error(format!("MySQL text is not UTF-8: {e}")))?;
                match kind {
                    LogicalType::String => Cell::Str(s),
                    LogicalType::Json => Cell::Json(s),
                    LogicalType::Decimal => Cell::Decimal(s),
                    _ => return col.push(Value::parse_text(s, kind)?),
                }
            }
        },
        My::Date(y, mo, d, h, mi, s, us) => match kind {
            LogicalType::Date => date(*y, *mo, *d).map(Cell::Date).unwrap_or(Cell::Null),
            LogicalType::DateTimeTz => datetime(*y, *mo, *d, *h, *mi, *s, *us).map(|t| Cell::DateTimeTz(t.and_utc())).unwrap_or(Cell::Null),
            _ => datetime(*y, *mo, *d, *h, *mi, *s, *us).map(Cell::DateTime).unwrap_or(Cell::Null),
        },
        My::Time(neg, days, h, mi, s, us) => {
            if *neg || *days > 0 {
                // MySQL TIME is an interval (-838:59:59 .. 838:59:59); only times of day fit
                let total = *days as i64 * 24 + *h as i64;
                return col.push(Value::String(format!("{}{total:02}:{mi:02}:{s:02}.{us:06}", if *neg { "-" } else { "" })));
            }
            Cell::Time(NaiveTime::from_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us).unwrap_or_default())
        }
    };
    col.push_cell(cell)
}

/// A cell as a statement parameter.
pub fn param(c: Cell<'_>) -> My {
    let dt = |t: NaiveDateTime| My::Date(t.year() as u16, t.month() as u8, t.day() as u8, t.hour() as u8, t.minute() as u8, t.second() as u8, t.nanosecond() / 1000);
    match c {
        Cell::Null => My::NULL,
        Cell::Bool(b) => My::Int(b as i64),
        Cell::I16(i) => My::Int(i as i64),
        Cell::I32(i) => My::Int(i as i64),
        Cell::I64(i) => My::Int(i),
        Cell::F32(f) => My::Float(f),
        Cell::F64(f) => My::Double(f),
        Cell::Decimal(s) | Cell::Str(s) | Cell::Json(s) => My::Bytes(s.as_bytes().to_vec()),
        Cell::JsonValue(j) => My::Bytes(serde_json::to_vec(j).unwrap_or_default()),
        Cell::Bytes(b) => My::Bytes(b.to_vec()),
        Cell::Date(d) => My::Date(d.year() as u16, d.month() as u8, d.day() as u8, 0, 0, 0, 0),
        Cell::Time(t) => My::Time(false, 0, t.hour() as u8, t.minute() as u8, t.second() as u8, t.nanosecond() / 1000),
        Cell::DateTime(t) => dt(t),
        Cell::DateTimeTz(t) => dt(t.naive_utc()),
        Cell::Uuid(u) => My::Bytes(u.hyphenated().to_string().into_bytes()),
        Cell::ObjectId(o) => My::Bytes(to_hex(&o).into_bytes()),
    }
}

/// Labels of an `enum('a','b')` / `set('a','b')` column type.
pub fn enum_labels(column_type: &str) -> Option<Vec<String>> {
    let inner = column_type.strip_prefix("enum(").or_else(|| column_type.strip_prefix("set("))?.strip_suffix(')')?;
    let mut labels = Vec::new();
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut label = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                '\'' => break,
                '\\' => label.extend(chars.next()),
                c => label.push(c),
            }
        }
        labels.push(label);
    }
    Some(labels)
}

/// Seconds (with an optional `.fraction`) since the epoch, as the binlog sends TIMESTAMP values.
pub fn timestamp_from_text(s: &str) -> Option<DateTime<Utc>> {
    let (secs, frac) = s.split_once('.').unwrap_or((s, ""));
    let micros = format!("{frac:0<6}").get(..6)?.parse::<u32>().ok()?;
    DateTime::from_timestamp(secs.parse().ok()?, micros * 1000)
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
    fn my4_declared_types() {
        use LogicalType::*;
        for (dt, ct, t) in [
            ("tinyint", "tinyint(1)", Bool),
            ("tinyint", "tinyint(4) unsigned", Int16),
            ("smallint", "smallint(5) unsigned", Int32),
            ("int", "int(10) unsigned", Int64),
            ("bigint", "bigint(20) unsigned", Decimal),
            ("decimal", "decimal(10,2)", Decimal),
            ("datetime", "datetime(6)", DateTime),
            ("timestamp", "timestamp", DateTimeTz),
            ("enum", "enum('a','b')", String),
            ("varbinary", "varbinary(16)", Bytes),
            ("json", "json", Json),
        ] {
            assert_eq!(logical_type_for(dt, ct), t, "{ct}");
        }
    }

    #[test]
    fn my4_target_types_round_trip() {
        use LogicalType::*;
        let back = |sql: &str| {
            let lower = sql.to_ascii_lowercase();
            let dt = lower.split('(').next().unwrap().to_string();
            let dt = if dt == "boolean" { "tinyint".to_string() } else { dt };
            let ct = if lower == "boolean" { "tinyint(1)".to_string() } else { lower.clone() };
            logical_type_for(&dt, &ct)
        };
        for t in [Int16, Int32, Int64, Float32, Float64, Decimal, String, Bool, Date, Time, DateTime, Json, Bytes] {
            assert_eq!(back(&mysql_type_for(&prop(t, None), false)), t, "{t:?}");
            assert_eq!(back(&mysql_type_for(&prop(t, None), true)), t, "{t:?} keyed");
        }
        assert_eq!(mysql_type_for(&prop(Decimal, Some("decimal(10,2)")), false), "DECIMAL(10,2)");
    }

    #[test]
    fn my4_values_into_columns() {
        let mut c = Column::new(LogicalType::DateTimeTz, 2);
        push_value(&mut c, &My::Date(2024, 2, 29, 13, 5, 7, 250_000)).unwrap();
        push_value(&mut c, &My::Date(0, 0, 0, 0, 0, 0, 0)).unwrap();
        assert_eq!(c.cell(0).to_value(), Value::DateTimeTz("2024-02-29T13:05:07.25Z".parse().unwrap()));
        assert!(c.is_null(1));
        let mut d = Column::new(LogicalType::Decimal, 1);
        push_value(&mut d, &My::UInt(u64::MAX)).unwrap();
        assert_eq!(d.cell(0).to_value(), Value::Decimal("18446744073709551615".into()));
        let mut b = Column::new(LogicalType::Bool, 2);
        push_value(&mut b, &My::Int(1)).unwrap();
        push_value(&mut b, &My::Bytes(vec![0])).unwrap();
        assert_eq!((b.cell(0).to_value(), b.cell(1).to_value()), (Value::Bool(true), Value::Bool(false)));
        let mut t = Column::new(LogicalType::Time, 1);
        push_value(&mut t, &My::Time(false, 0, 23, 59, 1, 5)).unwrap();
        assert_eq!(t.cell(0).to_value(), Value::Time(NaiveTime::from_hms_micro_opt(23, 59, 1, 5).unwrap()));
    }

    #[test]
    fn my4_parameters() {
        let t: DateTime<Utc> = "2024-02-29T13:05:07.000250Z".parse().unwrap();
        assert_eq!(param(Cell::DateTimeTz(t)), My::Date(2024, 2, 29, 13, 5, 7, 250));
        assert_eq!(param(Cell::Bool(true)), My::Int(1));
        assert_eq!(param(Cell::Str("x")), My::Bytes(b"x".to_vec()));
    }

    #[test]
    fn my8_enum_labels_and_binlog_timestamps() {
        assert_eq!(enum_labels("enum('a','it''s','b\\\\c')").unwrap(), vec!["a", "it's", "b\\c"]);
        assert_eq!(enum_labels("set('x','y')").unwrap(), vec!["x", "y"]);
        assert_eq!(enum_labels("varchar(3)"), None);
        assert_eq!(timestamp_from_text("1700000000.5").unwrap().to_rfc3339(), "2023-11-14T22:13:20.500+00:00");
        assert_eq!(timestamp_from_text("1700000000").unwrap().timestamp(), 1_700_000_000);
    }
}
