//! CQL <-> logical type mapping (SCY-4, SCY-10).

use chrono::{DateTime, NaiveDate, NaiveTime, Timelike};
use scylla::value::{CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlValue};

use crate::{
    dbc::batch::{Cell, Column},
    models::{
        data_definitions::JsonShape,
        value::{to_hex, LogicalType, Value},
    },
    DMError, DMResult,
};

/// Logical type of a CQL type name from `system_schema.columns.type`.
pub fn logical_type_for(cql: &str) -> LogicalType {
    match cql.trim().to_ascii_lowercase().as_str() {
        "ascii" | "text" | "varchar" | "inet" | "duration" => LogicalType::String,
        "boolean" => LogicalType::Bool,
        "blob" => LogicalType::Bytes,
        "tinyint" | "smallint" => LogicalType::Int16,
        "int" => LogicalType::Int32,
        "bigint" | "counter" => LogicalType::Int64,
        "varint" | "decimal" => LogicalType::Decimal,
        "float" => LogicalType::Float32,
        "double" => LogicalType::Float64,
        "date" => LogicalType::Date,
        "time" => LogicalType::Time,
        "timestamp" => LogicalType::DateTimeTz,
        "uuid" | "timeuuid" => LogicalType::Uuid,
        // list / set / map / tuple / frozen<..> / user defined types
        _ => LogicalType::Json,
    }
}

/// Scalar CQL type for a logical type.
pub fn cql_scalar(t: LogicalType) -> &'static str {
    match t {
        LogicalType::Int16 => "smallint",
        LogicalType::Int32 => "int",
        LogicalType::Int64 => "bigint",
        LogicalType::Float32 => "float",
        LogicalType::Float64 => "double",
        LogicalType::Decimal => "decimal",
        LogicalType::String | LogicalType::Json | LogicalType::ObjectId => "text",
        LogicalType::Bool => "boolean",
        LogicalType::Date => "date",
        LogicalType::Time => "time",
        LogicalType::DateTime | LogicalType::DateTimeTz => "timestamp",
        LogicalType::Uuid => "uuid",
        LogicalType::Bytes => "blob",
    }
}

/// Shape of a list or UDT type from the schema (`list<int>`, `frozen<list<text>>`); UDTs are
/// resolved by the provider.
pub fn list_shape(cql: &str) -> Option<JsonShape> {
    let mut t = cql.trim();
    if let Some(inner) = t.strip_prefix("frozen<").and_then(|r| r.strip_suffix('>')) {
        t = inner;
    }
    let inner = t.strip_prefix("list<").or_else(|| t.strip_prefix("set<"))?.strip_suffix('>')?;
    let lt = logical_type_for(inner);
    (lt != LogicalType::Json).then_some(JsonShape::List(lt))
}

pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ------------------------------------------------------------------ decimals

/// Two's complement big-endian integer -> decimal digits.
fn bigint_to_string(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0".into();
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut mag: Vec<u8> = if negative {
        // magnitude = two's complement
        let mut v: Vec<u8> = bytes.iter().map(|b| !b).collect();
        for b in v.iter_mut().rev() {
            let (r, o) = b.overflowing_add(1);
            *b = r;
            if !o {
                break;
            }
        }
        v
    } else {
        bytes.to_vec()
    };
    let mut digits = Vec::new();
    while mag.iter().any(|b| *b != 0) {
        let mut rem = 0u32;
        for b in mag.iter_mut() {
            let cur = (rem << 8) | *b as u32;
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    if digits.is_empty() {
        digits.push(b'0');
    }
    if negative {
        digits.push(b'-');
    }
    digits.reverse();
    String::from_utf8(digits).unwrap_or_default()
}

/// Decimal digits (optionally signed) -> minimal two's complement big-endian bytes.
fn string_to_bigint(digits: &str) -> DMResult<Vec<u8>> {
    let (negative, d) = match digits.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, digits.strip_prefix('+').unwrap_or(digits)),
    };
    if d.is_empty() || !d.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DMError::Error(format!("invalid integer '{}'", digits)));
    }
    // little-endian magnitude
    let mut mag: Vec<u8> = vec![0];
    for c in d.bytes() {
        let mut carry = (c - b'0') as u32;
        for b in mag.iter_mut() {
            let cur = *b as u32 * 10 + carry;
            *b = cur as u8;
            carry = cur >> 8;
        }
        while carry > 0 {
            mag.push(carry as u8);
            carry >>= 8;
        }
    }
    while mag.len() > 1 && *mag.last().unwrap() == 0 {
        mag.pop();
    }
    if mag.last().is_some_and(|b| b & 0x80 != 0) {
        mag.push(0);
    }
    if negative && mag.iter().any(|b| *b != 0) {
        let mut carry = 1u16;
        for b in mag.iter_mut() {
            let v = (!*b) as u16 + carry;
            *b = v as u8;
            carry = v >> 8;
        }
    }
    mag.reverse();
    // minimal two's complement: drop redundant sign bytes
    while mag.len() > 1 && ((mag[0] == 0 && mag[1] & 0x80 == 0) || (mag[0] == 0xff && mag[1] & 0x80 != 0)) {
        mag.remove(0);
    }
    Ok(mag)
}

pub fn decimal_to_string(bytes: &[u8], scale: i32) -> String {
    let s = bigint_to_string(bytes);
    if scale <= 0 {
        return if scale == 0 || s == "0" { s } else { format!("{}{}", s, "0".repeat((-scale) as usize)) };
    }
    let (sign, digits) = match s.strip_prefix('-') {
        Some(d) => ("-", d),
        None => ("", s.as_str()),
    };
    let scale = scale as usize;
    let padded = if digits.len() <= scale { format!("{}{}", "0".repeat(scale - digits.len() + 1), digits) } else { digits.to_string() };
    let (i, f) = padded.split_at(padded.len() - scale);
    format!("{}{}.{}", sign, i, f)
}

pub fn string_to_decimal(s: &str) -> DMResult<CqlDecimal> {
    let t = s.trim();
    let (int, frac) = t.split_once('.').unwrap_or((t, ""));
    let bytes = string_to_bigint(&format!("{}{}", int, frac))?;
    Ok(CqlDecimal::from_signed_be_bytes_and_exponent(bytes, frac.len() as i32))
}

// ------------------------------------------------------------------ dates

const DATE_EPOCH_OFFSET: i64 = 1 << 31;

fn unix_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date")
}

pub fn cql_date(d: NaiveDate) -> CqlDate {
    CqlDate(((d - unix_epoch()).num_days() + DATE_EPOCH_OFFSET) as u32)
}

pub fn from_cql_date(d: CqlDate) -> Option<NaiveDate> {
    unix_epoch().checked_add_signed(chrono::Duration::days(d.0 as i64 - DATE_EPOCH_OFFSET))
}

fn duration_text(d: &CqlDuration) -> String {
    format!("{}mo{}d{}ns", d.months, d.days, d.nanoseconds)
}

// ------------------------------------------------------------------ CQL -> model

/// JSON form of collections, tuples and UDTs.
pub fn cql_to_json(v: &CqlValue) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => J::Array(items.iter().map(cql_to_json).collect()),
        CqlValue::Tuple(items) => J::Array(items.iter().map(|i| i.as_ref().map(cql_to_json).unwrap_or(J::Null)).collect()),
        CqlValue::Map(entries) => {
            let mut m = serde_json::Map::new();
            for (k, val) in entries {
                let key = match cql_to_json(k) {
                    J::String(s) => s,
                    other => other.to_string(),
                };
                m.insert(key, cql_to_json(val));
            }
            J::Object(m)
        }
        CqlValue::UserDefinedType { fields, .. } => {
            J::Object(fields.iter().map(|(n, f)| (n.clone(), f.as_ref().map(cql_to_json).unwrap_or(J::Null))).collect())
        }
        other => cql_scalar_value(other).to_json(),
    }
}

/// Scalar CQL value as a [`Value`].
pub fn cql_scalar_value(v: &CqlValue) -> Value {
    match v {
        CqlValue::Ascii(s) | CqlValue::Text(s) => Value::String(s.clone()),
        CqlValue::Boolean(b) => Value::Bool(*b),
        CqlValue::Blob(b) => Value::Bytes(b.clone()),
        CqlValue::Counter(c) => Value::I64(c.0),
        CqlValue::Decimal(d) => {
            let (b, s) = d.as_signed_be_bytes_slice_and_exponent();
            Value::Decimal(decimal_to_string(b, s))
        }
        CqlValue::Varint(v) => Value::Decimal(bigint_to_string(v.as_signed_bytes_be_slice())),
        CqlValue::Date(d) => from_cql_date(*d).map(Value::Date).unwrap_or(Value::Null),
        CqlValue::Double(f) => Value::F64(*f),
        CqlValue::Float(f) => Value::F32(*f),
        CqlValue::Duration(d) => Value::String(duration_text(d)),
        CqlValue::Empty => Value::Null,
        CqlValue::Int(i) => Value::I32(*i),
        CqlValue::BigInt(i) => Value::I64(*i),
        CqlValue::SmallInt(i) => Value::I16(*i),
        CqlValue::TinyInt(i) => Value::I16(*i as i16),
        CqlValue::Timestamp(t) => DateTime::from_timestamp_millis(t.0).map(Value::DateTimeTz).unwrap_or(Value::Null),
        CqlValue::Time(t) => {
            let n = t.0;
            NaiveTime::from_num_seconds_from_midnight_opt((n / 1_000_000_000) as u32, (n % 1_000_000_000) as u32)
                .map(Value::Time)
                .unwrap_or(Value::Null)
        }
        CqlValue::Inet(ip) => Value::String(ip.to_string()),
        CqlValue::Uuid(u) => Value::Uuid(*u),
        CqlValue::Timeuuid(u) => Value::Uuid(*u.as_ref()),
        nested => Value::Json(cql_to_json(nested)),
    }
}

/// Appends a CQL value to a column (strings without an extra copy where possible).
pub fn push_cql(col: &mut Column, v: Option<&CqlValue>) -> DMResult<()> {
    match v {
        None => {
            col.push_null();
            Ok(())
        }
        Some(CqlValue::Text(s)) | Some(CqlValue::Ascii(s)) => col.push_cell(Cell::Str(s)),
        Some(CqlValue::Blob(b)) => col.push_cell(Cell::Bytes(b)),
        Some(CqlValue::Int(i)) => col.push_cell(Cell::I32(*i)),
        Some(CqlValue::BigInt(i)) => col.push_cell(Cell::I64(*i)),
        Some(CqlValue::Double(f)) => col.push_cell(Cell::F64(*f)),
        Some(CqlValue::Boolean(b)) => col.push_cell(Cell::Bool(*b)),
        Some(other) if col.kind == LogicalType::Json => col.push(Value::Json(cql_to_json(other))),
        Some(other) => col.push(cql_scalar_value(other)),
    }
}

// ------------------------------------------------------------------ model -> CQL

/// Target of one column in a ScyllaDB table.
#[derive(Debug, Clone, PartialEq)]
pub enum CqlTarget {
    Scalar(LogicalType),
    List(LogicalType),
    Udt { keyspace: String, name: String, fields: Vec<(String, LogicalType)> },
}

impl CqlTarget {
    pub fn cql_type(&self) -> String {
        match self {
            CqlTarget::Scalar(t) => cql_scalar(*t).to_string(),
            CqlTarget::List(t) => format!("list<{}>", cql_scalar(*t)),
            CqlTarget::Udt { name, .. } => format!("frozen<{}>", quote(name)),
        }
    }
}

/// Scalar value of a logical type as CQL.
pub fn value_to_cql(v: Value, t: LogicalType) -> DMResult<Option<CqlValue>> {
    let v = v.coerce(t)?;
    Ok(Some(match v {
        Value::Null => return Ok(None),
        Value::Bool(b) => CqlValue::Boolean(b),
        Value::I16(i) => CqlValue::SmallInt(i),
        Value::I32(i) => CqlValue::Int(i),
        Value::I64(i) => CqlValue::BigInt(i),
        Value::F32(f) => CqlValue::Float(f),
        Value::F64(f) => CqlValue::Double(f),
        Value::Decimal(s) => CqlValue::Decimal(string_to_decimal(&s)?),
        Value::String(s) => CqlValue::Text(s),
        Value::Bytes(b) => CqlValue::Blob(b),
        Value::Date(d) => CqlValue::Date(cql_date(d)),
        Value::Time(t) => CqlValue::Time(CqlTime(t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64)),
        Value::DateTime(t) => CqlValue::Timestamp(CqlTimestamp(t.and_utc().timestamp_millis())),
        Value::DateTimeTz(t) => CqlValue::Timestamp(CqlTimestamp(t.timestamp_millis())),
        Value::Uuid(u) => CqlValue::Uuid(u),
        Value::Json(j) => CqlValue::Text(j.to_string()),
        Value::ObjectId(o) => CqlValue::Text(to_hex(&o)),
    }))
}

/// A cell converted for its target column.
pub fn cell_to_cql(cell: Cell<'_>, target: &CqlTarget) -> DMResult<Option<CqlValue>> {
    if cell.is_null() {
        return Ok(None);
    }
    match target {
        CqlTarget::Scalar(t) => match (cell, t) {
            (Cell::Str(s), LogicalType::String) => Ok(Some(CqlValue::Text(s.to_string()))),
            (Cell::Json(s), LogicalType::Json) => Ok(Some(CqlValue::Text(s.to_string()))),
            (Cell::I32(i), LogicalType::Int32) => Ok(Some(CqlValue::Int(i))),
            (Cell::I64(i), LogicalType::Int64) => Ok(Some(CqlValue::BigInt(i))),
            (Cell::Decimal(s), LogicalType::Decimal) => Ok(Some(CqlValue::Decimal(string_to_decimal(s)?))),
            _ => value_to_cql(cell.to_value(), *t),
        },
        CqlTarget::List(t) => {
            let json = match cell.to_value().coerce(LogicalType::Json)? {
                Value::Json(j) => j,
                _ => unreachable!(),
            };
            let serde_json::Value::Array(items) = json else { return Ok(Some(CqlValue::List(Vec::new()))) };
            let mut out = Vec::with_capacity(items.len());
            for i in items {
                if let Some(v) = value_to_cql(json_scalar(i), *t)? {
                    out.push(v);
                }
            }
            Ok(Some(CqlValue::List(out)))
        }
        CqlTarget::Udt { keyspace, name, fields } => {
            let json = match cell.to_value().coerce(LogicalType::Json)? {
                Value::Json(j) => j,
                _ => unreachable!(),
            };
            let serde_json::Value::Object(mut map) = json else { return Ok(None) };
            let mut out = Vec::with_capacity(fields.len());
            for (f, t) in fields {
                let v = map.remove(f).map(json_scalar).unwrap_or(Value::Null);
                out.push((f.clone(), value_to_cql(v, *t)?));
            }
            Ok(Some(CqlValue::UserDefinedType { keyspace: keyspace.clone(), name: name.clone(), fields: out }))
        }
    }
}

fn json_scalar(j: serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => n.as_i64().map(Value::I64).unwrap_or_else(|| Value::F64(n.as_f64().unwrap_or_default())),
        serde_json::Value::String(s) => Value::String(s),
        // relaxed extended json from MongoDB ({"$oid": ..}, {"$date": ..}) and nested values
        other => Value::Json(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigint_round_trip() {
        for s in ["0", "1", "-1", "127", "128", "-128", "-129", "255", "256", "12345678901234567890123456789", "-98765432109876543210"] {
            let b = string_to_bigint(s).unwrap();
            assert_eq!(bigint_to_string(&b), s, "{s} -> {b:?}");
        }
        assert_eq!(string_to_bigint("128").unwrap(), vec![0, 128]);
        assert_eq!(string_to_bigint("-128").unwrap(), vec![128]);
        assert!(string_to_bigint("1a").is_err());
    }

    #[test]
    fn decimal_round_trip_keeps_scale() {
        for s in ["12345678901234567890.123456", "-0.0001", "1.50", "0.00", "42"] {
            let d = string_to_decimal(s).unwrap();
            let (b, scale) = d.as_signed_be_bytes_slice_and_exponent();
            assert_eq!(decimal_to_string(b, scale), s);
        }
        assert_eq!(decimal_to_string(&[5], -2), "500");
    }

    #[test]
    fn dates_and_types() {
        let d = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert_eq!(from_cql_date(cql_date(d)), Some(d));
        assert_eq!(cql_date(unix_epoch()).0, 1 << 31);
        assert_eq!(logical_type_for("timeuuid"), LogicalType::Uuid);
        assert_eq!(logical_type_for("frozen<address>"), LogicalType::Json);
        assert_eq!(list_shape("frozen<list<int>>"), Some(JsonShape::List(LogicalType::Int32)));
        assert_eq!(list_shape("map<text, int>"), None);
    }

    #[test]
    fn json_and_udt_conversion() {
        let t = CqlTarget::Udt { keyspace: "ks".into(), name: "addr".into(), fields: vec![("city".into(), LogicalType::String), ("zip".into(), LogicalType::Int32)] };
        let v = cell_to_cql(Cell::Json(r#"{"city": "X", "zip": 5, "extra": 1}"#), &t).unwrap().unwrap();
        assert_eq!(
            v,
            CqlValue::UserDefinedType {
                keyspace: "ks".into(),
                name: "addr".into(),
                fields: vec![("city".into(), Some(CqlValue::Text("X".into()))), ("zip".into(), Some(CqlValue::Int(5)))]
            }
        );
        assert_eq!(cql_to_json(&v), serde_json::json!({"city": "X", "zip": 5}));
        let l = cell_to_cql(Cell::Json("[1, 2]"), &CqlTarget::List(LogicalType::Int64)).unwrap().unwrap();
        assert_eq!(l, CqlValue::List(vec![CqlValue::BigInt(1), CqlValue::BigInt(2)]));
        assert_eq!(CqlTarget::List(LogicalType::Int64).cql_type(), "list<bigint>");
    }
}
