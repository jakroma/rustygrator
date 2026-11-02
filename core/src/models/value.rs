//! Database independent value model shared by all providers and receivers.
//!
//! Every provider converts its native values into [`Value`] and every receiver converts a
//! [`Value`] into its native representation. The [`LogicalType`] stored in the data definition
//! tells the receiver which native type a column should get, so the same mapping is used for
//! one-off migrations and for live transfer (LIVE-11).

use std::borrow::Cow;
use std::fmt::Write as _;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DMError, DMResult};

/// Logical (database independent) column type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogicalType {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// Arbitrary precision number kept as its exact decimal text.
    Decimal,
    String,
    Bool,
    Date,
    Time,
    DateTime,
    DateTimeTz,
    Uuid,
    /// Nested documents, arrays and json columns.
    Json,
    Bytes,
    /// MongoDB ObjectId (12 bytes).
    ObjectId,
}

impl LogicalType {
    pub fn is_integer(self) -> bool {
        matches!(self, LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64)
    }
}

/// A single value read from (or written to) a database.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    DateTimeTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    ObjectId([u8; 12]),
}

/// One record: values aligned with `DataDefinition::properties_info`.
pub type Row = Vec<Value>;
/// A batch of records moved between provider and receiver.
pub type RowBatch = Vec<Row>;

const DATETIME_FORMATS: [&str; 4] = [
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
];

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Text representation used for text columns and text based protocols.
    pub fn to_text(&self) -> Cow<'_, str> {
        match self {
            Value::Null => Cow::Borrowed(""),
            Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
            Value::I16(i) => Cow::Owned(i.to_string()),
            Value::I32(i) => Cow::Owned(i.to_string()),
            Value::I64(i) => Cow::Owned(i.to_string()),
            Value::F32(f) => Cow::Owned(f.to_string()),
            Value::F64(f) => Cow::Owned(f.to_string()),
            Value::Decimal(s) | Value::String(s) => Cow::Borrowed(s.as_str()),
            Value::Bytes(b) => Cow::Owned(to_hex(b)),
            Value::Date(d) => Cow::Owned(d.format("%Y-%m-%d").to_string()),
            Value::Time(t) => Cow::Owned(t.format("%H:%M:%S%.f").to_string()),
            Value::DateTime(t) => Cow::Owned(t.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
            Value::DateTimeTz(t) => Cow::Owned(t.to_rfc3339()),
            Value::Uuid(u) => Cow::Owned(u.to_string()),
            Value::Json(j) => match j {
                serde_json::Value::String(s) => Cow::Borrowed(s.as_str()),
                other => Cow::Owned(other.to_string()),
            },
            Value::ObjectId(o) => Cow::Owned(to_hex(o)),
        }
    }

    /// JSON representation (used for json columns and nested documents).
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value as J;
        match self {
            Value::Null => J::Null,
            Value::Bool(b) => J::Bool(*b),
            Value::I16(i) => J::from(*i),
            Value::I32(i) => J::from(*i),
            Value::I64(i) => J::from(*i),
            Value::F32(f) => serde_json::Number::from_f64(*f as f64).map(J::Number).unwrap_or(J::Null),
            Value::F64(f) => serde_json::Number::from_f64(*f).map(J::Number).unwrap_or(J::Null),
            Value::Json(j) => j.clone(),
            other => J::String(other.to_text().into_owned()),
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I16(i) => Some(*i as i64),
            Value::I32(i) => Some(*i as i64),
            Value::I64(i) => Some(*i),
            Value::Bool(b) => Some(*b as i64),
            Value::F32(f) if f.fract() == 0.0 => Some(*f as i64),
            Value::F64(f) if f.fract() == 0.0 => Some(*f as i64),
            Value::Decimal(s) | Value::String(s) => parse_integer_text(s),
            Value::Json(serde_json::Value::Number(n)) => n.as_i64(),
            Value::Json(serde_json::Value::String(s)) => parse_integer_text(s),
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::I16(i) => Some(*i as f64),
            Value::I32(i) => Some(*i as f64),
            Value::I64(i) => Some(*i as f64),
            Value::F32(f) => Some(*f as f64),
            Value::F64(f) => Some(*f),
            Value::Bool(b) => Some(*b as i64 as f64),
            Value::Decimal(s) | Value::String(s) => s.trim().parse().ok(),
            Value::Json(serde_json::Value::Number(n)) => n.as_f64(),
            Value::Json(serde_json::Value::String(s)) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// Converts the value into the representation required by `target`.
    ///
    /// Conversions are lossless where possible; a value that cannot be represented returns an
    /// error naming both types, so bad data is reported instead of silently corrupted.
    pub fn coerce(self, target: LogicalType) -> DMResult<Value> {
        if self.is_null() {
            return Ok(Value::Null);
        }
        let result = match target {
            LogicalType::Int16 => match self {
                Value::I16(_) => Some(self),
                ref v => v.as_i64().and_then(|i| i16::try_from(i).ok()).map(Value::I16),
            },
            LogicalType::Int32 => match self {
                Value::I32(_) => Some(self),
                ref v => v.as_i64().and_then(|i| i32::try_from(i).ok()).map(Value::I32),
            },
            LogicalType::Int64 => match self {
                Value::I64(_) => Some(self),
                ref v => v.as_i64().map(Value::I64),
            },
            LogicalType::Float32 => match self {
                Value::F32(_) => Some(self),
                ref v => v.as_f64().map(|f| Value::F32(f as f32)),
            },
            LogicalType::Float64 => match self {
                Value::F64(_) => Some(self),
                ref v => v.as_f64().map(Value::F64),
            },
            LogicalType::Decimal => match self {
                Value::Decimal(_) => Some(self),
                Value::String(s) => is_decimal_text(s.trim()).then(|| Value::Decimal(s.trim().to_string())),
                Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::F32(_) | Value::F64(_) => {
                    Some(Value::Decimal(self.to_text().into_owned()))
                }
                Value::Bool(b) => Some(Value::Decimal(if b { "1" } else { "0" }.to_string())),
                Value::Json(serde_json::Value::Number(n)) => Some(Value::Decimal(n.to_string())),
                _ => None,
            },
            LogicalType::String => match self {
                Value::String(_) => Some(self),
                Value::Decimal(s) => Some(Value::String(s)),
                ref v => Some(Value::String(v.to_text().into_owned())),
            },
            LogicalType::Bool => match self {
                Value::Bool(_) => Some(self),
                Value::String(ref s) | Value::Decimal(ref s) => parse_bool(s).map(Value::Bool),
                Value::Json(serde_json::Value::Bool(b)) => Some(Value::Bool(b)),
                ref v => v.as_i64().map(|i| Value::Bool(i != 0)),
            },
            LogicalType::Date => match self {
                Value::Date(_) => Some(self),
                Value::DateTime(t) => Some(Value::Date(t.date())),
                Value::DateTimeTz(t) => Some(Value::Date(t.naive_utc().date())),
                ref v => parse_date(&v.to_text()).map(Value::Date),
            },
            LogicalType::Time => match self {
                Value::Time(_) => Some(self),
                Value::DateTime(t) => Some(Value::Time(t.time())),
                Value::DateTimeTz(t) => Some(Value::Time(t.naive_utc().time())),
                ref v => parse_time(&v.to_text()).map(Value::Time),
            },
            LogicalType::DateTime => match self {
                Value::DateTime(_) => Some(self),
                Value::DateTimeTz(t) => Some(Value::DateTime(t.naive_utc())),
                Value::Date(d) => Some(Value::DateTime(d.and_time(NaiveTime::MIN))),
                ref v => parse_datetime(&v.to_text()).map(Value::DateTime),
            },
            LogicalType::DateTimeTz => match self {
                Value::DateTimeTz(_) => Some(self),
                Value::DateTime(t) => Some(Value::DateTimeTz(t.and_utc())),
                Value::Date(d) => Some(Value::DateTimeTz(d.and_time(NaiveTime::MIN).and_utc())),
                ref v => parse_datetime_tz(&v.to_text()).map(Value::DateTimeTz),
            },
            LogicalType::Uuid => match self {
                Value::Uuid(_) => Some(self),
                Value::Bytes(ref b) if b.len() == 16 => Uuid::from_slice(b).ok().map(Value::Uuid),
                ref v => Uuid::parse_str(v.to_text().trim()).ok().map(Value::Uuid),
            },
            LogicalType::Json => match self {
                Value::Json(_) => Some(self),
                Value::String(s) => Some(Value::Json(
                    serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s)),
                )),
                ref v => Some(Value::Json(v.to_json())),
            },
            LogicalType::Bytes => match self {
                Value::Bytes(_) => Some(self),
                Value::Uuid(u) => Some(Value::Bytes(u.as_bytes().to_vec())),
                Value::ObjectId(o) => Some(Value::Bytes(o.to_vec())),
                Value::String(s) => Some(Value::Bytes(parse_bytea_text(&s).unwrap_or_else(|| s.into_bytes()))),
                ref v => Some(Value::Bytes(v.to_text().into_owned().into_bytes())),
            },
            LogicalType::ObjectId => match self {
                Value::ObjectId(_) => Some(self),
                Value::Bytes(ref b) if b.len() == 12 => {
                    let mut o = [0u8; 12];
                    o.copy_from_slice(b);
                    Some(Value::ObjectId(o))
                }
                ref v => parse_object_id(v.to_text().trim()).map(Value::ObjectId),
            },
        };
        result.ok_or_else(|| DMError::Error(format!("cannot convert value to {:?}", target)))
    }

    /// Parses a textual representation (e.g. PostgreSQL logical decoding text output or SQLite
    /// text) into a value of the given type.
    pub fn parse_text(text: &str, target: LogicalType) -> DMResult<Value> {
        match target {
            LogicalType::String => Ok(Value::String(text.to_string())),
            LogicalType::Bool => Value::String(text.to_string()).coerce(target),
            LogicalType::Bytes => Ok(Value::Bytes(
                parse_bytea_text(text).unwrap_or_else(|| text.as_bytes().to_vec()),
            )),
            LogicalType::Json => Ok(Value::Json(parse_json_or_pg_array(text))),
            _ => Value::String(text.to_string()).coerce(target),
        }
    }
}

pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

pub fn from_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

fn parse_object_id(s: &str) -> Option<[u8; 12]> {
    let bytes = from_hex(s)?;
    if bytes.len() != 12 {
        return None;
    }
    let mut o = [0u8; 12];
    o.copy_from_slice(&bytes);
    Some(o)
}

/// PostgreSQL bytea text output (`\x0102...`).
fn parse_bytea_text(s: &str) -> Option<Vec<u8>> {
    s.strip_prefix("\\x").and_then(from_hex)
}

fn parse_integer_text(s: &str) -> Option<i64> {
    let t = s.trim();
    t.parse::<i64>().ok().or_else(|| {
        // "12.000" style decimals
        let f: f64 = t.parse().ok()?;
        (f.fract() == 0.0 && f.abs() < 9.0e15).then_some(f as i64)
    })
}

fn is_decimal_text(s: &str) -> bool {
    let body = s.strip_prefix(['-', '+']).unwrap_or(s);
    if body.eq_ignore_ascii_case("nan") {
        return true;
    }
    let mut digits = 0;
    let mut dots = 0;
    for c in body.chars() {
        match c {
            '0'..='9' => digits += 1,
            '.' => dots += 1,
            _ => return false,
        }
    }
    digits > 0 && dots <= 1
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "y" | "yes" | "on" => Some(true),
        "f" | "false" | "0" | "n" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    let t = s.trim();
    NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .ok()
        .or_else(|| parse_datetime(t).map(|d| d.date()))
}

fn parse_time(s: &str) -> Option<NaiveTime> {
    let t = s.trim();
    NaiveTime::parse_from_str(t, "%H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveTime::parse_from_str(t, "%H:%M").ok())
}

fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    let t = s.trim();
    for f in DATETIME_FORMATS {
        if let Ok(d) = NaiveDateTime::parse_from_str(t, f) {
            return Some(d);
        }
    }
    if let Some(tz) = parse_datetime_tz_only(t) {
        return Some(tz.naive_utc());
    }
    NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .ok()
        .map(|d| d.and_time(NaiveTime::MIN))
}

fn parse_datetime_tz_only(t: &str) -> Option<DateTime<Utc>> {
    if let Ok(d) = DateTime::parse_from_rfc3339(t) {
        return Some(d.with_timezone(&Utc));
    }
    // PostgreSQL text output: "2024-01-02 10:11:12.123+00" / "+05:30"
    for f in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f%z"] {
        if let Ok(d) = DateTime::parse_from_str(t, f) {
            return Some(d.with_timezone(&Utc));
        }
    }
    None
}

fn parse_datetime_tz(s: &str) -> Option<DateTime<Utc>> {
    let t = s.trim();
    parse_datetime_tz_only(t).or_else(|| {
        // naive values are interpreted as UTC
        for f in DATETIME_FORMATS {
            if let Ok(d) = NaiveDateTime::parse_from_str(t, f) {
                return Some(d.and_utc());
            }
        }
        None
    })
}

/// Parses JSON text; one-dimensional PostgreSQL array literals (`{a,b}`) become JSON arrays.
fn parse_json_or_pg_array(text: &str) -> serde_json::Value {
    if let Ok(v) = serde_json::from_str(text) {
        return v;
    }
    if let Some(inner) = text.strip_prefix('{').and_then(|t| t.strip_suffix('}')) {
        let mut items = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut escaped = false;
        let mut quoted = false;
        let push = |items: &mut Vec<serde_json::Value>, cur: &mut String, quoted: bool| {
            let v = if !quoted && cur == "NULL" {
                serde_json::Value::Null
            } else if !quoted {
                serde_json::from_str::<serde_json::Value>(cur)
                    .ok()
                    .filter(|v| v.is_number() || v.is_boolean())
                    .unwrap_or_else(|| serde_json::Value::String(cur.clone()))
            } else {
                serde_json::Value::String(cur.clone())
            };
            items.push(v);
            cur.clear();
        };
        if inner.is_empty() {
            return serde_json::Value::Array(items);
        }
        for c in inner.chars() {
            if escaped {
                current.push(c);
                escaped = false;
                continue;
            }
            match c {
                '\\' if in_quotes => escaped = true,
                '"' => {
                    in_quotes = !in_quotes;
                    quoted = true;
                }
                ',' if !in_quotes => {
                    push(&mut items, &mut current, quoted);
                    quoted = false;
                }
                _ => current.push(c),
            }
        }
        push(&mut items, &mut current, quoted);
        return serde_json::Value::Array(items);
    }
    serde_json::Value::String(text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerce_integers_checks_range() {
        assert_eq!(Value::I64(5).coerce(LogicalType::Int16).unwrap(), Value::I16(5));
        assert!(Value::I64(70000).coerce(LogicalType::Int16).is_err());
        assert_eq!(Value::String("42".into()).coerce(LogicalType::Int32).unwrap(), Value::I32(42));
    }

    #[test]
    fn coerce_dates_and_times() {
        let dt = Value::String("2024-01-02 10:11:12.5".into()).coerce(LogicalType::DateTime).unwrap();
        assert_eq!(dt.to_text(), "2024-01-02 10:11:12.500");
        let tz = Value::String("2024-01-02 10:11:12+02".into()).coerce(LogicalType::DateTimeTz).unwrap();
        assert_eq!(tz.to_text(), "2024-01-02T08:11:12+00:00");
        let d = Value::String("2024-01-02T00:00:00".into()).coerce(LogicalType::Date).unwrap();
        assert_eq!(d, Value::Date(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()));
    }

    #[test]
    fn decimal_keeps_exact_text() {
        assert_eq!(
            Value::String("123.4500".into()).coerce(LogicalType::Decimal).unwrap(),
            Value::Decimal("123.4500".into())
        );
        assert!(Value::String("12a".into()).coerce(LogicalType::Decimal).is_err());
    }

    #[test]
    fn parses_pg_text_formats() {
        assert_eq!(Value::parse_text("t", LogicalType::Bool).unwrap(), Value::Bool(true));
        assert_eq!(Value::parse_text("\\x0aff", LogicalType::Bytes).unwrap(), Value::Bytes(vec![10, 255]));
        assert_eq!(
            Value::parse_text("{1,2,\"a,b\",NULL}", LogicalType::Json).unwrap(),
            Value::Json(serde_json::json!([1, 2, "a,b", null]))
        );
    }

    #[test]
    fn object_id_round_trip() {
        let oid = Value::String("65a1b2c3d4e5f60718293a4b".into()).coerce(LogicalType::ObjectId).unwrap();
        assert_eq!(oid.to_text(), "65a1b2c3d4e5f60718293a4b");
    }
}
