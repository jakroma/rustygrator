//! PostgreSQL <-> logical type mapping (DB-1: all common types mapped).

use std::error::Error;

use bytes::{BufMut, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use tokio_postgres::{
    binary_copy::BinaryCopyOutRow,
    types::{to_sql_checked, FromSql, IsNull, ToSql, Type},
};
use uuid::Uuid;

use crate::{
    dbc::batch::{Cell, Column},
    models::value::{LogicalType, Value},
    DMError, DMResult,
};

/// Logical type for a column described by `information_schema.columns.data_type`.
pub fn logical_type_for(data_type: &str) -> LogicalType {
    match data_type {
        "smallint" => LogicalType::Int16,
        "integer" => LogicalType::Int32,
        "bigint" => LogicalType::Int64,
        "real" => LogicalType::Float32,
        "double precision" => LogicalType::Float64,
        "numeric" | "money" => LogicalType::Decimal,
        "boolean" => LogicalType::Bool,
        "date" => LogicalType::Date,
        "time without time zone" => LogicalType::Time,
        "timestamp without time zone" => LogicalType::DateTime,
        "timestamp with time zone" => LogicalType::DateTimeTz,
        "uuid" => LogicalType::Uuid,
        "json" | "jsonb" | "ARRAY" => LogicalType::Json,
        "bytea" => LogicalType::Bytes,
        // text, character varying, character, name, citext, enums (USER-DEFINED), inet, interval,
        // time with time zone, xml, ... are read as their text representation
        _ => LogicalType::String,
    }
}

/// SELECT expression that reads the column in a form matching its logical type.
pub fn select_expression(quoted_column: &str, data_type: &str) -> String {
    match data_type {
        "numeric" => format!("{}::text", quoted_column),
        "money" => format!("{}::numeric::text", quoted_column),
        "json" => format!("{}::jsonb", quoted_column),
        "ARRAY" => format!("to_jsonb({})", quoted_column),
        _ => match logical_type_for(data_type) {
            LogicalType::String
                if !matches!(data_type, "text" | "character varying" | "character" | "name") =>
            {
                format!("{}::text", quoted_column)
            }
            _ => quoted_column.to_string(),
        },
    }
}

/// Target column type for a logical type.
pub fn pg_type_for(t: LogicalType) -> &'static str {
    match t {
        LogicalType::Int16 => "smallint",
        LogicalType::Int32 => "integer",
        LogicalType::Int64 => "bigint",
        LogicalType::Float32 => "real",
        LogicalType::Float64 => "double precision",
        LogicalType::Decimal => "numeric",
        LogicalType::String => "text",
        LogicalType::Bool => "boolean",
        LogicalType::Date => "date",
        LogicalType::Time => "time",
        LogicalType::DateTime => "timestamp",
        LogicalType::DateTimeTz => "timestamptz",
        LogicalType::Uuid => "uuid",
        LogicalType::Json => "jsonb",
        LogicalType::Bytes => "bytea",
        LogicalType::ObjectId => "varchar(24)",
    }
}

/// Raw binary value of any type (zero copy): used for text-like columns and json.
pub struct Raw<'a>(pub &'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, BoxError> {
        Ok(Raw(raw))
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// Reads column `index` of a binary COPY row (columns produced by [`select_expression`]) into
/// `col` without intermediate allocations for text, bytes and json.
///
/// With `utf8` (server encoding is UTF8, so text is valid UTF-8) text, decimal and json values
/// are appended as raw bytes without a second UTF-8 check (PERF-9).
pub fn push_copy_value(row: &BinaryCopyOutRow, index: usize, ty: &Type, col: &mut Column, utf8: bool) -> DMResult<()> {
    if utf8 && matches!(col.kind, LogicalType::String | LogicalType::Decimal | LogicalType::Json) {
        match row.try_get::<Option<Raw>>(index)? {
            None => col.push_null(),
            Some(r) if col.kind == LogicalType::Json && *ty == Type::JSONB => col.push_bytes(r.0.get(1..).unwrap_or_default()),
            Some(r) => col.push_bytes(r.0),
        }
        return Ok(());
    }
    let cell = match col.kind {
        LogicalType::Int16 => row.try_get::<Option<i16>>(index)?.map(Cell::I16),
        LogicalType::Int32 => row.try_get::<Option<i32>>(index)?.map(Cell::I32),
        LogicalType::Int64 => row.try_get::<Option<i64>>(index)?.map(Cell::I64),
        LogicalType::Float32 => row.try_get::<Option<f32>>(index)?.map(Cell::F32),
        LogicalType::Float64 => row.try_get::<Option<f64>>(index)?.map(Cell::F64),
        LogicalType::Bool => row.try_get::<Option<bool>>(index)?.map(Cell::Bool),
        LogicalType::Date => row.try_get::<Option<NaiveDate>>(index)?.map(Cell::Date),
        LogicalType::Time => row.try_get::<Option<NaiveTime>>(index)?.map(Cell::Time),
        LogicalType::DateTime => row.try_get::<Option<NaiveDateTime>>(index)?.map(Cell::DateTime),
        LogicalType::DateTimeTz => row.try_get::<Option<DateTime<Utc>>>(index)?.map(Cell::DateTimeTz),
        LogicalType::Uuid => row.try_get::<Option<Uuid>>(index)?.map(Cell::Uuid),
        LogicalType::Bytes => row.try_get::<Option<Raw>>(index)?.map(|r| Cell::Bytes(r.0)),
        LogicalType::Json => match row.try_get::<Option<Raw>>(index)? {
            // jsonb binary format: version byte 1 followed by the json text
            Some(r) if *ty == Type::JSONB => Some(Cell::Json(std::str::from_utf8(r.0.get(1..).unwrap_or_default()).map_err(|e| DMError::Error(e.to_string()))?)),
            Some(r) => Some(Cell::Json(std::str::from_utf8(r.0).map_err(|e| DMError::Error(e.to_string()))?)),
            None => None,
        },
        LogicalType::Decimal => row.try_get::<Option<&str>>(index)?.map(Cell::Decimal),
        LogicalType::String | LogicalType::ObjectId => row.try_get::<Option<&str>>(index)?.map(Cell::Str),
    };
    col.push_cell(cell.unwrap_or(Cell::Null))
}

/// Binary parameter wrapper: converts a cell into whatever PostgreSQL type the target column
/// has (used by binary COPY and parameterized statements).
#[derive(Debug)]
pub struct PgValue<'a>(pub Cell<'a>);

/// Text-like value given as UTF-8 bytes that are already known to be valid (text, decimal and
/// json columns of a batch): written without re-validating or re-encoding (PERF-9).
#[derive(Debug)]
pub enum PgParam<'a> {
    Cell(Cell<'a>),
    Text(LogicalType, &'a [u8]),
}

impl ToSql for PgParam<'_> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        match *self {
            PgParam::Cell(c) => PgValue(c).to_sql(ty, out),
            PgParam::Text(kind, b) => match (kind, ty) {
                (LogicalType::Decimal, &Type::NUMERIC) => {
                    encode_numeric_bytes(b, out)?;
                    Ok(IsNull::No)
                }
                (LogicalType::Json, &Type::JSONB) => {
                    out.put_u8(1);
                    out.put_slice(b);
                    Ok(IsNull::No)
                }
                (_, &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME | &Type::JSON) => {
                    out.put_slice(b);
                    Ok(IsNull::No)
                }
                (kind, _) => {
                    let s = std::str::from_utf8(b)?;
                    let cell = match kind {
                        LogicalType::Decimal => Cell::Decimal(s),
                        LogicalType::Json => Cell::Json(s),
                        _ => Cell::Str(s),
                    };
                    PgValue(cell).to_sql(ty, out)
                }
            },
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

type BoxError = Box<dyn Error + Sync + Send>;

fn conv<'a>(c: Cell<'a>, t: LogicalType) -> Result<Value, BoxError> {
    c.to_value().coerce(t).map_err(|e| e.to_string().into())
}

impl ToSql for PgValue<'_> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        let c = self.0;
        macro_rules! via {
            ($lt:expr, $cell:path, $variant:path) => {
                match c {
                    $cell(x) => x.to_sql(ty, out),
                    other => match conv(other, $lt)? {
                        $variant(x) => x.to_sql(ty, out),
                        _ => unreachable!(),
                    },
                }
            };
        }
        match (ty, c) {
            (_, Cell::Null) => Ok(IsNull::Yes),
            (&Type::INT2, _) => via!(LogicalType::Int16, Cell::I16, Value::I16),
            (&Type::INT4, _) => via!(LogicalType::Int32, Cell::I32, Value::I32),
            (&Type::INT8, _) => via!(LogicalType::Int64, Cell::I64, Value::I64),
            (&Type::FLOAT4, _) => via!(LogicalType::Float32, Cell::F32, Value::F32),
            (&Type::FLOAT8, _) => via!(LogicalType::Float64, Cell::F64, Value::F64),
            (&Type::BOOL, _) => via!(LogicalType::Bool, Cell::Bool, Value::Bool),
            (&Type::DATE, _) => via!(LogicalType::Date, Cell::Date, Value::Date),
            (&Type::TIME, _) => via!(LogicalType::Time, Cell::Time, Value::Time),
            (&Type::TIMESTAMP, _) => via!(LogicalType::DateTime, Cell::DateTime, Value::DateTime),
            (&Type::TIMESTAMPTZ, _) => via!(LogicalType::DateTimeTz, Cell::DateTimeTz, Value::DateTimeTz),
            (&Type::UUID, _) => via!(LogicalType::Uuid, Cell::Uuid, Value::Uuid),
            (&Type::BYTEA, Cell::Bytes(b)) => {
                out.put_slice(b);
                Ok(IsNull::No)
            }
            (&Type::BYTEA, other) => match conv(other, LogicalType::Bytes)? {
                Value::Bytes(b) => b.to_sql(ty, out),
                _ => unreachable!(),
            },
            (&Type::JSONB, Cell::Json(s)) => {
                out.put_u8(1);
                out.put_slice(s.as_bytes());
                Ok(IsNull::No)
            }
            (&Type::JSON, Cell::Json(s)) => {
                out.put_slice(s.as_bytes());
                Ok(IsNull::No)
            }
            (&Type::JSON | &Type::JSONB, Cell::JsonValue(j)) => j.to_sql(ty, out),
            (&Type::JSON | &Type::JSONB, other) => match conv(other, LogicalType::Json)? {
                Value::Json(j) => j.to_sql(ty, out),
                _ => unreachable!(),
            },
            (&Type::NUMERIC, _) => {
                encode_numeric(&c.to_text(), out)?;
                Ok(IsNull::No)
            }
            _ => {
                // text, varchar, bpchar, name and enum labels share the text wire format
                out.put_slice(c.to_text().as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Values with more than 64 base-10000 digits (rare): heap buffer.
fn encode_numeric_large(negative: bool, int_part: &[u8], frac_part: &[u8], out: &mut BytesMut) -> Result<(), BoxError> {
    let mut digits: Vec<i16> = Vec::new();
    let lead = int_part.len() % 4;
    let g = |c: &[u8], pad: usize| c.iter().fold(0i16, |v, d| v * 10 + (d - b'0') as i16) * 10i16.pow(pad as u32);
    if lead > 0 {
        digits.push(g(&int_part[..lead], 0));
    }
    digits.extend(int_part[lead..].chunks_exact(4).map(|c| g(c, 0)));
    let int_groups = digits.len() as i16;
    digits.extend(frac_part.chunks(4).map(|c| g(c, 4 - c.len())));
    let mut weight = int_groups - 1;
    let start = digits.iter().position(|d| *d != 0).unwrap_or(digits.len());
    weight -= start as i16;
    let end = digits.iter().rposition(|d| *d != 0).map(|e| e + 1).unwrap_or(start);
    let digits = &digits[start..end.max(start)];
    if digits.is_empty() {
        weight = 0;
    }
    out.put_i16(digits.len() as i16);
    out.put_i16(weight);
    out.put_u16(if negative && !digits.is_empty() { 0x4000 } else { 0 });
    out.put_u16(frac_part.len() as u16);
    for d in digits {
        out.put_i16(*d);
    }
    Ok(())
}

/// Encodes decimal text in the PostgreSQL binary NUMERIC format (base 10000 digits), without
/// allocating (PERF-9 hotspot).
pub fn encode_numeric(text: &str, out: &mut BytesMut) -> Result<(), BoxError> {
    encode_numeric_bytes(text.as_bytes(), out)
}

pub fn encode_numeric_bytes(text: &[u8], out: &mut BytesMut) -> Result<(), BoxError> {
    let t = text.trim_ascii();
    if t.eq_ignore_ascii_case(b"nan") {
        out.put_i16(0);
        out.put_i16(0);
        out.put_u16(0xC000);
        out.put_u16(0);
        return Ok(());
    }
    let invalid = || -> BoxError { format!("invalid numeric value '{}'", String::from_utf8_lossy(text)).into() };
    let (negative, body) = match t.first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let dot = body.iter().position(|b| *b == b'.');
    let (int_part, frac_part) = match dot {
        Some(i) => (&body[..i], &body[i + 1..]),
        None => (body, &body[..0]),
    };
    if (int_part.is_empty() && frac_part.is_empty()) || !int_part.iter().chain(frac_part).all(u8::is_ascii_digit) {
        return Err(invalid());
    }
    let first_nz = int_part.iter().position(|b| *b != b'0').unwrap_or(int_part.len());
    let int_part = &int_part[first_nz..];
    let dscale = frac_part.len() as u16;

    // base-10000 digits: integer part grouped from the right, fraction from the left
    #[inline]
    fn group(chunk: &[u8], pad: usize) -> i16 {
        let mut v: i16 = 0;
        for d in chunk {
            v = v * 10 + (d - b'0') as i16;
        }
        for _ in 0..pad {
            v *= 10;
        }
        v
    }
    let int_groups = int_part.len().div_ceil(4);
    let total = int_groups + frac_part.len().div_ceil(4);
    if total > 64 {
        return encode_numeric_large(negative, int_part, frac_part, out);
    }
    let mut digits = [0i16; 64];
    let lead = int_part.len() % 4;
    let mut gi = 0;
    if lead > 0 {
        digits[0] = group(&int_part[..lead], 0);
        gi = 1;
    }
    for c in int_part[lead..].chunks_exact(4) {
        digits[gi] = group(c, 0);
        gi += 1;
    }
    for c in frac_part.chunks(4) {
        digits[gi] = group(c, 4 - c.len());
        gi += 1;
    }
    let mut weight = int_groups as i16 - 1;
    let mut start = 0;
    while start < total && digits[start] == 0 {
        start += 1;
        weight -= 1;
    }
    let mut end = total;
    while end > start && digits[end - 1] == 0 {
        end -= 1;
    }
    if start == end {
        weight = 0;
    }
    // header + digits assembled on the stack, one copy into the output
    let n = end - start;
    let mut bytes = [0u8; 8 + 128];
    bytes[0..2].copy_from_slice(&(n as i16).to_be_bytes());
    bytes[2..4].copy_from_slice(&weight.to_be_bytes());
    bytes[4..6].copy_from_slice(&(if negative && n > 0 { 0x4000u16 } else { 0 }).to_be_bytes());
    bytes[6..8].copy_from_slice(&dscale.to_be_bytes());
    for (i, d) in digits[start..end].iter().enumerate() {
        bytes[8 + i * 2..10 + i * 2].copy_from_slice(&d.to_be_bytes());
    }
    out.put_slice(&bytes[..8 + n * 2]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(s: &str) -> Vec<i16> {
        let mut b = BytesMut::new();
        encode_numeric(s, &mut b).unwrap();
        b.chunks(2).map(|c| i16::from_be_bytes([c[0], c[1]])).collect()
    }

    #[test]
    fn numeric_encoding() {
        // ndigits, weight, sign, dscale, digits...
        assert_eq!(enc("0"), vec![0, 0, 0, 0]);
        assert_eq!(enc("12345.678"), vec![3, 1, 0, 3, 1, 2345, 6780]);
        assert_eq!(enc("-0.0001"), vec![1, -1, 0x4000, 4, 1]);
        assert_eq!(enc("10000"), vec![1, 1, 0, 0, 1]);
        assert_eq!(enc("1.50"), vec![2, 0, 0, 2, 1, 5000]);
        assert_eq!(enc(" 007.0100 "), vec![2, 0, 0, 4, 7, 100]);
        // more than 64 base-10000 digits (heap path)
        let big = format!("{}.{}", "9".repeat(200), "1".repeat(100));
        let e = enc(&big);
        assert_eq!((e[0], e[1], e[3]), (75, 49, 100));
        let mut b = BytesMut::new();
        assert!(encode_numeric("1.2.3", &mut b).is_err());
        assert!(encode_numeric("", &mut b).is_err());
    }

    #[test]
    fn type_mapping_round_trips() {
        for t in ["smallint", "integer", "bigint", "real", "double precision", "numeric", "boolean", "date", "uuid", "jsonb", "bytea", "text"] {
            let lt = logical_type_for(t);
            assert_eq!(logical_type_for(pg_type_for(lt).split('(').next().unwrap()), lt, "{t}");
        }
    }
}
