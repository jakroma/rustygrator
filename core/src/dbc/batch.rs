//! Column-oriented batches (PERF-13): the unit moved from provider to receiver and the layout of
//! a `dbc` v3 block (FMT-2). Values of a column live in one typed vector, strings and bytes in one
//! offsets array plus one data buffer, so building and reading a batch does not allocate per
//! value.

use std::borrow::Cow;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use uuid::Uuid;

use crate::{
    models::value::{to_hex, LogicalType, Value},
    DMError, DMResult,
};

/// Typed storage of one column. Null slots keep a default value.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Bool(Vec<bool>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    /// Decimal / String / Json text / Bytes: `offsets.len() == rows + 1`.
    Var { offsets: Vec<u32>, data: Vec<u8> },
    /// Days from 0001-01-01 (CE).
    Date(Vec<i32>),
    /// Nanoseconds since midnight.
    Time(Vec<i64>),
    /// Microseconds since the Unix epoch (DateTime and DateTimeTz).
    Timestamp(Vec<i64>),
    Uuid(Vec<[u8; 16]>),
    ObjectId(Vec<[u8; 12]>),
}

impl ColumnData {
    pub fn new(kind: LogicalType, capacity: usize) -> Self {
        use LogicalType as L;
        match kind {
            L::Bool => ColumnData::Bool(Vec::with_capacity(capacity)),
            L::Int16 => ColumnData::I16(Vec::with_capacity(capacity)),
            L::Int32 => ColumnData::I32(Vec::with_capacity(capacity)),
            L::Int64 => ColumnData::I64(Vec::with_capacity(capacity)),
            L::Float32 => ColumnData::F32(Vec::with_capacity(capacity)),
            L::Float64 => ColumnData::F64(Vec::with_capacity(capacity)),
            L::Decimal | L::String | L::Json | L::Bytes => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnData::Var { offsets, data: Vec::with_capacity(capacity * 16) }
            }
            L::Date => ColumnData::Date(Vec::with_capacity(capacity)),
            L::Time => ColumnData::Time(Vec::with_capacity(capacity)),
            L::DateTime | L::DateTimeTz => ColumnData::Timestamp(Vec::with_capacity(capacity)),
            L::Uuid => ColumnData::Uuid(Vec::with_capacity(capacity)),
            L::ObjectId => ColumnData::ObjectId(Vec::with_capacity(capacity)),
        }
    }

    fn clear(&mut self) {
        match self {
            ColumnData::Bool(v) => v.clear(),
            ColumnData::I16(v) => v.clear(),
            ColumnData::I32(v) => v.clear(),
            ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => v.clear(),
            ColumnData::F32(v) => v.clear(),
            ColumnData::F64(v) => v.clear(),
            ColumnData::Var { offsets, data } => {
                offsets.truncate(1);
                data.clear();
            }
            ColumnData::Date(v) => v.clear(),
            ColumnData::Uuid(v) => v.clear(),
            ColumnData::ObjectId(v) => v.clear(),
        }
    }

    fn push_default(&mut self) {
        match self {
            ColumnData::Bool(v) => v.push(false),
            ColumnData::I16(v) => v.push(0),
            ColumnData::I32(v) => v.push(0),
            ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => v.push(0),
            ColumnData::F32(v) => v.push(0.0),
            ColumnData::F64(v) => v.push(0.0),
            ColumnData::Var { offsets, data } => offsets.push(data.len() as u32),
            ColumnData::Date(v) => v.push(0),
            ColumnData::Uuid(v) => v.push([0; 16]),
            ColumnData::ObjectId(v) => v.push([0; 12]),
        }
    }

    fn append(&mut self, other: &ColumnData) {
        match (self, other) {
            (ColumnData::Bool(a), ColumnData::Bool(b)) => a.extend_from_slice(b),
            (ColumnData::I16(a), ColumnData::I16(b)) => a.extend_from_slice(b),
            (ColumnData::I32(a), ColumnData::I32(b)) => a.extend_from_slice(b),
            (ColumnData::I64(a), ColumnData::I64(b))
            | (ColumnData::Time(a), ColumnData::Time(b))
            | (ColumnData::Timestamp(a), ColumnData::Timestamp(b)) => a.extend_from_slice(b),
            (ColumnData::F32(a), ColumnData::F32(b)) => a.extend_from_slice(b),
            (ColumnData::F64(a), ColumnData::F64(b)) => a.extend_from_slice(b),
            (ColumnData::Var { offsets: ao, data: ad }, ColumnData::Var { offsets: bo, data: bd }) => {
                let base = ad.len() as u32;
                ao.extend(bo[1..].iter().map(|o| o + base));
                ad.extend_from_slice(bd);
            }
            (ColumnData::Date(a), ColumnData::Date(b)) => a.extend_from_slice(b),
            (ColumnData::Uuid(a), ColumnData::Uuid(b)) => a.extend_from_slice(b),
            (ColumnData::ObjectId(a), ColumnData::ObjectId(b)) => a.extend_from_slice(b),
            _ => unreachable!("columns of a batch keep their kind"),
        }
    }
}

/// One column: kind, null bitmap (bit set = null) and values.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub kind: LogicalType,
    pub nulls: Vec<u64>,
    pub null_count: usize,
    pub data: ColumnData,
}

/// Borrowed view of one cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cell<'a> {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal(&'a str),
    Str(&'a str),
    Json(&'a str),
    JsonValue(&'a serde_json::Value),
    Bytes(&'a [u8]),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    DateTimeTz(DateTime<Utc>),
    Uuid(Uuid),
    ObjectId([u8; 12]),
}

pub(crate) fn micros_to_datetime(us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(us).unwrap_or_default()
}

pub(crate) fn date_from_days(days: i32) -> NaiveDate {
    NaiveDate::from_num_days_from_ce_opt(days).unwrap_or_default()
}

pub(crate) fn time_from_nanos(n: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight_opt((n / 1_000_000_000) as u32, (n % 1_000_000_000) as u32).unwrap_or_default()
}

impl<'a> Cell<'a> {
    pub fn is_null(&self) -> bool {
        matches!(self, Cell::Null)
    }

    /// Borrowed view of a value (live transfer and tests).
    pub fn from_value(v: &'a Value) -> Cell<'a> {
        match v {
            Value::Null => Cell::Null,
            Value::Bool(b) => Cell::Bool(*b),
            Value::I16(i) => Cell::I16(*i),
            Value::I32(i) => Cell::I32(*i),
            Value::I64(i) => Cell::I64(*i),
            Value::F32(f) => Cell::F32(*f),
            Value::F64(f) => Cell::F64(*f),
            Value::Decimal(s) => Cell::Decimal(s),
            Value::String(s) => Cell::Str(s),
            Value::Bytes(b) => Cell::Bytes(b),
            Value::Date(d) => Cell::Date(*d),
            Value::Time(t) => Cell::Time(*t),
            Value::DateTime(t) => Cell::DateTime(*t),
            Value::DateTimeTz(t) => Cell::DateTimeTz(*t),
            Value::Uuid(u) => Cell::Uuid(*u),
            Value::Json(j) => Cell::JsonValue(j),
            Value::ObjectId(o) => Cell::ObjectId(*o),
        }
    }

    pub fn to_value(self) -> Value {
        match self {
            Cell::Null => Value::Null,
            Cell::Bool(b) => Value::Bool(b),
            Cell::I16(i) => Value::I16(i),
            Cell::I32(i) => Value::I32(i),
            Cell::I64(i) => Value::I64(i),
            Cell::F32(f) => Value::F32(f),
            Cell::F64(f) => Value::F64(f),
            Cell::Decimal(s) => Value::Decimal(s.to_string()),
            Cell::Str(s) => Value::String(s.to_string()),
            Cell::Json(s) => Value::Json(serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string()))),
            Cell::JsonValue(j) => Value::Json(j.clone()),
            Cell::Bytes(b) => Value::Bytes(b.to_vec()),
            Cell::Date(d) => Value::Date(d),
            Cell::Time(t) => Value::Time(t),
            Cell::DateTime(t) => Value::DateTime(t),
            Cell::DateTimeTz(t) => Value::DateTimeTz(t),
            Cell::Uuid(u) => Value::Uuid(u),
            Cell::ObjectId(o) => Value::ObjectId(o),
        }
    }

    /// Text form; borrows for text-like cells.
    pub fn to_text(&self) -> Cow<'a, str> {
        match *self {
            Cell::Decimal(s) | Cell::Str(s) | Cell::Json(s) => Cow::Borrowed(s),
            Cell::Bytes(b) => Cow::Owned(to_hex(b)),
            other => match other.to_value().to_text() {
                Cow::Borrowed(s) => Cow::Owned(s.to_string()),
                Cow::Owned(s) => Cow::Owned(s),
            },
        }
    }
}

impl Column {
    pub fn new(kind: LogicalType, capacity: usize) -> Self {
        Column { kind, nulls: Vec::with_capacity(capacity / 64 + 1), null_count: 0, data: ColumnData::new(kind, capacity) }
    }

    fn set_null_bit(&mut self, row: usize) {
        let w = row / 64;
        if self.nulls.len() <= w {
            self.nulls.resize(w + 1, 0);
        }
        self.nulls[w] |= 1 << (row % 64);
        self.null_count += 1;
    }

    pub fn is_null(&self, row: usize) -> bool {
        self.null_count > 0 && self.nulls.get(row / 64).is_some_and(|w| w & (1 << (row % 64)) != 0)
    }

    pub fn len(&self) -> usize {
        match &self.data {
            ColumnData::Bool(v) => v.len(),
            ColumnData::I16(v) => v.len(),
            ColumnData::I32(v) => v.len(),
            ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => v.len(),
            ColumnData::F32(v) => v.len(),
            ColumnData::F64(v) => v.len(),
            ColumnData::Var { offsets, .. } => offsets.len() - 1,
            ColumnData::Date(v) => v.len(),
            ColumnData::Uuid(v) => v.len(),
            ColumnData::ObjectId(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_null(&mut self) {
        let row = self.len();
        self.set_null_bit(row);
        self.data.push_default();
    }

    /// Appends text / bytes to a variable-length column without an intermediate allocation.
    pub fn push_bytes(&mut self, b: &[u8]) {
        if let ColumnData::Var { offsets, data } = &mut self.data {
            data.extend_from_slice(b);
            offsets.push(data.len() as u32);
        } else {
            unreachable!("push_bytes on a fixed-size column");
        }
    }

    pub fn push_str(&mut self, s: &str) -> DMResult<()> {
        match self.kind {
            LogicalType::String | LogicalType::Json | LogicalType::Bytes => {
                self.push_bytes(s.as_bytes());
                Ok(())
            }
            _ => self.push(Value::String(s.to_string())),
        }
    }

    /// Appends a value, converting it to the column kind (slow path for mismatching types).
    pub fn push(&mut self, v: Value) -> DMResult<()> {
        if v.is_null() {
            self.push_null();
            return Ok(());
        }
        let v = v.coerce(self.kind)?;
        match (&mut self.data, v) {
            (ColumnData::Bool(c), Value::Bool(b)) => c.push(b),
            (ColumnData::I16(c), Value::I16(i)) => c.push(i),
            (ColumnData::I32(c), Value::I32(i)) => c.push(i),
            (ColumnData::I64(c), Value::I64(i)) => c.push(i),
            (ColumnData::F32(c), Value::F32(f)) => c.push(f),
            (ColumnData::F64(c), Value::F64(f)) => c.push(f),
            (ColumnData::Var { offsets, data }, v) => {
                match v {
                    Value::Decimal(s) | Value::String(s) => data.extend_from_slice(s.as_bytes()),
                    Value::Bytes(b) => data.extend_from_slice(&b),
                    Value::Json(j) => serde_json::to_writer(&mut *data, &j)?,
                    other => return Err(DMError::Error(format!("unexpected {:?} for a text column", other))),
                }
                offsets.push(data.len() as u32);
            }
            (ColumnData::Date(c), Value::Date(d)) => c.push(d.num_days_from_ce()),
            (ColumnData::Time(c), Value::Time(t)) => c.push(t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64),
            (ColumnData::Timestamp(c), Value::DateTime(t)) => c.push(t.and_utc().timestamp_micros()),
            (ColumnData::Timestamp(c), Value::DateTimeTz(t)) => c.push(t.timestamp_micros()),
            (ColumnData::Uuid(c), Value::Uuid(u)) => c.push(*u.as_bytes()),
            (ColumnData::ObjectId(c), Value::ObjectId(o)) => c.push(o),
            (_, other) => return Err(DMError::Error(format!("unexpected {:?} for a {:?} column", other, self.kind))),
        }
        Ok(())
    }

    /// Appends a borrowed cell. Matching kinds (and integer widening / narrowing, text into text
    /// columns) take a fast path without allocating; other combinations go through
    /// [`Value::coerce`].
    pub fn push_cell(&mut self, cell: Cell<'_>) -> DMResult<()> {
        let overflow = || DMError::Error(format!("value out of range for {:?}", self.kind));
        match (&mut self.data, cell) {
            (_, Cell::Null) => self.push_null(),
            (ColumnData::Bool(v), Cell::Bool(b)) => v.push(b),
            (ColumnData::I16(v), Cell::I16(i)) => v.push(i),
            (ColumnData::I16(v), Cell::I32(i)) => v.push(i16::try_from(i).map_err(|_| overflow())?),
            (ColumnData::I16(v), Cell::I64(i)) => v.push(i16::try_from(i).map_err(|_| overflow())?),
            (ColumnData::I32(v), Cell::I16(i)) => v.push(i as i32),
            (ColumnData::I32(v), Cell::I32(i)) => v.push(i),
            (ColumnData::I32(v), Cell::I64(i)) => v.push(i32::try_from(i).map_err(|_| overflow())?),
            (ColumnData::I64(v), Cell::I16(i)) => v.push(i as i64),
            (ColumnData::I64(v), Cell::I32(i)) => v.push(i as i64),
            (ColumnData::I64(v), Cell::I64(i)) => v.push(i),
            (ColumnData::F32(v), Cell::F32(f)) => v.push(f),
            (ColumnData::F64(v), Cell::F64(f)) => v.push(f),
            (ColumnData::F64(v), Cell::F32(f)) => v.push(f as f64),
            (ColumnData::Var { offsets, data }, Cell::Str(s)) if matches!(self.kind, LogicalType::String | LogicalType::Bytes) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
            }
            (ColumnData::Var { offsets, data }, Cell::Json(s)) if self.kind == LogicalType::Json => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
            }
            (ColumnData::Var { offsets, data }, Cell::Decimal(s)) if matches!(self.kind, LogicalType::Decimal | LogicalType::String) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
            }
            (ColumnData::Var { offsets, data }, Cell::Bytes(b)) if self.kind == LogicalType::Bytes => {
                data.extend_from_slice(b);
                offsets.push(data.len() as u32);
            }
            (ColumnData::Date(v), Cell::Date(d)) => v.push(d.num_days_from_ce()),
            (ColumnData::Time(v), Cell::Time(t)) => v.push(t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64),
            (ColumnData::Timestamp(v), Cell::DateTime(t)) => v.push(t.and_utc().timestamp_micros()),
            (ColumnData::Timestamp(v), Cell::DateTimeTz(t)) => v.push(t.timestamp_micros()),
            (ColumnData::Uuid(v), Cell::Uuid(u)) => v.push(*u.as_bytes()),
            (ColumnData::ObjectId(v), Cell::ObjectId(o)) => v.push(o),
            (_, other) => return self.push(other.to_value()),
        }
        Ok(())
    }

    /// Raw bytes of a text / decimal / json value (no UTF-8 check); `None` for nulls and other
    /// kinds.
    #[inline]
    pub fn text_bytes(&self, row: usize) -> Option<&[u8]> {
        match &self.data {
            ColumnData::Var { offsets, data } if self.kind != LogicalType::Bytes && !self.is_null(row) => {
                Some(&data[offsets[row] as usize..offsets[row + 1] as usize])
            }
            _ => None,
        }
    }

    pub fn is_text(&self) -> bool {
        matches!(self.kind, LogicalType::String | LogicalType::Decimal | LogicalType::Json)
    }

    pub fn cell(&self, row: usize) -> Cell<'_> {
        if self.is_null(row) {
            return Cell::Null;
        }
        match &self.data {
            ColumnData::Bool(v) => Cell::Bool(v[row]),
            ColumnData::I16(v) => Cell::I16(v[row]),
            ColumnData::I32(v) => Cell::I32(v[row]),
            ColumnData::I64(v) => Cell::I64(v[row]),
            ColumnData::F32(v) => Cell::F32(v[row]),
            ColumnData::F64(v) => Cell::F64(v[row]),
            ColumnData::Var { offsets, data } => {
                let b = &data[offsets[row] as usize..offsets[row + 1] as usize];
                // text kinds are only ever filled from &str / String, so they are valid UTF-8
                let text = || std::str::from_utf8(b).unwrap_or_default();
                match self.kind {
                    LogicalType::Decimal => Cell::Decimal(text()),
                    LogicalType::Json => Cell::Json(text()),
                    LogicalType::Bytes => Cell::Bytes(b),
                    _ => Cell::Str(text()),
                }
            }
            ColumnData::Date(v) => Cell::Date(date_from_days(v[row])),
            ColumnData::Time(v) => Cell::Time(time_from_nanos(v[row])),
            ColumnData::Timestamp(v) => {
                let t = micros_to_datetime(v[row]);
                if self.kind == LogicalType::DateTimeTz {
                    Cell::DateTimeTz(t)
                } else {
                    Cell::DateTime(t.naive_utc())
                }
            }
            ColumnData::Uuid(v) => Cell::Uuid(Uuid::from_bytes(v[row])),
            ColumnData::ObjectId(v) => Cell::ObjectId(v[row]),
        }
    }

    fn clear(&mut self) {
        self.nulls.clear();
        self.null_count = 0;
        self.data.clear();
    }

    fn append(&mut self, other: &Column) {
        let base = self.len();
        if other.null_count > 0 {
            for r in 0..other.len() {
                if other.is_null(r) {
                    self.set_null_bit(base + r);
                }
            }
        }
        self.data.append(&other.data);
    }
}

/// Rows of one container, stored column by column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnBatch {
    pub columns: Vec<Column>,
    pub rows: usize,
}

impl ColumnBatch {
    pub fn new(kinds: &[LogicalType], capacity: usize) -> Self {
        ColumnBatch { columns: kinds.iter().map(|k| Column::new(*k, capacity)).collect(), rows: 0 }
    }

    pub fn for_definition(def: &crate::models::data_definitions::DataDefinition, capacity: usize) -> Self {
        let kinds: Vec<LogicalType> = def.properties_info.iter().map(|p| p.data_type).collect();
        Self::new(&kinds, capacity)
    }

    pub fn kinds(&self) -> Vec<LogicalType> {
        self.columns.iter().map(|c| c.kind).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Call after one value was pushed to every column.
    pub fn end_row(&mut self) {
        self.rows += 1;
        debug_assert!(self.columns.iter().all(|c| c.len() == self.rows));
    }

    pub fn push_row(&mut self, row: Vec<Value>) -> DMResult<()> {
        if row.len() != self.columns.len() {
            return Err(DMError::Error(format!("row has {} values, batch has {} columns", row.len(), self.columns.len())));
        }
        for (c, v) in self.columns.iter_mut().zip(row) {
            c.push(v)?;
        }
        self.end_row();
        Ok(())
    }

    pub fn cell(&self, row: usize, col: usize) -> Cell<'_> {
        self.columns[col].cell(row)
    }

    pub fn row_values(&self, row: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c.cell(row).to_value()).collect()
    }

    /// Appends all rows of `other` (same column kinds).
    pub fn append(&mut self, other: &ColumnBatch) {
        for (a, b) in self.columns.iter_mut().zip(&other.columns) {
            a.append(b);
        }
        self.rows += other.rows;
    }

    /// Copy of rows `start..end`.
    pub fn slice(&self, start: usize, end: usize) -> ColumnBatch {
        let mut out = ColumnBatch::new(&self.kinds(), end - start);
        for (o, c) in out.columns.iter_mut().zip(&self.columns) {
            for r in start..end {
                if c.is_null(r) {
                    o.set_null_bit(r - start);
                }
            }
            match (&mut o.data, &c.data) {
                (ColumnData::Bool(a), ColumnData::Bool(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::I16(a), ColumnData::I16(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::I32(a), ColumnData::I32(b)) | (ColumnData::Date(a), ColumnData::Date(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::I64(a), ColumnData::I64(b))
                | (ColumnData::Time(a), ColumnData::Time(b))
                | (ColumnData::Timestamp(a), ColumnData::Timestamp(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::F32(a), ColumnData::F32(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::F64(a), ColumnData::F64(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::Var { offsets: ao, data: ad }, ColumnData::Var { offsets: bo, data: bd }) => {
                    let base = bo[start];
                    ao.extend(bo[start + 1..=end].iter().map(|x| x - base));
                    ad.extend_from_slice(&bd[base as usize..bo[end] as usize]);
                }
                (ColumnData::Uuid(a), ColumnData::Uuid(b)) => a.extend_from_slice(&b[start..end]),
                (ColumnData::ObjectId(a), ColumnData::ObjectId(b)) => a.extend_from_slice(&b[start..end]),
                _ => unreachable!(),
            }
        }
        out.rows = end - start;
        out
    }

    pub fn clear(&mut self) {
        for c in &mut self.columns {
            c.clear();
        }
        self.rows = 0;
    }

    /// Approximate heap size in bytes (adaptive batching, PERF-16).
    pub fn memory_size(&self) -> usize {
        self.columns
            .iter()
            .map(|c| {
                c.nulls.len() * 8
                    + match &c.data {
                        ColumnData::Bool(v) => v.len(),
                        ColumnData::I16(v) => v.len() * 2,
                        ColumnData::I32(v) | ColumnData::Date(v) => v.len() * 4,
                        ColumnData::F32(v) => v.len() * 4,
                        ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => v.len() * 8,
                        ColumnData::F64(v) => v.len() * 8,
                        ColumnData::Var { offsets, data } => offsets.len() * 4 + data.len(),
                        ColumnData::Uuid(v) => v.len() * 16,
                        ColumnData::ObjectId(v) => v.len() * 12,
                    }
            })
            .sum()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn sample_kinds() -> Vec<LogicalType> {
        use LogicalType::*;
        vec![Int32, String, Int64, Float64, Decimal, Bool, Date, Time, DateTime, DateTimeTz, Uuid, Json, Bytes, ObjectId, Int16, Float32]
    }

    pub fn sample_row(i: i64) -> Vec<Value> {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().and_hms_micro_opt(1, 2, 3, (i % 1000) as u32).unwrap();
        vec![
            Value::I32(i as i32),
            if i % 3 == 0 { Value::Null } else { Value::String(format!("name \u{1e} {i}")) },
            Value::I64(i * 1_000_000),
            Value::F64(i as f64 / 3.0),
            Value::Decimal(format!("{}.{:02}", i, i % 100)),
            Value::Bool(i % 2 == 0),
            Value::Date(dt.date()),
            Value::Time(dt.time()),
            Value::DateTime(dt),
            Value::DateTimeTz(dt.and_utc()),
            Value::Uuid(Uuid::from_u128(i as u128)),
            Value::Json(serde_json::json!({"i": i, "a": [1, "x"]})),
            if i % 5 == 0 { Value::Null } else { Value::Bytes(vec![0, 0x1f, i as u8]) },
            Value::ObjectId([i as u8; 12]),
            Value::I16((i % 30000) as i16),
            Value::F32(i as f32 * 0.5),
        ]
    }

    #[test]
    fn push_and_read_back_every_kind() {
        let mut b = ColumnBatch::new(&sample_kinds(), 4);
        for i in 0..100 {
            b.push_row(sample_row(i)).unwrap();
        }
        assert_eq!(b.rows, 100);
        for i in 0..100 {
            assert_eq!(b.row_values(i as usize), sample_row(i));
        }
        assert!(b.cell(0, 1).is_null());
        assert_eq!(b.cell(1, 1), Cell::Str("name \u{1e} 1"));
    }

    #[test]
    fn append_keeps_nulls_and_offsets() {
        let mut a = ColumnBatch::new(&sample_kinds(), 4);
        let mut b = ColumnBatch::new(&sample_kinds(), 4);
        for i in 0..70 {
            a.push_row(sample_row(i)).unwrap();
        }
        for i in 70..150 {
            b.push_row(sample_row(i)).unwrap();
        }
        a.append(&b);
        assert_eq!(a.rows, 150);
        for i in 0..150 {
            assert_eq!(a.row_values(i as usize), sample_row(i));
        }
        let s = a.slice(65, 140);
        assert_eq!(s.rows, 75);
        for i in 0..75 {
            assert_eq!(s.row_values(i), sample_row(i as i64 + 65));
        }
        a.clear();
        assert!(a.is_empty());
        a.push_row(sample_row(3)).unwrap();
        assert_eq!(a.row_values(0), sample_row(3));
    }

    #[test]
    fn coerces_on_push() {
        let mut c = Column::new(LogicalType::Int32, 2);
        c.push_cell(Cell::I64(5)).unwrap();
        c.push_cell(Cell::Str("6")).unwrap();
        assert!(c.push_cell(Cell::I64(1 << 40)).is_err());
        assert_eq!((c.cell(0), c.cell(1)), (Cell::I32(5), Cell::I32(6)));
        let mut b = ColumnBatch::new(&[LogicalType::Int16, LogicalType::Decimal], 1);
        b.push_row(vec![Value::String("7".into()), Value::F64(1.5)]).unwrap();
        assert_eq!(b.row_values(0), vec![Value::I16(7), Value::Decimal("1.5".into())]);
        assert!(b.push_row(vec![Value::I64(1 << 40), Value::Null]).is_err());
    }
}
