//! BSON <-> logical type mapping and schema analysis ("no schema resolve strategy").

use chrono::DateTime;
use mongodb::bson::{
    self,
    raw::{RawBinaryRef, RawBsonRef, RawDocumentBuf},
    spec::BinarySubtype,
    Binary, Bson, Document, RawBson,
};

use crate::{
    dbc::batch::{Cell, Column},
    models::{
        data_definitions::JsonShape,
        value::{LogicalType, Value},
    },
    DMResult,
};

/// Appends a BSON value to a column; strings, numbers, dates and ids without allocating.
pub fn push_bson(col: &mut Column, b: Option<&Bson>) -> DMResult<()> {
    let cell = match b {
        None | Some(Bson::Null) | Some(Bson::Undefined) => Cell::Null,
        Some(Bson::String(s)) => Cell::Str(s),
        Some(Bson::Int32(i)) => Cell::I32(*i),
        Some(Bson::Int64(i)) => Cell::I64(*i),
        Some(Bson::Double(f)) => Cell::F64(*f),
        Some(Bson::Boolean(v)) => Cell::Bool(*v),
        Some(Bson::ObjectId(o)) => Cell::ObjectId(o.bytes()),
        Some(Bson::DateTime(d)) => match DateTime::from_timestamp_millis(d.timestamp_millis()) {
            Some(t) => Cell::DateTimeTz(t),
            None => Cell::Null,
        },
        Some(Bson::Binary(bin)) if !(bin.subtype == BinarySubtype::Uuid && bin.bytes.len() == 16) => Cell::Bytes(&bin.bytes),
        Some(other) => return col.push(bson_to_value(other)),
    };
    col.push_cell(cell)
}

/// Appends a cell to a raw BSON document (strings and bytes are copied once, straight into the
/// document buffer).
pub fn append_cell(doc: &mut RawDocumentBuf, key: &str, cell: Cell<'_>) {
    match cell {
        Cell::Null => doc.append_ref(key, RawBsonRef::Null),
        Cell::Bool(b) => doc.append_ref(key, RawBsonRef::Boolean(b)),
        Cell::I16(i) => doc.append_ref(key, RawBsonRef::Int32(i as i32)),
        Cell::I32(i) => doc.append_ref(key, RawBsonRef::Int32(i)),
        Cell::I64(i) => doc.append_ref(key, RawBsonRef::Int64(i)),
        Cell::F32(f) => doc.append_ref(key, RawBsonRef::Double(f as f64)),
        Cell::F64(f) => doc.append_ref(key, RawBsonRef::Double(f)),
        Cell::Str(s) => doc.append_ref(key, RawBsonRef::String(s)),
        Cell::Bytes(b) => doc.append_ref(key, RawBsonRef::Binary(RawBinaryRef { subtype: BinarySubtype::Generic, bytes: b })),
        Cell::ObjectId(o) => doc.append_ref(key, RawBsonRef::ObjectId(bson::oid::ObjectId::from_bytes(o))),
        Cell::DateTimeTz(t) => doc.append_ref(key, RawBsonRef::DateTime(bson::DateTime::from_millis(t.timestamp_millis()))),
        Cell::DateTime(t) => doc.append_ref(key, RawBsonRef::DateTime(bson::DateTime::from_millis(t.and_utc().timestamp_millis()))),
        Cell::Date(d) => doc.append_ref(
            key,
            RawBsonRef::DateTime(bson::DateTime::from_millis(d.and_time(chrono::NaiveTime::MIN).and_utc().timestamp_millis())),
        ),
        Cell::Decimal(s) => match s.parse::<bson::Decimal128>() {
            Ok(d) => doc.append_ref(key, RawBsonRef::Decimal128(d)),
            Err(_) => doc.append_ref(key, RawBsonRef::String(s)),
        },
        other => {
            let b = value_to_bson(&other.to_value());
            match RawBson::try_from(b) {
                Ok(r) => doc.append(key, r),
                Err(_) => doc.append_ref(key, RawBsonRef::Null),
            }
        }
    }
}

/// `bsonType` used in the `$jsonSchema` validator; `None` = no type restriction.
pub fn get_bson_type(t: LogicalType) -> Option<&'static str> {
    Some(match t {
        LogicalType::Int16 | LogicalType::Int32 => "int",
        LogicalType::Int64 => "long",
        LogicalType::Float32 | LogicalType::Float64 => "double",
        LogicalType::Decimal => "decimal",
        LogicalType::String | LogicalType::Time | LogicalType::Uuid => "string",
        LogicalType::Bool => "bool",
        LogicalType::Date | LogicalType::DateTime | LogicalType::DateTimeTz => "date",
        LogicalType::Bytes => "binData",
        LogicalType::ObjectId => "objectId",
        LogicalType::Json => return None,
    })
}

pub fn bson_to_value(b: &Bson) -> Value {
    match b {
        Bson::Double(f) => Value::F64(*f),
        Bson::String(s) => Value::String(s.clone()),
        Bson::Array(_) | Bson::Document(_) => Value::Json(b.clone().into_relaxed_extjson()),
        Bson::Boolean(v) => Value::Bool(*v),
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Int32(i) => Value::I32(*i),
        Bson::Int64(i) => Value::I64(*i),
        Bson::DateTime(d) => DateTime::from_timestamp_millis(d.timestamp_millis())
            .map(Value::DateTimeTz)
            .unwrap_or(Value::Null),
        Bson::ObjectId(o) => Value::ObjectId(o.bytes()),
        Bson::Binary(bin) if bin.subtype == BinarySubtype::Uuid && bin.bytes.len() == 16 => {
            uuid::Uuid::from_slice(&bin.bytes).map(Value::Uuid).unwrap_or(Value::Bytes(bin.bytes.clone()))
        }
        Bson::Binary(bin) => Value::Bytes(bin.bytes.clone()),
        Bson::Decimal128(d) => Value::Decimal(d.to_string()),
        Bson::Timestamp(t) => Value::I64(((t.time as i64) << 32) | t.increment as i64),
        other => Value::String(other.to_string()),
    }
}

/// Converts a value (already coerced to its column type) to BSON.
pub fn value_to_bson(v: &Value) -> Bson {
    match v {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::I16(i) => Bson::Int32(*i as i32),
        Value::I32(i) => Bson::Int32(*i),
        Value::I64(i) => Bson::Int64(*i),
        Value::F32(f) => Bson::Double(*f as f64),
        Value::F64(f) => Bson::Double(*f),
        Value::Decimal(s) => s.parse::<bson::Decimal128>().map(Bson::Decimal128).unwrap_or_else(|_| Bson::String(s.clone())),
        Value::String(s) => Bson::String(s.clone()),
        Value::Bytes(b) => Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: b.clone() }),
        Value::Date(d) => Bson::DateTime(bson::DateTime::from_millis(d.and_time(chrono::NaiveTime::MIN).and_utc().timestamp_millis())),
        Value::DateTime(t) => Bson::DateTime(bson::DateTime::from_millis(t.and_utc().timestamp_millis())),
        Value::DateTimeTz(t) => Bson::DateTime(bson::DateTime::from_millis(t.timestamp_millis())),
        Value::Uuid(u) => Bson::String(u.to_string()),
        Value::Json(j) => Bson::try_from(j.clone()).unwrap_or_else(|_| Bson::String(j.to_string())),
        Value::ObjectId(o) => Bson::ObjectId(bson::oid::ObjectId::from_bytes(*o)),
        Value::Time(_) => Bson::String(v.to_text().into_owned()),
    }
}

const K_I32: u16 = 1;
const K_I64: u16 = 1 << 1;
const K_F64: u16 = 1 << 2;
const K_DEC: u16 = 1 << 3;
const K_STR: u16 = 1 << 4;
const K_BOOL: u16 = 1 << 5;
const K_DATE: u16 = 1 << 6;
const K_OID: u16 = 1 << 7;
const K_BIN: u16 = 1 << 8;
const K_UUID: u16 = 1 << 9;
const K_DOC: u16 = 1 << 10;
const K_OTHER: u16 = 1 << 11;
const NUMERIC: u16 = K_I32 | K_I64 | K_F64 | K_DEC;

#[derive(Default)]
enum ShapeState {
    #[default]
    Unknown,
    List(u16),
    Object(Vec<(String, u16)>),
    Unstable,
}

#[derive(Default)]
struct FieldStats {
    kinds: u16,
    present: u64,
    shape: ShapeState,
}

fn scalar_kind(v: &Bson) -> Option<u16> {
    Some(match v {
        Bson::Null | Bson::Undefined => 0,
        Bson::Int32(_) => K_I32,
        Bson::Int64(_) => K_I64,
        Bson::Double(_) => K_F64,
        Bson::Decimal128(_) => K_DEC,
        Bson::String(_) => K_STR,
        Bson::Boolean(_) => K_BOOL,
        Bson::DateTime(_) => K_DATE,
        Bson::ObjectId(_) => K_OID,
        Bson::Binary(b) if b.subtype == BinarySubtype::Uuid => K_UUID,
        Bson::Binary(_) => K_BIN,
        _ => return None,
    })
}

impl FieldStats {
    /// Tracks whether nested values keep one shape (SCY-10).
    fn observe_shape(&mut self, v: &Bson) {
        let next = match (std::mem::take(&mut self.shape), v) {
            (ShapeState::Unstable, _) => ShapeState::Unstable,
            (state, Bson::Array(items)) => {
                let mut mask = match state {
                    ShapeState::Unknown => 0,
                    ShapeState::List(m) => m,
                    _ => {
                        self.shape = ShapeState::Unstable;
                        return;
                    }
                };
                for i in items {
                    match scalar_kind(i) {
                        Some(0) | None => {
                            // null elements cannot be stored in a collection
                            self.shape = ShapeState::Unstable;
                            return;
                        }
                        Some(k) => mask |= k,
                    }
                }
                ShapeState::List(mask)
            }
            (state, Bson::Document(d)) => {
                let mut fields = match state {
                    ShapeState::Unknown => Vec::new(),
                    ShapeState::Object(f) => f,
                    _ => {
                        self.shape = ShapeState::Unstable;
                        return;
                    }
                };
                for (k, val) in d {
                    let Some(kind) = scalar_kind(val) else {
                        self.shape = ShapeState::Unstable;
                        return;
                    };
                    match fields.iter_mut().find(|(n, _)| n == k) {
                        Some((_, m)) => *m |= kind,
                        None => fields.push((k.clone(), kind)),
                    }
                }
                ShapeState::Object(fields)
            }
            (state, Bson::Null | Bson::Undefined) => state,
            _ => ShapeState::Unstable,
        };
        self.shape = next;
    }
}

/// A shape is stable when every element / field resolves to one scalar type.
fn stable_scalar(mask: u16) -> Option<LogicalType> {
    if mask == 0 {
        return None;
    }
    let t = SchemaAnalyzer::resolve(mask);
    (t != LogicalType::String || mask == K_STR).then_some(t)
}

/// Infers top-level fields and their types from documents. Fields missing or null in any
/// document are nullable; nested documents and arrays become json columns; incompatible mixed
/// types fall back to string (or json when a nested value is involved).
#[derive(Default)]
pub struct SchemaAnalyzer {
    fields: Vec<(String, FieldStats)>,
    index: std::collections::HashMap<String, usize>,
    documents: u64,
}

impl SchemaAnalyzer {
    pub fn new() -> Self {
        let mut a = SchemaAnalyzer::default();
        a.slot("_id");
        a
    }

    fn slot(&mut self, name: &str) -> usize {
        if let Some(&i) = self.index.get(name) {
            return i;
        }
        self.fields.push((name.to_string(), FieldStats::default()));
        self.index.insert(name.to_string(), self.fields.len() - 1);
        self.fields.len() - 1
    }

    pub fn add(&mut self, doc: &Document) {
        self.documents += 1;
        for (k, v) in doc {
            let kind = match v {
                Bson::Null | Bson::Undefined => 0,
                Bson::Int32(_) => K_I32,
                Bson::Int64(_) | Bson::Timestamp(_) => K_I64,
                Bson::Double(_) => K_F64,
                Bson::Decimal128(_) => K_DEC,
                Bson::String(_) => K_STR,
                Bson::Boolean(_) => K_BOOL,
                Bson::DateTime(_) => K_DATE,
                Bson::ObjectId(_) => K_OID,
                Bson::Binary(b) if b.subtype == BinarySubtype::Uuid => K_UUID,
                Bson::Binary(_) => K_BIN,
                Bson::Document(_) | Bson::Array(_) => K_DOC,
                _ => K_OTHER,
            };
            let i = self.slot(k);
            let f = &mut self.fields[i].1;
            if kind != 0 {
                f.kinds |= kind;
                f.present += 1;
            }
            if matches!(v, Bson::Document(_) | Bson::Array(_)) || f.kinds & K_DOC != 0 {
                f.observe_shape(v);
            }
        }
    }

    fn resolve(kinds: u16) -> LogicalType {
        if kinds == 0 {
            return LogicalType::String;
        }
        if kinds & !NUMERIC == 0 {
            return if kinds & K_DEC != 0 {
                LogicalType::Decimal
            } else if kinds & K_F64 != 0 {
                LogicalType::Float64
            } else if kinds & K_I64 != 0 {
                LogicalType::Int64
            } else {
                LogicalType::Int32
            };
        }
        match kinds {
            K_STR => LogicalType::String,
            K_BOOL => LogicalType::Bool,
            K_DATE => LogicalType::DateTimeTz,
            K_OID => LogicalType::ObjectId,
            K_BIN => LogicalType::Bytes,
            K_UUID => LogicalType::Uuid,
            K_DOC => LogicalType::Json,
            k if k & K_DOC != 0 => LogicalType::Json,
            _ => LogicalType::String,
        }
    }

    /// (field name, type, nullable, stable json shape) in first-seen order, `_id` first.
    pub fn finish(self) -> Vec<(String, LogicalType, bool, Option<JsonShape>)> {
        let docs = self.documents;
        self.fields
            .into_iter()
            .filter(|(n, f)| f.present > 0 || n != "_id" || docs == 0)
            .map(|(n, f)| {
                let t = Self::resolve(f.kinds);
                let shape = match (&f.shape, t) {
                    (ShapeState::List(m), LogicalType::Json) => stable_scalar(*m).map(JsonShape::List),
                    (ShapeState::Object(fields), LogicalType::Json) if !fields.is_empty() => fields
                        .iter()
                        .map(|(name, m)| stable_scalar(*m).map(|t| (name.clone(), t)))
                        .collect::<Option<Vec<_>>>()
                        .map(JsonShape::Object),
                    _ => None,
                };
                (n, t, f.present < docs, shape)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn infers_types_and_nullability() {
        let mut a = SchemaAnalyzer::new();
        a.add(&doc! {"_id": bson::oid::ObjectId::new(), "n": 1, "x": 1, "s": "a", "d": {"k": 1}, "m": 1});
        a.add(&doc! {"_id": bson::oid::ObjectId::new(), "n": 2_i64, "x": 2.5, "s": "b", "d": [1], "m": "z", "late": true});
        let f = a.finish();
        let get = |n: &str| f.iter().find(|x| x.0 == n).cloned().unwrap();
        assert_eq!(get("_id").1, LogicalType::ObjectId);
        assert!(!get("_id").2);
        assert_eq!(get("n").1, LogicalType::Int64);
        assert_eq!(get("x").1, LogicalType::Float64);
        assert_eq!((get("s").1, get("s").2), (LogicalType::String, false));
        assert_eq!(get("d").1, LogicalType::Json);
        assert_eq!(get("d").3, None, "object in one document, array in another: unstable");
        assert_eq!(get("m").1, LogicalType::String);
        assert_eq!((get("late").1, get("late").2), (LogicalType::Bool, true));
        assert_eq!(f[0].0, "_id");
    }

    #[test]
    fn stable_shapes_become_lists_and_objects() {
        let mut a = SchemaAnalyzer::new();
        a.add(&doc! {"tags": ["a", "b"], "addr": {"city": "X", "zip": 1}, "mixed": [1, "a"], "deep": {"x": {"y": 1}}});
        a.add(&doc! {"tags": [], "addr": {"city": "Y", "street": "S"}, "mixed": [2]});
        a.add(&doc! {"addr": null});
        let f = a.finish();
        let shape = |n: &str| f.iter().find(|x| x.0 == n).unwrap().3.clone();
        assert_eq!(shape("tags"), Some(JsonShape::List(LogicalType::String)));
        assert_eq!(
            shape("addr"),
            Some(JsonShape::Object(vec![("city".into(), LogicalType::String), ("zip".into(), LogicalType::Int32), ("street".into(), LogicalType::String)]))
        );
        assert_eq!(shape("mixed"), None);
        assert_eq!(shape("deep"), None);
    }

    #[test]
    fn bson_value_round_trip() {
        let oid = bson::oid::ObjectId::new();
        for b in [Bson::Int32(1), Bson::Int64(2), Bson::Double(1.5), Bson::String("x".into()), Bson::ObjectId(oid), Bson::Boolean(true)] {
            assert_eq!(value_to_bson(&bson_to_value(&b)), b);
        }
        let d = Bson::Decimal128("12.50".parse().unwrap());
        assert_eq!(bson_to_value(&d), Value::Decimal("12.50".into()));
        assert_eq!(value_to_bson(&bson_to_value(&d)), d);
    }
}
