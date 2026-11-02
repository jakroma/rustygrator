//! Conversion micro-benchmarks (PERF-1): value conversion and per-database type mapping on the
//! hot paths found by profiling (PERF-9).
//!
//! cargo bench -p rustygrator-core --bench conversions

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rustygrator_core::{
    db::{
        mongodb::mongodb_data_types::append_cell,
        postgresql::postgresql_data_types::{encode_numeric, PgParam, PgValue},
        sqlite::sqlite_data_types::SqlValue,
    },
    dbc::batch::{Cell, Column},
    models::value::{LogicalType, Value},
};
use mongodb::bson::RawDocumentBuf;
use rusqlite::types::ToSql as _;
use tokio_postgres::types::{ToSql, Type};

/// The pre-PERF-9 NUMERIC encoder (allocating, `str::parse` per group), kept as baseline.
fn encode_numeric_v1(text: &str, out: &mut BytesMut) {
    use bytes::BufMut;
    let t = text.trim();
    let (negative, body) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        _ => (false, t),
    };
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    let int_part = int_part.trim_start_matches('0');
    let mut digits: Vec<i16> = Vec::new();
    let lead = int_part.len() % 4;
    let mut pos = 0;
    if lead > 0 {
        digits.push(int_part[..lead].parse().unwrap());
        pos = lead;
    }
    while pos < int_part.len() {
        digits.push(int_part[pos..pos + 4].parse().unwrap());
        pos += 4;
    }
    let int_groups = digits.len() as i16;
    let mut fpos = 0;
    while fpos < frac_part.len() {
        let end = (fpos + 4).min(frac_part.len());
        let mut g: i16 = frac_part[fpos..end].parse().unwrap();
        for _ in end - fpos..4 {
            g *= 10;
        }
        digits.push(g);
        fpos += 4;
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    out.put_i16(digits.len() as i16);
    out.put_i16(int_groups - 1);
    out.put_u16(if negative { 0x4000 } else { 0 });
    out.put_u16(frac_part.len() as u16);
    for d in digits {
        out.put_i16(d);
    }
}

fn bench(c: &mut Criterion) {
    let decimals: Vec<String> = (0..10_000).map(|i| format!("{}.{:02}", i * 1013, i % 100)).collect();
    let texts: Vec<String> = (0..10_000).map(|i| format!("user {i} <u{i}@example.com>")).collect();
    let mut text_col = Column::new(LogicalType::String, texts.len());
    for t in &texts {
        text_col.push_str(t).unwrap();
    }
    let mut dec_col = Column::new(LogicalType::Decimal, decimals.len());
    for d in &decimals {
        dec_col.push(Value::Decimal(d.clone())).unwrap();
    }

    let mut g = c.benchmark_group("postgres_param");
    g.throughput(Throughput::Elements(10_000));
    let mut buf = BytesMut::with_capacity(1 << 20);
    g.bench_function("numeric_v1_alloc", |b| {
        b.iter(|| {
            buf.clear();
            for d in &decimals {
                encode_numeric_v1(black_box(d), &mut buf);
            }
        })
    });
    g.bench_function("numeric_v2_no_alloc", |b| {
        b.iter(|| {
            buf.clear();
            for d in &decimals {
                encode_numeric(black_box(d), &mut buf).unwrap();
            }
        })
    });
    g.bench_function("text_via_cell_utf8_check", |b| {
        b.iter(|| {
            buf.clear();
            for r in 0..text_col.len() {
                PgValue(text_col.cell(r)).to_sql(&Type::TEXT, &mut buf).unwrap();
            }
        })
    });
    g.bench_function("text_raw_bytes", |b| {
        b.iter(|| {
            buf.clear();
            for r in 0..text_col.len() {
                PgParam::Text(LogicalType::String, text_col.text_bytes(r).unwrap()).to_sql(&Type::TEXT, &mut buf).unwrap();
            }
        })
    });
    g.bench_function("decimal_column_raw_bytes", |b| {
        b.iter(|| {
            buf.clear();
            for r in 0..dec_col.len() {
                PgParam::Text(LogicalType::Decimal, dec_col.text_bytes(r).unwrap()).to_sql(&Type::NUMERIC, &mut buf).unwrap();
            }
        })
    });
    g.finish();

    let mut g = c.benchmark_group("sqlite_param");
    g.throughput(Throughput::Elements(10_000));
    g.bench_function("text_via_cell_utf8_check", |b| {
        b.iter(|| {
            for r in 0..text_col.len() {
                black_box(SqlValue::Cell(text_col.cell(r)).to_sql().unwrap());
            }
        })
    });
    g.bench_function("text_raw_bytes", |b| {
        b.iter(|| {
            for r in 0..text_col.len() {
                black_box(SqlValue::Text(text_col.text_bytes(r).unwrap()).to_sql().unwrap());
            }
        })
    });
    g.finish();

    let mut g = c.benchmark_group("batch");
    g.throughput(Throughput::Elements(10_000));
    g.bench_function("push_cell_str", |b| {
        b.iter(|| {
            let mut col = Column::new(LogicalType::String, texts.len());
            for t in &texts {
                col.push_cell(Cell::Str(t)).unwrap();
            }
            col
        })
    });
    g.bench_function("push_value_string", |b| {
        b.iter(|| {
            let mut col = Column::new(LogicalType::String, texts.len());
            for t in &texts {
                col.push(Value::String(t.clone())).unwrap();
            }
            col
        })
    });
    g.bench_function("coerce_i64_to_i32", |b| b.iter(|| (0..10_000i64).map(|i| Value::I64(i).coerce(LogicalType::Int32).unwrap()).count()));
    g.bench_function("mongodb_raw_bson_row", |b| {
        b.iter(|| {
            for r in 0..text_col.len() {
                let mut d = RawDocumentBuf::new();
                append_cell(&mut d, "_id", Cell::I64(r as i64));
                append_cell(&mut d, "name", text_col.cell(r));
                append_cell(&mut d, "amount", dec_col.cell(r));
                black_box(d);
            }
        })
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
