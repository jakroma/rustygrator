//! dbc micro-benchmarks (PERF-1, FMT-15): v3 write / read with and without compression and
//! encryption, against the retired v2 format on the same data.
//!
//! cargo bench -p rustygrator-core --bench dbc

mod v2_codec;

use std::path::Path;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustygrator_core::{
    dbc::{
        batch::ColumnBatch,
        crypto::{DbcCrypto, KeySource},
        format::{Compression, DbcFileReader, DbcFileWriter, DbcOptions, FileKind},
    },
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo},
        value::{LogicalType, Value},
    },
};

pub fn definition() -> DataDefinition {
    let cols = [
        ("id", LogicalType::Int32, true),
        ("username", LogicalType::String, false),
        ("email", LogicalType::String, false),
        ("user_type_id", LogicalType::Int32, false),
        ("note", LogicalType::String, false),
    ];
    DataDefinition::new(
        "users".into(),
        cols.iter()
            .map(|(n, t, id)| DataPropertyInfo {
                property_name: n.to_string(),
                data_type: *t,
                source_type: None,
                is_nullable: !id,
                is_identifier: *id,
                reference_container_name: None,
                reference_property_name: None,
                shape: None,

            })
            .collect(),
    )
}

pub fn rows(n: usize) -> Vec<Vec<Value>> {
    (0..n as i32)
        .map(|i| {
            vec![
                Value::I32(i),
                Value::String(format!("user {i}")),
                Value::String(format!("u{i}@example.com")),
                Value::I32(i % 7),
                if i % 4 == 0 { Value::Null } else { Value::String(format!("note number {}", i % 100)) },
            ]
        })
        .collect()
}

fn batches(rows: &[Vec<Value>], def: &DataDefinition, block: usize) -> Vec<ColumnBatch> {
    rows.chunks(block)
        .map(|c| {
            let mut b = ColumnBatch::for_definition(def, c.len());
            for r in c {
                b.push_row(r.clone()).unwrap();
            }
            b
        })
        .collect()
}

fn write_v3(path: &Path, def: &DataDefinition, batches: &[ColumnBatch], opts: &DbcOptions) {
    let mut w = DbcFileWriter::create(path, FileKind::Data, Some(def), opts).unwrap();
    let codec = w.codec();
    if opts.workers > 1 {
        // FMT-16: blocks encoded on worker threads, written in order
        for chunk in batches.chunks(opts.workers) {
            let first = w.next_block_no();
            let encoded: Vec<_> = std::thread::scope(|s| {
                let hs: Vec<_> = chunk
                    .iter()
                    .enumerate()
                    .map(|(i, b)| {
                        let c = codec.clone();
                        s.spawn(move || c.encode(first + i as u64, b).unwrap())
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for e in encoded {
                w.write_block(e).unwrap();
            }
        }
    } else {
        for b in batches {
            let n = w.next_block_no();
            w.write_block(codec.encode(n, b).unwrap()).unwrap();
        }
    }
    w.finish().unwrap();
}

fn read_v3(path: &Path, def: &DataDefinition, opts: &DbcOptions) -> usize {
    let mut r = DbcFileReader::open(path, FileKind::Data, Some(def), &opts.crypto).unwrap();
    let codec = r.codec();
    let n = r.blocks.len();
    let mut total = 0;
    if opts.workers > 1 {
        let mut i = 0;
        while i < n {
            let raws: Vec<_> = (i..(i + opts.workers).min(n)).map(|b| (b, r.read_raw(b).unwrap())).collect();
            i += raws.len();
            total += std::thread::scope(|s| {
                let hs: Vec<_> = raws
                    .into_iter()
                    .map(|(b, raw)| {
                        let c = codec.clone();
                        s.spawn(move || c.decode(b as u64, raw).unwrap().rows)
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap()).sum::<usize>()
            });
        }
    } else {
        for b in 0..n {
            total += r.read_block(b).unwrap().rows;
        }
    }
    total
}

fn bench(c: &mut Criterion) {
    let def = definition();
    let dir = tempfile::tempdir().unwrap();
    let crypto = DbcCrypto::new(KeySource::Ephemeral);
    let workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2).clamp(1, 8);

    for n in [20_000usize, 200_000] {
        let data = rows(n);
        let blocks = batches(&data, &def, 8192);
        let mut g = c.benchmark_group(format!("dbc_{}k", n / 1000));
        g.throughput(Throughput::Elements(n as u64));
        g.sample_size(20);

        let v2_path = dir.path().join("v2.dbc");
        g.bench_function("v2_write", |b| {
            b.iter(|| {
                let mut w = v2_codec::V2Writer::create(&v2_path);
                for r in &data {
                    w.write_row(r);
                }
                w.finish();
            })
        });
        g.bench_function("v2_read", |b| {
            b.iter(|| {
                let mut r = v2_codec::V2Reader::open(&v2_path);
                let mut k = 0;
                while let Some(row) = r.read_row() {
                    k += row.len();
                }
                k
            })
        });
        let v2_size = std::fs::metadata(&v2_path).unwrap().len();

        for (label, comp, w) in [
            ("v3_lz4_aead", Compression::Lz4, 1),
            ("v3_lz4_aead_parallel", Compression::Lz4, workers),
            ("v3_zstd_aead_parallel", Compression::Zstd, workers),
            ("v3_none_aead", Compression::None, 1),
        ] {
            let mut opts = DbcOptions::new(crypto.clone());
            opts.compression = comp;
            opts.workers = w;
            let p = dir.path().join(format!("{label}.dbc"));
            g.bench_function(BenchmarkId::new("write", label), |b| b.iter(|| write_v3(&p, &def, &blocks, &opts)));
            g.bench_function(BenchmarkId::new("read", label), |b| b.iter(|| assert_eq!(read_v3(&p, &def, &opts), n)));
            let size = std::fs::metadata(&p).unwrap().len();
            println!("SIZE {} rows {label}: {} bytes (v2: {} bytes, {:.1}%)", n, size, v2_size, size as f64 * 100.0 / v2_size as f64);
        }
        g.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
