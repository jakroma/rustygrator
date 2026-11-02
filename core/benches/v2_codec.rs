//! Copy of the retired dbc v2 codec, kept only as the baseline for FMT-15.
#![allow(dead_code)]
use std::{
    fs::File,
    io::{BufReader, BufWriter, ErrorKind, Read, Write},
    path::Path,
};

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike};
use rustygrator_core::models::value::Value;
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"DBC\x02";

pub struct V2Writer {
    out: BufWriter<File>,
}

impl V2Writer {
    pub fn create(path: &Path) -> Self {
        let mut out = BufWriter::with_capacity(256 * 1024, File::create(path).unwrap());
        out.write_all(MAGIC).unwrap();
        V2Writer { out }
    }
    pub fn write_row(&mut self, row: &[Value]) {
        self.out.write_all(&(row.len() as u32).to_le_bytes()).unwrap();
        for v in row {
            write_value(&mut self.out, v);
        }
    }
    pub fn finish(mut self) {
        self.out.flush().unwrap();
    }
}

fn wb<W: Write>(w: &mut W, b: &[u8]) {
    w.write_all(&(b.len() as u32).to_le_bytes()).unwrap();
    w.write_all(b).unwrap();
}

fn write_value<W: Write>(w: &mut W, v: &Value) {
    match v {
        Value::Null => w.write_all(&[0]).unwrap(),
        Value::Bool(b) => w.write_all(&[1, *b as u8]).unwrap(),
        Value::I16(i) => { w.write_all(&[2]).unwrap(); w.write_all(&i.to_le_bytes()).unwrap() }
        Value::I32(i) => { w.write_all(&[3]).unwrap(); w.write_all(&i.to_le_bytes()).unwrap() }
        Value::I64(i) => { w.write_all(&[4]).unwrap(); w.write_all(&i.to_le_bytes()).unwrap() }
        Value::F32(f) => { w.write_all(&[5]).unwrap(); w.write_all(&f.to_le_bytes()).unwrap() }
        Value::F64(f) => { w.write_all(&[6]).unwrap(); w.write_all(&f.to_le_bytes()).unwrap() }
        Value::Decimal(s) => { w.write_all(&[7]).unwrap(); wb(w, s.as_bytes()) }
        Value::String(s) => { w.write_all(&[8]).unwrap(); wb(w, s.as_bytes()) }
        Value::Bytes(b) => { w.write_all(&[9]).unwrap(); wb(w, b) }
        Value::Date(d) => { w.write_all(&[10]).unwrap(); w.write_all(&d.num_days_from_ce().to_le_bytes()).unwrap() }
        Value::Time(t) => { w.write_all(&[11]).unwrap(); w.write_all(&t.num_seconds_from_midnight().to_le_bytes()).unwrap(); w.write_all(&t.nanosecond().to_le_bytes()).unwrap() }
        Value::DateTime(t) => { w.write_all(&[12]).unwrap(); let u = t.and_utc(); w.write_all(&u.timestamp().to_le_bytes()).unwrap(); w.write_all(&u.timestamp_subsec_nanos().to_le_bytes()).unwrap() }
        Value::DateTimeTz(t) => { w.write_all(&[13]).unwrap(); w.write_all(&t.timestamp().to_le_bytes()).unwrap(); w.write_all(&t.timestamp_subsec_nanos().to_le_bytes()).unwrap() }
        Value::Uuid(u) => { w.write_all(&[14]).unwrap(); w.write_all(u.as_bytes()).unwrap() }
        Value::Json(j) => { w.write_all(&[15]).unwrap(); wb(w, serde_json::to_string(j).unwrap().as_bytes()) }
        Value::ObjectId(o) => { w.write_all(&[16]).unwrap(); w.write_all(o).unwrap() }
    }
}

pub struct V2Reader {
    input: BufReader<File>,
}

impl V2Reader {
    pub fn open(path: &Path) -> Self {
        let mut input = BufReader::with_capacity(256 * 1024, File::open(path).unwrap());
        let mut m = [0u8; 4];
        input.read_exact(&mut m).unwrap();
        V2Reader { input }
    }
    fn arr<const N: usize>(&mut self) -> [u8; N] {
        let mut b = [0u8; N];
        self.input.read_exact(&mut b).unwrap();
        b
    }
    fn vec(&mut self) -> Vec<u8> {
        let n = u32::from_le_bytes(self.arr()) as usize;
        let mut v = vec![0u8; n];
        self.input.read_exact(&mut v).unwrap();
        v
    }
    pub fn read_row(&mut self) -> Option<Vec<Value>> {
        let mut c = [0u8; 4];
        match self.input.read_exact(&mut c) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return None,
            Err(e) => panic!("{e}"),
        }
        let n = u32::from_le_bytes(c) as usize;
        let mut row = Vec::with_capacity(n);
        for _ in 0..n {
            let t = self.arr::<1>()[0];
            row.push(match t {
                0 => Value::Null,
                1 => Value::Bool(self.arr::<1>()[0] != 0),
                2 => Value::I16(i16::from_le_bytes(self.arr())),
                3 => Value::I32(i32::from_le_bytes(self.arr())),
                4 => Value::I64(i64::from_le_bytes(self.arr())),
                5 => Value::F32(f32::from_le_bytes(self.arr())),
                6 => Value::F64(f64::from_le_bytes(self.arr())),
                7 => Value::Decimal(String::from_utf8(self.vec()).unwrap()),
                8 => Value::String(String::from_utf8(self.vec()).unwrap()),
                9 => Value::Bytes(self.vec()),
                10 => Value::Date(NaiveDate::from_num_days_from_ce_opt(i32::from_le_bytes(self.arr())).unwrap()),
                11 => { let s = u32::from_le_bytes(self.arr()); let n = u32::from_le_bytes(self.arr()); Value::Time(NaiveTime::from_num_seconds_from_midnight_opt(s, n).unwrap()) }
                12 | 13 => { let s = i64::from_le_bytes(self.arr()); let n = u32::from_le_bytes(self.arr()); let d = DateTime::from_timestamp(s, n).unwrap(); if t == 12 { Value::DateTime(d.naive_utc()) } else { Value::DateTimeTz(d) } }
                14 => Value::Uuid(Uuid::from_bytes(self.arr())),
                15 => Value::Json(serde_json::from_slice(&self.vec()).unwrap()),
                16 => Value::ObjectId(self.arr()),
                _ => panic!("bad tag"),
            });
        }
        Some(row)
    }
}
