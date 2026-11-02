//! `dbc` v3 file format (section 7 of the spec).
//!
//! ```text
//! header (76 bytes, FMT-1)
//!   "DMB3" | version u16 | kind u8 | compression u8 | cipher u8 | kdf u8 | zstd level i16
//!   | block rows u32 | salt [16] | key id [8] | schema hash u64 | file nonce [12]
//!   | header tag [16]        (AEAD over the first 60 bytes: detects a changed header / wrong key)
//! blocks (FMT-2..FMT-5, FMT-9)
//!   len u32 | AEAD( rows u32 | raw len u32 | compress( column 0 | column 1 | ... ) )
//!   column = flags u8 (1 = has nulls, 2 = delta) | null bitmap | values
//!     integers / dates / times / timestamps: zig-zag varints (delta from the previous value
//!     for key or sorted columns), floats: little endian, bool: bit set, uuid / objectid: raw,
//!     text / bytes: varint lengths followed by one data buffer
//! footer (FMT-6)
//!   len u32 | AEAD( total rows u64 | blocks u32 | (offset u64, stored len u32, rows u32)* )
//! trailer
//!   footer offset u64 | footer len u32 | "DMB3END\0"
//! ```
//! Every AEAD uses the 60 authenticated header bytes plus the block number as associated data
//! and the file nonce XOR the block number as nonce, so blocks cannot be swapped, cut, edited or
//! moved to another file without detection.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    dbc::{
        batch::{Column, ColumnBatch, ColumnData},
        crypto::{random_bytes, Cipher, CipherKind, DbcCrypto, Kdf, TAG_LEN},
    },
    models::{data_definitions::DataDefinition, value::LogicalType},
    DMError, DMResult,
};

pub const MAGIC: &[u8; 4] = b"DMB3";
pub const VERSION: u16 = 3;
const V2_MAGIC: &[u8; 4] = b"DBC\x02";
pub const HEADER_LEN: usize = 76;
const AUTH_LEN: usize = 60;
const TRAILER_MAGIC: &[u8; 8] = b"DMB3END\0";
const TRAILER_LEN: usize = 20;
const HEADER_BLOCK: u64 = u64::MAX;
const FOOTER_BLOCK: u64 = u64::MAX - 1;
pub const DEFAULT_BLOCK_ROWS: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FileKind {
    Data = 1,
    Meta = 2,
}

/// Block compression (FMT-5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

impl Compression {
    fn code(self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Lz4 => 1,
            Compression::Zstd => 2,
        }
    }
    fn from_code(c: u8) -> DMResult<Self> {
        Ok(match c {
            0 => Compression::None,
            1 => Compression::Lz4,
            2 => Compression::Zstd,
            _ => return Err(DMError::Error(format!("dbc: unknown compression {}", c))),
        })
    }
}

/// Settings for writing / reading dbc files.
#[derive(Debug, Clone)]
pub struct DbcOptions {
    pub crypto: DbcCrypto,
    pub compression: Compression,
    pub zstd_level: i32,
    pub block_rows: usize,
    /// Blocks compressed / encrypted (or decrypted / decompressed) in parallel (FMT-16).
    pub workers: usize,
}

impl DbcOptions {
    pub fn new(crypto: DbcCrypto) -> Self {
        DbcOptions {
            crypto,
            compression: Compression::Lz4,
            zstd_level: 3,
            block_rows: DEFAULT_BLOCK_ROWS,
            workers: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2).clamp(1, 8),
        }
    }
}

fn kind_code(k: LogicalType) -> u8 {
    use LogicalType::*;
    match k {
        Int16 => 1,
        Int32 => 2,
        Int64 => 3,
        Float32 => 4,
        Float64 => 5,
        Decimal => 6,
        String => 7,
        Bool => 8,
        Date => 9,
        Time => 10,
        DateTime => 11,
        DateTimeTz => 12,
        Uuid => 13,
        Json => 14,
        Bytes => 15,
        ObjectId => 16,
    }
}

/// FNV-1a over column names, kinds and key flags: the reader refuses data written for another
/// definition.
pub fn schema_hash(def: &DataDefinition) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut eat = |b: u8| {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    };
    for p in &def.properties_info {
        for b in p.property_name.bytes() {
            eat(b);
        }
        eat(0);
        eat(kind_code(p.data_type));
        eat(p.is_identifier as u8);
    }
    h
}

#[derive(Debug, Clone)]
pub struct Header {
    pub kind: FileKind,
    pub compression: Compression,
    pub zstd_level: i16,
    pub cipher: CipherKind,
    pub kdf: Kdf,
    pub block_rows: u32,
    pub salt: [u8; 16],
    pub key_id: [u8; 8],
    pub schema_hash: u64,
    pub nonce: [u8; 12],
}

impl Header {
    fn auth_bytes(&self) -> [u8; AUTH_LEN] {
        let mut b = [0u8; AUTH_LEN];
        b[0..4].copy_from_slice(MAGIC);
        b[4..6].copy_from_slice(&VERSION.to_le_bytes());
        b[6] = self.kind as u8;
        b[7] = self.compression.code();
        b[8] = self.cipher as u8;
        b[9] = self.kdf as u8;
        b[10..12].copy_from_slice(&self.zstd_level.to_le_bytes());
        b[12..16].copy_from_slice(&self.block_rows.to_le_bytes());
        b[16..32].copy_from_slice(&self.salt);
        b[32..40].copy_from_slice(&self.key_id);
        b[40..48].copy_from_slice(&self.schema_hash.to_le_bytes());
        b[48..60].copy_from_slice(&self.nonce);
        b
    }

    fn parse(b: &[u8; HEADER_LEN]) -> DMResult<Header> {
        if &b[0..4] == V2_MAGIC {
            return Err(DMError::Error(
                "dbc: this is a version 2 data file, which is no longer supported; run the export (provide) again".to_string(),
            ));
        }
        if &b[0..4] != MAGIC {
            return Err(DMError::Error("dbc: not a rustygrator file".to_string()));
        }
        let version = u16::from_le_bytes([b[4], b[5]]);
        if version != VERSION {
            return Err(DMError::Error(format!("dbc: unsupported format version {} (this rustygrator reads version {})", version, VERSION)));
        }
        let kind = match b[6] {
            1 => FileKind::Data,
            2 => FileKind::Meta,
            k => return Err(DMError::Error(format!("dbc: unknown file kind {}", k))),
        };
        let kdf = match b[9] {
            0 => Kdf::None,
            1 => Kdf::Argon2id,
            k => return Err(DMError::Error(format!("dbc: unknown key derivation {}", k))),
        };
        let arr = |r: std::ops::Range<usize>| b[r].to_vec();
        Ok(Header {
            kind,
            compression: Compression::from_code(b[7])?,
            cipher: CipherKind::from_u8(b[8])?,
            kdf,
            zstd_level: i16::from_le_bytes([b[10], b[11]]),
            block_rows: u32::from_le_bytes([b[12], b[13], b[14], b[15]]),
            salt: arr(16..32).try_into().unwrap(),
            key_id: arr(32..40).try_into().unwrap(),
            schema_hash: u64::from_le_bytes(arr(40..48).try_into().unwrap()),
            nonce: arr(48..60).try_into().unwrap(),
        })
    }
}

// ------------------------------------------------------------------ varints

#[inline]
fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn err() -> DMError {
        DMError::Error("dbc: corrupted block".to_string())
    }
    #[inline]
    fn varint(&mut self) -> DMResult<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let byte = *self.b.get(self.pos).ok_or_else(Self::err)?;
            self.pos += 1;
            v |= ((byte & 0x7f) as u64) << shift;
            if byte < 0x80 {
                return Ok(v);
            }
            shift += 7;
            if shift > 63 {
                return Err(Self::err());
            }
        }
    }
    fn take(&mut self, n: usize) -> DMResult<&'a [u8]> {
        let s = self.b.get(self.pos..self.pos + n).ok_or_else(Self::err)?;
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> DMResult<u8> {
        Ok(self.take(1)?[0])
    }
}

fn put_ints<I: Iterator<Item = i64> + Clone>(out: &mut Vec<u8>, values: I, delta: bool) {
    if delta {
        let mut prev = 0i64;
        for v in values {
            put_varint(out, zigzag(v.wrapping_sub(prev)));
            prev = v;
        }
    } else {
        for v in values {
            put_varint(out, zigzag(v));
        }
    }
}

fn get_ints(c: &mut Cursor<'_>, rows: usize, delta: bool, mut push: impl FnMut(i64)) -> DMResult<()> {
    let mut prev = 0i64;
    for _ in 0..rows {
        let mut v = unzigzag(c.varint()?);
        if delta {
            v = prev.wrapping_add(v);
            prev = v;
        }
        push(v);
    }
    Ok(())
}

fn is_sorted<T: PartialOrd>(v: &[T]) -> bool {
    v.windows(2).all(|w| w[0] <= w[1])
}

// ------------------------------------------------------------------ column codec

fn encode_column(col: &Column, rows: usize, key: bool, out: &mut Vec<u8>) {
    let delta = match &col.data {
        ColumnData::I16(v) => key || is_sorted(v),
        ColumnData::I32(v) | ColumnData::Date(v) => key || is_sorted(v),
        ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => key || is_sorted(v),
        _ => false,
    };
    let has_nulls = col.null_count > 0;
    out.push((has_nulls as u8) | ((delta as u8) << 1));
    if has_nulls {
        let n = rows.div_ceil(8);
        let mut written = 0;
        for w in &col.nulls {
            let bytes = w.to_le_bytes();
            let take = (n - written).min(8);
            out.extend_from_slice(&bytes[..take]);
            written += take;
            if written == n {
                break;
            }
        }
        out.resize(out.len() + (n - written), 0);
    }
    match &col.data {
        ColumnData::Bool(v) => {
            let mut byte = 0u8;
            for (i, b) in v.iter().enumerate() {
                byte |= (*b as u8) << (i % 8);
                if i % 8 == 7 {
                    out.push(byte);
                    byte = 0;
                }
            }
            if v.len() % 8 != 0 {
                out.push(byte);
            }
        }
        ColumnData::I16(v) => put_ints(out, v.iter().map(|x| *x as i64), delta),
        ColumnData::I32(v) | ColumnData::Date(v) => put_ints(out, v.iter().map(|x| *x as i64), delta),
        ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => put_ints(out, v.iter().copied(), delta),
        ColumnData::F32(v) => {
            out.reserve(v.len() * 4);
            for f in v {
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        ColumnData::F64(v) => {
            out.reserve(v.len() * 8);
            for f in v {
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        ColumnData::Var { offsets, data } => {
            for w in offsets.windows(2) {
                put_varint(out, (w[1] - w[0]) as u64);
            }
            out.extend_from_slice(data);
        }
        ColumnData::Uuid(v) => {
            for u in v {
                out.extend_from_slice(u);
            }
        }
        ColumnData::ObjectId(v) => {
            for o in v {
                out.extend_from_slice(o);
            }
        }
    }
}

fn decode_column(c: &mut Cursor<'_>, kind: LogicalType, rows: usize) -> DMResult<Column> {
    let flags = c.u8()?;
    let has_nulls = flags & 1 != 0;
    let delta = flags & 2 != 0;
    let mut col = Column::new(kind, rows);
    if has_nulls {
        let bm = c.take(rows.div_ceil(8))?;
        col.nulls = bm
            .chunks(8)
            .map(|ch| {
                let mut w = [0u8; 8];
                w[..ch.len()].copy_from_slice(ch);
                u64::from_le_bytes(w)
            })
            .collect();
        col.null_count = col.nulls.iter().map(|w| w.count_ones() as usize).sum();
    }
    match &mut col.data {
        ColumnData::Bool(v) => {
            let bytes = c.take(rows.div_ceil(8))?;
            v.extend((0..rows).map(|i| bytes[i / 8] & (1 << (i % 8)) != 0));
        }
        ColumnData::I16(v) => get_ints(c, rows, delta, |x| v.push(x as i16))?,
        ColumnData::I32(v) | ColumnData::Date(v) => get_ints(c, rows, delta, |x| v.push(x as i32))?,
        ColumnData::I64(v) | ColumnData::Time(v) | ColumnData::Timestamp(v) => get_ints(c, rows, delta, |x| v.push(x))?,
        ColumnData::F32(v) => {
            let b = c.take(rows * 4)?;
            v.extend(b.chunks_exact(4).map(|x| f32::from_le_bytes(x.try_into().unwrap())));
        }
        ColumnData::F64(v) => {
            let b = c.take(rows * 8)?;
            v.extend(b.chunks_exact(8).map(|x| f64::from_le_bytes(x.try_into().unwrap())));
        }
        ColumnData::Var { offsets, data } => {
            let mut total = 0u32;
            for _ in 0..rows {
                total = total.checked_add(c.varint()? as u32).ok_or_else(Cursor::err)?;
                offsets.push(total);
            }
            data.extend_from_slice(c.take(total as usize)?);
            if matches!(kind, LogicalType::String | LogicalType::Decimal | LogicalType::Json) && std::str::from_utf8(data).is_err() {
                return Err(Cursor::err());
            }
        }
        ColumnData::Uuid(v) => {
            let b = c.take(rows * 16)?;
            v.extend(b.chunks_exact(16).map(|x| <[u8; 16]>::try_from(x).unwrap()));
        }
        ColumnData::ObjectId(v) => {
            let b = c.take(rows * 12)?;
            v.extend(b.chunks_exact(12).map(|x| <[u8; 12]>::try_from(x).unwrap()));
        }
    }
    Ok(col)
}

// ------------------------------------------------------------------ block codec

/// Everything needed to encode / decode blocks of one file; cheap to clone into worker threads.
#[derive(Clone)]
pub struct BlockCodec {
    cipher: Cipher,
    aad: [u8; AUTH_LEN + 8],
    nonce: [u8; 12],
    compression: Compression,
    zstd_level: i32,
    kinds: Vec<LogicalType>,
    keys: Vec<bool>,
}

/// An encoded (compressed + encrypted) block ready to be written.
pub struct EncodedBlock {
    pub block_no: u64,
    pub rows: u32,
    pub bytes: Vec<u8>,
}

impl BlockCodec {
    fn nonce_for(&self, block_no: u64) -> [u8; 12] {
        let mut n = self.nonce;
        for (i, b) in block_no.to_le_bytes().iter().enumerate() {
            n[4 + i] ^= b;
        }
        n
    }

    fn aad_for(&self, block_no: u64) -> [u8; AUTH_LEN + 8] {
        let mut a = self.aad;
        a[AUTH_LEN..].copy_from_slice(&block_no.to_le_bytes());
        a
    }

    fn seal(&self, block_no: u64, rows: u32, raw: &[u8]) -> DMResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(raw.len() / 2 + 32);
        buf.extend_from_slice(&rows.to_le_bytes());
        buf.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        match self.compression {
            Compression::None => buf.extend_from_slice(raw),
            Compression::Lz4 => {
                let start = buf.len();
                buf.resize(start + lz4_flex::block::get_maximum_output_size(raw.len()), 0);
                let n = lz4_flex::block::compress_into(raw, &mut buf[start..]).map_err(|e| DMError::Error(format!("lz4: {}", e)))?;
                buf.truncate(start + n);
            }
            Compression::Zstd => {
                let c = zstd::bulk::compress(raw, self.zstd_level).map_err(|e| DMError::Error(format!("zstd: {}", e)))?;
                buf.extend_from_slice(&c);
            }
        }
        buf.reserve(TAG_LEN);
        self.cipher.seal(&self.nonce_for(block_no), &self.aad_for(block_no), &mut buf)?;
        Ok(buf)
    }

    fn unseal(&self, block_no: u64, mut bytes: Vec<u8>) -> DMResult<(u32, Vec<u8>)> {
        self.cipher.open(&self.nonce_for(block_no), &self.aad_for(block_no), &mut bytes)?;
        if bytes.len() < 8 {
            return Err(Cursor::err());
        }
        let rows = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let raw_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let payload = &bytes[8..];
        let raw = match self.compression {
            Compression::None => payload.to_vec(),
            Compression::Lz4 => lz4_flex::block::decompress(payload, raw_len).map_err(|_| Cursor::err())?,
            Compression::Zstd => zstd::bulk::decompress(payload, raw_len).map_err(|_| Cursor::err())?,
        };
        if raw.len() != raw_len {
            return Err(Cursor::err());
        }
        Ok((rows, raw))
    }

    /// Column encoding + compression + encryption of one batch.
    pub fn encode(&self, block_no: u64, batch: &ColumnBatch) -> DMResult<EncodedBlock> {
        let mut raw = Vec::with_capacity(batch.memory_size() / 2 + 64);
        for (i, col) in batch.columns.iter().enumerate() {
            encode_column(col, batch.rows, self.keys.get(i).copied().unwrap_or(false), &mut raw);
        }
        Ok(EncodedBlock { block_no, rows: batch.rows as u32, bytes: self.seal(block_no, batch.rows as u32, &raw)? })
    }

    pub fn decode(&self, block_no: u64, bytes: Vec<u8>) -> DMResult<ColumnBatch> {
        let (rows, raw) = self.unseal(block_no, bytes)?;
        let rows = rows as usize;
        let mut c = Cursor { b: &raw, pos: 0 };
        let mut columns = Vec::with_capacity(self.kinds.len());
        for k in &self.kinds {
            columns.push(decode_column(&mut c, *k, rows)?);
        }
        if c.pos != raw.len() {
            return Err(Cursor::err());
        }
        Ok(ColumnBatch { columns, rows })
    }

    /// Authenticates a block without decoding it (FMT-13 pre-check).
    pub fn verify(&self, block_no: u64, mut bytes: Vec<u8>) -> DMResult<()> {
        self.cipher.open(&self.nonce_for(block_no), &self.aad_for(block_no), &mut bytes)
    }
}

/// Column kinds and key flags of a definition (meta files have none).
fn layout(def: Option<&DataDefinition>) -> (Vec<LogicalType>, Vec<bool>, u64) {
    match def {
        Some(d) => (
            d.properties_info.iter().map(|p| p.data_type).collect(),
            d.properties_info.iter().map(|p| p.is_identifier).collect(),
            schema_hash(d),
        ),
        None => (Vec::new(), Vec::new(), 0),
    }
}

// ------------------------------------------------------------------ files

/// Creates a file readable and writable by the current user only (FMT-14). On Windows the file
/// inherits the ACL of the temporary directory.
pub fn create_private_file(path: &Path) -> DMResult<File> {
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    Ok(o.open(path)?)
}

pub struct DbcFileWriter {
    out: BufWriter<File>,
    offset: u64,
    index: Vec<(u64, u32, u32)>,
    total_rows: u64,
    codec: BlockCodec,
}

impl DbcFileWriter {
    pub fn create(path: &Path, kind: FileKind, def: Option<&DataDefinition>, opts: &DbcOptions) -> DMResult<Self> {
        let (key, salt) = opts.crypto.write_key()?;
        let cipher_kind = CipherKind::preferred();
        let cipher = Cipher::new(cipher_kind, &key);
        let (kinds, keys, hash) = layout(def);
        let header = Header {
            kind,
            compression: opts.compression,
            zstd_level: opts.zstd_level as i16,
            cipher: cipher_kind,
            kdf: opts.crypto.kdf(),
            block_rows: opts.block_rows as u32,
            salt,
            key_id: cipher.key_id()?,
            schema_hash: hash,
            nonce: random_bytes::<12>()?,
        };
        let auth = header.auth_bytes();
        let mut aad = [0u8; AUTH_LEN + 8];
        aad[..AUTH_LEN].copy_from_slice(&auth);
        let codec = BlockCodec { cipher, aad, nonce: header.nonce, compression: opts.compression, zstd_level: opts.zstd_level, kinds, keys };
        let mut tag = Vec::new();
        codec.cipher.seal(&codec.nonce_for(HEADER_BLOCK), &codec.aad_for(HEADER_BLOCK), &mut tag)?;

        let mut out = BufWriter::with_capacity(1 << 20, create_private_file(path)?);
        out.write_all(&auth)?;
        out.write_all(&tag)?;
        Ok(DbcFileWriter { out, offset: HEADER_LEN as u64, index: Vec::new(), total_rows: 0, codec })
    }

    pub fn codec(&self) -> BlockCodec {
        self.codec.clone()
    }

    pub fn next_block_no(&self) -> u64 {
        self.index.len() as u64
    }

    pub fn write_block(&mut self, block: EncodedBlock) -> DMResult<()> {
        if block.block_no != self.index.len() as u64 {
            return Err(DMError::Error("dbc: blocks written out of order".to_string()));
        }
        self.out.write_all(&(block.bytes.len() as u32).to_le_bytes())?;
        self.out.write_all(&block.bytes)?;
        self.index.push((self.offset, block.bytes.len() as u32, block.rows));
        self.offset += 4 + block.bytes.len() as u64;
        self.total_rows += block.rows as u64;
        Ok(())
    }

    pub fn finish(mut self) -> DMResult<u64> {
        let mut raw = Vec::with_capacity(12 + self.index.len() * 16);
        raw.extend_from_slice(&self.total_rows.to_le_bytes());
        raw.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for (off, len, rows) in &self.index {
            raw.extend_from_slice(&off.to_le_bytes());
            raw.extend_from_slice(&len.to_le_bytes());
            raw.extend_from_slice(&rows.to_le_bytes());
        }
        let footer = self.codec.seal(FOOTER_BLOCK, self.index.len() as u32, &raw)?;
        let footer_offset = self.offset;
        self.out.write_all(&(footer.len() as u32).to_le_bytes())?;
        self.out.write_all(&footer)?;
        self.out.write_all(&footer_offset.to_le_bytes())?;
        self.out.write_all(&(footer.len() as u32).to_le_bytes())?;
        self.out.write_all(TRAILER_MAGIC)?;
        self.out.flush()?;
        Ok(self.total_rows)
    }
}

/// Block index entry (FMT-6).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BlockInfo {
    pub offset: u64,
    pub stored_len: u32,
    pub rows: u32,
}

pub struct DbcFileReader {
    input: BufReader<File>,
    pub header: Header,
    codec: BlockCodec,
    pub blocks: Vec<BlockInfo>,
    pub total_rows: u64,
}

impl DbcFileReader {
    /// Opens and fully authenticates header and footer: version, key and file integrity are
    /// checked before any block is handed out (FMT-13, FMT-17).
    pub fn open(path: &Path, kind: FileKind, def: Option<&DataDefinition>, crypto: &DbcCrypto) -> DMResult<Self> {
        let mut input = BufReader::with_capacity(1 << 20, File::open(path).map_err(|e| DMError::Error(format!("{}: {}", path.display(), e)))?);
        let mut hb = [0u8; HEADER_LEN];
        input.read_exact(&mut hb[..4]).map_err(|_| DMError::Error(format!("{}: not a rustygrator file", path.display())))?;
        if &hb[..4] == V2_MAGIC || &hb[..4] != MAGIC {
            Header::parse(&hb)?; // reports v2 / foreign files
        }
        input.read_exact(&mut hb[4..]).map_err(|_| DMError::Error(format!("{}: incomplete file", path.display())))?;
        let header = Header::parse(&hb).map_err(|e| DMError::Error(format!("{}: {}", path.display(), e)))?;
        if header.kind != kind {
            return Err(DMError::Error(format!("{}: unexpected dbc file kind", path.display())));
        }
        let key = crypto.read_key(header.kdf, &header.salt)?;
        let cipher = Cipher::new(header.cipher, &key);
        if cipher.key_id()? != header.key_id {
            return Err(DMError::Error(format!("{}: wrong key (the file was written with a different key)", path.display())));
        }
        let mut aad = [0u8; AUTH_LEN + 8];
        aad[..AUTH_LEN].copy_from_slice(&hb[..AUTH_LEN]);
        let (kinds, keys, hash) = layout(def);
        let codec = BlockCodec { cipher, aad, nonce: header.nonce, compression: header.compression, zstd_level: header.zstd_level as i32, kinds, keys };
        codec.verify(HEADER_BLOCK, hb[AUTH_LEN..].to_vec())?;
        if def.is_some() && hash != header.schema_hash {
            return Err(DMError::Error(format!("{}: the data file does not match the data definition", path.display())));
        }

        let len = input.seek(SeekFrom::End(0))?;
        if len < (HEADER_LEN + TRAILER_LEN) as u64 {
            return Err(DMError::Error(format!("{}: incomplete file", path.display())));
        }
        input.seek(SeekFrom::End(-(TRAILER_LEN as i64)))?;
        let mut tr = [0u8; TRAILER_LEN];
        input.read_exact(&mut tr)?;
        if &tr[12..] != TRAILER_MAGIC {
            return Err(DMError::Error(format!("{}: incomplete file (writer did not finish)", path.display())));
        }
        let footer_offset = u64::from_le_bytes(tr[0..8].try_into().unwrap());
        let footer_len = u32::from_le_bytes(tr[8..12].try_into().unwrap()) as usize;
        if footer_offset + 4 + footer_len as u64 + TRAILER_LEN as u64 != len {
            return Err(DMError::Error(format!("{}: the file was changed or damaged", path.display())));
        }
        input.seek(SeekFrom::Start(footer_offset + 4))?;
        let mut fb = vec![0u8; footer_len];
        input.read_exact(&mut fb)?;
        let (count, raw) = codec.unseal(FOOTER_BLOCK, fb)?;
        let mut c = Cursor { b: &raw, pos: 0 };
        let total_rows = u64::from_le_bytes(c.take(8)?.try_into().unwrap());
        let n = u32::from_le_bytes(c.take(4)?.try_into().unwrap());
        if n != count {
            return Err(Cursor::err());
        }
        let mut blocks = Vec::with_capacity(n as usize);
        let mut expected = HEADER_LEN as u64;
        for _ in 0..n {
            let b = BlockInfo {
                offset: u64::from_le_bytes(c.take(8)?.try_into().unwrap()),
                stored_len: u32::from_le_bytes(c.take(4)?.try_into().unwrap()),
                rows: u32::from_le_bytes(c.take(4)?.try_into().unwrap()),
            };
            if b.offset != expected {
                return Err(Cursor::err());
            }
            expected += 4 + b.stored_len as u64;
            blocks.push(b);
        }
        if expected != footer_offset || blocks.iter().map(|b| b.rows as u64).sum::<u64>() != total_rows {
            return Err(Cursor::err());
        }
        input.seek(SeekFrom::Start(HEADER_LEN as u64))?;
        Ok(DbcFileReader { input, header, codec, blocks, total_rows })
    }

    pub fn codec(&self) -> BlockCodec {
        self.codec.clone()
    }

    /// Raw (still encrypted) bytes of block `i`.
    pub fn read_raw(&mut self, i: usize) -> DMResult<Vec<u8>> {
        let b = self.blocks.get(i).copied().ok_or_else(Cursor::err)?;
        self.input.seek(SeekFrom::Start(b.offset))?;
        let mut len = [0u8; 4];
        self.input.read_exact(&mut len)?;
        if u32::from_le_bytes(len) != b.stored_len {
            return Err(Cursor::err());
        }
        let mut v = vec![0u8; b.stored_len as usize];
        self.input.read_exact(&mut v)?;
        Ok(v)
    }

    pub fn read_block(&mut self, i: usize) -> DMResult<ColumnBatch> {
        let raw = self.read_raw(i)?;
        self.codec.decode(i as u64, raw)
    }
}

// ------------------------------------------------------------------ meta containers (FMT-8)

/// Writes a definition / migration order / checkpoint as an encrypted container.
pub fn write_meta<T: serde::Serialize>(path: &Path, value: &T, opts: &DbcOptions) -> DMResult<()> {
    let w = DbcFileWriter::create(path, FileKind::Meta, None, opts)?;
    let json = serde_json::to_vec(value)?;
    let mut w = w;
    let bytes = w.codec.seal(0, 1, &json)?;
    w.write_block(EncodedBlock { block_no: 0, rows: 1, bytes })?;
    w.finish()?;
    Ok(())
}

pub fn read_meta<T: serde::de::DeserializeOwned>(path: &Path, crypto: &DbcCrypto) -> DMResult<T> {
    let mut r = DbcFileReader::open(path, FileKind::Meta, None, crypto)?;
    if r.blocks.len() != 1 {
        return Err(Cursor::err());
    }
    let raw = r.read_raw(0)?;
    let (_, json) = r.codec.unseal(0, raw)?;
    Ok(serde_json::from_slice(&json)?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        dbc::{batch::tests::{sample_kinds, sample_row}, crypto::KeySource},
        models::data_definitions::DataPropertyInfo,
    };

    pub(crate) fn sample_def() -> DataDefinition {
        DataDefinition::new(
            "t".into(),
            sample_kinds()
                .into_iter()
                .enumerate()
                .map(|(i, k)| DataPropertyInfo {
                    property_name: format!("c{i}"),
                    data_type: k,
                    source_type: None,
                    is_nullable: true,
                    is_identifier: i == 0,
                    reference_container_name: None,
                    reference_property_name: None,
                    shape: None,

                })
                .collect(),
        )
    }

    fn batch(from: i64, to: i64) -> ColumnBatch {
        let mut b = ColumnBatch::new(&sample_kinds(), (to - from) as usize);
        for i in from..to {
            b.push_row(sample_row(i)).unwrap();
        }
        b
    }

    fn write(path: &Path, opts: &DbcOptions, blocks: &[(i64, i64)]) {
        let def = sample_def();
        let mut w = DbcFileWriter::create(path, FileKind::Data, Some(&def), opts).unwrap();
        let codec = w.codec();
        for (n, (a, b)) in blocks.iter().enumerate() {
            w.write_block(codec.encode(n as u64, &batch(*a, *b)).unwrap()).unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn round_trip_with_every_compression() {
        let dir = tempfile::tempdir().unwrap();
        for comp in [Compression::None, Compression::Lz4, Compression::Zstd] {
            let mut opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
            opts.compression = comp;
            let path = dir.path().join(format!("{comp:?}.dbc"));
            write(&path, &opts, &[(0, 300), (300, 301), (301, 1000)]);
            let mut r = DbcFileReader::open(&path, FileKind::Data, Some(&sample_def()), &opts.crypto).unwrap();
            assert_eq!(r.total_rows, 1000);
            assert_eq!(r.blocks.len(), 3);
            // skip straight to the last block (resume)
            assert_eq!(r.read_block(2).unwrap(), batch(301, 1000));
            assert_eq!(r.read_block(0).unwrap(), batch(0, 300));
            assert_eq!(r.read_block(1).unwrap(), batch(300, 301));
        }
    }

    #[test]
    fn compression_makes_files_smaller() {
        let dir = tempfile::tempdir().unwrap();
        let mut size = Vec::new();
        for comp in [Compression::None, Compression::Lz4, Compression::Zstd] {
            let mut opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
            opts.compression = comp;
            let path = dir.path().join(format!("{comp:?}.dbc"));
            write(&path, &opts, &[(0, 5000)]);
            size.push(std::fs::metadata(&path).unwrap().len());
        }
        assert!(size[1] < size[0] && size[2] < size[1], "{size:?}");
    }

    #[test]
    fn wrong_key_tampering_and_versions_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dbc");
        let opts = DbcOptions::new(DbcCrypto::new(KeySource::Passphrase("secret passphrase".into())));
        write(&path, &opts, &[(0, 100), (100, 200)]);
        let def = sample_def();
        let open = |c: &DbcCrypto| DbcFileReader::open(&path, FileKind::Data, Some(&def), c).map(|_| ());
        // same passphrase in another "process" works
        open(&DbcCrypto::new(KeySource::Passphrase("secret passphrase".into()))).unwrap();
        let e = open(&DbcCrypto::new(KeySource::Passphrase("another passphrase".into()))).unwrap_err().to_string();
        assert!(e.contains("wrong key"), "{e}");
        let e = open(&DbcCrypto::new(KeySource::Ephemeral)).unwrap_err().to_string();
        assert!(e.contains("DM_DBC_KEY"), "{e}");

        let original = std::fs::read(&path).unwrap();
        let tamper = |pos: usize| {
            let mut b = original.clone();
            b[pos] ^= 0x01;
            std::fs::write(&path, &b).unwrap();
        };
        // header field (schema hash), block payload, footer
        for pos in [44, HEADER_LEN + 20, original.len() - TRAILER_LEN - 5] {
            tamper(pos);
            let r = DbcFileReader::open(&path, FileKind::Data, Some(&def), &opts.crypto)
                .and_then(|mut r| (0..r.blocks.len()).try_for_each(|i| r.read_block(i).map(|_| ())));
            assert!(r.is_err(), "tampering at {pos} not detected");
        }
        // cut the file
        std::fs::write(&path, &original[..original.len() - 30]).unwrap();
        assert!(open(&opts.crypto).is_err());
        // unknown version / v2 file
        let mut b = original.clone();
        b[4] = 9;
        std::fs::write(&path, &b).unwrap();
        assert!(open(&opts.crypto).unwrap_err().to_string().contains("version 9"));
        std::fs::write(&path, b"DBC\x02rest").unwrap();
        let e = DbcFileReader::open(&path, FileKind::Data, Some(&def), &opts.crypto).err().unwrap().to_string();
        assert!(e.contains("version 2") && e.contains("again"), "{e}");
    }

    #[test]
    fn swapped_blocks_are_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dbc");
        let mut opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
        opts.compression = Compression::None;
        write(&path, &opts, &[(0, 10), (0, 10)]);
        let def = sample_def();
        let r = DbcFileReader::open(&path, FileKind::Data, Some(&def), &opts.crypto).unwrap();
        let (a, b) = (r.blocks[0], r.blocks[1]);
        assert_eq!(a.stored_len, b.stored_len);
        let mut bytes = std::fs::read(&path).unwrap();
        let (x, y) = (a.offset as usize, b.offset as usize);
        let len = 4 + a.stored_len as usize;
        let block_a = bytes[x..x + len].to_vec();
        let block_b = bytes[y..y + len].to_vec();
        bytes[x..x + len].copy_from_slice(&block_b);
        bytes[y..y + len].copy_from_slice(&block_a);
        std::fs::write(&path, &bytes).unwrap();
        let mut r = DbcFileReader::open(&path, FileKind::Data, Some(&def), &opts.crypto).unwrap();
        assert!(r.read_block(0).is_err());
    }

    #[test]
    fn schema_mismatch_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.dbc");
        let opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
        write(&path, &opts, &[(0, 10)]);
        let mut def = sample_def();
        def.properties_info[1].data_type = LogicalType::Int64;
        assert!(DbcFileReader::open(&path, FileKind::Data, Some(&def), &opts.crypto).is_err());
    }

    #[test]
    fn meta_container_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("definition_t.dmb");
        let opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
        write_meta(&path, &sample_def(), &opts).unwrap();
        let back: DataDefinition = read_meta(&path, &opts.crypto).unwrap();
        assert_eq!(back, sample_def());
        // the plain-text definition is not visible in the file
        let raw = std::fs::read(&path).unwrap();
        assert!(!raw.windows(13).any(|w| w == b"property_name"), "definition must be encrypted");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn varints() {
        let mut out = Vec::new();
        for v in [0i64, 1, -1, 63, -64, 64, i64::MAX, i64::MIN] {
            out.clear();
            put_varint(&mut out, zigzag(v));
            let mut c = Cursor { b: &out, pos: 0 };
            assert_eq!(unzigzag(c.varint().unwrap()), v);
        }
    }
}
