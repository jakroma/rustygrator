//! Async pipelines around the dbc v3 files: blocks are encoded (compressed + encrypted) and
//! decoded on the blocking pool, several at a time, while the provider keeps reading and the
//! receiver keeps writing (FMT-7, FMT-16). Memory is bounded by `workers` blocks.

use std::{collections::VecDeque, path::Path};

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    dbc::{
        batch::ColumnBatch,
        format::{DbcFileReader, DbcFileWriter, DbcOptions, EncodedBlock, FileKind},
    },
    models::data_definitions::DataDefinition,
    DMError, DMResult,
};

/// Writes every batch received from `rx` to a dbc file; batches are regrouped into blocks of
/// `opts.block_rows` rows. Returns the number of rows.
pub async fn write_dbc_file(path: &Path, def: &DataDefinition, mut rx: mpsc::Receiver<ColumnBatch>, opts: &DbcOptions) -> DMResult<u64> {
    let p = path.to_path_buf();
    let d = def.clone();
    let o = opts.clone();
    let mut writer = tokio::task::spawn_blocking(move || DbcFileWriter::create(&p, FileKind::Data, Some(&d), &o)).await??;
    let codec = writer.codec();
    let workers = opts.workers.max(1);
    let mut pending: VecDeque<JoinHandle<DMResult<EncodedBlock>>> = VecDeque::with_capacity(workers);
    let mut current: Option<ColumnBatch> = None;
    let mut next_block = 0u64;

    let spawn = |b: ColumnBatch, n: u64| {
        let c = codec.clone();
        tokio::task::spawn_blocking(move || c.encode(n, &b))
    };

    loop {
        let incoming = rx.recv().await;
        let done = incoming.is_none();
        if let Some(b) = incoming {
            match &mut current {
                Some(c) => c.append(&b),
                None => current = Some(b),
            }
        }
        while let Some(c) = current.as_ref() {
            if c.rows == 0 || (c.rows < opts.block_rows && !done) {
                break;
            }
            // exactly `block_rows` per block (the last one may be smaller)
            let full = if c.rows > opts.block_rows {
                let head = c.slice(0, opts.block_rows);
                current = Some(c.slice(opts.block_rows, c.rows));
                head
            } else {
                current.take().unwrap()
            };
            pending.push_back(spawn(full, next_block));
            next_block += 1;
            if pending.len() >= workers {
                writer.write_block(pending.pop_front().unwrap().await??)?;
            }
        }
        if done {
            break;
        }
    }
    while let Some(h) = pending.pop_front() {
        writer.write_block(h.await??)?;
    }
    tokio::task::spawn_blocking(move || writer.finish()).await?
}

/// Authenticates every block of a file without decoding it, so a damaged or changed file is
/// rejected before the receiver writes a single row (FMT-13).
pub async fn verify_dbc_file(path: &Path, def: &DataDefinition, opts: &DbcOptions) -> DMResult<()> {
    let p = path.to_path_buf();
    let d = def.clone();
    let crypto = opts.crypto.clone();
    let workers = opts.workers.max(1);
    tokio::task::spawn_blocking(move || {
        let mut r = DbcFileReader::open(&p, FileKind::Data, Some(&d), &crypto)?;
        let codec = r.codec();
        let n = r.blocks.len();
        let mut raws = Vec::with_capacity(workers);
        let mut i = 0;
        while i < n {
            raws.clear();
            while raws.len() < workers && i < n {
                raws.push((i as u64, r.read_raw(i)?));
                i += 1;
            }
            std::thread::scope(|s| {
                let handles: Vec<_> = raws.drain(..).map(|(no, raw)| {
                    let c = codec.clone();
                    s.spawn(move || c.verify(no, raw))
                }).collect();
                handles.into_iter().try_for_each(|h| h.join().map_err(|_| DMError::Error("verify thread panicked".into()))?)
            })?;
        }
        Ok(())
    })
    .await?
}

/// Streams a dbc file into `tx`, starting at block `from_block` (resume). Blocks are decoded in
/// parallel and delivered in order.
pub async fn read_dbc_file(path: &Path, def: &DataDefinition, tx: mpsc::Sender<ColumnBatch>, opts: &DbcOptions, from_block: usize) -> DMResult<u64> {
    let p = path.to_path_buf();
    let d = def.clone();
    let crypto = opts.crypto.clone();
    let mut reader = tokio::task::spawn_blocking(move || DbcFileReader::open(&p, FileKind::Data, Some(&d), &crypto)).await??;
    let codec = reader.codec();
    let n = reader.blocks.len();
    let workers = opts.workers.max(1);
    let mut pending: VecDeque<JoinHandle<DMResult<ColumnBatch>>> = VecDeque::with_capacity(workers);
    let mut total = 0u64;
    let mut i = from_block;
    while i < n || !pending.is_empty() {
        while pending.len() < workers && i < n {
            // reading is sequential and cheap; decryption + decompression + decoding run in parallel
            let raw = reader.read_raw(i)?;
            let c = codec.clone();
            let no = i as u64;
            pending.push_back(tokio::task::spawn_blocking(move || c.decode(no, raw)));
            i += 1;
        }
        if let Some(h) = pending.pop_front() {
            let b = h.await??;
            total += b.rows as u64;
            if tx.send(b).await.is_err() {
                return Err(DMError::Error("receiver stopped".to_string()));
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbc::{
        batch::tests::{sample_kinds, sample_row},
        crypto::{DbcCrypto, KeySource},
    };

    fn def() -> DataDefinition {
        crate::dbc::format::tests::sample_def()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pipeline_regroups_batches_into_blocks_and_reads_them_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.dbc");
        let mut opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
        opts.block_rows = 100;
        opts.workers = 3;
        let (tx, rx) = mpsc::channel(2);
        let producer = async move {
            for chunk in 0..10 {
                let mut b = ColumnBatch::new(&sample_kinds(), 37);
                for i in chunk * 37..(chunk + 1) * 37 {
                    b.push_row(sample_row(i)).unwrap();
                }
                tx.send(b).await.unwrap();
            }
        };
        let d = def();
        let (_, written) = tokio::join!(producer, write_dbc_file(&path, &d, rx, &opts));
        assert_eq!(written.unwrap(), 370);
        verify_dbc_file(&path, &d, &opts).await.unwrap();

        let r = DbcFileReader::open(&path, FileKind::Data, Some(&d), &opts.crypto).unwrap();
        assert_eq!(r.blocks.iter().map(|b| b.rows).collect::<Vec<_>>(), vec![100, 100, 100, 70]);

        let (tx, mut rx) = mpsc::channel::<ColumnBatch>(2);
        let collect = async move {
            let mut rows = Vec::new();
            while let Some(b) = rx.recv().await {
                for i in 0..b.rows {
                    rows.push(b.row_values(i));
                }
            }
            rows
        };
        let (read, rows) = tokio::join!(read_dbc_file(&path, &d, tx, &opts, 0), collect);
        assert_eq!(read.unwrap(), 370);
        assert_eq!(rows.len(), 370);
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r, &sample_row(i as i64));
        }

        // resume from block 2
        let (tx, mut rx) = mpsc::channel::<ColumnBatch>(2);
        let count = async move {
            let mut n = 0;
            while let Some(b) = rx.recv().await {
                n += b.rows;
            }
            n
        };
        let (read, n) = tokio::join!(read_dbc_file(&path, &d, tx, &opts, 2), count);
        assert_eq!((read.unwrap(), n), (170, 170));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn verify_rejects_a_changed_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.dbc");
        let opts = DbcOptions::new(DbcCrypto::new(KeySource::Ephemeral));
        let (tx, rx) = mpsc::channel(1);
        let d = def();
        let mut b = ColumnBatch::new(&sample_kinds(), 10);
        for i in 0..10 {
            b.push_row(sample_row(i)).unwrap();
        }
        tx.send(b).await.unwrap();
        drop(tx);
        write_dbc_file(&path, &d, rx, &opts).await.unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[crate::dbc::format::HEADER_LEN + 10] ^= 0x80;
        std::fs::write(&path, &bytes).unwrap();
        assert!(verify_dbc_file(&path, &d, &opts).await.is_err());
    }
}
