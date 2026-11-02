//! Live transfer (CDC) event model.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::value::Value;
use crate::{
    dbc::{
        crypto::DbcCrypto,
        format::{read_meta, write_meta, DbcOptions},
    },
    DMResult,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    /// Insert or update; applied as an upsert by primary key / `_id` (LIVE-5).
    Upsert,
    /// Delete by primary key / `_id`; `row` holds only key columns.
    Delete,
}

/// One change read from the source.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// Source container name (same as `DataDefinition::container_name`).
    pub container: String,
    pub op: ChangeOp,
    /// Named values; for upserts the columns present in the change, for deletes the key.
    pub row: Vec<(String, Value)>,
    /// Commit time in the source, used for lag statistics (LIVE-14).
    pub source_time: Option<DateTime<Utc>>,
    /// `true` when `row` is the complete record (columns missing from it are null); `false`
    /// when only some columns are known (e.g. PostgreSQL unchanged TOAST values).
    pub complete: bool,
}

/// Position in the source change log (LIVE-6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Checkpoint {
    Postgresql { lsn: String },
    Sqlite { id: i64 },
    Mongodb {
        /// Canonical extended JSON of the resume token.
        resume_token: Option<serde_json::Value>,
        /// Cluster time (seconds, increment) captured before the snapshot.
        operation_time: Option<(u32, u32)>,
    },
    /// ScyllaDB CDC: every delta up to this time (ms since epoch) was applied.
    Scylla { watermark_ms: i64 },
    /// MySQL binlog file and the position after the last applied transaction.
    Mysql { file: String, pos: u64 },
    /// Oracle LogMiner: every transaction committed up to `scn` was applied; mining restarts at
    /// `start_scn` (start of the oldest transaction open at that point).
    Oracle { scn: u64, start_scn: u64 },
}

/// What is persisted in the checkpoint file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LiveState {
    pub snapshot_done: bool,
    pub checkpoint: Option<Checkpoint>,
}

impl LiveState {
    /// Reads the checkpoint container (FMT-8). A checkpoint that cannot be read with the current
    /// key (e.g. written with a one-time key of an earlier run) is ignored: the run starts with a
    /// new snapshot, which is safe because loads are idempotent.
    pub fn load(path: &Path, crypto: &DbcCrypto) -> DMResult<Option<LiveState>> {
        if !path.exists() {
            return Ok(None);
        }
        match read_meta(path, crypto) {
            Ok(s) => Ok(Some(s)),
            Err(e) => {
                log::warn!("[Live] checkpoint {} cannot be used ({}); starting with a new snapshot", path.display(), e);
                Ok(None)
            }
        }
    }

    /// Atomic save: write a temporary file and rename it over the old checkpoint.
    pub fn save(&self, path: &Path, opts: &DbcOptions) -> DMResult<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let tmp = path.with_extension("tmp");
        write_meta(&tmp, self, opts)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// Parses `X/Y` PostgreSQL LSN text into a comparable number.
pub fn parse_lsn(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_round_trip() {
        let v = parse_lsn("16/B374D848").unwrap();
        assert_eq!(format_lsn(v), "16/B374D848");
        assert!(parse_lsn("0/10").unwrap() > parse_lsn("0/F").unwrap());
    }

    #[test]
    fn state_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.dmb");
        let crypto = DbcCrypto::new(crate::dbc::crypto::KeySource::Passphrase("checkpoint key".into()));
        assert_eq!(LiveState::load(&path, &crypto).unwrap(), None);
        let s = LiveState { snapshot_done: true, checkpoint: Some(Checkpoint::Sqlite { id: 7 }) };
        s.save(&path, &DbcOptions::new(crypto.clone())).unwrap();
        assert_eq!(LiveState::load(&path, &crypto).unwrap(), Some(s.clone()));
        // another process with the same passphrase resumes; a one-time key cannot
        let again = DbcCrypto::new(crate::dbc::crypto::KeySource::Passphrase("checkpoint key".into()));
        assert_eq!(LiveState::load(&path, &again).unwrap(), Some(s));
        assert_eq!(LiveState::load(&path, &DbcCrypto::new(crate::dbc::crypto::KeySource::Ephemeral)).unwrap(), None);
    }
}
