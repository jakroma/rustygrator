use std::path::PathBuf;
use std::time::Duration;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::dbc::{
    crypto::KeySource,
    format::{Compression, DEFAULT_BLOCK_ROWS},
};

/// SQL -> NoSQL strategy (section 4 of the spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum SqlToNoSqlStrategy {
    /// Referenced rows are embedded into the referencing document (SN-1, SN-3).
    Denormalized,
    /// One collection per table, foreign keys kept as plain reference fields (SN-2, SN-4).
    Normalized,
}

/// How data travels from provider to receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TransferMode {
    /// In-process bounded channels, no temporary files (FN-1).
    Channel,
    /// Through `dbc` data files in the temporary directory (FN-2).
    File,
}

/// Which part of the process runs in this invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunMode {
    /// Provider and receiver in one process.
    Full,
    /// Only the provider: writes definitions, data files and migration order.
    Provide,
    /// Only the receiver: imports what a previous `provide` run wrote.
    Receive,
}

/// Include / exclude list of containers (LIVE-9, also used for one-off migrations).
#[derive(Debug, Clone, Default)]
pub struct ContainerFilter {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl ContainerFilter {
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        ContainerFilter { include, exclude }
    }

    fn matches(list: &[String], container: &str) -> bool {
        let short = crate::models::data_definitions::short_name(container);
        list.iter().any(|x| x == container || x == short)
    }

    pub fn accepts(&self, container: &str) -> bool {
        (self.include.is_empty() || Self::matches(&self.include, container))
            && !Self::matches(&self.exclude, container)
    }
}

/// Live transfer settings (section 5).
#[derive(Debug, Clone)]
pub struct LiveSettings {
    pub enabled: bool,
    pub batch_size: usize,
    pub batch_wait: Duration,
    pub checkpoint_file: Option<PathBuf>,
    pub stats_interval: Duration,
    pub max_retries: u32,
    pub slot_name: String,
    pub publication_name: String,
}

impl Default for LiveSettings {
    fn default() -> Self {
        LiveSettings {
            enabled: false,
            batch_size: 500,
            batch_wait: Duration::from_millis(1000),
            checkpoint_file: None,
            stats_interval: Duration::from_secs(10),
            max_retries: 10,
            slot_name: "data_mover_slot".to_string(),
            publication_name: "data_mover_pub".to_string(),
        }
    }
}

/// Temporary `dbc` files (section 7).
#[derive(Debug, Clone)]
pub struct DbcSettings {
    pub compression: Compression,
    pub zstd_level: i32,
    pub block_rows: usize,
    pub key: KeySource,
    /// Keep the temporary files after a successful run (FMT-14).
    pub keep_temp: bool,
}

impl Default for DbcSettings {
    fn default() -> Self {
        DbcSettings { compression: Compression::Lz4, zstd_level: 3, block_rows: DEFAULT_BLOCK_ROWS, key: KeySource::Ephemeral, keep_temp: false }
    }
}

/// Adaptive batch size limits (PERF-16).
#[derive(Debug, Clone)]
pub struct BatchSettings {
    pub adaptive: bool,
    pub min_rows: usize,
    pub max_rows: usize,
    /// Upper bound for the memory of one batch.
    pub max_bytes: usize,
}

impl Default for BatchSettings {
    fn default() -> Self {
        BatchSettings { adaptive: true, min_rows: 256, max_rows: 16_384, max_bytes: 8 << 20 }
    }
}

/// ScyllaDB settings (SCY-5..SCY-8).
#[derive(Debug, Clone)]
pub struct ScyllaSettings {
    /// Replication map used when the receiver creates the keyspace.
    pub replication: String,
    /// Write consistency level name (`local_quorum`, `one`, `all`, ...).
    pub consistency: String,
    /// Requests in flight per table.
    pub concurrency: usize,
    /// Token ranges each table is split into when read.
    pub token_ranges: usize,
    /// Per table key override: table -> (partition key, clustering key).
    pub key_overrides: std::collections::HashMap<String, (Vec<String>, Vec<String>)>,
}

impl Default for ScyllaSettings {
    fn default() -> Self {
        ScyllaSettings {
            replication: "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}".into(),
            consistency: "local_quorum".into(),
            concurrency: 64,
            token_ranges: 32,
            key_overrides: Default::default(),
        }
    }
}

impl ScyllaSettings {
    /// Parses `table=(a,b),c` (partition key a, b; clustering key c) or `table=a,c`.
    pub fn parse_key_override(s: &str) -> Result<(String, (Vec<String>, Vec<String>)), String> {
        let (table, key) = s.split_once('=').ok_or_else(|| format!("expected table=key, got '{}'", s))?;
        let key = key.trim();
        let (partition, rest) = match key.strip_prefix('(') {
            Some(r) => {
                let (p, rest) = r.split_once(')').ok_or_else(|| format!("missing ')' in '{}'", s))?;
                (p.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect::<Vec<_>>(), rest.trim_start_matches(','))
            }
            None => {
                let mut it = key.splitn(2, ',');
                (vec![it.next().unwrap_or("").trim().to_string()], it.next().unwrap_or(""))
            }
        };
        let clustering = rest.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect();
        if partition.is_empty() || partition[0].is_empty() {
            return Err(format!("empty partition key in '{}'", s));
        }
        Ok((table.trim().to_string(), (partition, clustering)))
    }
}

/// Number of CPU cores (1..=8): measured best value for parallel tables (PERF-10).
pub fn default_parallelism() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2).clamp(1, 8)
}

/// Everything a run needs besides the two connections.
#[derive(Debug, Clone)]
pub struct MigrationExecutionSettings {
    pub thread_limit: u32,
    /// Rows per fetched / written batch.
    pub pagination_limit: u32,
    pub temp_dir: PathBuf,
    pub mode: RunMode,
    pub transfer: TransferMode,
    pub strategy: SqlToNoSqlStrategy,
    pub filter: ContainerFilter,
    /// MongoDB schema analysis: number of sampled documents, 0 = all documents.
    pub schema_sample: u32,
    pub live: LiveSettings,
    pub dbc: DbcSettings,
    pub batch: BatchSettings,
    /// Independent tables migrated at the same time (PERF-10).
    pub parallel_tables: usize,
    /// Parallel range readers for one large table (PERF-11).
    pub parallel_ranges: usize,
    pub scylla: ScyllaSettings,
}

impl MigrationExecutionSettings {
    pub fn new(thread_limit: u32, pagination_limit: u32) -> Self {
        MigrationExecutionSettings {
            thread_limit,
            pagination_limit,
            temp_dir: PathBuf::from(crate::constants::TEMP_DIRECTORY.join(std::path::MAIN_SEPARATOR_STR)),
            mode: RunMode::Full,
            transfer: TransferMode::Channel,
            strategy: SqlToNoSqlStrategy::Denormalized,
            filter: ContainerFilter::default(),
            schema_sample: 0,
            live: LiveSettings::default(),
            dbc: DbcSettings::default(),
            batch: BatchSettings::default(),
            parallel_tables: default_parallelism(),
            parallel_ranges: 1,
            scylla: ScyllaSettings::default(),
        }
    }

    pub fn batch_size(&self) -> usize {
        self.pagination_limit.max(1) as usize
    }

    pub fn checkpoint_file(&self) -> PathBuf {
        self.live
            .checkpoint_file
            .clone()
            .unwrap_or_else(|| self.temp_dir.join(crate::constants::CHECKPOINT_FILE_NAME))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scylla_key_override_syntax() {
        assert_eq!(ScyllaSettings::parse_key_override("t=(a,b),c,d").unwrap(), ("t".into(), (vec!["a".into(), "b".into()], vec!["c".into(), "d".into()])));
        assert_eq!(ScyllaSettings::parse_key_override("t=a,c").unwrap(), ("t".into(), (vec!["a".into()], vec!["c".into()])));
        assert_eq!(ScyllaSettings::parse_key_override("t=a").unwrap(), ("t".into(), (vec!["a".into()], vec![])));
        assert!(ScyllaSettings::parse_key_override("t").is_err());
    }

    #[test]
    fn filter_matches_full_and_short_names() {
        let f = ContainerFilter::new(vec!["users".into()], vec![]);
        assert!(f.accepts("public.users"));
        assert!(!f.accepts("public.orders"));
        let f = ContainerFilter::new(vec![], vec!["public.logs".into()]);
        assert!(f.accepts("public.users"));
        assert!(!f.accepts("public.logs"));
    }
}
