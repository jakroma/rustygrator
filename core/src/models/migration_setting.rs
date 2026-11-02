use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::dbc::{crypto::KeySource, format::Compression};

use super::{
    migration_execution_settings::{
        BatchSettings, ContainerFilter, DbcSettings, LiveSettings, MigrationExecutionSettings, RunMode,
        ScyllaSettings, SqlToNoSqlStrategy, TransferMode,
    },
};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct MigrationSetting {
    /// Data provider connection string (postgresql://, mongodb://, sqlite://)
    #[arg(long, short = 'f')]
    pub migrate_from: Option<String>,
    /// Data receiver connection string (postgresql://, mongodb://, sqlite://)
    #[arg(long, short = 't')]
    pub migrate_to: Option<String>,
    /// Number of batches buffered between provider and receiver
    #[arg(long = "tl", default_value = "4")]
    pub threads_limit: u32,
    /// Rows per fetched / written batch (start value when --adaptive-batch is on)
    #[arg(long, short = 'p', default_value = "4096")]
    pub pagination_limit: u32,
    /// Tune the batch size during the run from the measured throughput (PERF-16)
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub adaptive_batch: bool,
    /// Adaptive batch: smallest / largest batch in rows and largest batch in MiB
    #[arg(long, default_value = "256")]
    pub min_batch: usize,
    #[arg(long, default_value = "16384")]
    pub max_batch: usize,
    #[arg(long, default_value = "8")]
    pub max_batch_mb: usize,
    /// Independent tables migrated at the same time (0 = number of CPU cores)
    #[arg(long, default_value = "0")]
    pub parallel_tables: usize,
    /// Parallel range readers for one large table (PostgreSQL / MongoDB / ScyllaDB); off by
    /// default because it only pays off when the database has spare CPU cores
    #[arg(long, default_value = "1")]
    pub parallel_ranges: usize,
    /// dbc files: block compression
    #[arg(long, value_enum, default_value = "lz4")]
    pub compression: Compression,
    /// dbc files: zstd level (with --compression zstd)
    #[arg(long, default_value = "3")]
    pub zstd_level: i32,
    /// dbc files: rows per block
    #[arg(long, default_value = "8192")]
    pub block_rows: usize,
    /// dbc files: key file (separate provide / receive runs); otherwise DM_DBC_KEY, otherwise
    /// a one-time key kept in memory
    #[arg(long)]
    pub key_file: Option<PathBuf>,
    /// Keep the temporary files after a successful run
    #[arg(long)]
    pub keep_temp: bool,
    /// ScyllaDB receiver: replication map for a new keyspace
    #[arg(long, default_value = "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")]
    pub scylla_replication: String,
    /// ScyllaDB: write consistency level
    #[arg(long, default_value = "local_quorum")]
    pub scylla_consistency: String,
    /// ScyllaDB: requests in flight per table
    #[arg(long, default_value = "64")]
    pub scylla_concurrency: usize,
    /// ScyllaDB provider: token ranges per table (read in parallel)
    #[arg(long, default_value = "32")]
    pub scylla_token_ranges: usize,
    /// ScyllaDB receiver: key of a table, `table=(partition,cols),clustering,cols` (repeatable)
    #[arg(long = "scylla-key", value_parser = parse_key_override)]
    pub scylla_keys: Vec<(String, (Vec<String>, Vec<String>))>,
    /// Which part of the process to run
    #[arg(long, value_enum, default_value = "full")]
    pub mode: RunMode,
    /// How data travels from provider to receiver in `full` mode
    #[arg(long, value_enum, default_value = "channel")]
    pub transfer: TransferMode,
    /// SQL -> NoSQL strategy
    #[arg(long, value_enum, default_value = "denormalized")]
    pub strategy: SqlToNoSqlStrategy,
    /// Temporary files location (definitions, dbc data files, checkpoint)
    #[arg(long, default_value = "./dbcTemp")]
    pub temp_dir: PathBuf,
    /// Only these tables / collections (comma separated, full or short names)
    #[arg(long, value_delimiter = ',')]
    pub include: Vec<String>,
    /// Skip these tables / collections (comma separated, full or short names)
    #[arg(long, value_delimiter = ',')]
    pub exclude: Vec<String>,
    /// MongoDB schema analysis sample size (0 = analyze all documents)
    #[arg(long, default_value = "0")]
    pub schema_sample: u32,
    /// After the initial copy keep streaming changes (CDC)
    #[arg(long)]
    pub live: bool,
    /// Live: max events per batch
    #[arg(long, default_value = "500")]
    pub batch_size: usize,
    /// Live: max time to wait for a batch to fill, in milliseconds
    #[arg(long, default_value = "1000")]
    pub batch_wait_ms: u64,
    /// Live: checkpoint file (default: <temp-dir>/checkpoint.json)
    #[arg(long)]
    pub checkpoint_file: Option<PathBuf>,
    /// Live: statistics log interval in seconds
    #[arg(long, default_value = "10")]
    pub stats_interval_secs: u64,
    /// Live: retries (with exponential backoff) before giving up on a failing batch
    #[arg(long, default_value = "10")]
    pub max_retries: u32,
    /// Live (PostgreSQL source): logical replication slot name
    #[arg(long, default_value = "data_mover_slot")]
    pub slot_name: String,
    /// Live (PostgreSQL source): publication name
    #[arg(long, default_value = "data_mover_pub")]
    pub publication: String,
}

fn parse_key_override(s: &str) -> Result<(String, (Vec<String>, Vec<String>)), String> {
    ScyllaSettings::parse_key_override(s)
}

impl MigrationSetting {
    pub fn execution_settings(&self) -> MigrationExecutionSettings {
        let mut s = MigrationExecutionSettings::new(self.threads_limit, self.pagination_limit);
        s.temp_dir = self.temp_dir.clone();
        s.mode = self.mode;
        s.transfer = self.transfer;
        s.strategy = self.strategy;
        s.filter = ContainerFilter::new(self.include.clone(), self.exclude.clone());
        s.schema_sample = self.schema_sample;
        s.batch = BatchSettings {
            adaptive: self.adaptive_batch,
            min_rows: self.min_batch.max(1),
            max_rows: self.max_batch.max(self.min_batch.max(1)),
            max_bytes: self.max_batch_mb.max(1) << 20,
        };
        if self.parallel_tables > 0 {
            s.parallel_tables = self.parallel_tables;
        }
        s.parallel_ranges = self.parallel_ranges.max(1);
        s.dbc = DbcSettings {
            compression: self.compression,
            zstd_level: self.zstd_level,
            block_rows: self.block_rows.max(1),
            key: KeySource::from_options(self.key_file.clone()),
            keep_temp: self.keep_temp,
        };
        s.scylla = ScyllaSettings {
            replication: self.scylla_replication.clone(),
            consistency: self.scylla_consistency.clone(),
            concurrency: self.scylla_concurrency.max(1),
            token_ranges: self.scylla_token_ranges.max(1),
            key_overrides: self.scylla_keys.iter().cloned().collect(),
        };
        s.live = LiveSettings {
            enabled: self.live,
            batch_size: self.batch_size.max(1),
            batch_wait: Duration::from_millis(self.batch_wait_ms),
            checkpoint_file: self.checkpoint_file.clone(),
            stats_interval: Duration::from_secs(self.stats_interval_secs.max(1)),
            max_retries: self.max_retries,
            slot_name: self.slot_name.clone(),
            publication_name: self.publication.clone(),
        };
        s
    }
}
