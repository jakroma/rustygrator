use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use futures_util::{stream, StreamExt, TryStreamExt};
use log::{info, warn};
use tokio::sync::{mpsc, watch};

use crate::{
    db::database::{connect, order_definitions, DatabaseConnectionType, DatabaseType},
    dbc::{
        batch::ColumnBatch,
        crypto::DbcCrypto,
        format::{read_meta, write_meta, DbcOptions},
        io::{read_dbc_file, verify_dbc_file, write_dbc_file},
    },
    migrator::{
        batching::BatchSizer,
        live::{run_live, LiveContext, LiveSummary},
        migrator_steps::{ChangeSource, DataProvider, DataReceiver, MigrationStats},
    },
    models::{
        change::LiveState,
        connection::MigrationConnection,
        data_definitions::DataDefinition,
        migration_execution_settings::{MigrationExecutionSettings, RunMode, TransferMode},
    },
    utils::file_utils::{
        data_file_path, definition_file_path, load_sorted_containers_from_file, order_file_path, prepare_temp_folder,
        remove_files, save_sorted_containers_to_file,
    },
    DMError, DMResult,
};

/// Result of a run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MigrationSummary {
    pub containers: u64,
    pub rows: u64,
    pub live: Option<LiveSummary>,
}

pub struct Migrator {
    data_provider: Option<DatabaseConnectionType>,
    data_receiver: Option<DatabaseConnectionType>,
    /// Connection settings for extra workers (parallel tables, PERF-10).
    from: Option<MigrationConnection>,
    to: Option<MigrationConnection>,
    settings: MigrationExecutionSettings,
    dbc: DbcOptions,
    stats: Arc<MigrationStats>,
}

fn missing(what: &str) -> DMError {
    DMError::Error(format!("{} connection is required in this mode", what))
}

fn both<T>(a: DMResult<T>, b: DMResult<u64>) -> DMResult<u64> {
    // the consumer's error is the root cause when both fail
    match (a, b) {
        (_, Err(e)) => Err(e),
        (Err(e), _) => Err(e),
        (Ok(_), Ok(n)) => Ok(n),
    }
}

/// Dependency levels: tables of one level do not reference each other (FN-5 order).
fn dependency_levels(defs: &[DataDefinition]) -> Vec<Vec<&DataDefinition>> {
    let mut level_of: HashMap<&str, usize> = HashMap::new();
    let mut levels: Vec<Vec<&DataDefinition>> = Vec::new();
    for d in defs {
        let lvl = d
            .dependencies()
            .iter()
            .filter_map(|dep| level_of.get(dep.as_str()).map(|l| l + 1))
            .max()
            .unwrap_or(0);
        level_of.insert(&d.container_name, lvl);
        if levels.len() <= lvl {
            levels.resize_with(lvl + 1, Vec::new);
        }
        levels[lvl].push(d);
    }
    levels
}

/// Provider -> bounded channel -> receiver; the middle stage feeds the adaptive batch size
/// (PERF-16).
async fn pipe(
    provider: &mut DatabaseConnectionType,
    sink: impl std::future::Future<Output = DMResult<u64>>,
    def: &DataDefinition,
    settings: &MigrationExecutionSettings,
    stats: &MigrationStats,
    tx_out: mpsc::Sender<ColumnBatch>,
) -> DMResult<u64> {
    let cap = settings.thread_limit.max(1) as usize;
    let sizer = Arc::new(BatchSizer::new(settings.batch_size(), &settings.batch));
    let (tx, mut rx) = mpsc::channel::<ColumnBatch>(cap);
    let s = sizer.clone();
    let forward = async move {
        while let Some(b) = rx.recv().await {
            s.observe(b.rows, b.memory_size());
            stats.table_batch(&def.container_name, b.rows, b.memory_size());
            if tx_out.send(b).await.is_err() {
                return Err(DMError::Error("receiver stopped".to_string()));
            }
        }
        Ok::<_, DMError>(())
    };
    let (read, fwd, written) = tokio::join!(provider.read_data(def, sizer, tx), forward, sink);
    both(read.and(fwd), written)
}

/// One table through in-memory channels (FN-1).
async fn transfer_channel(
    provider: &mut DatabaseConnectionType,
    receiver: &mut DatabaseConnectionType,
    def: &DataDefinition,
    settings: &MigrationExecutionSettings,
    stats: &MigrationStats,
) -> DMResult<u64> {
    stats.table_started(&def.container_name, "transfer");
    let (tx, rx) = mpsc::channel(settings.thread_limit.max(1) as usize);
    pipe(provider, receiver.write_data(def, rx), def, settings, stats, tx).await
}

/// Provider -> dbc data file (FN-2).
async fn provide_to_file(
    provider: &mut DatabaseConnectionType,
    def: &DataDefinition,
    temp_dir: &Path,
    settings: &MigrationExecutionSettings,
    dbc: &DbcOptions,
    stats: &MigrationStats,
) -> DMResult<u64> {
    info!("[Migration][{}] Migrate Data to file", def.container_name);
    stats.table_started(&def.container_name, "write file");
    let (tx, rx) = mpsc::channel(settings.thread_limit.max(1) as usize);
    let path = data_file_path(temp_dir, &def.container_name);
    pipe(provider, write_dbc_file(&path, def, rx, dbc), def, settings, stats, tx).await
}

/// dbc data file -> receiver, block by block (FN-4).
async fn receive_from_file(
    receiver: &mut DatabaseConnectionType,
    def: &DataDefinition,
    temp_dir: &Path,
    settings: &MigrationExecutionSettings,
    dbc: &DbcOptions,
    stats: &MigrationStats,
) -> DMResult<u64> {
    stats.table_started(&def.container_name, "load file");
    let cap = settings.thread_limit.max(1) as usize;
    let (tx, mut from_file) = mpsc::channel::<ColumnBatch>(cap);
    let (to_receiver, rx) = mpsc::channel(cap);
    let path = data_file_path(temp_dir, &def.container_name);
    let forward = async move {
        while let Some(b) = from_file.recv().await {
            stats.table_batch(&def.container_name, b.rows, b.memory_size());
            if to_receiver.send(b).await.is_err() {
                return Err(DMError::Error("receiver stopped".to_string()));
            }
        }
        Ok::<_, DMError>(())
    };
    let (read, fwd, written) = tokio::join!(read_dbc_file(&path, def, tx, dbc, 0), forward, receiver.write_data(def, rx));
    both(read.and(fwd), written)
}

#[derive(Clone, Copy, PartialEq)]
enum Step {
    Channel,
    ToFile,
    FromFile,
}

impl Migrator {
    /// Migrator over already opened connections (tables are migrated one by one).
    pub fn with_settings(
        data_provider: Option<DatabaseConnectionType>,
        data_receiver: Option<DatabaseConnectionType>,
        settings: MigrationExecutionSettings,
    ) -> Self {
        let mut dbc = DbcOptions::new(DbcCrypto::new(settings.dbc.key.clone()));
        dbc.compression = settings.dbc.compression;
        dbc.zstd_level = settings.dbc.zstd_level;
        dbc.block_rows = settings.dbc.block_rows.max(1);
        Migrator { data_provider, data_receiver, from: None, to: None, settings, dbc, stats: Arc::new(MigrationStats::new()) }
    }

    /// Opens the connections; extra connections are opened later for parallel tables.
    pub async fn connect(
        from: Option<MigrationConnection>,
        to: Option<MigrationConnection>,
        settings: MigrationExecutionSettings,
    ) -> DMResult<Self> {
        let provider = match &from {
            Some(c) => Some(connect(c.clone(), settings.clone()).await?),
            None => None,
        };
        let receiver = match &to {
            Some(c) => Some(connect(c.clone(), settings.clone()).await?),
            None => None,
        };
        let mut m = Self::with_settings(provider, receiver, settings);
        m.from = from;
        m.to = to;
        Ok(m)
    }

    /// Connection settings for the extra connections of parallel tables (PERF-10) when the main
    /// connections were opened elsewhere (the desktop app's pool).
    pub fn with_workers(mut self, from: Option<MigrationConnection>, to: Option<MigrationConnection>) -> Self {
        self.from = from;
        self.to = to;
        self
    }

    pub fn settings(&self) -> &MigrationExecutionSettings {
        &self.settings
    }

    pub fn dbc_options(&self) -> &DbcOptions {
        &self.dbc
    }

    /// Counters that can be read while the migration runs (MAPP-2, MAPP-3).
    pub fn stats(&self) -> Arc<MigrationStats> {
        self.stats.clone()
    }

    pub async fn run(&mut self) -> DMResult<MigrationSummary> {
        let (_tx, rx) = watch::channel(false);
        self.run_with_shutdown(rx).await
    }

    /// Runs until done; in live mode until `shutdown` becomes `true`.
    pub async fn run_with_shutdown(&mut self, shutdown: watch::Receiver<bool>) -> DMResult<MigrationSummary> {
        info!("[Migration] Started!");
        let started = Instant::now();
        let temp_dir = prepare_temp_folder(&self.settings.temp_dir)?;
        let mut live = None;
        match self.settings.mode {
            RunMode::Provide => {
                let defs = self.definitions().await?;
                self.write_definitions(&temp_dir, &defs)?;
                self.run_tables(&temp_dir, &defs, Step::ToFile).await?;
            }
            RunMode::Receive => {
                let order = load_sorted_containers_from_file(&temp_dir, &self.dbc.crypto)?;
                let defs: Vec<DataDefinition> = order
                    .iter()
                    .map(|c| read_meta(&definition_file_path(&temp_dir, c), &self.dbc.crypto))
                    .collect::<DMResult<Vec<DataDefinition>>>()?
                    .into_iter()
                    .filter(|d| self.settings.filter.accepts(&d.container_name))
                    .collect();
                self.stats.set_order(defs.iter().map(|d| d.container_name.clone()).collect());
                self.load_files(&temp_dir, &defs).await?;
                self.cleanup(&temp_dir, &defs);
            }
            RunMode::Full => {
                let defs = self.definitions().await?;
                if self.settings.live.enabled {
                    live = Some(self.run_full_live(&temp_dir, &defs, shutdown).await?);
                } else {
                    self.snapshot(&temp_dir, &defs).await?;
                }
            }
        }
        let (containers, rows) = self.stats.snapshot();
        info!("[Migration] Ended! {} containers, {} rows in {:.2?}", containers, rows, started.elapsed());
        Ok(MigrationSummary { containers, rows, live })
    }

    fn receiver(&mut self) -> DMResult<&mut DatabaseConnectionType> {
        self.data_receiver.as_mut().ok_or_else(|| missing("receiver (--migrate-to)"))
    }

    async fn definitions(&mut self) -> DMResult<Vec<DataDefinition>> {
        info!("[Migration][Provider] Read definitions.");
        let filter = self.settings.filter.clone();
        let provider = self.data_provider.as_mut().ok_or_else(|| missing("provider (--migrate-from)"))?;
        let defs = order_definitions(provider.get_definitions(&filter).await?)?;
        self.stats.set_order(defs.iter().map(|d| d.container_name.clone()).collect());
        Ok(defs)
    }

    /// Definitions and migration order as encrypted containers (FMT-8).
    fn write_definitions(&self, temp_dir: &Path, defs: &[DataDefinition]) -> DMResult<()> {
        for def in defs {
            info!("[Migration][{}] Write definitions", def.container_name);
            write_meta(&definition_file_path(temp_dir, &def.container_name), def, &self.dbc)?;
        }
        let order: Vec<String> = defs.iter().map(|d| d.container_name.clone()).collect();
        save_sorted_containers_to_file(temp_dir, &order, &self.dbc)
    }

    /// Removes the temporary files of a finished run unless `--keep-temp` (FMT-14).
    fn cleanup(&self, temp_dir: &Path, defs: &[DataDefinition]) {
        if self.settings.dbc.keep_temp {
            return;
        }
        let mut files: Vec<PathBuf> = defs
            .iter()
            .flat_map(|d| [data_file_path(temp_dir, &d.container_name), definition_file_path(temp_dir, &d.container_name)])
            .collect();
        files.push(order_file_path(temp_dir));
        remove_files(&files);
    }

    async fn run_full_live(&mut self, temp_dir: &Path, defs: &[DataDefinition], shutdown: watch::Receiver<bool>) -> DMResult<LiveSummary> {
        let checkpoint_file = self.settings.checkpoint_file();
        if self.dbc.crypto.source().is_ephemeral() {
            warn!("[Live] no --key-file / DM_DBC_KEY: the checkpoint uses a one-time key, a restart will take a new snapshot");
        }
        let mut state = LiveState::load(&checkpoint_file, &self.dbc.crypto)?.unwrap_or_default();
        let provider = self.data_provider.as_mut().ok_or_else(|| missing("provider (--migrate-from)"))?;
        // capture starts before the snapshot so nothing changed meanwhile is lost (LIVE-4)
        provider.start_capture(defs, state.checkpoint.as_ref()).await?;
        if !state.snapshot_done {
            self.snapshot(temp_dir, defs).await?;
            let provider = self.data_provider.as_mut().ok_or_else(|| missing("provider"))?;
            state.checkpoint = provider.acknowledge().await?;
            state.snapshot_done = true;
            state.save(&checkpoint_file, &self.dbc)?;
        } else {
            info!("[Live] Resuming from checkpoint {:?}", state.checkpoint);
        }
        let settings = self.settings.clone();
        let dbc = self.dbc.clone();
        let (Some(provider), Some(receiver)) = (self.data_provider.as_mut(), self.data_receiver.as_mut()) else {
            return Err(missing("provider and receiver"));
        };
        run_live(provider, receiver, defs, LiveContext { settings: &settings, dbc: &dbc, counters: &self.stats }, state, shutdown).await
    }

    async fn snapshot(&mut self, temp_dir: &Path, defs: &[DataDefinition]) -> DMResult<()> {
        match self.settings.transfer {
            TransferMode::Channel => {
                self.receiver()?.prepare(defs).await?;
                self.run_tables(temp_dir, defs, Step::Channel).await?;
                self.receiver()?.finalize(defs).await
            }
            TransferMode::File => {
                self.write_definitions(temp_dir, defs)?;
                self.run_tables(temp_dir, defs, Step::ToFile).await?;
                self.load_files(temp_dir, defs).await?;
                self.cleanup(temp_dir, defs);
                Ok(())
            }
        }
    }

    /// Every data file is authenticated before the first row goes to the receiver (FMT-13).
    async fn load_files(&mut self, temp_dir: &Path, defs: &[DataDefinition]) -> DMResult<()> {
        for def in defs {
            verify_dbc_file(&data_file_path(temp_dir, &def.container_name), def, &self.dbc).await?;
        }
        self.receiver()?.prepare(defs).await?;
        self.run_tables(temp_dir, defs, Step::FromFile).await?;
        self.receiver()?.finalize(defs).await
    }

    fn parallelism(&self, step: Step) -> usize {
        let wanted = self.settings.parallel_tables.max(1);
        let provider_ok = step == Step::FromFile || self.from.is_some();
        let receiver_ok = step == Step::ToFile || self.to.as_ref().is_some_and(|c| c.db_type != DatabaseType::Sqlite);
        // SQLite has a single writer: parallel writers would only wait for each other
        if provider_ok && receiver_ok {
            wanted
        } else {
            1
        }
    }

    /// Runs `step` for every table; independent tables (same dependency level) in parallel on
    /// their own connections (PERF-10).
    async fn run_tables(&mut self, temp_dir: &Path, defs: &[DataDefinition], step: Step) -> DMResult<()> {
        let parallel = self.parallelism(step);
        if parallel <= 1 {
            for def in defs {
                let started = Instant::now();
                let rows = match step {
                    Step::Channel => {
                        let (Some(p), Some(r)) = (self.data_provider.as_mut(), self.data_receiver.as_mut()) else {
                            return Err(missing("provider and receiver"));
                        };
                        transfer_channel(p, r, def, &self.settings, &self.stats).await?
                    }
                    Step::ToFile => {
                        let p = self.data_provider.as_mut().ok_or_else(|| missing("provider"))?;
                        provide_to_file(p, def, temp_dir, &self.settings, &self.dbc, &self.stats).await?
                    }
                    Step::FromFile => {
                        let r = self.data_receiver.as_mut().ok_or_else(|| missing("receiver"))?;
                        receive_from_file(r, def, temp_dir, &self.settings, &self.dbc, &self.stats).await?
                    }
                };
                self.count(step, def, rows, started);
            }
            return Ok(());
        }
        for level in dependency_levels(defs) {
            type TableFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = DMResult<(String, u64, Instant)>> + Send + 'a>>;
            let mut futures: Vec<TableFuture<'_>> = Vec::with_capacity(level.len());
            for def in level {
                futures.push(Box::pin(self.table_on_new_connections(temp_dir, def, step)));
            }
            let results: Vec<(String, u64, Instant)> = stream::iter(futures).buffer_unordered(parallel).try_collect().await?;
            for (name, rows, started) in results {
                if let Some(def) = defs.iter().find(|d| d.container_name == name) {
                    self.count(step, def, rows, started);
                }
            }
        }
        Ok(())
    }

    fn count(&self, step: Step, def: &DataDefinition, rows: u64, started: Instant) {
        info!("[Migration][{}] {} rows in {:.2?}", def.container_name, rows, started.elapsed());
        self.stats.table_done(&def.container_name);
        // a file run counts each table once, when it is loaded (or written in provide mode)
        if step != Step::ToFile || self.settings.mode == RunMode::Provide {
            self.stats.add_container(rows);
        }
    }

    async fn table_on_new_connections(&self, temp_dir: &Path, def: &DataDefinition, step: Step) -> DMResult<(String, u64, Instant)> {
        let started = Instant::now();
        let open = |c: &Option<MigrationConnection>| {
            let c = c.clone();
            let s = self.settings.clone();
            async move { connect(c.ok_or_else(|| missing("worker"))?, s).await }
        };
        let rows = match step {
            Step::Channel => {
                let (mut p, mut r) = (open(&self.from).await?, open(&self.to).await?);
                transfer_channel(&mut p, &mut r, def, &self.settings, &self.stats).await?
            }
            Step::ToFile => provide_to_file(&mut open(&self.from).await?, def, temp_dir, &self.settings, &self.dbc, &self.stats).await?,
            Step::FromFile => receive_from_file(&mut open(&self.to).await?, def, temp_dir, &self.settings, &self.dbc, &self.stats).await?,
        };
        Ok((def.container_name.clone(), rows, started))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::data_definitions::ForeignKeyInfo;

    #[test]
    fn levels_group_independent_tables() {
        let mk = |n: &str, deps: &[&str]| {
            let mut d = DataDefinition::new(n.into(), vec![]);
            d.foreign_keys = deps
                .iter()
                .map(|r| ForeignKeyInfo { name: "fk".into(), columns: vec![], reference_container_name: r.to_string(), reference_columns: vec![] })
                .collect();
            d
        };
        let defs = vec![mk("a", &[]), mk("b", &[]), mk("c", &["a"]), mk("d", &["c", "b"]), mk("e", &["a"])];
        let names: Vec<Vec<&str>> = dependency_levels(&defs).iter().map(|l| l.iter().map(|d| d.container_name.as_str()).collect()).collect();
        assert_eq!(names, vec![vec!["a", "b"], vec!["c", "e"], vec!["d"]]);
    }
}
