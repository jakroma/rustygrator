use clap::Parser;
use rustygrator_core::{
    migrator::migrator::Migrator,
    models::{
        connection::MigrationConnection,
        migration_execution_settings::RunMode,
        migration_setting::MigrationSetting,
    },
    DMError, DMResult,
};
use env_logger::Env;
use log::{info, warn};
use tokio::sync::watch;

/// Ctrl+C / SIGTERM request a graceful stop (LIVE-13): the current batch is flushed and the
/// checkpoint saved before exiting.
fn shutdown_signal() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = term.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        warn!("Shutdown requested, finishing the current batch...");
        let _ = tx.send(true);
    });
    rx
}

#[tokio::main]
async fn main() -> DMResult<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Migration started.");
    let settings: MigrationSetting = MigrationSetting::parse();
    let exec = settings.execution_settings();

    let needs_provider = exec.mode != RunMode::Receive;
    let needs_receiver = exec.mode != RunMode::Provide;
    let connection = |url: &Option<String>, flag: &str| -> DMResult<MigrationConnection> {
        let url = url
            .as_ref()
            .ok_or_else(|| DMError::InvalidConnectionString(format!("{} is required", flag)))?;
        MigrationConnection::new(url)
    };

    let from = if needs_provider { Some(connection(&settings.migrate_from, "--migrate-from")?) } else { None };
    let to = if needs_receiver { Some(connection(&settings.migrate_to, "--migrate-to")?) } else { None };

    let mut migrator = Migrator::connect(from, to, exec).await?;
    let summary = migrator.run_with_shutdown(shutdown_signal()).await?;
    info!("Migration ended: {:?}", summary);

    Ok(())
}
