//! Migration / live transfer jobs (MAPP-1, MAPP-4). A job is turned into the CLI arguments and
//! parsed by the CLI's own parser, so the app and the CLI always have the same options.

use clap::Parser;
use serde::{Deserialize, Serialize};

use rustygrator_core::models::migration_setting::MigrationSetting;

/// Fields missing in an older settings file take their default (FIX-3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Job {
    pub name: String,
    pub source: String,
    pub target: String,
    pub strategy: String,
    pub transfer: String,
    pub batch: u32,
    pub compression: String,
    pub key_file: Option<String>,
    pub include: String,
    pub exclude: String,
    pub live: bool,
}

impl Default for Job {
    fn default() -> Self {
        Job {
            name: String::new(),
            source: String::new(),
            target: String::new(),
            strategy: "denormalized".into(),
            transfer: "channel".into(),
            batch: 4096,
            compression: "lz4".into(),
            key_file: None,
            include: String::new(),
            exclude: String::new(),
            live: false,
        }
    }
}

pub const STRATEGIES: [&str; 2] = ["denormalized", "normalized"];
pub const TRANSFERS: [&str; 2] = ["channel", "file"];
pub const COMPRESSIONS: [&str; 3] = ["lz4", "zstd", "none"];

impl Job {
    /// CLI arguments for the job with the given connection strings.
    pub fn args(&self, from: &str, to: &str) -> Vec<String> {
        let mut a = vec![
            "--migrate-from".to_string(),
            from.to_string(),
            "--migrate-to".into(),
            to.to_string(),
            "--strategy".into(),
            self.strategy.clone(),
            "--transfer".into(),
            self.transfer.clone(),
            "--pagination-limit".into(),
            self.batch.to_string(),
            "--compression".into(),
            self.compression.clone(),
        ];
        if let Some(k) = self.key_file.as_ref().filter(|k| !k.is_empty()) {
            a.extend(["--key-file".into(), k.clone()]);
        }
        for (flag, list) in [("--include", &self.include), ("--exclude", &self.exclude)] {
            let list: Vec<&str> = list.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
            if !list.is_empty() {
                a.extend([flag.to_string(), list.join(",")]);
            }
        }
        if self.live {
            a.push("--live".into());
        }
        a
    }

    /// Settings exactly as the CLI would build them.
    pub fn settings(&self, from: &str, to: &str) -> Result<MigrationSetting, String> {
        MigrationSetting::try_parse_from(std::iter::once("rustygrator".to_string()).chain(self.args(from, to))).map_err(|e| e.to_string())
    }

    /// MAPP-4: the job as a command line (POSIX shell quoting).
    pub fn command_line(&self, from: &str, to: &str) -> String {
        std::iter::once("rustygrator".to_string()).chain(self.args(from, to).iter().map(|a| shell_quote(a))).collect::<Vec<_>>().join(" ")
    }
}

fn shell_quote(s: &str) -> String {
    if !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || "-_./:=,@%+".contains(c)) {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustygrator_core::models::migration_execution_settings::{SqlToNoSqlStrategy, TransferMode};

    #[test]
    fn mapp1_job_gives_the_same_settings_as_the_cli() {
        let job = Job { strategy: "normalized".into(), transfer: "file".into(), batch: 1000, include: " users, orders ,".into(), live: true, ..Job::default() };
        let s = job.settings("sqlite://a.db", "postgresql://u@h/db").unwrap();
        let e = s.execution_settings();
        assert_eq!(e.strategy, SqlToNoSqlStrategy::Normalized);
        assert_eq!(e.transfer, TransferMode::File);
        assert!(e.live.enabled);
        assert_eq!(s.include, ["users", "orders"]);
        assert_eq!(s.pagination_limit, 1000);
        assert!(Job { strategy: "sideways".into(), ..Job::default() }.settings("sqlite://a.db", "sqlite://b.db").is_err());
    }

    #[test]
    fn mapp4_command_line_is_quoted() {
        let job = Job { key_file: Some("/keys/my key".into()), ..Job::default() };
        let cmd = job.command_line("sqlite:///tmp/a b.db", "mongodb://h/db");
        assert!(cmd.starts_with("rustygrator --migrate-from 'sqlite:///tmp/a b.db' --migrate-to mongodb://h/db "));
        assert!(cmd.ends_with("--key-file '/keys/my key'"));
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }
}
