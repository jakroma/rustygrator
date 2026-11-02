//! Saved connections (CONN-1..CONN-3). The settings file keeps connection strings without
//! passwords; passwords live in the system keychain (CONN-2).

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use url::Url;

// the old app name: passwords saved before the rename stay in the keyring under it
const KEYRING_SERVICE: &str = "data-mover";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TlsMode {
    #[default]
    Off,
    /// Encrypted, certificate not checked
    Require,
    /// Certificate chain checked against the CA, host name not checked
    VerifyCa,
    /// Certificate chain and host name checked
    VerifyFull,
}

impl TlsMode {
    pub const ALL: [TlsMode; 4] = [TlsMode::Off, TlsMode::Require, TlsMode::VerifyCa, TlsMode::VerifyFull];
}

impl std::fmt::Display for TlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TlsMode::Off => "Off",
            TlsMode::Require => "Encrypt only",
            TlsMode::VerifyCa => "Verify certificate",
            TlsMode::VerifyFull => "Verify certificate and host",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Tls {
    pub mode: TlsMode,
    pub ca_file: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshTunnel {
    pub host: String,
    pub port: u16,
    pub user: String,
    /// Private key; without it the ssh agent is used.
    pub key_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    pub id: String,
    pub name: String,
    /// Connection string as used by the CLI, without the password.
    pub url: String,
    #[serde(default)]
    pub tls: Tls,
    #[serde(default)]
    pub ssh: Option<SshTunnel>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Store {
    pub profiles: Vec<Profile>,
    /// MAPP-4: saved migration / live transfer profiles.
    #[serde(default)]
    pub jobs: Vec<crate::jobs::Job>,
}

/// Folder for app settings: `%APPDATA%\rustygrator`, `~/Library/Application Support/rustygrator`
/// or `$XDG_CONFIG_HOME/rustygrator` (`~/.config/rustygrator`).
pub fn config_dir() -> PathBuf {
    let var = |n: &str| std::env::var_os(n).filter(|v| !v.is_empty()).map(PathBuf::from);
    let base = if cfg!(windows) {
        var("APPDATA")
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|h| h.join("Library/Application Support"))
    } else {
        var("XDG_CONFIG_HOME").or_else(|| var("HOME").map(|h| h.join(".config")))
    };
    let base = base.unwrap_or_else(std::env::temp_dir);
    let dir = base.join("rustygrator");
    // settings saved before the rename move over once
    let old = base.join("data-mover");
    if !dir.exists() && old.is_dir() {
        let _ = std::fs::rename(&old, &dir);
    }
    dir
}

impl Store {
    pub fn path() -> PathBuf {
        config_dir().join("connections.json")
    }

    pub fn load(path: &Path) -> Store {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                // keep the unreadable file: the next save must not overwrite the user's connections
                let bad = path.with_extension("json.bad");
                log::error!("{} is not readable ({e}); moved to {}, starting with no connections", path.display(), bad.display());
                let _ = std::fs::rename(path, &bad);
                Store::default()
            }),
            Err(_) => Store::default(),
        }
    }

    /// Writes through a temporary file so a crash never leaves a half written file.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self).map_err(std::io::Error::other)?)?;
        std::fs::rename(tmp, path)
    }
}

/// Splits the password out of a connection string: (url without password, password).
pub fn split_password(url: &str) -> (String, Option<String>) {
    match Url::parse(url) {
        Ok(mut u) if u.password().is_some() => {
            let pw = u.password().map(rustygrator_core::models::connection::decode);
            let _ = u.set_password(None);
            (u.to_string(), pw)
        }
        _ => (url.to_string(), None),
    }
}

/// Puts the password into the connection string.
pub fn with_password(url: &str, password: Option<&str>) -> String {
    match (Url::parse(url), password) {
        (Ok(mut u), Some(pw)) if !pw.is_empty() && u.has_host() => {
            let _ = u.set_password(Some(pw));
            u.to_string()
        }
        _ => url.to_string(),
    }
}

/// Adds the TLS options as connection string parameters (the same ones the CLI reads).
pub fn with_tls(url: &str, tls: &Tls) -> String {
    let Ok(mut u) = Url::parse(url) else { return url.to_string() };
    if tls.mode == TlsMode::Off || u.scheme() == "sqlite" {
        return url.to_string();
    }
    let mongo = u.scheme().starts_with("mongodb");
    let path = |p: &Option<PathBuf>| p.as_ref().map(|p| p.display().to_string());
    let mut q = u.query_pairs_mut();
    if mongo {
        q.append_pair("tls", "true");
        if tls.mode == TlsMode::Require {
            q.append_pair("tlsAllowInvalidCertificates", "true");
        }
        if tls.mode == TlsMode::VerifyCa {
            q.append_pair("tlsAllowInvalidHostnames", "true");
        }
        if let Some(ca) = path(&tls.ca_file) {
            q.append_pair("tlsCAFile", &ca);
        }
        // MongoDB takes certificate and key in one PEM file
        if let Some(cert) = path(&tls.client_cert) {
            q.append_pair("tlsCertificateKeyFile", &cert);
        }
    } else {
        q.append_pair(
            "sslmode",
            match tls.mode {
                TlsMode::Require => "require",
                TlsMode::VerifyCa => "verify-ca",
                _ => "verify-full",
            },
        );
        for (k, v) in [("sslrootcert", path(&tls.ca_file)), ("sslcert", path(&tls.client_cert)), ("sslkey", path(&tls.client_key))] {
            if let Some(v) = v {
                q.append_pair(k, &v);
            }
        }
    }
    drop(q);
    u.to_string()
}

pub fn keyring_entry(profile_id: &str) -> keyring::Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, profile_id)
}

pub fn store_password(profile_id: &str, password: Option<&str>) -> Result<(), String> {
    let entry = keyring_entry(profile_id).map_err(|e| e.to_string())?;
    match password.filter(|p| !p.is_empty()) {
        Some(p) => entry.set_password(p).map_err(|e| format!("keychain: {e}")),
        None => match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(format!("keychain: {e}")),
        },
    }
}

pub fn load_password(profile_id: &str) -> Result<Option<String>, String> {
    match keyring_entry(profile_id).and_then(|e| e.get_password()) {
        Ok(p) => Ok(Some(p)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(format!("keychain: {e}")),
    }
}

/// An `ssh -L` port forward to the database (CONN-3); the process ends when this is dropped.
pub struct Tunnel {
    child: Child,
    pub local_port: u16,
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_local_port() -> std::io::Result<u16> {
    Ok(TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?.local_addr()?.port())
}

/// Arguments of the `ssh` command that forwards `127.0.0.1:local_port` to `target`.
pub fn ssh_args(ssh: &SshTunnel, local_port: u16, target_host: &str, target_port: u16) -> Vec<String> {
    let mut args = vec![
        "-N".into(),
        "-L".into(),
        format!("127.0.0.1:{local_port}:{target_host}:{target_port}"),
        "-p".into(),
        ssh.port.to_string(),
        // never prompt: keys or the agent only, the app has no terminal
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
    ];
    if let Some(k) = &ssh.key_file {
        args.push("-i".into());
        args.push(k.display().to_string());
    }
    args.push(if ssh.user.is_empty() { ssh.host.clone() } else { format!("{}@{}", ssh.user, ssh.host) });
    args
}

/// Starts the tunnel with the system OpenSSH client and waits until the local port accepts
/// connections.
pub fn open_tunnel(ssh: &SshTunnel, target_host: &str, target_port: u16) -> Result<Tunnel, String> {
    let local_port = free_local_port().map_err(|e| e.to_string())?;
    let child = Command::new("ssh")
        .args(ssh_args(ssh, local_port, target_host, target_port))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("cannot start ssh (is OpenSSH installed?): {e}"))?;
    let mut tunnel = Tunnel { child, local_port };
    let deadline = Instant::now() + Duration::from_secs(15);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, local_port));
    loop {
        if let Ok(Some(status)) = tunnel.child.try_wait() {
            let mut err = String::new();
            if let Some(mut e) = tunnel.child.stderr.take() {
                use std::io::Read;
                let _ = e.read_to_string(&mut err);
            }
            return Err(format!("ssh exited ({status}): {}", err.trim()));
        }
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(tunnel);
        }
        if Instant::now() > deadline {
            return Err("ssh tunnel did not open within 15 s".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Host and port of a connection string (SQLite has none).
pub fn host_port(url: &str) -> Option<(String, u16)> {
    let u = Url::parse(url).ok()?;
    let default = match u.scheme() {
        "postgresql" | "postgres" => 5432,
        "mongodb" => 27017,
        "scylla" | "cassandra" => 9042,
        "mysql" | "mariadb" => 3306,
        "oracle" => 1521,
        _ => return None,
    };
    Some((u.host_str()?.to_string(), u.port().unwrap_or(default)))
}

/// Connection string pointing at the local end of a tunnel.
pub fn through_tunnel(url: &str, local_port: u16) -> String {
    match Url::parse(url) {
        Ok(mut u) => {
            let _ = u.set_host(Some("127.0.0.1"));
            let _ = u.set_port(Some(local_port));
            u.to_string()
        }
        Err(_) => url.to_string(),
    }
}

/// Short unique id for a new profile.
pub fn new_id() -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
    format!("c{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn2_password_is_split_out_and_put_back() {
        let (url, pw) = split_password("postgresql://user:p%40ss%2Bw@db.local:5433/app?schema=s");
        assert_eq!(url, "postgresql://user@db.local:5433/app?schema=s");
        assert_eq!(pw.as_deref(), Some("p@ss+w"));
        let back = with_password(&url, pw.as_deref());
        assert_eq!(split_password(&back).1.as_deref(), Some("p@ss+w"));
        assert_eq!(split_password("sqlite://C:/data/app.db"), ("sqlite://C:/data/app.db".to_string(), None));
        assert_eq!(with_password("sqlite:///tmp/a.db", Some("x")), "sqlite:///tmp/a.db");
    }

    #[test]
    fn fix1_password_with_special_characters_reaches_the_database_unchanged() {
        let url = with_password("postgresql://user@db.local/app", Some("p@ss:w/rd#%"));
        let c = rustygrator_core::models::connection::MigrationConnection::new(&url).unwrap();
        assert_eq!(c.password, "p@ss:w/rd#%");
    }

    #[test]
    fn fix3_unreadable_settings_file_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("connections.json");
        std::fs::write(&path, b"{ not json").unwrap();
        assert_eq!(Store::load(&path), Store::default());
        Store::default().save(&path).unwrap();
        assert_eq!(std::fs::read(dir.path().join("connections.json.bad")).unwrap(), b"{ not json");
        // a job saved by an older version (fewer fields) still loads
        std::fs::write(&path, br#"{"profiles":[],"jobs":[{"name":"j","source":"a","target":"b"}]}"#).unwrap();
        assert_eq!(Store::load(&path).jobs[0].batch, 4096);
    }

    #[test]
    fn conn2_settings_file_never_contains_passwords() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("connections.json");
        let (url, _) = split_password("mongodb://admin:secret@localhost:27017/app");
        let store = Store { profiles: vec![Profile { id: "a".into(), name: "Mongo".into(), url, tls: Tls::default(), ssh: None }], jobs: vec![] };
        store.save(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("secret"));
        assert_eq!(Store::load(&path), store);
        assert_eq!(Store::load(&dir.path().join("missing.json")), Store::default());
    }

    #[test]
    fn conn3_tls_options_become_connection_parameters() {
        let tls = Tls { mode: TlsMode::VerifyCa, ca_file: Some("ca.pem".into()), client_cert: Some("c.pem".into()), client_key: Some("k.pem".into()) };
        assert_eq!(with_tls("postgresql://u@h/db", &tls), "postgresql://u@h/db?sslmode=verify-ca&sslrootcert=ca.pem&sslcert=c.pem&sslkey=k.pem");
        assert_eq!(
            with_tls("mongodb://h/db", &tls),
            "mongodb://h/db?tls=true&tlsAllowInvalidHostnames=true&tlsCAFile=ca.pem&tlsCertificateKeyFile=c.pem"
        );
        assert_eq!(with_tls("scylla://h/ks", &Tls { mode: TlsMode::Require, ..Tls::default() }), "scylla://h/ks?sslmode=require");
        assert_eq!(with_tls("postgresql://h/db", &Tls::default()), "postgresql://h/db");
    }

    #[test]
    fn conn3_ssh_tunnel_command_and_target() {
        let ssh = SshTunnel { host: "bastion".into(), port: 2222, user: "ops".into(), key_file: Some("id_ed25519".into()) };
        let args = ssh_args(&ssh, 40000, "db.internal", 5432);
        assert_eq!(args[..3], ["-N", "-L", "127.0.0.1:40000:db.internal:5432"]);
        assert!(args.windows(2).any(|w| w == ["-p", "2222"]));
        assert!(args.windows(2).any(|w| w == ["-i", "id_ed25519"]));
        assert_eq!(args.last().unwrap(), "ops@bastion");
        assert_eq!(host_port("postgresql://u@db.internal/app"), Some(("db.internal".into(), 5432)));
        assert_eq!(host_port("sqlite://a.db"), None);
        // ORA-10: Oracle (and MySQL) connections can go through an SSH tunnel too
        assert_eq!(host_port("oracle://u@db.internal/FREEPDB1"), Some(("db.internal".into(), 1521)));
        assert_eq!(host_port("mysql://u@db.internal/shop"), Some(("db.internal".into(), 3306)));
        assert_eq!(crate::scheme_label("oracle://u@h/svc"), "Oracle");
        assert_eq!(through_tunnel("postgresql://u@db.internal:5432/app", 40000), "postgresql://u@127.0.0.1:40000/app");
    }
}

#[cfg(test)]
mod keychain_tests {
    use super::*;

    /// CONN-2 against keyring's in-memory store (the real keychain is not touched by tests).
    #[test]
    fn conn2_password_round_trip_through_the_keychain_api() {
        keyring::set_default_credential_builder(keyring::mock::default_credential_builder());
        // the mock keeps a credential per entry object, so one entry is reused here
        let entry = keyring_entry("test-profile").unwrap();
        entry.set_password("s3cret").unwrap();
        assert_eq!(entry.get_password().unwrap(), "s3cret");
        entry.delete_credential().unwrap();
        assert!(matches!(entry.get_password(), Err(keyring::Error::NoEntry)));
        assert_eq!(load_password("never-saved").unwrap(), None);
        store_password("never-saved", None).unwrap();
    }
}
