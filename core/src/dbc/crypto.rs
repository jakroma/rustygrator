//! Keys and authenticated encryption for `dbc` v3 (FMT-9..FMT-12).
//!
//! * one process (provider + receiver): a random key that only lives in memory (FMT-10),
//! * separate processes: a key file (`--key-file`) or a passphrase in `DM_DBC_KEY`, stretched
//!   with Argon2id and a random per-run salt stored in the file header (FMT-11),
//! * nothing is ever embedded in the binary (FMT-12) and the key is never written or logged.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes256Gcm, Nonce,
};
use chacha20poly1305::ChaCha20Poly1305;

use crate::{DMError, DMResult};

pub const KEY_ENV: &str = "DM_DBC_KEY";

/// Where the encryption key comes from.
#[derive(Clone)]
pub enum KeySource {
    /// Random key for this run, kept only in memory.
    Ephemeral,
    /// Contents of a key file.
    KeyFile(PathBuf),
    /// Passphrase (e.g. from `DM_DBC_KEY`).
    Passphrase(String),
}

impl std::fmt::Debug for KeySource {
    // never print secrets
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeySource::Ephemeral => write!(f, "Ephemeral"),
            KeySource::KeyFile(p) => write!(f, "KeyFile({})", p.display()),
            KeySource::Passphrase(_) => write!(f, "Passphrase(***)"),
        }
    }
}

impl KeySource {
    /// `--key-file` if given, else `DM_DBC_KEY` if set, else a one-time in-memory key.
    pub fn from_options(key_file: Option<PathBuf>) -> KeySource {
        if let Some(p) = key_file {
            return KeySource::KeyFile(p);
        }
        match std::env::var(KEY_ENV) {
            Ok(v) if !v.is_empty() => KeySource::Passphrase(v),
            _ => KeySource::Ephemeral,
        }
    }

    pub fn is_ephemeral(&self) -> bool {
        matches!(self, KeySource::Ephemeral)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CipherKind {
    Aes256Gcm = 1,
    ChaCha20Poly1305 = 2,
}

impl CipherKind {
    /// AES-256-GCM when the CPU accelerates AES, ChaCha20-Poly1305 otherwise (FMT-9).
    pub fn preferred() -> CipherKind {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if std::arch::is_x86_feature_detected!("aes") && std::arch::is_x86_feature_detected!("pclmulqdq") {
                return CipherKind::Aes256Gcm;
            }
            CipherKind::ChaCha20Poly1305
        }
        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("aes") {
                return CipherKind::Aes256Gcm;
            }
            CipherKind::ChaCha20Poly1305
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
        {
            CipherKind::ChaCha20Poly1305
        }
    }

    pub fn from_u8(v: u8) -> DMResult<CipherKind> {
        match v {
            1 => Ok(CipherKind::Aes256Gcm),
            2 => Ok(CipherKind::ChaCha20Poly1305),
            _ => Err(DMError::Error(format!("dbc: unknown cipher {}", v))),
        }
    }
}

pub const TAG_LEN: usize = 16;

/// An initialized AEAD cipher.
#[derive(Clone)]
pub enum Cipher {
    Aes(Box<Aes256Gcm>),
    ChaCha(Box<ChaCha20Poly1305>),
}

impl Cipher {
    pub fn new(kind: CipherKind, key: &[u8; 32]) -> Cipher {
        match kind {
            CipherKind::Aes256Gcm => Cipher::Aes(Box::new(Aes256Gcm::new(key.into()))),
            CipherKind::ChaCha20Poly1305 => Cipher::ChaCha(Box::new(ChaCha20Poly1305::new(key.into()))),
        }
    }

    /// Encrypts in place and appends the tag.
    pub fn seal(&self, nonce: &[u8; 12], aad: &[u8], buf: &mut Vec<u8>) -> DMResult<()> {
        let n = Nonce::from_slice(nonce);
        match self {
            Cipher::Aes(c) => c.encrypt_in_place(n, aad, buf),
            Cipher::ChaCha(c) => c.encrypt_in_place(n.as_slice().into(), aad, buf),
        }
        .map_err(|_| DMError::Error("dbc: encryption failed".to_string()))
    }

    /// Verifies the tag and decrypts in place.
    pub fn open(&self, nonce: &[u8; 12], aad: &[u8], buf: &mut Vec<u8>) -> DMResult<()> {
        let n = Nonce::from_slice(nonce);
        match self {
            Cipher::Aes(c) => c.decrypt_in_place(n, aad, buf),
            Cipher::ChaCha(c) => c.decrypt_in_place(n.as_slice().into(), aad, buf),
        }
        .map_err(|_| DMError::Error("dbc: the file was changed or damaged (authentication failed)".to_string()))
    }

    /// Public 8-byte identifier of the key (tag of a constant message): lets the reader report a
    /// wrong key without revealing anything about it.
    pub fn key_id(&self) -> DMResult<[u8; 8]> {
        let mut buf = Vec::new();
        // the old app name: changing it would make existing encrypted files unreadable
        self.seal(&[0u8; 12], b"data-mover key id", &mut buf)?;
        let mut id = [0u8; 8];
        id.copy_from_slice(&buf[..8]);
        Ok(id)
    }
}

pub fn random_bytes<const N: usize>() -> DMResult<[u8; N]> {
    let mut b = [0u8; N];
    getrandom::getrandom(&mut b).map_err(|e| DMError::Error(format!("random generator: {}", e)))?;
    Ok(b)
}

/// Key derivation: 0 = the run key itself (one-time key), 1 = Argon2id(secret, salt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kdf {
    None = 0,
    Argon2id = 1,
}

struct KeyState {
    /// key used for new files
    write_key: Option<([u8; 32], [u8; 16])>,
    /// derived keys by salt (reading files written by another process)
    derived: HashMap<[u8; 16], [u8; 32]>,
}

/// Key material of a run, shared by all writers and readers of the process.
#[derive(Clone)]
pub struct DbcCrypto {
    source: KeySource,
    state: Arc<Mutex<KeyState>>,
}

impl std::fmt::Debug for DbcCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DbcCrypto({:?})", self.source)
    }
}

impl DbcCrypto {
    pub fn new(source: KeySource) -> Self {
        DbcCrypto { source, state: Arc::new(Mutex::new(KeyState { write_key: None, derived: HashMap::new() })) }
    }

    pub fn source(&self) -> &KeySource {
        &self.source
    }

    pub fn kdf(&self) -> Kdf {
        if self.source.is_ephemeral() {
            Kdf::None
        } else {
            Kdf::Argon2id
        }
    }

    fn secret(&self) -> DMResult<Vec<u8>> {
        match &self.source {
            KeySource::Ephemeral => Err(DMError::Error("dbc: no key".to_string())),
            KeySource::KeyFile(p) => {
                let s = std::fs::read(p).map_err(|e| DMError::Error(format!("cannot read key file {}: {}", p.display(), e)))?;
                if s.len() < 16 {
                    return Err(DMError::Error("key file must contain at least 16 bytes".to_string()));
                }
                Ok(s)
            }
            KeySource::Passphrase(p) => {
                if p.len() < 8 {
                    return Err(DMError::Error(format!("{} must be at least 8 characters long", KEY_ENV)));
                }
                Ok(p.as_bytes().to_vec())
            }
        }
    }

    fn derive(&self, salt: &[u8; 16]) -> DMResult<[u8; 32]> {
        let secret = self.secret()?;
        let mut out = [0u8; 32];
        argon2::Argon2::default()
            .hash_password_into(&secret, salt, &mut out)
            .map_err(|e| DMError::Error(format!("key derivation failed: {}", e)))?;
        Ok(out)
    }

    /// (key, salt) for writing; derived once per run.
    pub fn write_key(&self) -> DMResult<([u8; 32], [u8; 16])> {
        let mut st = self.state.lock().map_err(|_| DMError::Error("key state poisoned".into()))?;
        if let Some(k) = st.write_key {
            return Ok(k);
        }
        let salt = random_bytes::<16>()?;
        let key = if self.source.is_ephemeral() { random_bytes::<32>()? } else { self.derive(&salt)? };
        st.write_key = Some((key, salt));
        st.derived.insert(salt, key);
        Ok((key, salt))
    }

    /// Key for a file written with `kdf` and `salt`.
    pub fn read_key(&self, kdf: Kdf, salt: &[u8; 16]) -> DMResult<[u8; 32]> {
        let st = self.state.lock().map_err(|_| DMError::Error("key state poisoned".into()))?;
        if let Some(k) = st.derived.get(salt) {
            return Ok(*k);
        }
        match kdf {
            Kdf::None => Err(DMError::Error(
                "dbc: the file was written with a one-time key of another run; export again, or use --key-file / DM_DBC_KEY for separate provider and receiver runs".to_string(),
            )),
            Kdf::Argon2id => {
                if self.source.is_ephemeral() {
                    return Err(DMError::Error(format!(
                        "dbc: the file is encrypted with a key file or passphrase; pass --key-file or set {}",
                        KEY_ENV
                    )));
                }
                drop(st);
                let k = self.derive(salt)?;
                self.state.lock().map_err(|_| DMError::Error("key state poisoned".into()))?.derived.insert(*salt, k);
                Ok(k)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_and_tamper() {
        for kind in [CipherKind::Aes256Gcm, CipherKind::ChaCha20Poly1305] {
            let c = Cipher::new(kind, &[7u8; 32]);
            let mut buf = b"hello".to_vec();
            c.seal(&[1u8; 12], b"aad", &mut buf).unwrap();
            assert_eq!(buf.len(), 5 + TAG_LEN);
            let mut ok = buf.clone();
            c.open(&[1u8; 12], b"aad", &mut ok).unwrap();
            assert_eq!(ok, b"hello");
            let mut bad = buf.clone();
            bad[0] ^= 1;
            assert!(c.open(&[1u8; 12], b"aad", &mut bad).is_err());
            assert!(c.open(&[1u8; 12], b"other", &mut buf.clone()).is_err());
            assert_ne!(c.key_id().unwrap(), Cipher::new(kind, &[8u8; 32]).key_id().unwrap());
        }
    }

    #[test]
    fn passphrase_derivation_is_shared_between_processes() {
        let writer = DbcCrypto::new(KeySource::Passphrase("correct horse".into()));
        let (k, salt) = writer.write_key().unwrap();
        let reader = DbcCrypto::new(KeySource::Passphrase("correct horse".into()));
        assert_eq!(reader.read_key(Kdf::Argon2id, &salt).unwrap(), k);
        let other = DbcCrypto::new(KeySource::Passphrase("wrong horse".into()));
        assert_ne!(other.read_key(Kdf::Argon2id, &salt).unwrap(), k);
        assert!(DbcCrypto::new(KeySource::Ephemeral).read_key(Kdf::None, &salt).is_err());
        assert!(format!("{:?}", writer).contains("***"));
    }
}
