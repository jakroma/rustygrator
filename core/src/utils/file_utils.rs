use std::{
    fs,
    path::{Path, PathBuf},
};

use log::info;

use crate::{
    constants::MIGRATION_ORDER_FILE_NAME,
    dbc::{
        crypto::DbcCrypto,
        format::{read_meta, write_meta, DbcOptions},
    },
    DMResult,
};

/// Creates the temporary directory (FN-7), accessible by the current user only (FMT-14).
pub fn prepare_temp_folder(temp_dir: &Path) -> DMResult<PathBuf> {
    if !temp_dir.exists() {
        info!("[Migration] Create folder {}!", temp_dir.display());
        fs::create_dir_all(temp_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(temp_dir, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(temp_dir.to_path_buf())
}

fn file_safe(container: &str) -> String {
    container
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' { c } else { '_' })
        .collect()
}

pub fn definition_file_path(temp_dir: &Path, container: &str) -> PathBuf {
    temp_dir.join(format!("definition_{}.dmb", file_safe(container)))
}

pub fn data_file_path(temp_dir: &Path, container: &str) -> PathBuf {
    temp_dir.join(format!("{}_data.dbc", file_safe(container)))
}

pub fn order_file_path(temp_dir: &Path) -> PathBuf {
    temp_dir.join(MIGRATION_ORDER_FILE_NAME)
}

pub fn save_sorted_containers_to_file(temp_dir: &Path, sorted_containers: &[String], opts: &DbcOptions) -> DMResult<()> {
    write_meta(&order_file_path(temp_dir), &sorted_containers, opts)
}

pub fn load_sorted_containers_from_file(temp_dir: &Path, crypto: &DbcCrypto) -> DMResult<Vec<String>> {
    read_meta(&order_file_path(temp_dir), crypto)
}

/// Removes files written by this run (FMT-14); missing files are ignored.
pub fn remove_files(paths: &[PathBuf]) {
    for p in paths {
        if let Err(e) = fs::remove_file(p) {
            if e.kind() != std::io::ErrorKind::NotFound {
                log::warn!("[Migration] could not remove {}: {}", p.display(), e);
            }
        }
    }
}
