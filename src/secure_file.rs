//! Permission-restricted creation of wrapper-owned directories and files.
//!
//! Generated Redis and Sentinel configs carry `requirepass`, `masterauth`, and
//! TLS key passphrases in plaintext, because that is the only form Redis reads
//! them in. They are created under the default umask otherwise, which on most
//! systems leaves them readable by every user on the machine.
//!
//! These helpers narrow that: wrapper-owned directories become `0700` and
//! wrapper-written config files become `0600`. On non-Unix targets they fall
//! back to the plain [`std::fs`] calls, since the crate already documents that
//! process lifecycle management is Unix-only.

use std::fs;
use std::path::Path;

use crate::error::Result;

/// Create a directory and every missing parent, owner-accessible only.
///
/// The mode is applied to `path` itself. Parents created along the way get the
/// default mode, matching `mkdir -p`: the wrapper owns the leaf, not the tree
/// above it.
pub(crate) fn create_dir_all(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    fs::create_dir_all(path)?;
    set_mode(path, 0o700)
}

/// Write a file containing credentials, readable and writable only by owner.
///
/// The mode is set after the write rather than at creation, so an existing
/// file left over from an earlier run is narrowed too.
pub(crate) fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> Result<()> {
    let path = path.as_ref();
    fs::write(path, contents)?;
    set_mode(path, 0o600)
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode_of(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rsw-secure-file-{name}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn directory_is_owner_only() {
        let dir = scratch("dir");
        create_dir_all(&dir).unwrap();
        assert_eq!(mode_of(&dir), 0o700);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn nested_directory_leaf_is_owner_only() {
        let base = scratch("nested");
        let leaf = base.join("a/b/c");
        create_dir_all(&leaf).unwrap();
        assert_eq!(mode_of(&leaf), 0o700);
        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn file_is_owner_only() {
        let dir = scratch("file");
        create_dir_all(&dir).unwrap();
        let file = dir.join("redis.conf");
        write(&file, "requirepass hunter2\n").unwrap();
        assert_eq!(mode_of(&file), 0o600);
        assert_eq!(fs::read_to_string(&file).unwrap(), "requirepass hunter2\n");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn existing_permissive_file_is_narrowed() {
        let dir = scratch("narrow");
        create_dir_all(&dir).unwrap();
        let file = dir.join("redis.conf");
        fs::write(&file, "old").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        write(&file, "new").unwrap();
        assert_eq!(mode_of(&file), 0o600);
        fs::remove_dir_all(&dir).unwrap();
    }
}
