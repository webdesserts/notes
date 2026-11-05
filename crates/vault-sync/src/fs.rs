//! FileSystem trait abstraction for platform-independent file operations.
//!
//! Implementations:
//! - `InMemoryFs` - For testing
//! - `ObsidianFs` (in sync-wasm) - Uses Obsidian's Vault API via JS bridge
//! - `NativeFs` (in sync-daemon) - Uses tokio::fs
//!
//! Uses `cfg(target_arch = "wasm32")` (not `cfg(feature = "native")`) to vary the
//! `Send + Sync` trait bounds. See the crate-level doc in `lib.rs` for why.
//!
//! ## Error contract
//!
//! All `FileSystem` implementations **must** return `FsError::NotFound` when an
//! operation targets a path that does not exist (read, stat, delete, rename with a
//! missing source). Callers branch on this variant — most critically, the boot
//! reconcile's per-file loop (`Vault::reconcile`) matches `FsError::NotFound` to
//! safely skip a `.md` that vanished between the directory scan and the reconcile of
//! that file. Returning `FsError::Io` for ENOENT breaks that guard and aborts the
//! entire boot reconcile pass over a single race-deleted file. Every implementation
//! must pass the shared conformance suite in the [`conformance`] module.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("File not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Is a directory: {0}")]
    IsDirectory(String),

    #[error("Not a directory: {0}")]
    NotDirectory(String),

    #[error("IO error: {0}")]
    Io(String),
}

pub type Result<T> = std::result::Result<T, FsError>;

/// File metadata
#[derive(Debug, Clone)]
pub struct FileStat {
    /// Modification time in milliseconds since epoch
    pub mtime_millis: u64,
    /// File size in bytes
    pub size: u64,
    /// Whether this is a directory
    pub is_dir: bool,
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// File or directory name (not full path)
    pub name: String,
    /// Whether this is a directory
    pub is_dir: bool,
}

/// Platform-independent filesystem abstraction.
///
/// On native platforms, implementations must be `Send + Sync` for use across threads.
/// On WASM (wasm32), these bounds are relaxed since WASM is single-threaded.
///
/// See the module-level doc for the required error contract all implementations must
/// uphold. Every implementation must pass the [`conformance`] suite.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg(not(target_arch = "wasm32"))]
pub trait FileSystem: Send + Sync {
    /// Read file contents. Returns `FsError::NotFound` if the path does not exist.
    async fn read(&self, path: &str) -> Result<Vec<u8>>;

    /// Write file contents (creates parent directories if needed).
    async fn write(&self, path: &str, content: &[u8]) -> Result<()>;

    /// List directory contents.
    async fn list(&self, path: &str) -> Result<Vec<FileEntry>>;

    /// Delete file or empty directory. Returns `FsError::NotFound` if the path does
    /// not exist.
    async fn delete(&self, path: &str) -> Result<()>;

    /// Check if path exists.
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get file metadata. Returns `FsError::NotFound` if the path does not exist.
    async fn stat(&self, path: &str) -> Result<FileStat>;

    /// Create directory (and parents if needed).
    async fn mkdir(&self, path: &str) -> Result<()>;

    /// Rename/move a file or directory. Returns `FsError::NotFound` if `from` does
    /// not exist.
    async fn rename(&self, from: &str, to: &str) -> Result<()>;

    /// Write file contents atomically via write-to-temp + rename.
    ///
    /// If the process crashes mid-write, the original file is preserved.
    /// The temp file is written to `{path}.tmp` in the same directory to
    /// ensure the rename stays on the same filesystem (required for POSIX).
    async fn atomic_write(&self, path: &str, content: &[u8]) -> Result<()> {
        let tmp_path = format!("{}.tmp", path);
        self.write(&tmp_path, content).await?;
        if let Err(e) = self.rename(&tmp_path, path).await {
            // Best-effort cleanup of temp file
            let _ = self.delete(&tmp_path).await;
            return Err(e);
        }
        Ok(())
    }
}

/// Platform-independent filesystem abstraction (WASM version without Send + Sync).
///
/// See the module-level doc for the required error contract all implementations must
/// uphold. Every implementation must pass the [`conformance`] suite.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg(target_arch = "wasm32")]
pub trait FileSystem {
    /// Read file contents. Returns `FsError::NotFound` if the path does not exist.
    async fn read(&self, path: &str) -> Result<Vec<u8>>;

    /// Write file contents (creates parent directories if needed).
    async fn write(&self, path: &str, content: &[u8]) -> Result<()>;

    /// List directory contents.
    async fn list(&self, path: &str) -> Result<Vec<FileEntry>>;

    /// Delete file or empty directory. Returns `FsError::NotFound` if the path does
    /// not exist.
    async fn delete(&self, path: &str) -> Result<()>;

    /// Check if path exists.
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get file metadata. Returns `FsError::NotFound` if the path does not exist.
    async fn stat(&self, path: &str) -> Result<FileStat>;

    /// Create directory (and parents if needed).
    async fn mkdir(&self, path: &str) -> Result<()>;

    /// Rename/move a file or directory. Returns `FsError::NotFound` if `from` does
    /// not exist.
    async fn rename(&self, from: &str, to: &str) -> Result<()>;

    /// Write file contents atomically via write-to-temp + rename.
    ///
    /// If the process crashes mid-write, the original file is preserved.
    /// The temp file is written to `{path}.tmp` in the same directory to
    /// ensure the rename stays on the same filesystem (required for POSIX).
    async fn atomic_write(&self, path: &str, content: &[u8]) -> Result<()> {
        let tmp_path = format!("{}.tmp", path);
        self.write(&tmp_path, content).await?;
        if let Err(e) = self.rename(&tmp_path, path).await {
            let _ = self.delete(&tmp_path).await;
            return Err(e);
        }
        Ok(())
    }
}

/// In-memory filesystem for testing
pub struct InMemoryFs {
    files: RwLock<HashMap<String, Vec<u8>>>,
    dirs: RwLock<HashMap<String, ()>>,
    /// Tracks file modification times (path -> mtime in ms)
    mtimes: RwLock<HashMap<String, u64>>,
}

impl InMemoryFs {
    pub fn new() -> Self {
        let mut dirs = HashMap::new();
        dirs.insert(String::new(), ()); // Root directory
        Self {
            files: RwLock::new(HashMap::new()),
            dirs: RwLock::new(dirs),
            mtimes: RwLock::new(HashMap::new()),
        }
    }

    /// Set a specific mtime for testing "latest wins" scenarios
    pub fn set_mtime(&self, path: &str, mtime: u64) {
        let path = Self::normalize_path(path);
        let mut mtimes = self.mtimes.write().unwrap();
        mtimes.insert(path, mtime);
    }

    /// Get current time in milliseconds (monotonically increasing for tests)
    fn current_time_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn normalize_path(path: &str) -> String {
        path.trim_matches('/').to_string()
    }

    fn parent_path(path: &str) -> Option<String> {
        let normalized = Self::normalize_path(path);
        if normalized.is_empty() {
            None
        } else {
            match normalized.rfind('/') {
                Some(pos) => Some(normalized[..pos].to_string()),
                None => Some(String::new()),
            }
        }
    }
}

impl Default for InMemoryFs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl FileSystem for InMemoryFs {
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        let path = Self::normalize_path(path);
        let files = self.files.read().unwrap();
        files.get(&path).cloned().ok_or(FsError::NotFound(path))
    }

    async fn write(&self, path: &str, content: &[u8]) -> Result<()> {
        let path = Self::normalize_path(path);

        // Create parent directories
        if let Some(parent) = Self::parent_path(&path) {
            self.mkdir(&parent).await?;
        }

        let mut files = self.files.write().unwrap();
        files.insert(path.clone(), content.to_vec());
        drop(files);

        // Update mtime
        let mut mtimes = self.mtimes.write().unwrap();
        mtimes.insert(path, Self::current_time_ms());
        Ok(())
    }

    async fn list(&self, path: &str) -> Result<Vec<FileEntry>> {
        let path = Self::normalize_path(path);
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{}/", path)
        };

        let dirs = self.dirs.read().unwrap();
        if !path.is_empty() && !dirs.contains_key(&path) {
            return Err(FsError::NotFound(path));
        }

        let mut entries = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // List files
        let files = self.files.read().unwrap();
        for file_path in files.keys() {
            if let Some(rest) = file_path.strip_prefix(&prefix) {
                let name = rest.split('/').next().unwrap();
                if !rest.contains('/') && seen.insert(name.to_string()) {
                    entries.push(FileEntry {
                        name: name.to_string(),
                        is_dir: false,
                    });
                }
            } else if prefix.is_empty()
                && !file_path.contains('/')
                && seen.insert(file_path.clone())
            {
                entries.push(FileEntry {
                    name: file_path.clone(),
                    is_dir: false,
                });
            }
        }

        // List subdirectories
        for dir_path in dirs.keys() {
            if let Some(rest) = dir_path.strip_prefix(&prefix) {
                let name = rest.split('/').next().unwrap();
                if !name.is_empty() && seen.insert(name.to_string()) {
                    entries.push(FileEntry {
                        name: name.to_string(),
                        is_dir: true,
                    });
                }
            } else if prefix.is_empty()
                && !dir_path.is_empty()
                && !dir_path.contains('/')
                && seen.insert(dir_path.clone())
            {
                entries.push(FileEntry {
                    name: dir_path.clone(),
                    is_dir: true,
                });
            }
        }

        Ok(entries)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let path = Self::normalize_path(path);

        // Try to delete as file first
        {
            let mut files = self.files.write().unwrap();
            if files.remove(&path).is_some() {
                return Ok(());
            }
        }

        // Try to delete as directory
        {
            let mut dirs = self.dirs.write().unwrap();
            if dirs.remove(&path).is_some() {
                return Ok(());
            }
        }

        Err(FsError::NotFound(path))
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let path = Self::normalize_path(path);
        let files = self.files.read().unwrap();
        let dirs = self.dirs.read().unwrap();
        Ok(files.contains_key(&path) || dirs.contains_key(&path))
    }

    async fn stat(&self, path: &str) -> Result<FileStat> {
        let path = Self::normalize_path(path);

        let files = self.files.read().unwrap();
        if let Some(content) = files.get(&path) {
            let mtimes = self.mtimes.read().unwrap();
            let mtime = mtimes.get(&path).copied().unwrap_or(0);
            return Ok(FileStat {
                mtime_millis: mtime,
                size: content.len() as u64,
                is_dir: false,
            });
        }

        let dirs = self.dirs.read().unwrap();
        if dirs.contains_key(&path) {
            return Ok(FileStat {
                mtime_millis: 0,
                size: 0,
                is_dir: true,
            });
        }

        Err(FsError::NotFound(path))
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        let path = Self::normalize_path(path);
        if path.is_empty() {
            return Ok(()); // Root always exists
        }

        // Create parent first
        if let Some(parent) = Self::parent_path(&path) {
            Box::pin(self.mkdir(&parent)).await?;
        }

        let mut dirs = self.dirs.write().unwrap();
        dirs.insert(path, ());
        Ok(())
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from = Self::normalize_path(from);
        let to = Self::normalize_path(to);

        // Try file first
        let mut files = self.files.write().unwrap();
        if let Some(content) = files.remove(&from) {
            files.insert(to.clone(), content);
            drop(files);

            // Move mtime too
            let mut mtimes = self.mtimes.write().unwrap();
            if let Some(mtime) = mtimes.remove(&from) {
                mtimes.insert(to, mtime);
            }
            return Ok(());
        }
        drop(files);

        // Try directory
        let mut dirs = self.dirs.write().unwrap();
        if dirs.remove(&from).is_some() {
            dirs.insert(to, ());
            return Ok(());
        }

        Err(FsError::NotFound(from))
    }
}

/// Shared conformance suite that validates the `FileSystem` error contract.
///
/// Any `FileSystem` implementation can call [`assert_fs_contract`] in its test suite
/// to verify it upholds the contract documented on the trait. Passing this suite is
/// required for all implementations (see module-level doc).
///
/// The module is compiled unconditionally (no `cfg(test)`) so crates outside
/// vault-sync (e.g. sync-daemon) can import it without enabling a separate feature.
/// It has no native-only dependencies and compiles for `wasm32`.
pub mod conformance {
    use super::{FileSystem, FsError};

    /// Assert that `read` on a missing path returns `FsError::NotFound`.
    pub async fn assert_read_missing_returns_not_found<F: FileSystem>(fs: &F) {
        let err = fs
            .read("__conformance_missing__.md")
            .await
            .expect_err("read of missing path should fail");
        assert!(
            matches!(err, FsError::NotFound(_)),
            "read: expected FsError::NotFound for missing path, got: {:?}",
            err
        );
    }

    /// Assert that `stat` on a missing path returns `FsError::NotFound`.
    pub async fn assert_stat_missing_returns_not_found<F: FileSystem>(fs: &F) {
        let err = fs
            .stat("__conformance_missing__.md")
            .await
            .expect_err("stat of missing path should fail");
        assert!(
            matches!(err, FsError::NotFound(_)),
            "stat: expected FsError::NotFound for missing path, got: {:?}",
            err
        );
    }

    /// Assert that `delete` on a missing path returns `FsError::NotFound`.
    pub async fn assert_delete_missing_returns_not_found<F: FileSystem>(fs: &F) {
        let err = fs
            .delete("__conformance_missing__.md")
            .await
            .expect_err("delete of missing path should fail");
        assert!(
            matches!(err, FsError::NotFound(_)),
            "delete: expected FsError::NotFound for missing path, got: {:?}",
            err
        );
    }

    /// Assert that `rename` with a missing source returns `FsError::NotFound`.
    pub async fn assert_rename_missing_returns_not_found<F: FileSystem>(fs: &F) {
        let err = fs
            .rename("__conformance_missing__.md", "__conformance_dest__.md")
            .await
            .expect_err("rename of missing source should fail");
        assert!(
            matches!(err, FsError::NotFound(_)),
            "rename: expected FsError::NotFound for missing source, got: {:?}",
            err
        );
    }

    /// Assert that writing then reading a file round-trips the bytes correctly.
    pub async fn assert_write_then_read_roundtrips<F: FileSystem>(fs: &F) {
        let content = b"conformance test content";
        fs.write("__conformance_roundtrip__.txt", content)
            .await
            .expect("write should succeed");
        let read_back = fs
            .read("__conformance_roundtrip__.txt")
            .await
            .expect("read after write should succeed");
        assert_eq!(
            read_back, content,
            "write-then-read: bytes did not round-trip"
        );
    }

    /// Assert that `stat` on an existing file reports `is_dir = false`.
    pub async fn assert_stat_existing_file_is_not_dir<F: FileSystem>(fs: &F) {
        fs.write("__conformance_stat__.txt", b"data")
            .await
            .expect("write should succeed");
        let stat = fs
            .stat("__conformance_stat__.txt")
            .await
            .expect("stat of existing file should succeed");
        assert!(
            !stat.is_dir,
            "stat: expected is_dir=false for a regular file"
        );
    }

    /// Assert that deleting an existing file, then reading it, returns `FsError::NotFound`.
    pub async fn assert_delete_then_read_returns_not_found<F: FileSystem>(fs: &F) {
        fs.write("__conformance_delete__.txt", b"data")
            .await
            .expect("write should succeed");
        fs.delete("__conformance_delete__.txt")
            .await
            .expect("delete of existing file should succeed");
        let err = fs
            .read("__conformance_delete__.txt")
            .await
            .expect_err("read after delete should fail");
        assert!(
            matches!(err, FsError::NotFound(_)),
            "delete-then-read: expected FsError::NotFound, got: {:?}",
            err
        );
    }

    /// Assert that renaming an existing file makes the old path return `FsError::NotFound`
    /// and the new path readable with the original content.
    pub async fn assert_rename_then_read_old_not_found_new_succeeds<F: FileSystem>(fs: &F) {
        let content = b"rename me";
        fs.write("__conformance_rename_src__.txt", content)
            .await
            .expect("write should succeed");
        fs.rename(
            "__conformance_rename_src__.txt",
            "__conformance_rename_dst__.txt",
        )
        .await
        .expect("rename should succeed");

        let old_err = fs
            .read("__conformance_rename_src__.txt")
            .await
            .expect_err("read of old path after rename should fail");
        assert!(
            matches!(old_err, FsError::NotFound(_)),
            "rename-old-path: expected FsError::NotFound, got: {:?}",
            old_err
        );

        let new_content = fs
            .read("__conformance_rename_dst__.txt")
            .await
            .expect("read of new path after rename should succeed");
        assert_eq!(
            new_content, content,
            "rename: new path content did not match original"
        );
    }

    /// Run the full `FileSystem` error contract conformance suite against `fs`.
    ///
    /// Call this from both the `InMemoryFs` and `NativeFs` test suites (and any future
    /// implementation) to ensure all implementations agree on the contract.
    pub async fn assert_fs_contract<F: FileSystem>(fs: &F) {
        assert_read_missing_returns_not_found(fs).await;
        assert_stat_missing_returns_not_found(fs).await;
        assert_delete_missing_returns_not_found(fs).await;
        assert_rename_missing_returns_not_found(fs).await;
        assert_write_then_read_roundtrips(fs).await;
        assert_stat_existing_file_is_not_dir(fs).await;
        assert_delete_then_read_returns_not_found(fs).await;
        assert_rename_then_read_old_not_found_new_succeeds(fs).await;
    }
}

// Implement FileSystem for Arc<T> where T: FileSystem
// This allows sharing a filesystem between multiple Vaults in tests.
//
// `atomic_write` is intentionally NOT overridden — it uses the trait default,
// which composes the overridden `write` + `rename` below, so it transparently
// inherits the inner `T`'s atomicity without a hand-written delegation.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg(not(target_arch = "wasm32"))]
impl<T: FileSystem + Send + Sync> FileSystem for std::sync::Arc<T> {
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        (**self).read(path).await
    }

    async fn write(&self, path: &str, content: &[u8]) -> Result<()> {
        (**self).write(path, content).await
    }

    async fn list(&self, path: &str) -> Result<Vec<FileEntry>> {
        (**self).list(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        (**self).delete(path).await
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        (**self).exists(path).await
    }

    async fn stat(&self, path: &str) -> Result<FileStat> {
        (**self).stat(path).await
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        (**self).mkdir(path).await
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        (**self).rename(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `InMemoryFs` upholds the full `FileSystem` error contract.
    #[tokio::test]
    async fn inmemory_fs_passes_conformance_suite() {
        let fs = InMemoryFs::new();
        conformance::assert_fs_contract(&fs).await;
    }

    #[tokio::test]
    async fn test_inmemory_fs_basic_operations() {
        let fs = InMemoryFs::new();

        // Write a file
        fs.write("test.txt", b"hello world").await.unwrap();

        // Read it back
        let content = fs.read("test.txt").await.unwrap();
        assert_eq!(content, b"hello world");

        // Check exists
        assert!(fs.exists("test.txt").await.unwrap());
        assert!(!fs.exists("nonexistent.txt").await.unwrap());

        // Delete
        fs.delete("test.txt").await.unwrap();
        assert!(!fs.exists("test.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_inmemory_fs_rename() {
        let fs = InMemoryFs::new();

        fs.write("old.txt", b"content").await.unwrap();
        fs.rename("old.txt", "new.txt").await.unwrap();

        assert!(!fs.exists("old.txt").await.unwrap());
        assert!(fs.exists("new.txt").await.unwrap());
        assert_eq!(fs.read("new.txt").await.unwrap(), b"content");
    }

    #[tokio::test]
    async fn test_inmemory_fs_rename_not_found() {
        let fs = InMemoryFs::new();

        let result = fs.rename("nonexistent.txt", "new.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_inmemory_fs_atomic_write_new_file() {
        let fs = InMemoryFs::new();

        fs.atomic_write("file.loro", b"data").await.unwrap();

        assert_eq!(fs.read("file.loro").await.unwrap(), b"data");
        assert!(
            !fs.exists("file.loro.tmp").await.unwrap(),
            "temp file should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_inmemory_fs_atomic_write_overwrites() {
        let fs = InMemoryFs::new();

        fs.write("file.loro", b"old").await.unwrap();
        fs.atomic_write("file.loro", b"new").await.unwrap();

        assert_eq!(fs.read("file.loro").await.unwrap(), b"new");
        assert!(!fs.exists("file.loro.tmp").await.unwrap());
    }

    #[tokio::test]
    async fn test_inmemory_fs_directories() {
        let fs = InMemoryFs::new();

        // Write creates parent directories
        fs.write("a/b/c.txt", b"content").await.unwrap();

        // Parent directories exist
        assert!(fs.exists("a").await.unwrap());
        assert!(fs.exists("a/b").await.unwrap());

        // List directory
        let entries = fs.list("a").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "b");
        assert!(entries[0].is_dir);

        let entries = fs.list("a/b").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "c.txt");
        assert!(!entries[0].is_dir);
    }
}
