//! Self-contained index types: the error taxonomy, the reconcile report, the
//! per-session sync-state machinery, and the vault metadata (`.sync/metadata.toml`).
//!
//! These carry no `Index` coupling, so they live apart from the tree-op impls.
//! `index/mod.rs` re-exports everything here so both the crate's public surface
//! and the crate-internal `crate::index::IndexError` paths resolve.
//!
//! Carried from the legacy sync crate's `vault/state.rs`. Two deliberate departures from the
//! port:
//! - The twin-dedupe read models are gone — UUID identity dissolves the path-hash
//!   collision class they guarded, so the dedupe subsystem does not exist in this
//!   crate.
//! - `SyncState`'s flag-TTL no longer routes through the process-global
//!   test-time-scaling lever (that subsystem is not ported here yet); it uses the
//!   raw `FLAG_TTL`. That lever is the identity at the production scale of 1.0, so
//!   this is behavior-preserving for production and for the in-memory test harness.

use crate::content_doc::DocumentError;
use crate::fs::{FileSystem, FsError};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use web_time::Instant;

/// Vault metadata file (contains the VaultId and format version).
pub(crate) const METADATA_FILE: &str = ".sync/metadata.toml";

/// Current version of the `.sync/` directory format.
///
/// Bump this when any breaking change is made to the file layout, the Loro
/// schema, or naming. This is a greenfield format (the `vault-sync` rewrite is
/// not wire-compatible with the legacy sync crate's `.sync/` directory), so it starts
/// fresh at version 1.
pub(crate) const CURRENT_SYNC_VERSION: u32 = 1;

/// A stable author identity for a vault, used as the Loro peer id for the index
/// CRDT's vault-shared operations.
///
/// `VaultId` identifies the vault itself and is shared across every device holding
/// a copy of the same vault. Generated once and persisted in `.sync/metadata.toml`.
/// A `u64` internally (Loro-native), displayed as a 16-character hex string.
///
/// Lives here alongside `SyncMetadata` (its only consumer in this chunk). The 1e
/// public handle, which sets the device author on the index doc, is the heavier
/// identity consumer; if a dedicated identity module emerges there, `VaultId` is a
/// candidate to relocate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VaultId(u64);

impl VaultId {
    /// Generate a new random vault id. Uses cryptographically secure randomness;
    /// never returns zero (Loro treats 0 as an invalid peer id).
    pub fn generate() -> Self {
        use rand::Rng;
        loop {
            let id: u64 = rand::rng().random();
            if id != 0 {
                return Self(id);
            }
        }
    }

    /// Get the underlying `u64` value (for the Loro author API).
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for VaultId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::str::FromStr for VaultId {
    type Err = IndexError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // 16 hex chars only — VaultId has no legacy UUID format.
        if s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            let id = u64::from_str_radix(&s.to_ascii_lowercase(), 16)
                .map_err(|e| IndexError::CorruptMetadata(format!("invalid VaultId hex: {}", e)))?;
            return Ok(Self(id));
        }
        Err(IndexError::CorruptMetadata(format!(
            "invalid VaultId format: {}",
            s
        )))
    }
}

impl From<u64> for VaultId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<VaultId> for u64 {
    fn from(vault_id: VaultId) -> u64 {
        vault_id.0
    }
}

impl serde::Serialize for VaultId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for VaultId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Vault-level metadata persisted in `.sync/metadata.toml`.
///
/// The `version` field represents the entire `.sync/` directory format — not just
/// this struct's schema. Migrations can move files, rewrite Loro docs, create
/// directories, etc. The version is just the marker for what state the directory is in.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncMetadata {
    /// Format version of the `.sync/` directory.
    pub version: u32,
    /// Vault identity (gossip topic seed + mesh grouping), shared across replicas.
    pub vault_id: VaultId,
}

impl SyncMetadata {
    /// Create metadata for a new vault with a freshly generated VaultId.
    pub fn new() -> Self {
        Self {
            version: CURRENT_SYNC_VERSION,
            vault_id: VaultId::generate(),
        }
    }
}

impl Default for SyncMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncMetadata {
    /// Load metadata from `.sync/metadata.toml`, running migrations if needed.
    ///
    /// If `metadata.toml` doesn't exist, runs the v0→v1 migration (generates a new
    /// VaultId). If it does exist but has a version newer than supported, returns an
    /// error. Each migration is idempotent — safe to re-run if a previous attempt
    /// crashed.
    pub async fn load_or_migrate<F: FileSystem>(fs: &F) -> Result<Self> {
        if fs.exists(METADATA_FILE).await? {
            let bytes = fs.read(METADATA_FILE).await?;
            let content = String::from_utf8(bytes.to_vec())
                .map_err(|e| IndexError::CorruptMetadata(format!("invalid UTF-8: {}", e)))?;
            let meta: SyncMetadata = toml::from_str(&content)
                .map_err(|e| IndexError::CorruptMetadata(format!("invalid TOML: {}", e)))?;

            if meta.version > CURRENT_SYNC_VERSION {
                return Err(IndexError::VersionTooNew(
                    meta.version,
                    CURRENT_SYNC_VERSION,
                ));
            }

            tracing::info!(vault_id = %meta.vault_id, version = meta.version, "Loaded vault metadata");
            Ok(meta)
        } else {
            // v0 → v1: Generate VaultId and write metadata.toml.
            tracing::info!("Running migration v0 → v1: generating VaultId");
            let meta = SyncMetadata::new();

            let toml_str = toml::to_string(&meta).map_err(|e| {
                IndexError::MetadataSerialization(format!("Failed to serialize metadata: {}", e))
            })?;
            fs.atomic_write(METADATA_FILE, toml_str.as_bytes()).await?;

            // Verify the write succeeded by re-reading. Does NOT protect against
            // concurrent writers — vault locking (a later phase) addresses that.
            let bytes = fs.read(METADATA_FILE).await?;
            let content = String::from_utf8(bytes.to_vec())
                .map_err(|e| IndexError::CorruptMetadata(format!("invalid UTF-8: {}", e)))?;
            let written: SyncMetadata = toml::from_str(&content).map_err(|e| {
                IndexError::CorruptMetadata(format!("invalid TOML after write: {}", e))
            })?;

            tracing::info!(vault_id = %written.vault_id, "Migration v0 → v1 complete");
            Ok(written)
        }
    }

    /// Persist this metadata to `.sync/metadata.toml` (atomic write).
    pub async fn save<F: FileSystem>(&self, fs: &F) -> Result<()> {
        let toml_str = toml::to_string(self).map_err(|e| {
            IndexError::MetadataSerialization(format!("Failed to serialize metadata: {}", e))
        })?;
        fs.atomic_write(METADATA_FILE, toml_str.as_bytes()).await?;
        Ok(())
    }
}

/// Error taxonomy for the index and its persistence.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Filesystem error: {0}")]
    Fs(#[from] FsError),

    #[error("Document error: {0}")]
    Document(#[from] DocumentError),

    #[error("Vault not initialized")]
    NotInitialized,

    #[error("Sync data version {0} is newer than supported (max {1}), update your client")]
    VersionTooNew(u32, u32),

    #[error("Corrupt metadata: {0}")]
    CorruptMetadata(String),

    #[error("Corrupt index: {0}")]
    CorruptIndex(String),

    /// Failed to serialize vault metadata (metadata.toml) for persistence.
    #[error("{0}")]
    MetadataSerialization(String),

    /// Failed to export the index CRDT to bytes for persistence.
    #[error("{0}")]
    IndexExport(String),

    /// Failed to import index bytes into the in-memory index CRDT.
    #[error("{0}")]
    IndexImport(String),

    /// A Loro tree mutation (create/move/delete node or set node metadata) failed.
    #[error("{0}")]
    TreeOperation(String),

    /// A move was requested but its source path has no index node.
    #[error("{0}")]
    MoveSourceMissing(String),

    /// A move was requested but its target path already has an index node.
    #[error("{0}")]
    MoveTargetExists(String),

    /// A sync path failed validation (traversal, absolute, non-markdown, etc.).
    #[error("{0}")]
    InvalidPath(String),
}

pub type Result<T> = std::result::Result<T, IndexError>;

/// A detected file move.
#[derive(Debug, Clone)]
pub struct FileMove {
    /// Original path (from the index node's `path` meta).
    pub from: String,
    /// New path (current filesystem location).
    pub to: String,
}

/// Report from the boot reconciliation process.
#[derive(Debug, Default)]
pub struct ReconcileReport {
    /// Files that were newly indexed (no `.loro` existed, not a move).
    pub indexed: Vec<String>,
    /// Files that were re-indexed (markdown differed from Loro).
    pub reindexed: Vec<String>,
    /// Files that were moved/renamed (detected via content matching).
    pub moved: Vec<FileMove>,
    /// Orphaned `.loro` content (the file was deleted, not moved).
    pub orphaned: Vec<String>,
    /// Disk orphans (tombstoned-in-index `.md` files still on disk) that were
    /// moved to `.trash/`. Deliberately excluded from `has_changes()`: quarantine
    /// is local disk cleanup with no index mutation and no sync implications, so it
    /// must not cause the daemon to broadcast.
    pub quarantined: Vec<String>,
    /// Disk `.md` files that already had a `.loro` (a peer's content) but no index
    /// node, for which boot reconcile registered a node *adopting the existing
    /// `.loro`* — preserving the peer's lineage UUID rather than rebuilding from
    /// markdown. This is the fs↔loro divergence heal. It IS a change: the
    /// freshly-registered node is genuinely new to this device's index and peers
    /// should learn it, so it counts in `has_changes()`.
    pub adopted: Vec<String>,
    /// Alive index nodes whose backing `.md` file is missing from disk (the inverse
    /// divergence). REPORT-ONLY: reconcile takes NO action here — it neither
    /// recreates the file (resurrection) nor tombstones the node
    /// (deletion-propagation). Like `quarantined`, excluded from `has_changes()`.
    pub missing_files: Vec<String>,
}

impl ReconcileReport {
    /// Whether any changes were made that peers should learn.
    ///
    /// `quarantined` and `missing_files` are intentionally NOT consulted — both are
    /// local-only (disk cleanup / report-only) with no sync implications, and the
    /// daemon gates broadcasts on this method. `adopted` IS consulted: it registers
    /// a new index node peers should learn.
    pub fn has_changes(&self) -> bool {
        !self.indexed.is_empty()
            || !self.reindexed.is_empty()
            || !self.moved.is_empty()
            || !self.adopted.is_empty()
    }

    /// Total number of files processed.
    pub fn total_processed(&self) -> usize {
        self.indexed.len() + self.reindexed.len() + self.moved.len()
    }
}

/// Tracks sync state for echo detection and consistency reconciliation.
///
/// When a file is received from sync, it is marked here BEFORE writing to disk;
/// when the file watcher fires, the flag is checked and consumed to skip broadcast.
///
/// Also tracks paths that need reconciliation before processing the next sync
/// message, so Loro documents match the filesystem before importing sync data.
#[derive(Clone)]
pub struct SyncState {
    /// Map of path -> timestamp when marked as synced (for echo detection).
    synced_paths: Arc<Mutex<HashMap<String, Instant>>>,
    /// Paths that may need reconciliation before the next sync import.
    pending_reconcile: Arc<Mutex<HashSet<String>>>,
    /// Index may need reconciliation before the next sync import.
    index_pending: Arc<Mutex<bool>>,
    /// Paths whose index tree node is currently deleted. Used to prevent inbound
    /// document updates from resurrecting files the local device (or a peer) deleted.
    ///
    /// Derived from index truth: `rebuild_caches` recomputes it from the persisted
    /// tree on every rebuild (so it survives a restart), and `delete_node` inserts
    /// synchronously so a local delete guards inbound updates before the next index
    /// sync. The two are complementary, not exclusive.
    deleted_paths: Arc<Mutex<HashSet<String>>>,
}

/// Time-to-live for sync flags. Flags older than this are considered stale.
/// Set to 30s to provide safety margin for echo detection even with delayed file
/// watchers.
///
/// Production always uses this fixed 30s window. Tests need to expire a flag
/// without sleeping a real 30s, so under `cfg(test)` the TTL is read through
/// [`flag_ttl`], which a test can shrink via [`set_flag_ttl_for_test`]. The
/// override is strictly test-only — it does not exist in production builds.
const FLAG_TTL: Duration = Duration::from_secs(30);

/// The active sync-flag TTL. Production is always [`FLAG_TTL`]; a test may shrink
/// the window (per-thread) so the over-suppression expiry path is checkable without
/// a real 30s sleep.
#[cfg(not(test))]
#[inline]
fn flag_ttl() -> Duration {
    FLAG_TTL
}

#[cfg(test)]
thread_local! {
    static FLAG_TTL_OVERRIDE: std::cell::Cell<Option<Duration>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn flag_ttl() -> Duration {
    FLAG_TTL_OVERRIDE.with(|c| c.get()).unwrap_or(FLAG_TTL)
}

/// Override the sync-flag TTL for the current test thread (test-only).
///
/// Lets a test drive the genuine-later-delete-after-expiry path deterministically:
/// set a tiny TTL, wait past it, and the consumed flag returns `false` so a real
/// delete tombstones. Production has no such lever — the 30s window is fixed.
#[cfg(test)]
pub(crate) fn set_flag_ttl_for_test(ttl: Duration) {
    FLAG_TTL_OVERRIDE.with(|c| c.set(Some(ttl)));
}

impl Default for SyncState {
    fn default() -> Self {
        Self {
            synced_paths: Arc::new(Mutex::new(HashMap::new())),
            pending_reconcile: Arc::new(Mutex::new(HashSet::new())),
            index_pending: Arc::new(Mutex::new(false)),
            deleted_paths: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl SyncState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a path as having been synced (call before writing to disk).
    /// Adds to both `synced_paths` (for echo detection) and `pending_reconcile`
    /// (to ensure consistency before the next sync import).
    pub fn mark_synced(&self, path: &str) {
        self.synced_paths
            .lock()
            .unwrap()
            .insert(path.to_string(), Instant::now());
        self.pending_reconcile
            .lock()
            .unwrap()
            .insert(path.to_string());
    }

    /// Check if a path was synced and consume the flag (returns true once).
    /// Also removes it from the `pending_reconcile` set.
    /// Returns false if the flag has expired (older than `FLAG_TTL`).
    pub fn consume_synced(&self, path: &str) -> bool {
        self.pending_reconcile.lock().unwrap().remove(path);
        let mut paths = self.synced_paths.lock().unwrap();
        if let Some(timestamp) = paths.remove(path) {
            if timestamp.elapsed() < flag_ttl() {
                return true;
            }
            tracing::debug!(
                "Sync flag expired for {} (age={}ms)",
                path,
                timestamp.elapsed().as_millis()
            );
        }
        false
    }

    /// Take all paths pending reconciliation (called before a sync import).
    /// Returns the set of paths and clears the pending set.
    pub fn take_pending_reconcile(&self) -> HashSet<String> {
        std::mem::take(&mut *self.pending_reconcile.lock().unwrap())
    }

    /// Mark the index as needing reconciliation before the next sync import.
    pub fn mark_index_synced(&self) {
        *self.index_pending.lock().unwrap() = true;
    }

    /// Check and clear the index-pending flag (atomic take).
    /// Returns true if the index needs reconciliation.
    pub fn take_index_pending(&self) -> bool {
        std::mem::take(&mut *self.index_pending.lock().unwrap())
    }

    /// Mark a single path as deleted in the index tree.
    ///
    /// Called synchronously by `delete_node` so an inbound document update arriving
    /// before the next index sync is still guarded against resurrecting the
    /// just-deleted path.
    pub fn mark_path_deleted(&self, path: &str) {
        self.deleted_paths.lock().unwrap().insert(path.to_string());
    }

    /// Replace the entire deleted-paths set with one freshly derived from index truth.
    ///
    /// Called by `rebuild_caches`, which recomputes the set from the persisted tree —
    /// so the guard survives a restart, and "alive wins" (a path occupied by any alive
    /// node) is honored by construction (such paths are simply not in `paths`).
    pub fn replace_deleted_paths(&self, paths: HashSet<String>) {
        *self.deleted_paths.lock().unwrap() = paths;
    }

    /// Whether the given path's index tree node is currently deleted.
    ///
    /// True only for paths known-deleted in the index. Paths not in the set are
    /// treated as potentially new, preserving create-if-new for brand-new paths.
    pub fn is_path_deleted_in_index(&self, path: &str) -> bool {
        self.deleted_paths.lock().unwrap().contains(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vault_id_round_trips_through_string() {
        let original = VaultId::generate();
        let parsed: VaultId = original.to_string().parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn vault_id_display_is_zero_padded_hex() {
        let id = VaultId::from(0xff_u64);
        assert_eq!(id.to_string(), "00000000000000ff");
    }

    #[test]
    fn vault_id_generate_is_never_zero() {
        for _ in 0..1000 {
            assert_ne!(VaultId::generate().as_u64(), 0);
        }
    }

    #[test]
    fn vault_id_display_is_lower_hex() {
        let id = VaultId(0xa1b2c3d4e5f67890);
        assert_eq!(id.to_string(), "a1b2c3d4e5f67890");
    }

    #[test]
    fn vault_id_parses_hex() {
        let id: VaultId = "a1b2c3d4e5f67890".parse().unwrap();
        assert_eq!(id.as_u64(), 0xa1b2c3d4e5f67890);
    }

    #[test]
    fn vault_id_rejects_uuid() {
        // VaultId has no legacy UUID support.
        assert!(
            "550e8400-e29b-41d4-a716-446655440000"
                .parse::<VaultId>()
                .is_err()
        );
    }

    #[test]
    fn vault_id_serde_round_trips() {
        let original = VaultId::generate();
        let json = serde_json::to_string(&original).unwrap();
        let parsed: VaultId = serde_json::from_str(&json).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn sync_metadata_serde_round_trips() {
        let original = SyncMetadata::new();
        let toml_str = toml::to_string(&original).unwrap();
        let parsed: SyncMetadata = toml::from_str(&toml_str).unwrap();
        assert_eq!(original.version, parsed.version);
        assert_eq!(original.vault_id, parsed.vault_id);
    }
}
