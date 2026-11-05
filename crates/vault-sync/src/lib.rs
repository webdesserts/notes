// Deny holding RefCell borrows across await points - causes WASM panics
#![deny(clippy::await_holding_refcell_ref)]

//! vault-sync: the loro-half of P2P vault synchronization.
//!
//! This crate owns the vault data model and sync protocol — the Loro CRDT layer
//! that turns a folder of markdown notes into a convergent, peer-syncable vault.
//! It knows nothing about transport (iroh, sockets, relays); a consumer (the
//! sync daemon) wires it to a network and drives it through the byte seam.
//!
//! ## Feature gates
//!
//! - `cfg(target_arch = "wasm32")` — WASM-specific runtime differences: `Rc`/`RefCell`
//!   instead of `Arc`/`RwLock`, and `?Send` async trait bounds. These gates exist
//!   because Cargo feature unification can't express "Send only when native feature is
//!   enabled" — the arch check is the only reliable way to vary trait bounds.

pub mod conflict;
pub mod content_doc;
pub mod events;
pub mod fs;
pub mod hash;
pub mod index;
pub mod markdown;
#[cfg(feature = "network")]
pub mod network;
pub mod protocol;
pub mod vault;

pub use conflict::{NodeKind, StructuralOp, StructuralView, conflict_name, resolve_structure};
pub use content_doc::{ContentDoc, DocumentError};
pub use events::{EventBus, Subscription, SyncEvent};
pub use fs::{FileEntry, FileStat, FileSystem, InMemoryFs};
pub use hash::{ContentSummary, content_hash, content_summary, content_version_fingerprint};
pub use index::{
    CaseDrift, FileCaseMove, FileMove, FolderCaseMove, Index, IndexError, JournalReStitch,
    ReconcileReport, SyncMetadata, SyncState, VaultId, content_doc_path,
};
pub use markdown::ParsedMarkdown;
pub use protocol::{
    ChangeManifest, DocComparison, DocId, StructuralComparison, SyncError, SyncMessage,
    SyncOutcome, SyncRequestData,
};
pub use vault::Vault;
