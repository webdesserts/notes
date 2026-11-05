//! Note-editing core: heading-path section addressing and content-hash-guarded
//! storage. Extracted from the `notes-sync` MCP server crate so the same
//! outline/section-resolve/hash-verify mechanism can be shared by any
//! note-editing consumer (the MCP server today; a harness's native
//! hash-guarded read/write tools are the intended second consumer).
//!
//! This crate is a lift-out of the MCP server's `sections/` and `storage/`
//! modules, unmodified in behavior. It has no dependency on any MCP
//! protocol type, vault-path resolution, or graph/embedding state - see
//! `sections`'s and `storage`'s own module docs for the addressing grammar
//! and the read-first-write (optimistic locking) contract.

pub mod sections;
pub mod storage;

pub use sections::create::{ResolvedSectionCreate, SectionCreateError, create_section};
pub use sections::outline::{Outline, Section, SectionKind, build_outline, extract_section};
pub use sections::path::{SectionResolveError, resolve_section};
pub use sections::write::{
    ResolvedSectionWrite, SectionWriteError, resolve_section_for_write, splice_section,
};
pub use storage::{ContentHash, FileStorage, NoteMetadata, Storage, StorageError, WriteResult};
