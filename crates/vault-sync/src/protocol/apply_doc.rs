//! Apply inbound document updates: merge an existing document or materialize a new
//! one, keyed by UUID.
//!
//! Carried from the legacy sync crate's `sync_engine/document_apply.rs`, with the headline
//! simplification the UUID model buys: the entire "silent latest-wins divergence"
//! branch is GONE. The old code keyed updates by path, so two replicas that
//! independently created different documents at the same path would collide; it
//! resolved that by comparing machine-local mtimes and silently overwriting the
//! loser (INV-3 violation, non-deterministic). Under UUID keying a same-UUID merge
//! is ALWAYS a normal CRDT merge (INV-2); a distinct-UUID same-path collision is the
//! P2 conflict cascade's job (handled in `apply_index.rs` over the merged state),
//! never here. No mtime, no divergence branch, no latest-wins.
//!
//! ## The Flow-2 gate is structural (INV-8)
//!
//! A `DocUpdate{uuid}` can only materialize once its Index node has arrived: with no
//! node, the UUID resolves to no path, so there is nowhere to write the `.md` and no
//! `docs/<uuid>.loro` to address. The resolution returning `None` IS the gate — the
//! update is warned-and-held, recovered when the node lands (the send side ships the
//! node with the doc, C3) or at boot reconcile (C4).

use crate::content_doc::ContentDoc;
use crate::fs::FileSystem;
use crate::vault::Vault;

use std::collections::HashMap;
use tracing::{debug, info, warn};

use super::{DocId, Result};

impl<F: FileSystem> Vault<F> {
    /// Apply a batch of document updates from a sync response.
    ///
    /// Contains per-document failures: one corrupt entry is logged and skipped so it
    /// can't abort the whole batch and drop every other valid document with it (there
    /// is no per-item retry path, so a partial set of applied UUIDs is the correct
    /// outcome — the caller emits events only for documents that landed).
    pub(super) async fn apply_doc_updates(
        &self,
        updates: HashMap<DocId, Vec<u8>>,
    ) -> Result<Vec<DocId>> {
        let mut modified = Vec::new();

        for (uuid, data) in updates {
            match self.apply_doc_update(&uuid, &data).await {
                Ok(true) => modified.push(uuid),
                Ok(false) => {}
                Err(e) => warn!("apply_doc_updates: skipping {}: {}", uuid, e),
            }
        }

        Ok(modified)
    }

    /// Apply a single document update, keyed by UUID. Returns `true` if the document
    /// was modified.
    ///
    /// Resolves `uuid → path` via the Index. With no node the update is held (the
    /// structural Flow-2 gate). With a node: an existing document CRDT-merges the
    /// update (the normal same-UUID path, INV-2); a not-yet-materialized one is
    /// written to disk (`.md` + `docs/<uuid>.loro`). A previously-deleted path is
    /// never resurrected.
    pub(super) async fn apply_doc_update(&self, uuid: &DocId, data: &[u8]) -> Result<bool> {
        debug!("apply_doc_update: {} - data_len={}", uuid, data.len());

        // Flow-2 gate (structural, INV-8): no Index node for this UUID ⇒ no resolved
        // path ⇒ nothing to materialize. Hold the update; it re-applies cleanly once
        // the node arrives (C3 ships it with the doc) or boot reconcile heals it (C4).
        // Nothing is stranded — no `.loro` was written.
        let Some(path) = self.path_for_doc(uuid) else {
            warn!(
                "apply_doc_update: holding update for {} — no Index node yet (Flow-2 gate)",
                uuid
            );
            return Ok(false);
        };

        // Resurrection guard: a path tombstoned in the Index must not be re-created by
        // an inbound update (that would ping-pong deletes between peers). The
        // deleted-paths set is Index-truth, rederived on every `rebuild_caches`, so it
        // guards across a restart. A legit re-create clears the path from the set in
        // `apply_index_updates` (alive wins) before its document update reaches here.
        if self.index().is_path_deleted(&path) {
            info!(
                "apply_doc_update: skipping resurrection of registry-deleted path: {} ({})",
                path, uuid
            );
            return Ok(false);
        }

        let loro_path = Self::doc_content_path(uuid);
        // Decide merge-vs-materialize by THIS document's UUID, not by the path. During a
        // distinct-UUID same-path collision, the path-keyed `documents` cache may hold a
        // DIFFERENT document's doc, so "is the path cached?" is the wrong question —
        // "does THIS uuid's content already exist (its `.loro` on disk, or a cached doc
        // whose id matches)?" is the right one. Asking by path would route a brand-new
        // colliding doc into `merge_into_existing` against the other doc's content and
        // corrupt both `.loro`s (the body merges into the wrong document).
        let cached_for_uuid = self.cached_doc_matches_uuid(&path, uuid);
        let exists_on_disk = self.fs().exists(&loro_path).await?;

        if cached_for_uuid || exists_on_disk {
            self.merge_into_existing(uuid, &path, &loro_path, data)
                .await
        } else {
            self.materialize_new(uuid, &path, &loro_path, data).await
        }
    }

    /// Whether the document cached at `path` is the one for `uuid` (its `doc_id`
    /// matches). False when the path is uncached OR — the collision case — the cached
    /// doc belongs to a different UUID that happens to share this display path.
    ///
    /// A `false` here routes the caller to the disk-load path (read `<uuid>.loro`),
    /// which is the safe direction: an uncached path, a cached doc with no readable
    /// `doc_id`, and an unparseable `doc_id` string all fall through to loading THIS
    /// uuid's own content from disk rather than risking a merge into the wrong document.
    fn cached_doc_matches_uuid(&self, path: &str, uuid: &DocId) -> bool {
        self.documents()
            .get(path)
            .and_then(|d| d.doc_id())
            .and_then(|id| uuid::Uuid::parse_str(&id).ok())
            .map(|id| id == uuid.0)
            .unwrap_or(false)
    }

    /// Merge an inbound update into an existing document (the normal same-UUID CRDT
    /// path). Persists `.md` + `.loro` and bumps `content_version` only on a real
    /// change.
    async fn merge_into_existing(
        &self,
        uuid: &DocId,
        path: &str,
        loro_path: &str,
        data: &[u8],
    ) -> Result<bool> {
        // Load the document for THIS uuid: the path-cached doc only if its id matches
        // (during a same-path collision the cache may hold a different document), else
        // its own `<uuid>.loro` on disk. Loading the wrong doc here would merge the
        // inbound body into another document and corrupt both. Bind the cache lookup to
        // a local so the `documents()` guard is released before the `.await` below —
        // never hold a guard across an await point (keeps the Flow-2 future Send).
        let cached = if self.cached_doc_matches_uuid(path, uuid) {
            self.documents().get(path).cloned()
        } else {
            None
        };
        let mut doc = match cached {
            Some(doc) => doc,
            None => {
                let bytes = self.fs().read(loro_path).await?;
                ContentDoc::from_bytes(&bytes, self.loro_author())?
            }
        };

        // Import is a CRDT merge — same-UUID histories always merge cleanly (INV-2).
        // The change is whatever the merge added beyond our current state.
        let version_before = doc.version();
        doc.import(data)?;
        let modified = version_before != doc.version();

        debug!(
            "apply_doc_update: {} ({}) - merged, modified={}",
            path, uuid, modified
        );

        if modified {
            doc.commit();
            // Echo detection: mark synced before writing the materialized `.md`, so
            // the local watcher recognizes our own write and does not re-broadcast.
            self.mark_synced(path);
            self.write_materialized(path, loro_path, &doc).await?;
            self.bump_content_version(path, &doc)?;
            self.documents_mut().insert(path.to_string(), doc);
        }

        Ok(modified)
    }

    /// Materialize a brand-new document the Index node already vouches for: write its
    /// `.md` + `docs/<uuid>.loro`, cache it, and set its initial `content_version`.
    ///
    /// Reaching here means the node arrived (the Flow-2 gate passed) but no content
    /// exists yet locally — the new-document case. The Index node is NOT created here
    /// (Index sync owns that; creating one would mint a duplicate).
    async fn materialize_new(
        &self,
        uuid: &DocId,
        path: &str,
        loro_path: &str,
        data: &[u8],
    ) -> Result<bool> {
        // New ops author under this device (imported ops keep their authors).
        let doc = ContentDoc::from_bytes(data, self.loro_author())?;

        self.mark_synced(path);
        self.write_materialized(path, loro_path, &doc).await?;
        self.bump_content_version(path, &doc)?;
        self.documents_mut().insert(path.to_string(), doc);

        debug!("apply_doc_update: materialized new {} ({})", path, uuid);
        Ok(true)
    }

    /// Write a document's `.md` (the user's file) and `docs/<uuid>.loro` (the CRDT
    /// snapshot) to disk.
    async fn write_materialized(
        &self,
        path: &str,
        loro_path: &str,
        doc: &ContentDoc,
    ) -> Result<()> {
        let snapshot = doc.export_snapshot()?;
        self.fs().atomic_write(loro_path, &snapshot).await?;
        self.fs().write(path, doc.to_markdown().as_bytes()).await?;
        Ok(())
    }

    /// Refresh a node's denormalized `content_version` fingerprint after an inbound
    /// merge/materialize (the derived digest cache the compare protocol reads).
    ///
    /// Resolves the node by the doc's OWN UUID, not by `path`: during a distinct-UUID
    /// same-path collision the path cache points at the OTHER colliding node, so a
    /// path-keyed lookup would write the wrong UUID's local content_version entry and
    /// leave this doc's unset (the digest reads a per-UUID local table now, so a
    /// mis-keyed write diverges the two replicas). Falls back to the path lookup only
    /// when the doc carries no parseable UUID (malformed — nothing to key by).
    fn bump_content_version(&self, path: &str, doc: &ContentDoc) -> Result<()> {
        let node = doc
            .doc_id()
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .and_then(|uuid| self.index().find_node_by_uuid(&uuid))
            .or_else(|| self.index().node_for_path(path));
        if let Some(node) = node {
            let fingerprint = crate::hash::content_version_fingerprint(&doc.version());
            self.index().set_content_version(&node, &fingerprint)?;
        }
        Ok(())
    }

    /// Apply a real-time `DocDeleted{uuid}` push: tombstone the Index node and remove
    /// the document's `.md` + `docs/<uuid>.loro` from disk. Returns `true` if a live
    /// document was deleted.
    ///
    /// Resolves `uuid → path` via the Index. A delete arriving for an unknown UUID
    /// (no node) is a no-op — there is nothing to delete and no node to tombstone, so
    /// the delete simply doesn't propagate further (mirrors the Index `delete_node`
    /// "no node, no tombstone" contract). The path is marked synced before the `.md`
    /// is removed (echo detection), and the node is tombstoned via the Index so the
    /// deletion is a tracked CRDT op that propagates and survives a restart.
    pub(super) async fn apply_doc_deleted(&self, uuid: &DocId) -> Result<bool> {
        let Some(path) = self.path_for_doc(uuid) else {
            debug!(
                "apply_doc_deleted: no Index node for {} — nothing to delete",
                uuid
            );
            return Ok(false);
        };

        // Tombstone the node first (this also arms the deleted-paths guard so an
        // in-flight document update can't resurrect the path).
        let tombstoned = self.index().delete_node(&path)?;
        self.index().save_index(self.fs()).await?;

        // Remove the materialized `.md` (mark synced first for echo detection).
        self.mark_synced(&path);
        let loro_path = Self::doc_content_path(uuid);
        if self.fs().exists(&path).await.unwrap_or(false)
            && let Err(e) = self.fs().delete(&path).await
        {
            warn!("apply_doc_deleted: failed to delete {}: {}", path, e);
        }
        if self.fs().exists(&loro_path).await.unwrap_or(false)
            && let Err(e) = self.fs().delete(&loro_path).await
        {
            warn!("apply_doc_deleted: failed to delete {}: {}", loro_path, e);
        }
        self.documents_mut().remove(&path);

        debug!(
            "apply_doc_deleted: deleted {} ({}), tombstoned={}",
            path, uuid, tombstoned
        );
        Ok(tombstoned)
    }
}
