//! Filesystem events: change/create/delete handling, the move coalescer and
//! case-drift sweeps.

use super::*;

impl<FS: FileSystem + 'static, AL: AllowlistStorage + 'static> Daemon<FS, AL> {
    /// Handle a file change event from the watcher, routing through the
    /// move-coalescer so a native rename (delivered as an unlinked delete+create —
    /// see [`crate::move_coalescer`]) collapses into one same-UUID move rather than
    /// a tombstone plus a fresh-UUID create.
    ///
    /// Only events that COULD be half of a move enter the window:
    /// - a `Modified` at a path with a still-live node and NO buffered delete is an
    ///   edit — dispatched immediately, the no-latency common case;
    /// - a `Modified` at a not-yet-tracked path (or a path whose deletion is
    ///   currently buffered — the re-create-at-a-just-deleted-path edge, OQ-D) is a
    ///   create-candidate;
    /// - a `Deleted` is always a move-candidate (its standalone meaning on expiry is
    ///   a real tombstone).
    pub(super) async fn on_file_changed(&mut self, event: FileEvent) {
        match event.kind {
            FileEventKind::Modified => {
                // An in-place edit of an already-tracked file is NOT a move
                // candidate — dispatch it immediately so the common case adds zero
                // latency. The buffered-delete check keeps OQ-D (a re-create at a
                // path whose deletion we are deliberately holding) on the create
                // path: the node still reads "live" only because the buffer has not
                // tombstoned it yet.
                let is_create = !self.path_has_live_node(&event.path).await
                    || self.move_coalescer.has_pending_delete(&event.path);
                if !is_create {
                    self.on_file_modified(&event.path).await;
                    return;
                }
                self.on_create_candidate(&event.path).await;
            }
            FileEventKind::Deleted => {
                self.on_delete_candidate(&event.path).await;
            }
        }
    }

    /// Whether the index currently has a live node at `path`.
    pub(super) async fn path_has_live_node(&self, path: &str) -> bool {
        let vault = self.vault.lock().await;
        vault.index().node_for_path(path).is_some()
    }

    /// Feed a create-candidate `Modified` into the coalescer and act on its
    /// decision: pair it into a move now, or buffer it for the window.
    pub(super) async fn on_create_candidate(&mut self, path: &str) {
        let Some(hash) = self.hash_new_file(path).await else {
            // Couldn't read/parse the new file — fall back to the standalone create
            // path rather than buffering an event we can't pair. The coalescer is
            // untouched, so the journal needs no rewrite here.
            self.on_file_modified(path).await;
            return;
        };
        match self.move_coalescer.on_create(hash, path) {
            MoveDecision::Move { old_path, new_path } => {
                self.emit_file_move(&old_path, &new_path).await;
            }
            MoveDecision::Buffered => {}
        }
        // Persist AFTER the mutation (a buffered create added a record, or a paired
        // move popped both halves) so the journal reflects the post-commit state —
        // the persist-after-commit invariant that makes pruning automatic (P4f-2).
        self.persist_pending_moves().await;
    }

    /// Feed a delete-candidate `Deleted` into the coalescer and act on its
    /// decision: pair it into a move now, or buffer it for the window.
    ///
    /// The doc's content hash is captured from its still-present `ContentDoc`
    /// BEFORE any tombstone (the buffer holds the event; nothing is deleted until
    /// the window resolves).
    pub(super) async fn on_delete_candidate(&mut self, path: &str) {
        let Some((hash, uuid)) = self.hash_tracked_doc(path).await else {
            // No content doc resolves for this path (an unknown/already-gone path) —
            // it can't be the old half of a move; commit it standalone. The
            // coalescer is untouched, so the journal needs no rewrite here.
            self.on_file_deleted(path).await;
            return;
        };
        match self.move_coalescer.on_delete(hash, path, uuid) {
            MoveDecision::Move { old_path, new_path } => {
                self.emit_file_move(&old_path, &new_path).await;
            }
            MoveDecision::Buffered => {}
        }
        // Persist AFTER the mutation (a buffered delete added a record, or a paired
        // move popped both halves) — the post-commit snapshot (P4f-2).
        self.persist_pending_moves().await;
    }

    /// Content hash AND document UUID of a tracked document at `path`. The hash is
    /// in the same domain as [`Self::hash_new_file`] (both hash the canonical
    /// materialized markdown); the UUID is the node's identity, carried through the
    /// coalescer into the crash-recovery journal so a buffered move's lineage can be
    /// re-stitched on boot (P4f-2). Both are read under ONE vault lock so they
    /// describe the same snapshot of the node. `None` if no node resolves for the
    /// path.
    pub(super) async fn hash_tracked_doc(&self, path: &str) -> Option<([u8; 32], Uuid)> {
        let vault = self.vault.lock().await;
        let node = vault.index().node_for_path(path)?;
        let uuid = vault.index().node_uuid(&node)?;
        let doc = vault.get_document(path).await.ok()?;
        Some((vault_sync::content_hash(&doc), uuid))
    }

    /// Content hash of a not-yet-tracked `.md` on disk, built from its markdown so
    /// it lands in the same hash domain as the delete side. The author is
    /// irrelevant — [`vault_sync::content_hash`] is taken over the document's
    /// materialized markdown, not its peer id.
    pub(super) async fn hash_new_file(&self, path: &str) -> Option<[u8; 32]> {
        let vault = self.vault.lock().await;
        let bytes = vault.read_raw(path).await.ok()?;
        let content = String::from_utf8_lossy(&bytes);
        let doc = vault_sync::ContentDoc::from_markdown(&content, 0).ok()?;
        Some(vault_sync::content_hash(&doc))
    }

    /// Execute a matched move: re-parent the existing node (preserving its UUID and
    /// content — `move_node` re-transfers nothing, INV-1), persist the index, and
    /// broadcast the new path so peers apply the structural `tree.mov`.
    ///
    /// A same-path move (the OQ-D re-create) is a no-op in the index — the doc
    /// stays alive under its UUID and no broadcast is needed.
    pub(super) async fn emit_file_move(&mut self, old_path: &str, new_path: &str) {
        if old_path == new_path {
            info!("Move coalesced to a same-path no-op: {}", old_path);
            return;
        }

        let vault = self.vault.lock().await;
        if let Err(e) = vault.index().move_node(old_path, new_path) {
            error!(
                "Failed to coalesce move {} -> {}: {}",
                old_path, new_path, e
            );
            return;
        }
        if let Err(e) = vault.save_index().await {
            error!(
                "Failed to persist index after move {} -> {}: {}",
                old_path, new_path, e
            );
            return;
        }
        drop(vault);

        info!("Coalesced native move: {} -> {}", old_path, new_path);

        let has_peers = self.peer_registry.lock().await.alive_count() > 0;
        if has_peers && let Err(e) = self.vault_gossip.broadcast_change(new_path).await {
            error!("Failed to broadcast move of {}: {}", new_path, e);
        }
    }

    /// List the vault and re-home any case-only drift between disk and index
    /// (Bug 1). Folder-level renames go through `move_subtree` (one per folder),
    /// leaf-filename renames through `move_node` — both UUID-preserving. Exactly
    /// ONE `save_index` covers the whole sweep (not one per move): a 198-file
    /// folder rename is one subtree move + one save, and a crash mid-sweep leaves
    /// the index either fully pre- or fully post-save, never partial.
    ///
    /// Driven by `case_drift_sweep_tick` and also callable on demand. Idempotent:
    /// once a move re-paths the node, the next sweep finds the casing already in
    /// agreement and detects no drift.
    pub async fn sweep_case_drift(&mut self) {
        // Detect → apply → persist must run as ONE critical section under a single
        // continuous vault-lock hold: a concurrent mutation slipping in between the
        // drift snapshot and the apply could make the moves act on stale paths
        // (re-homing a node that no longer lives where the snapshot saw it). The lock
        // is released BEFORE the network broadcast — never held across an await on the
        // wire — so the gossip send below runs unlocked.
        let mut applied: Vec<String> = Vec::new();
        {
            let vault = self.vault.lock().await;
            let disk_paths = match vault.list_files().await {
                Ok(paths) => paths,
                Err(e) => {
                    error!("Case-drift sweep failed to list vault files: {}", e);
                    return;
                }
            };
            let drift = vault.index().detect_case_drift(&disk_paths);
            if drift.is_empty() {
                return;
            }

            // Apply every move, then persist ONCE.
            // Folder moves first so a re-homed subtree settles before any leaf move.
            for fm in &drift.folder_moves {
                if let Err(e) = vault.index().move_subtree(&fm.old_prefix, &fm.new_prefix) {
                    error!(
                        "Case-drift sweep: failed to re-home folder {} -> {}: {}",
                        fm.old_prefix, fm.new_prefix, e
                    );
                    continue;
                }
                info!(
                    "Case-drift sweep re-homed folder: {} -> {}",
                    fm.old_prefix, fm.new_prefix
                );
                applied.push(fm.new_prefix.clone());
            }
            for fm in &drift.file_moves {
                if let Err(e) = vault.index().move_node(&fm.old_path, &fm.new_path) {
                    error!(
                        "Case-drift sweep: failed to re-home file {} -> {}: {}",
                        fm.old_path, fm.new_path, e
                    );
                    continue;
                }
                info!(
                    "Case-drift sweep re-homed file: {} -> {}",
                    fm.old_path, fm.new_path
                );
                applied.push(fm.new_path.clone());
            }
            // ONE batched save for the whole sweep (OQ-5).
            if let Err(e) = vault.save_index().await {
                error!(
                    "Case-drift sweep: failed to persist index after re-homing: {}",
                    e
                );
                return;
            }
        }

        let has_peers = self.peer_registry.lock().await.alive_count() > 0;
        if has_peers {
            for new_path in &applied {
                if let Err(e) = self.vault_gossip.broadcast_change(new_path).await {
                    error!("Case-drift sweep: failed to broadcast {}: {}", new_path, e);
                }
            }
        }
    }

    /// Sweep the coalescer for events whose window expired with no partner and
    /// commit each to its standalone meaning. Driven by `move_sweep_tick`.
    pub(super) async fn sweep_pending_moves(&mut self) {
        let expired = self.move_coalescer.sweep();
        if expired.is_empty() {
            // The common idle tick: nothing expired, so the pending set is
            // unchanged and the journal needs no rewrite. Skipping the write here
            // keeps the sweep off the journal's hot path.
            return;
        }
        self.commit_expired(expired).await;
        // The sweep removed expired records (and `commit_expired` finalized them
        // standalone), so rewrite the journal from the now-smaller snapshot — the
        // post-commit prune for the expiry path (P4f-2).
        self.persist_pending_moves().await;
    }

    /// Graceful-shutdown drain (P4f-2c): force-expire EVERY buffered move record to
    /// its standalone meaning, then clear the journal — so a clean quit leaves nothing
    /// pending and the next boot has zero recovery work (OQ-A). Reuses the exact
    /// standalone sinks the sweep and 2b boot-finalize use (`on_file_deleted` /
    /// `on_file_modified` via `commit_expired`).
    ///
    /// Wait-free: pairing is eager, so the buffer holds only unpaired singletons at
    /// shutdown — there is no partner to wait for, so the drain is bounded by the
    /// commit work, not a timer. Layered on 2b's safety net: a crash mid-drain leaves
    /// the un-cleared journal, which 2b recovers on the next boot to the SAME end
    /// state — so 2c is a graceful-path optimization, not a data-safety requirement.
    ///
    /// The journal clear is ONE write AFTER the full drain (never incremental): a
    /// crash before it re-runs the whole idempotent pass from the intact journal.
    pub async fn drain_pending_moves(&mut self) {
        let drained = self.move_coalescer.drain_all();
        if !drained.is_empty() {
            self.commit_expired(drained).await;
        }
        // Rewrite the journal empty now that the buffer is drained, so the next boot
        // recovers nothing. `clear_pending_journal` is the 2b helper (re-exported from
        // `daemon`); it writes a well-formed empty journal via the daemon's wired fs.
        // Mirrors `persist_pending_moves`'s guard — an unwired fs degrades to "drained
        // standalone but journal not re-cleared," which 2b then handles on next boot.
        if let Some(fs) = self.fs.clone() {
            move_recovery::clear_pending_journal(fs.as_ref()).await;
        }
    }

    /// Dispatch each expired record to its standalone sink (the same handlers the
    /// immediate-dispatch path used before coalescing).
    pub(super) async fn commit_expired(&mut self, expired: Vec<Expired>) {
        for record in expired {
            match record {
                Expired::StandaloneDelete { path } => {
                    self.on_file_deleted(&path).await;
                }
                Expired::StandaloneCreate { path } => {
                    self.on_file_modified(&path).await;
                }
            }
        }
    }

    /// Rewrite the move-coalescer's crash-recovery journal
    /// (`.sync/pending-moves.json`) from the coalescer's current pending set, so a
    /// move buffered in-memory survives a daemon crash within its window (P4f-2).
    ///
    /// **Persist-after-commit:** callers invoke this AFTER a buffer mutation
    /// completes — a committed record has already left the coalescer's maps, so the
    /// snapshot omits it and the rewrite prunes it. The full-rewrite-from-snapshot
    /// (rather than an incremental shrink) makes pruning fall out for free and
    /// avoids the partial-update bug class (a stale committed record that boot would
    /// re-commit).
    ///
    /// **Durability — `atomic_write`, NOT fsync:** the temp+rename write fixes the
    /// torn-file bug (a plain `write` crashed mid-flight could leave half-written
    /// JSON the next boot mis-parses) AND survives a process crash (the kernel page
    /// cache outlives process death). A per-mutation fsync is deliberately omitted:
    /// it only buys power-loss durability, whose consequence here is benign (an
    /// in-flight move degrades to delete+create — UUID churn, NO data loss) and
    /// whose cost is an fsync per buffer mutation on every folder-move burst.
    ///
    /// No-op (with a `warn!`) if the fs handle was never wired — degrades to the
    /// crash-tail rather than silently swallowing the journal.
    pub(super) async fn persist_pending_moves(&self) {
        // Borrow discipline (NFR-1): take an owned snapshot and clone the fs handle
        // out of `&self` BEFORE the write `.await` — never hold a coalescer borrow
        // across it.
        let Some(fs) = self.fs.clone() else {
            warn!("Move-coalescer journal fs unwired; pending moves are not persisted");
            return;
        };
        let file = PendingMovesFile {
            version: PENDING_MOVES_VERSION,
            pending: self.move_coalescer.snapshot(),
        };
        let bytes = match serde_json::to_vec(&file) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize pending-moves journal: {}", e);
                return;
            }
        };
        if let Err(e) = fs.atomic_write(PENDING_MOVES_FILE, &bytes).await {
            error!("Failed to write pending-moves journal: {}", e);
        }
    }

    /// Standalone-finalize the journaled records that boot re-stitch did NOT resolve
    /// (P4f-2b-ii). Run once at boot AFTER `Vault::load_with_journal`, before the
    /// run-loop spawns.
    ///
    /// A journaled DELETE whose `old_path` STILL has a live node after load is one the
    /// re-stitch left alone — no content-matching orphan was found, so it was a real
    /// deletion, not a move. Reconcile never tombstones a live node (that is a
    /// data-loss class it refuses), so the daemon commits the deletion here via the
    /// idempotent `on_file_deleted` sink. CREATE records need no finalize: reconcile's
    /// per-file loop already minted a fresh-UUID node for any unmatched create (§1), so
    /// the daemon ignores them.
    ///
    /// Idempotent on a re-boot (crash after re-stitch, before the journal was cleared):
    /// a successful re-stitch vacated `old_path` and a prior finalize tombstoned it, so
    /// `path_has_live_node(old_path)` is `false` either way and this skips. Even a stale
    /// check is safe — `on_file_deleted` early-returns when `delete_file` reports the
    /// path was already absent. At boot no peer is alive yet (`alive_count() == 0`), so
    /// `on_file_deleted`'s broadcast is a structural no-op; the tombstone lands locally
    /// and peers learn it on their next normal sync (correct CRDT behavior — §3.3).
    pub async fn finalize_recovered_journal(&mut self, records: &[JournaledMove]) {
        for record in records {
            if record.kind != PendingKind::Delete {
                continue; // creates are reconcile's job (§1)
            }
            if self.path_has_live_node(&record.path).await {
                info!(
                    "Boot finalize: unmatched journaled delete -> tombstone {}",
                    record.path
                );
                self.on_file_deleted(&record.path).await;
            }
        }
    }

    /// Handle a file deletion — update vault state then notify peers.
    ///
    /// The sync-flag early-return was removed: `delete_file` is idempotent (returns
    /// false for an already-absent path) so we always apply the event and gate the
    /// broadcast on whether a live node was actually tombstoned.
    pub(super) async fn on_file_deleted(&mut self, path: &str) {
        info!("File deleted: {}", path);

        let vault = self.vault.lock().await;

        let deleted = match vault.delete_file(path).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to delete file {}: {}", path, e);
                return;
            }
        };

        drop(vault);

        if !deleted {
            return;
        }

        if self.peer_registry.lock().await.alive_count() > 0 {
            if let Err(e) = self.vault_gossip.broadcast_change(path).await {
                error!("Failed to broadcast deletion of {}: {}", path, e);
            } else {
                info!(
                    "Broadcast deletion of {} to {} peer(s)",
                    path,
                    self.peer_registry.lock().await.alive_count()
                );
            }
        } else {
            info!(
                "Deleted {} from registry tree (no peers to broadcast)",
                path
            );
        }
    }

    /// Handle a file modification — update vault state then notify peers.
    ///
    /// The sync-flag early-return was removed: `on_file_changed` is echo-safe
    /// (diff-and-merge returns false when content is unchanged) so we always apply
    /// the event and gate the broadcast on whether the doc actually changed.
    pub(super) async fn on_file_modified(&mut self, path: &str) {
        let vault = self.vault.lock().await;

        // Always update vault state so .loro files stay current for future sync exchanges.
        // Without this, edits made while no peers are connected would be silently lost
        // when a peer connects — prepare_request trusts .loro state, with no fallback.
        let changed = match vault.on_file_changed(path).await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to process file change for {}: {}", path, e);
                return;
            }
        };

        // Persist any new registry registration produced by on_file_changed.
        // on_file_changed is sync-agnostic (called from both the watcher and reconcile
        // paths); the caller owns the flush decision. Reconcile batches to one save;
        // watcher events each flush here so a restart doesn't lose newly-created file nodes.
        if let Err(e) = vault.save_index().await {
            error!(
                "Failed to persist registry after file change for {}: {}",
                path, e
            );
            return;
        }

        drop(vault);

        if !changed || self.peer_registry.lock().await.alive_count() == 0 {
            return;
        }

        // Notify peers via gossip; they will open a QUIC stream to pull the full update.
        if let Err(e) = self.vault_gossip.broadcast_change(path).await {
            error!("Failed to broadcast change for {}: {}", path, e);
        } else {
            info!(
                "Broadcast change for {} to {} peer(s)",
                path,
                self.peer_registry.lock().await.alive_count()
            );
        }
    }
}
