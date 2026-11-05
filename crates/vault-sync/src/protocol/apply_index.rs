//! Apply inbound Index CRDT updates: import the delta, rebuild the caches, detect
//! which paths the Index vacated on this device (deletes and moves), and clean up
//! the filesystem for them.
//!
//! Carried from the legacy sync crate's `sync_engine/registry_apply.rs`. Two structural
//! departures the UUID model forces:
//!
//! - **The twin-dedupe machinery is gone.** It guarded a path-hash collision class
//!   (two alive nodes colliding on one `simple_hash(path)` doc_id) that UUID
//!   identity dissolves — distinct documents now have distinct UUIDs, so they never
//!   shadow each other in the caches. The inline duplicate-twin data-loss tests went
//!   with it.
//! - **`.loro` cleanup splits by reason, and a move re-materializes the `.md`.** A
//!   content `.loro` is addressed by UUID (`docs/<uuid>.loro`), path-independently.
//!   So a DELETED path's `.loro` is removed (the document is gone), but a MOVED-away
//!   path keeps its `.loro` (the same document lives on at the new path under the
//!   same UUID). Crucially, because a move re-transfers ZERO content (INV-1), the
//!   moved document's `.md` never arrives over the wire — so the receiver must
//!   re-materialize the `.md` at the NEW path from its existing local `<uuid>.loro`.
//!   (The old path-keyed sync sidestepped this by re-sending the whole document under
//!   the new path; the UUID model makes the move zero-content and re-materializes
//!   locally instead.)

use crate::conflict::{NodeKind, StructuralOp, StructuralView, resolve_structure};
use crate::content_doc::{ContentDoc, warn_if_pending};
use crate::fs::FileSystem;
use crate::hash::content_summary;
use crate::index::{StructuralNode, SweptOrphan};
use crate::vault::Vault;

use loro::TreeID;
use std::collections::BTreeMap;
use std::collections::HashMap;
use tracing::{debug, error, warn};

use super::{DocId, Result};

/// Whether `from` and `to` differ ONLY by ASCII case (a pure case rename).
fn is_case_only(from: &str, to: &str) -> bool {
    from != to && from.eq_ignore_ascii_case(to)
}

/// For a case-only move, find the shallowest DIRECTORY-segment prefix whose casing
/// differs — the folder rename that subsumes this child move. Returns `None` when only
/// the leaf filename's casing differs (a leaf case rename, not a folder one).
///
/// Both paths are assumed `is_case_only` (the caller's guard) and to have equal segment
/// counts (a pure case rename never changes the path shape).
fn case_drift_prefix(from: &str, to: &str) -> Option<(String, String)> {
    let from_segs: Vec<&str> = from.split('/').collect();
    let to_segs: Vec<&str> = to.split('/').collect();
    if from_segs.len() != to_segs.len() {
        return None;
    }
    let last = from_segs.len() - 1;
    for (i, (fseg, tseg)) in from_segs.iter().zip(to_segs.iter()).enumerate() {
        if fseg == tseg {
            continue;
        }
        if i < last {
            // A directory segment changed case: the shallowest such segment names the
            // folder rename (all descendants follow via the one directory rename).
            return Some((from_segs[..=i].join("/"), to_segs[..=i].join("/")));
        }
        // Only the leaf filename changed case.
        return None;
    }
    None
}

/// A move detected during an Index import: a still-alive node that now lives at a
/// different path than before.
struct DetectedMove {
    /// The path the node moved away from. Its old `.md` is removed only if
    /// [`Self::old_path_vacated`] — when some other node now occupies it (a swap, or a
    /// conflict-cascade survivor taking the loser's old path), the `.md` is the new
    /// occupant's and must be kept.
    from: String,
    /// The path the node now lives at. Its `.md` is ALWAYS re-materialized here from the
    /// path-independent `<uuid>.loro` (the move carried zero content, so the moved `.md`
    /// arrives no other way — INV-1).
    to: String,
    /// The moved document's UUID (stable across the move) — addresses the `.loro`.
    uuid: uuid::Uuid,
    /// Whether no alive node occupies the old path now (so its `.md` is genuinely
    /// stale). `false` for a swap / cascade-survivor that re-took the old path.
    old_path_vacated: bool,
}

/// What an Index import vacated on this device, classified by reason so the caller
/// can both filter document updates and clean/re-materialize the filesystem
/// correctly.
///
/// The distinction is load-bearing under UUID keying: a DELETED document's update
/// must be dropped (it would resurrect a tombstoned file), but a MOVED document's
/// update must still APPLY — it arrives under the same UUID, now valid at the new
/// path. So the caller filters document updates by [`Self::deleted_uuids`] only,
/// never by moved ones.
#[derive(Default)]
pub(super) struct VacatedPaths {
    /// `(path, uuid)` for nodes that were tombstoned (the document is gone). Both the
    /// `.md` and the `docs/<uuid>.loro` are removed, and the UUID's document update
    /// is filtered out (resurrection guard).
    deleted: Vec<(String, uuid::Uuid)>,
    /// Moves: a still-alive node relocated. The old `.md` is removed and the new `.md`
    /// is re-materialized from the unchanged `<uuid>.loro`; the document's update is
    /// NOT filtered (it would apply cleanly at the new path).
    moves: Vec<DetectedMove>,
}

impl VacatedPaths {
    /// The UUIDs of deleted documents, for filtering out document updates that would
    /// otherwise resurrect a tombstoned file. Moved documents are deliberately
    /// excluded — their updates apply at the new path.
    pub(super) fn deleted_uuids(&self) -> impl Iterator<Item = super::DocId> + '_ {
        self.deleted.iter().map(|(_, u)| super::DocId(*u))
    }
}

impl<F: FileSystem> Vault<F> {
    /// Apply Index updates from a sync response.
    ///
    /// Errors here `?`-propagate (whole-batch-fatal): an Index-delta corruption means
    /// we can't trust any of the batch, whereas a single corrupt document is
    /// per-item-recoverable (see `apply_doc_updates`).
    ///
    /// Imports the Index CRDT delta, rebuilds the caches, then cleans up the
    /// filesystem for paths the Index vacated on this device:
    ///
    /// - **Deletes** — a node whose path is now tombstoned. Detected from the
    ///   pre-rebuild cache (Loro doesn't expose a deleted node's parent, so its path
    ///   isn't walkable after rebuild). The `.md` and `docs/<uuid>.loro` are removed.
    /// - **Moves** — a node (same `TreeID`) that now lives at a different path than
    ///   before this import (a `tree.mov` on the sender, including a conflict-cascade
    ///   rename). The moved `.md` is ALWAYS re-materialized at the new path (zero-content
    ///   move — INV-1), and the old `.md` is removed only if no alive node re-took the
    ///   old path. When some node DID re-take it — a swap (A leaves P1 while B moves into
    ///   P1), or a cascade survivor keeping the loser's old path — the old `.md` is the
    ///   new occupant's and is kept (deleting it would be data loss). This is the B1
    ///   guard, applied per-path in `cleanup_vacated_paths` rather than by suppressing
    ///   the whole move (suppressing it would strand a zero-content move's new-path
    ///   `.md`, which has no document update to backfill it).
    ///
    /// Returns the vacated-path classification so the caller can strip those paths
    /// from subsequent document updates (which would otherwise re-create the file on
    /// disk under the old path).
    pub(super) async fn apply_index_updates(&self, data: &[u8]) -> Result<VacatedPaths> {
        debug!("apply_index_updates: data_len={}", data.len());

        // Snapshot the pre-import path→node mapping so we can detect moves after the
        // cache is rebuilt. `TreeID` is `Copy`; cloning copies the map out of the
        // guard, a temporary dropped before `rebuild_caches` re-acquires the lock.
        let pre_import_paths: HashMap<String, TreeID> = self.index().path_to_node().clone();
        // Capture each pre-import path's document UUID too, so a vacated path's
        // content `.loro` can be located by UUID for cleanup (after rebuild a deleted
        // node is tombstoned and a moved node lives elsewhere, so neither path
        // resolves to its UUID through the rebuilt cache).
        let pre_import_uuids: HashMap<String, uuid::Uuid> = pre_import_paths
            .iter()
            .filter_map(|(path, id)| self.index().node_uuid(id).map(|u| (path.clone(), u)))
            .collect();

        // Import the Index delta (or snapshot — Loro merges either).
        let status = self.index().import_updates(data)?;

        // Surface an Index delta that arrived with unsatisfied causal deps (e.g. a
        // node-create op buffered pending because an ancestor folder-create is
        // missing). Logging only — the buffered ops apply when their deps land via a
        // later exchange, and boot reconciliation backstops anything still stranded.
        warn_if_pending(&status, "index");

        // Collect deleted paths from the cache BEFORE rebuilding it. After import the
        // tree marks deleted nodes internally, but `get_node_path` returns None for
        // them (Loro doesn't expose a deleted node's parent), so we check each cached
        // path against the tree while the pre-deletion mapping is still present.
        let deleted_paths: Vec<String> = {
            let tree = self.index().index_tree();
            self.index()
                .path_to_node()
                .iter()
                .filter(|(_, node_id)| tree.is_node_deleted(node_id).unwrap_or(false))
                .map(|(path, _)| path.clone())
                .collect()
        };

        // Rebuild the caches from the updated tree. This also re-derives the
        // deleted-paths guard from Index truth (reading each deleted node's `path`
        // meta) and applies "alive wins", so a peer's legitimate re-create at a
        // previously-deleted path is not blocked.
        self.index().rebuild_caches();

        // Reactive folder-orphan rescue (EC-7/OQ-6) — BEFORE delete-detection, on
        // purpose. A peer's folder-NODE delete sweeps a concurrently-added child
        // (`Index::delete_folder`): the child reads tombstoned yet its OWN parent is
        // still the (dead) folder node, never `Deleted`. Running the rescue here revives
        // each such orphan to a LIVE path, so it is alive again BEFORE the delete-
        // detection below classifies vacated paths. That ordering is load-bearing for
        // INV-3: a still-swept orphan's path would otherwise enter `vacated.deleted`,
        // which both removes its `docs/<uuid>.loro` (`cleanup_vacated_paths`) AND filters
        // its content update out of the document batch (`deleted_uuids` in
        // `apply_response_updates`) — destroying the concurrent add's content before any
        // post-merge pass could recover it. Reviving first keeps the node alive at its
        // path so neither happens, and its content materializes normally (it is on disk
        // already, or arrives in this message's document updates for the now-live node).
        // The cascade (folder-merge) that collapses two replicas' independently-revived
        // folder nodes still runs later (`resolve_structural_conflicts`, after content).
        self.rescue_swept_orphans().await?;

        // NOTE: the structural-conflict cascade (INV-5.0/5.3) does NOT fire here.
        // It runs in `resolve_structural_conflicts`, called AFTER `apply_doc_updates`
        // (from `apply_response_updates` on the bulk path, and from the `DocUpdate` arm
        // on the live-push path) — because a colliding peer's CONTENT lands in the same
        // message's document updates, which apply AFTER this Index-apply (INV-8
        // registry-before-documents). Firing the cascade HERE is useless: at this point
        // `apply_doc_updates` has not yet run, so the colliding node's content is still
        // absent, the DP-6 defensive omit drops it from the view, and the pass produces
        // no ops. And the cascade is not invoked a second time within this same
        // `process_message`, so a here-fire would simply be a wasted no-op — the real
        // resolution always happens at the downstream call once the merged Index AND the
        // just-arrived content are both present. The move/delete detection below is
        // unaffected: it consumes the pre-import snapshots captured above, all of which
        // predate the cascade.

        // Alive-wins for the captured deleted set: `deleted_paths` was built from the
        // PRE-import cache, so a path some alive node still occupies after rebuild
        // must be dropped here — otherwise the fs cleanup below would physically
        // delete a live file (the re-create-after-delete / swap case). Pair each
        // surviving deleted path with the UUID it held pre-import (the tombstoned
        // node's UUID is no longer cache-resolvable).
        //
        // This `!rebuilt.contains_key` filter has a SECOND role that depends on the
        // `rescue_swept_orphans` call ABOVE running first: a just-revived swept orphan is
        // ALIVE at its original path after the rescue's `rebuild_caches`, so it is excluded
        // from `vacated.deleted` here. Were the rescue ordered AFTER this, the orphan would
        // still read swept (tombstoned) and its path would enter `deleted` — removing its
        // `docs/<uuid>.loro` and (via `deleted_uuids` in `apply_response_updates`) filtering
        // its content update out of the batch, destroying the concurrent add (INV-3). The
        // rescue-before-delete-detection ordering is what makes this filter cover that case.
        let deleted: Vec<(String, uuid::Uuid)> = {
            let rebuilt = self.index().path_to_node();
            deleted_paths
                .into_iter()
                .filter(|p| !rebuilt.contains_key(p.as_str()))
                .filter_map(|p| pre_import_uuids.get(&p).map(|u| (p, *u)))
                .collect()
        };

        // Detect moves: an old path whose node (same `TreeID`) now lives at a
        // different path. Orthogonal to the deleted set (which keys on tombstoned
        // nodes); a moved node stays alive, just under a new path. Capture the new
        // path and the (stable) UUID so the `.md` can be re-materialized there.
        //
        // A move's `.md` re-materialization at the NEW path always runs — that is the
        // defining behavior of a zero-content move (INV-1): the moved `.md` arrives no
        // other way. This matters for a conflict-cascade rename, which IS a zero-content
        // `tree.mov`: when one replica resolves a collision and its rename op
        // propagates here, the moved (renamed) node's conflict `.md` must materialize on
        // this replica, and there is no document update to backfill it. The OLD-path
        // cleanup, by contrast, is conditional (handled in `cleanup_vacated_paths`):
        // when the old path is re-occupied — the swap case, or a cascade survivor taking
        // the loser's old path — the old `.md` is the new occupant's and must NOT be
        // removed. So we detect the move unconditionally and let the cleanup decide
        // whether the old `.md` is stale. (`captured_at` records whether the old path is
        // still occupied so the cleanup needn't re-check the cache.)
        let mut moves: Vec<DetectedMove> = Vec::new();
        {
            let rebuilt = self.index().path_to_node();
            let node_to_new_path: HashMap<TreeID, &String> =
                rebuilt.iter().map(|(p, id)| (*id, p)).collect();

            for (old_path, node_id) in &pre_import_paths {
                if let Some(new_path) = node_to_new_path.get(node_id)
                    && *new_path != old_path
                    && let Some(uuid) = pre_import_uuids.get(old_path)
                {
                    moves.push(DetectedMove {
                        from: old_path.clone(),
                        to: (*new_path).clone(),
                        uuid: *uuid,
                        // The old path is "vacated" (its `.md` is stale and removable)
                        // only if no alive node occupies it now — the swap / cascade-
                        // survivor case keeps it.
                        old_path_vacated: !rebuilt.contains_key(old_path),
                    });
                }
            }
        }

        let vacated = VacatedPaths { deleted, moves };

        // Clean up the filesystem for vacated paths: deletes remove `.md` + `.loro`;
        // a move removes the old `.md` and re-materializes the new `.md` from the
        // unchanged `<uuid>.loro` (the move carried zero content — INV-1).
        self.cleanup_vacated_paths(&vacated).await?;

        // Persist the merged Index, then mark it synced so it reconciles before the
        // next sync import.
        self.index().save_index(self.fs()).await?;
        self.index().sync_state.mark_index_synced();

        // Materialize the folder set: a synced empty folder appears as a real directory
        // and a tombstoned one's empty directory is removed (INV-1.5a). Folders are
        // invisible to the file-level cleanup above (which keys on `.md` files), so this
        // is the only place an inbound apply reflects folder-node creates/deletes on disk.
        self.materialize_folders().await?;

        debug!(
            "apply_index_updates: complete, deleted={:?}, moves={}",
            vacated.deleted,
            vacated.moves.len()
        );
        Ok(vacated)
    }

    /// Fire the structural-conflict cascade ONCE over the fully-merged state
    /// (INV-5.0/5.3), resolving every distinct-UUID same-path collision into a
    /// deterministic, replica-identical plan and applying it.
    ///
    /// ## Why this fires AFTER document updates, not at the Index-apply
    ///
    /// A colliding peer's NODE arrives in the Index delta but its CONTENT arrives in
    /// the SAME message's document updates, which apply AFTER `apply_index_updates`
    /// (INV-8 registry-before-documents). The cascade needs both contents present to
    /// decide identical-vs-empty-vs-distinct, so it fires only after `apply_doc_updates`
    /// — from `apply_response_updates` on the bulk SyncResponse/SyncExchange path, and
    /// from the `DocUpdate` arm on the live-push path (a push can complete a collision
    /// whose other content was Flow-2-gated). Firing it inside the Index-apply instead
    /// would be useless: at that point `apply_doc_updates` has not run, so the colliding
    /// node's content is absent, the DP-6 defensive omit drops it from the view, and the
    /// pass produces no ops — and the cascade is not invoked again from there within the
    /// same `process_message`, so it would be a wasted no-op rather than a resolution.
    ///
    /// ## Cheap when there is no collision
    ///
    /// A collision is a path with ≥2 nodes. A cheap tree scan (no content load) finds
    /// them; with none, this returns immediately — the common no-conflict sync pays
    /// only one tree walk, never a content load. Only when a collision exists is the
    /// full content-bearing view built and the resolver run.
    ///
    /// ## Persistence + DP-5
    ///
    /// `apply_index_updates` already persisted and marked the Index synced before
    /// returning, so any structural ops this produces are re-persisted and re-marked
    /// here. The cascade's own fs cleanup (collapsed/renamed losers) is intentionally
    /// SEPARATE from `cleanup_vacated_paths` (which keys on the import's deletes/moves)
    /// — see [`Self::apply_structural_ops`].
    ///
    /// Returns the index-layer `Result` (not the protocol `Result`) so it is callable
    /// both from the protocol apply path (where `?` widens `IndexError` into `SyncError`)
    /// AND from boot reconcile, whose `Result` is `IndexError` — mirroring the sibling
    /// [`Self::rescue_swept_orphans`]. Boot reconcile needs it because a swept-orphan
    /// rescue mints a fresh folder node that can collide with an existing folder node at
    /// the same path; only this cascade collapses that two-folder collision. Every error
    /// it produces — tree mutation, fs read/write, content-doc decode — is already an
    /// `IndexError` variant (or converts via `#[from]`).
    pub(crate) async fn resolve_structural_conflicts(&self) -> crate::index::Result<()> {
        // Cheap collision gate: group alive nodes by path; a path with ≥2 nodes is a
        // collision. No content is loaded for this check.
        let nodes = self.index().scan_structural_nodes();
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for node in &nodes {
            *counts.entry(node.path()).or_insert(0) += 1;
        }
        if !counts.values().any(|&c| c >= 2) {
            return Ok(());
        }

        // A collision exists — build the FULL content-bearing view (the resolver's
        // fixpoint can rename a loser onto a pre-existing single-occupant path, so the
        // view must carry every occupied path, not just the ≥2-node ones).
        let view = self.build_structural_view(nodes).await;
        let ops = resolve_structure(view);
        if ops.is_empty() {
            return Ok(());
        }

        // `apply_structural_ops` already rebuilt the caches after its tree ops; persist
        // the structural ops and re-mark the Index synced (the apply tail in
        // `apply_index_updates` ran before this).
        self.apply_structural_ops(ops).await?;
        self.index().save_index(self.fs()).await?;
        self.index().sync_state.mark_index_synced();
        Ok(())
    }

    /// Recover concurrent adds a peer's folder-NODE delete SWEPT (EC-7/OQ-6) — the
    /// reactive folder-orphan rescue, a SEPARATE pass from the conflict cascade.
    ///
    /// Unlike the cascade (which resolves same-path collisions among ALIVE nodes from a
    /// path-keyed `StructuralView`), this reads loro's deleted-node enumeration and
    /// classifies each dead node by its OWN parent pointer — a pure merged-tree-state
    /// signal, PROVEN deterministic and depth-independent. A dead file whose parent is
    /// still a (dead) folder node was only swept by an ancestor folder's deletion, never
    /// itself deleted (a genuine per-file delete reads `parent == Deleted`); it is a
    /// concurrent add the user never removed, so EC-7 requires recovering it rather than
    /// letting the folder delete silently subsume it.
    ///
    /// Each orphan is re-homed to its ORIGINAL path by reviving the folder chain to LIVE
    /// nodes and `mov`-ing it under the live parent (see [`Index::rescue_orphan`]).
    /// Re-homing un-deletes the node; its content `.md` is then re-materialized from its
    /// path-independent `docs/<uuid>.loro` (present on disk already, or — for a brand-new
    /// add whose body is still in flight — backfilled when the content update for the
    /// now-live node arrives in this message's document batch, or by boot reconcile).
    ///
    /// **Where it runs.** Called from `apply_index_updates` BEFORE delete-detection
    /// (reviving the orphan keeps its path out of the vacated set, so its `.loro` is not
    /// deleted and its content update is not filtered) and from boot reconcile (a swept
    /// orphan that persisted across a restart is recovered on load). When two replicas
    /// each revive independently they mint distinct live folder nodes at one path; the
    /// resulting folder collision is collapsed by `resolve_structural_conflicts` (which
    /// runs later in the same `process_message`).
    ///
    /// Cheap when there is nothing to rescue — a single dead-node scan that returns early
    /// when no swept orphan exists, and persists its own mutations only when it acted.
    ///
    /// Returns the index-layer `Result` (not the protocol `Result`) so it is callable
    /// both from the protocol apply path (where `?` widens `IndexError` into `SyncError`)
    /// AND from boot reconcile, whose `Result` is `IndexError`. Every error it produces —
    /// tree mutation, fs read, content-doc decode — is already an `IndexError` variant.
    pub(crate) async fn rescue_swept_orphans(&self) -> crate::index::Result<()> {
        let orphans = self.index().swept_orphan_files();
        if orphans.is_empty() {
            return Ok(());
        }

        debug!(
            "rescue_swept_orphans: recovering {} orphan(s)",
            orphans.len()
        );

        // Revive each orphan in the tree (folder chain to live + re-home). Collect the
        // (path, uuid) of each so the `.md` can be re-materialized after the caches are
        // rebuilt to reflect the revived nodes.
        let mut rescued: Vec<SweptOrphan> = Vec::new();
        for orphan in orphans {
            match self.index().rescue_orphan(&orphan) {
                Ok(()) => rescued.push(orphan),
                Err(e) => warn!(
                    "rescue_swept_orphans: failed to rescue {} ({}): {} — left for reconcile",
                    orphan.uuid, orphan.original_path, e
                ),
            }
        }
        if rescued.is_empty() {
            return Ok(());
        }

        // Rebuild caches so the revived nodes are live in `path_to_node` and their paths
        // are cleared from the deleted-paths guard (alive-wins), then materialize the
        // revived folder directories and re-render each rescued `.md` from its `.loro`.
        self.index().rebuild_caches();
        self.materialize_folders().await?;
        for orphan in &rescued {
            self.rematerialize_rescued_md(orphan).await?;
        }

        // Persist the revived tree + re-mark synced (this pass runs before the
        // `apply_index_updates` save tail, but a second save keeps the on-disk Index
        // consistent if a later step in this apply path early-returns).
        self.index().save_index(self.fs()).await?;
        self.index().sync_state.mark_index_synced();
        Ok(())
    }

    /// Re-render a rescued orphan's `.md` at its original path from its path-independent
    /// `docs/<uuid>.loro` — the content the revived node already owns.
    ///
    /// The `.loro` is preserved through the rescue (the orphan was revived BEFORE the
    /// vacated-path cleanup that would have removed it), so it is normally on disk. If it
    /// is genuinely absent — a brand-new concurrent add whose body has not yet landed on
    /// this replica — the `.md` is left for the content update (the node is now live, so
    /// the update materializes it) or boot reconcile to backfill; this is warned, not
    /// failed, mirroring the cascade's `rematerialize_owner_md`.
    async fn rematerialize_rescued_md(&self, orphan: &SweptOrphan) -> crate::index::Result<()> {
        let path = &orphan.original_path;
        let loro_path = Self::doc_content_path(&DocId(orphan.uuid));
        if !self.fs().exists(&loro_path).await.unwrap_or(false) {
            warn!(
                "rematerialize_rescued_md: no local .loro for rescued {} — .md at {} absent \
                 until the content update or reconcile backstops it",
                orphan.uuid, path
            );
            return Ok(());
        }

        let bytes = self.fs().read(&loro_path).await?;
        let doc = ContentDoc::from_bytes(&bytes, self.loro_author())?;
        self.mark_synced(path);
        self.fs().write(path, doc.to_markdown().as_bytes()).await?;
        self.documents_mut().insert(path.to_string(), doc);
        debug!(
            "rematerialize_rescued_md: rendered rescued {} at {}",
            orphan.uuid, path
        );
        Ok(())
    }

    /// Build the [`StructuralView`] snapshot the conflict cascade resolves from the
    /// pre-scanned merged-tree `nodes` (NOT the rebuilt caches).
    ///
    /// Why the tree, not the caches: after `rebuild_caches`, a collision's *loser* is
    /// alive in the tree but has no `path_to_node` slot (the cache keys uniquely per
    /// path), and folders are never cached at all. The scan
    /// ([`Index::scan_structural_nodes`]) surfaces EVERY node at a contested path.
    ///
    /// Each file node is enriched with its [`content_summary`] (an async content-doc
    /// load), so the cascade can decide identical-vs-empty-vs-distinct.
    ///
    /// **Defensive content-load (DP-6 / NFR-6):** if a colliding file's
    /// `docs/<uuid>.loro` is not yet on disk — its content is Flow-2-gated and hasn't
    /// landed — the member is `warn!`'d and OMITTED from the view rather than
    /// panicking. The collision re-resolves on the next exchange once the content
    /// arrives: this can never lose a body, because the next sync re-fires the cascade
    /// with the content present.
    async fn build_structural_view(&self, nodes: Vec<StructuralNode>) -> StructuralView {
        let mut occupants: BTreeMap<String, Vec<NodeKind>> = BTreeMap::new();

        for node in nodes {
            match node {
                StructuralNode::Folder { path, tree_id } => {
                    occupants
                        .entry(path)
                        .or_default()
                        .push(NodeKind::Folder { tree_id });
                }
                StructuralNode::File { path, uuid } => {
                    let loro_path = Self::doc_content_path(&DocId(uuid));
                    // Content not yet on disk → omit the member (DP-6). It is NOT a
                    // collision we can resolve correctly without the body, and the
                    // next exchange re-fires the cascade once the content lands.
                    if !self.fs().exists(&loro_path).await.unwrap_or(false) {
                        warn!(
                            "build_structural_view: no local .loro for {} at {} — omitting from \
                             the cascade view; it re-resolves once the content lands",
                            uuid, path
                        );
                        continue;
                    }
                    let bytes = match self.fs().read(&loro_path).await {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            warn!(
                                "build_structural_view: failed to read .loro for {} at {}: {} — \
                                 omitting from the cascade view",
                                uuid, path, e
                            );
                            continue;
                        }
                    };
                    let doc = match ContentDoc::from_bytes(&bytes, self.loro_author()) {
                        Ok(doc) => doc,
                        Err(e) => {
                            warn!(
                                "build_structural_view: corrupt .loro for {} at {}: {} — \
                                 omitting from the cascade view",
                                uuid, path, e
                            );
                            continue;
                        }
                    };
                    occupants.entry(path).or_default().push(NodeKind::File {
                        uuid,
                        summary: content_summary(&doc),
                    });
                }
            }
        }

        StructuralView { occupants }
    }

    /// Replay a resolved structural plan against the loro tree + filesystem.
    ///
    /// Three phases keep the materialization correct when several nodes share a path
    /// (which is exactly when the resolver runs):
    ///
    /// 0. **Shape ops (`MergeFolder`).** Settle the tree's folder shape FIRST, because a
    ///    folder merge unions two folders' children onto shared child paths and so
    ///    CREATES the same-name file collisions the cascade then resolves — the cascade
    ///    must run against the post-merge tree (the shape-before-content order, INV-5.3).
    ///    Keyed by `TreeID` ([`Index::merge_folder_into`]): re-parent the loser's alive
    ///    children under the survivor, tombstone the loser. Because both folders sit at
    ///    one display path, the merge moves no `.md` (every child keeps its display
    ///    path), so it contributes no touched path of its own — only the file collisions
    ///    it surfaces do. Caches are rebuilt after, so the cascade's lookups see the
    ///    merged tree.
    /// 1. **File ops (`CollapseFile`, `RenameFile`, `RelocateFile`).** Tombstone each
    ///    collapsed loser by its `TreeID` (NOT by path — the survivor shares the path, so
    ///    a path-keyed delete could remove the wrong node) and remove its `.loro`; then
    ///    MOVE each file with a target (a conflict rename OR a file-vs-folder relocate —
    ///    both are a pure file move keyed by UUID) to its final path. Collapses run
    ///    before moves so a move onto a just-collapsed loser's old path can't hit
    ///    `MoveTargetExists`. Moves apply in DEPENDENCY order (a target may be another
    ///    file's current path — the transitive case).
    /// 2. **Re-materialize.** After rebuilding caches, re-render the `.md` for every
    ///    touched path (each survivor/relocation/conflict path) from the `.loro` the
    ///    merged tree now says owns it. Load-bearing: on disk a colliding path's `.md`
    ///    was last written by whichever doc update landed last, so it may hold the wrong
    ///    body; re-materializing from the resolved owner puts the right body at each
    ///    path. A path the resolver fully vacates (e.g. the old file path in a
    ///    file-vs-folder relocate, now owned by the folder) has no file owner, so its
    ///    stale `.md` is removed instead — which is also what makes a file-and-folder
    ///    collision materializable (a real filesystem can't hold both at one path).
    ///
    /// ## Distinct from the existing vacated-path cleanup (DP-5)
    ///
    /// This cleans the resolver's OWN losers — it is intentionally SEPARATE from
    /// `cleanup_vacated_paths`, which keys on the IMPORT's deletes/moves
    /// (`deleted_paths`/`pre_import_paths`, captured before the resolver fired, and
    /// already consumed by the time it runs after `apply_doc_updates`). A `CollapseFile`
    /// is not in `deleted_paths`, and a `RenameFile`/`RelocateFile` of a pre-existing
    /// node is not in the import's move set either — the import's move/delete detection
    /// ran against snapshots captured before the resolver, so it never sees these tree
    /// ops at all. (Even if it did, the import's move path no longer suppresses a whole
    /// move: a move is detected unconditionally and its per-path `old_path_vacated`
    /// flag — `false` here, since the survivor occupies the loser's old path — only
    /// governs whether `cleanup_vacated_paths` deletes the OLD `.md`.) Keeping the two
    /// cleanups distinct is what preserves that interplay.
    ///
    /// **S1 (fail loud):** a `MoveTargetExists` on a move means the resolver's fixpoint
    /// produced two ops targeting one path — a real measure bug. We `error!` + return
    /// `Err` rather than `warn!`+skip, because a silently-skipped move drops the file's
    /// conflict/relocation = INV-3 content loss.
    ///
    /// Folder-orphan rescue (a concurrent add swept by a peer's folder delete) is NOT a
    /// structural op — it is a separate post-merge pass (`rescue_swept_orphans`) that
    /// reads loro's deleted-node enumeration, not the resolver's path-keyed plan.
    ///
    /// Returns the index-layer `Result` so the whole cascade chain is `IndexError`-typed
    /// (see [`Self::resolve_structural_conflicts`]); every error it raises is a tree
    /// mutation, an fs op, or a content-doc decode — all `IndexError` variants.
    async fn apply_structural_ops(&self, ops: Vec<StructuralOp>) -> crate::index::Result<()> {
        // Partition the plan. Folder merges (shape) apply first; collapses next; file
        // moves (conflict renames AND file-vs-folder relocates — both a pure file move
        // keyed by UUID) last, in dependency order. The resolver's fixpoint can emit
        // SEVERAL move ops for one file (it relocates it step by step in its working view
        // when a target is itself occupied — the transitive case), so collapse them to
        // each file's FINAL target (the last emitted `to` wins), keeping emission order
        // for the dependency ordering below.
        let mut merges: Vec<(loro::TreeID, loro::TreeID)> = Vec::new();
        let mut collapses: Vec<uuid::Uuid> = Vec::new();
        let mut final_target: std::collections::HashMap<uuid::Uuid, String> =
            std::collections::HashMap::new();
        let mut move_order: Vec<uuid::Uuid> = Vec::new();
        for op in ops {
            match op {
                StructuralOp::MergeFolder { survivor, loser } => merges.push((survivor, loser)),
                StructuralOp::CollapseFile { loser } => collapses.push(loser),
                StructuralOp::RenameFile { loser, to } => {
                    if final_target.insert(loser, to).is_none() {
                        move_order.push(loser);
                    }
                }
                StructuralOp::RelocateFile { uuid, to } => {
                    if final_target.insert(uuid, to).is_none() {
                        move_order.push(uuid);
                    }
                }
            }
        }

        // A file that is also collapsed must not be moved — the collapse (tombstone)
        // wins. This happens when a file-vs-folder relocate lands on an identical
        // occupant: the relocate moves the file in, then the cascade collapses it.
        final_target.retain(|uuid, _| !collapses.contains(uuid));
        move_order.retain(|uuid| final_target.contains_key(uuid));

        // Phase 0 — folder merges, in plan order (parents before children, the BTreeMap
        // order the resolver emits), then rebuild caches so phase 1's lookups see the
        // post-merge tree (the merge surfaced the file collisions phase 1 resolves).
        for (survivor, loser) in merges {
            self.index().merge_folder_into(survivor, loser)?;
        }
        self.index().rebuild_caches();

        // Phase 1 — file ops. Collect every path touched so phase 2 can re-materialize
        // each from its resolved owner.
        let mut touched_paths: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();

        for loser in collapses {
            if let Some(path) = self.collapse_loser_node(loser).await? {
                touched_paths.insert(path);
            }
        }

        // Apply file moves in DEPENDENCY order: a file can only move into its final path
        // once that path is free, but one file's final path may be ANOTHER file's current
        // path (the transitive case — e.g. a loser wants the conflict path an occupant
        // currently sits at, while the occupant moves further out; or a relocated file
        // and the occupant of its target). So apply any move whose target is currently
        // unoccupied, and loop until all are placed. Targets embed the file's own full
        // UUID (a conflict path) or the content-independent `<folder>/<filename>` (a
        // relocate), so they are unique and form no cycle — a pass that places nothing
        // while moves remain is a resolver bug (S1), surfaced loudly. The bound is the
        // move count (each iteration of the outer loop places ≥1).
        let mut pending: Vec<uuid::Uuid> = move_order;
        while !pending.is_empty() {
            let mut placed_any = false;
            let mut still_pending = Vec::new();
            for uuid in pending {
                let to = final_target[&uuid].clone();
                // The target is free iff no node — file OR folder — currently occupies it.
                // `node_for_path` sees only FILE nodes (the path cache excludes folders),
                // so a folder occupying `to` would be invisible to a file-only check; a
                // move would then silently land a file onto a folder path (a tree
                // inconsistency: a file and a folder at one display path). Treating a folder
                // occupant as occupied routes that case into the `!placed_any` deadlock arm
                // below — a loud S1 error rather than silent corruption. The current
                // resolver never emits such a plan (its fixpoint relocates inside an
                // occupying folder instead), so this is defense-in-depth for a future
                // resolver bug, matching the "fail loud, never silently corrupt" principle.
                let target_free = self.index().node_for_path(&to).is_none()
                    && self.index().find_folder_node(&to).is_none();
                if target_free {
                    if let Some(from) = self.move_file_node(uuid, &to)? {
                        touched_paths.insert(from);
                        touched_paths.insert(to);
                    }
                    placed_any = true;
                } else {
                    still_pending.push(uuid);
                }
            }
            if !placed_any {
                // No move could be placed yet every remaining target is occupied — a
                // cycle the unique naming should make impossible. Fail loud (S1):
                // silently skipping would drop conflict/relocation files (INV-3 loss).
                let blocked: Vec<String> = still_pending
                    .iter()
                    .map(|u| format!("{u} -> {}", final_target[u]))
                    .collect();
                error!(
                    "apply_structural_ops: deadlocked placing structural moves (every \
                     remaining target occupied) — a resolver bug: {blocked:?}"
                );
                return Err(crate::index::IndexError::MoveTargetExists(format!(
                    "structural move deadlock: {blocked:?}"
                )));
            }
            pending = still_pending;
        }

        // Re-derive the caches so the resolved tree's path→owner mapping is current,
        // then re-materialize every touched path from the node that now owns it.
        self.index().rebuild_caches();
        for path in touched_paths {
            self.rematerialize_owner_md(&path).await?;
        }

        Ok(())
    }

    /// Tombstone a collapsed loser by its `TreeID` and drop its content `.loro`,
    /// returning the path it occupied (which the survivor keeps) so the caller can
    /// re-materialize the survivor's `.md` there.
    ///
    /// The `.md` at the shared path is NOT removed and the path's cached doc is left
    /// in place — the survivor keeps both, and the collapsed loser's content is
    /// identical to, or empty against, the survivor (INV-3-safe). Only the loser's
    /// own `docs/<loser-uuid>.loro` is removed.
    async fn collapse_loser_node(&self, loser: uuid::Uuid) -> crate::index::Result<Option<String>> {
        let Some(node) = self.index().find_node_by_uuid(&loser) else {
            debug!("collapse_loser_node: no node for {loser} — already resolved");
            return Ok(None);
        };
        let Some(path) = self.index().path_for_node(&node) else {
            debug!("collapse_loser_node: no path for {loser} — already resolved");
            return Ok(None);
        };

        // Tombstone THIS node by id (the survivor shares the path; a path-keyed delete
        // could remove the survivor instead).
        self.index().delete_node_by_id(node, &path)?;

        // Remove only the loser's content `.loro`; the survivor keeps the `.md`.
        let loro_path = Self::doc_content_path(&DocId(loser));
        if self.fs().exists(&loro_path).await.unwrap_or(false)
            && let Err(e) = self.fs().delete(&loro_path).await
        {
            warn!("collapse_loser_node: failed to delete .loro {loro_path}: {e}");
        }

        debug!("collapse_loser_node: collapsed {loser} (was at {path})");
        Ok(Some(path))
    }

    /// Move a file node (by its document UUID) to a resolved target path — the shared
    /// primitive behind both a conflict-cascade `RenameFile` and a file-vs-folder
    /// `RelocateFile` (both are a pure-structural `tree.mov`, UUID + content preserved).
    /// Returns the old path so the caller can re-materialize whatever now owns it.
    ///
    /// Keyed by `TreeID` (resolved from the UUID), not by path, because a cascade rename
    /// targets a loser whose old path is SHARED with the survivor — a path-keyed move
    /// could relocate the survivor instead. No `.md` is written or removed here — phase 2
    /// re-materializes both the old path and the new path from their resolved owners. A
    /// `MoveTargetExists` fails loudly (S1).
    fn move_file_node(&self, uuid: uuid::Uuid, to: &str) -> crate::index::Result<Option<String>> {
        let Some(node) = self.index().find_node_by_uuid(&uuid) else {
            debug!("move_file_node: no node for {uuid} — already resolved");
            return Ok(None);
        };
        let Some(from) = self.index().path_for_node(&node) else {
            debug!("move_file_node: no path for {uuid} — already resolved");
            return Ok(None);
        };

        // Move THIS node by id (the survivor may share `from`; a path-keyed move could
        // relocate the survivor instead).
        if let Err(e) = self.index().move_node_by_id(node, &from, to) {
            error!(
                "move_file_node: move {from} -> {to} failed for {uuid}: {e} — the resolver \
                 plan is inconsistent (a measure bug); failing loudly rather than dropping \
                 the file (INV-3)"
            );
            return Err(e);
        }

        debug!("move_file_node: moved {uuid} ({from} -> {to})");
        Ok(Some(from.clone()))
    }

    /// Re-materialize the `.md` at `path` from the `.loro` of the node the (rebuilt)
    /// merged tree says owns it — the cascade's correctness step.
    ///
    /// A colliding path's on-disk `.md` may hold the wrong document's body after
    /// `apply_doc_updates` (both colliding docs resolved to the same path, so the last
    /// write won); this rewrites it from the resolved owner. Marks the path synced
    /// first (echo detection) and refreshes the in-memory document cache. If no node
    /// owns `path` (defensive — the cascade should always leave a survivor), the stale
    /// `.md` is removed instead of left dangling.
    async fn rematerialize_owner_md(&self, path: &str) -> crate::index::Result<()> {
        let Some(uuid) = self.uuid_for_path(path) else {
            // No owner: the path was fully vacated (not expected for the file cascade,
            // which always leaves a survivor). Remove the stale `.md` rather than leave
            // an untracked orphan.
            debug!("rematerialize_owner_md: no owner for {path} — removing stale .md");
            self.remove_md_file(path).await;
            self.documents_mut().remove(path);
            return Ok(());
        };

        let loro_path = Self::doc_content_path(&DocId(uuid));
        if !self.fs().exists(&loro_path).await.unwrap_or(false) {
            // The owner's content never landed on this replica — boot reconcile / a
            // later full sync backstops the `.md`. (Should not happen post-cascade: the
            // owner's content was present in the view that selected it.)
            warn!(
                "rematerialize_owner_md: no local .loro for owner {uuid} of {path} — \
                 .md absent until reconcile backstops it"
            );
            return Ok(());
        }

        let bytes = self.fs().read(&loro_path).await?;
        let doc = ContentDoc::from_bytes(&bytes, self.loro_author())?;
        self.mark_synced(path);
        self.fs().write(path, doc.to_markdown().as_bytes()).await?;
        self.documents_mut().insert(path.to_string(), doc);
        debug!("rematerialize_owner_md: rendered owner {uuid} at {path}");
        Ok(())
    }

    /// Filesystem cleanup for vacated paths.
    ///
    /// Deletes remove the `.md` and the `docs/<uuid>.loro` (the document is gone). A
    /// move ALWAYS re-materializes the `.md` at the NEW path from the unchanged
    /// `<uuid>.loro` (zero-content move — the new-path `.md` arrives no other way,
    /// INV-1), and removes the OLD `.md` only if the old path is genuinely vacated
    /// ([`DetectedMove::old_path_vacated`]); a swap or a conflict-cascade survivor that
    /// re-took the old path keeps it. Every fs mutation marks the path synced first
    /// (echo detection).
    async fn cleanup_vacated_paths(&self, vacated: &VacatedPaths) -> Result<()> {
        for (path, uuid) in &vacated.deleted {
            self.remove_md_file(path).await;

            // Remove the content `.loro` by the UUID this path held (the document is
            // gone). A move would NOT reach here — its `.loro` is path-independent
            // and the node still owns it at the new path.
            let loro_path = Self::doc_content_path(&DocId(*uuid));
            if self.fs().exists(&loro_path).await.unwrap_or(false)
                && let Err(e) = self.fs().delete(&loro_path).await
            {
                warn!("Failed to delete .loro file {}: {}", loro_path, e);
            }

            self.documents_mut().remove(path);
        }

        // Fix-3 Facet A: a CASE-ONLY move (`from.eq_ignore_ascii_case(to) && from != to`)
        // converges the on-disk casing via a real two-step `fs.rename` here, run purely
        // for its side-effects.
        self.converge_case_only_moves(&vacated.moves).await;

        for mv in &vacated.moves {
            // A case-only move is NEVER routed through the write+delete path below —
            // regardless of whether `converge_case_only_moves` above succeeded or its
            // `fs.rename` failed. On a case-insensitive volume (APFS) the move's old and
            // new paths share ONE physical inode and the old-cased directory still
            // exists, so `remove_md_file(from)` + `rematerialize(to)` would:
            //   - rewrite the `.md` back into the still-physical old-cased dir (APFS
            //     resolves `plans/a.md` into the existing `Plans/`), leaving the casing
            //     stuck at the old `Plans/` while the index says `plans/` — a
            //     non-converging ping-pong against the sender; and
            //   - on a device whose `.loro` is absent, delete-then-fail-rematerialize
            //     loses the file outright.
            // Skipping by `is_case_only` (not by a "handled" set) makes the skip total:
            // it never depends on the converge step having populated a set on every
            // failure branch. An unconverged-casing end state on rename failure is the
            // honest outcome — no data loss, no write+delete. It does NOT self-heal here:
            // the case-drift sweep is disk-as-truth (it emits `old=index_path →
            // new=disk_path`), so the next sweep REVERTS this move fleet-wide toward the
            // still-old-cased disk rather than re-converging to the new casing (the
            // receiver-side index-as-truth re-convergence lives in a separate ticket).
            if is_case_only(&mv.from, &mv.to) {
                continue;
            }

            // Remove the stale `.md` at the old path ONLY if no node re-took it. A swap
            // (another node moved in) or a cascade survivor (kept the loser's old path)
            // leaves the old `.md` belonging to the new occupant — removing it would
            // delete a live file.
            if mv.old_path_vacated {
                self.remove_md_file(&mv.from).await;
                self.documents_mut().remove(&mv.from);
            }

            // Re-materialize the `.md` at the new path from the unchanged `<uuid>.loro`.
            // The content never crossed the wire (zero-content move), so the receiver
            // renders it locally from the document it already holds.
            if let Err(e) = self.rematerialize_moved_md(mv).await {
                warn!(
                    "apply_index_updates: failed to re-materialize moved doc {} at {}: {}",
                    mv.uuid, mv.to, e
                );
            }
        }

        Ok(())
    }

    /// Suffix used for the recognizable intermediate of a two-step directory case
    /// rename (`Foo` → `Foo.casemv-tmp` → `foo`). A boot-time sweep
    /// ([`Vault::sweep_stray_casemv_tmp`]) detects + recovers any stray directory
    /// left by a crash between the two steps, so reconcile never ghosts on it.
    pub(crate) const CASEMV_TMP_SUFFIX: &'static str = ".casemv-tmp";

    /// Converge every CASE-ONLY move's on-disk casing via a real `fs.rename`. Run purely
    /// for its filesystem side-effects — the caller skips every case-only move from the
    /// write+delete loop by `is_case_only`, independent of what this does, so there is
    /// no return value. A non-case-only move is left untouched.
    ///
    /// Folder-segment case renames are grouped by the shallowest differing directory
    /// prefix and converged with ONE two-step directory rename per folder
    /// (`Foo` → `Foo.casemv-tmp` → `foo`) — on APFS a single-step case-only directory
    /// rename is a no-op, so the recognizable intermediate is required. A bare
    /// leaf-FILENAME case change converges with one direct file rename (APFS re-cases a
    /// file directly).
    ///
    /// For every renamed child this marks BOTH `mv.from` and `mv.to` synced BEFORE the
    /// rename (inside the apply's vault lock-hold) — the load-bearing coupling with the
    /// Bug-2 lib-side suppress: the directory rename makes the OS fire
    /// `Deleted(Foo/child.md)` watcher events, and those are recognized as echoes (and
    /// dropped) only because the mark was set first. NEVER tombstones / `delete_file`s
    /// in this path (a rename is one relocation — the {old-alive,new-tombstoned}
    /// intermediate must stay unconstructable).
    async fn converge_case_only_moves(&self, moves: &[DetectedMove]) {
        use std::collections::BTreeMap;

        // Partition the case-only moves into folder-prefix groups and leaf-only moves.
        // A folder group maps `(old_prefix, new_prefix)` → the child `from` paths under
        // it; a leaf move re-cases only its filename. `BTreeMap` keeps the order stable
        // (deterministic across replicas) and groups identical prefixes.
        let mut folder_groups: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        let mut leaf_moves: Vec<&DetectedMove> = Vec::new();

        for mv in moves {
            if !is_case_only(&mv.from, &mv.to) {
                continue;
            }
            match case_drift_prefix(&mv.from, &mv.to) {
                Some((old_prefix, new_prefix)) => {
                    folder_groups
                        .entry((old_prefix, new_prefix))
                        .or_default()
                        .push(mv.from.clone());
                }
                None => leaf_moves.push(mv),
            }
        }

        // Folder case renames: ONE two-step directory rename per renamed folder.
        for ((old_prefix, new_prefix), children) in &folder_groups {
            // Mark every child's from+to synced BEFORE the rename so the watcher-deletes
            // the directory rename generates are recognized as echoes (Bug-2 coupling).
            for from in children {
                let to = format!("{}{}", new_prefix, &from[old_prefix.len()..]);
                self.mark_synced(from);
                self.mark_synced(&to);
            }

            let tmp = format!("{}{}", old_prefix, Self::CASEMV_TMP_SUFFIX);
            if let Err(e) = self.fs().rename(old_prefix, &tmp).await {
                // The casing stays at the old prefix this round — the caller's
                // `is_case_only` skip already keeps these children off the write+delete
                // path (which would have rewritten them back into the old-cased dir and
                // re-armed the ping-pong), so the unconverged casing simply re-attempts
                // the next time a case-only move for the node is applied. No file is
                // touched here on failure, so there is nothing to recover.
                warn!(
                    "converge_case_only_moves: step 1 rename {} -> {} failed: {} \
                     (casing left unconverged; re-attempts on the next case-only move)",
                    old_prefix, tmp, e
                );
                continue;
            }
            if let Err(e) = self.fs().rename(&tmp, new_prefix).await {
                // Step 1 ran but step 2 did not — a stray `*.casemv-tmp` dir is left on
                // disk. The boot guard (`sweep_stray_casemv_tmp`) completes the rename to
                // the index-tracked casing on the next load, UNLESS the target prefix is
                // already occupied by a different real dir (then it loudly warns and
                // leaves the stray — children never lost, but auto-heal can't fire).
                error!(
                    "converge_case_only_moves: step 2 rename {} -> {} failed: {} \
                     (stray {} left for boot recovery via sweep_stray_casemv_tmp)",
                    tmp, new_prefix, e, tmp
                );
                continue;
            }

            // Re-key the in-memory document cache for each renamed child to the new path.
            for from in children {
                let to = format!("{}{}", new_prefix, &from[old_prefix.len()..]);
                let mut docs = self.documents_mut();
                if let Some(doc) = docs.remove(from) {
                    docs.insert(to.clone(), doc);
                }
                drop(docs);
            }
            debug!(
                "converge_case_only_moves: directory case-rename {} -> {} ({} children)",
                old_prefix,
                new_prefix,
                children.len()
            );
        }

        // Leaf-filename case renames: one direct file rename each.
        for mv in leaf_moves {
            self.mark_synced(&mv.from);
            self.mark_synced(&mv.to);
            if let Err(e) = self.fs().rename(&mv.from, &mv.to).await {
                // No file touched on failure — the caller's `is_case_only` skip keeps
                // this leaf off the write+delete path (which would rewrite it into the
                // old-cased name and re-arm the ping-pong). The casing stays unconverged
                // and re-attempts the next time a case-only move for the node is applied.
                warn!(
                    "converge_case_only_moves: leaf rename {} -> {} failed: {} \
                     (casing left unconverged; re-attempts on the next case-only move)",
                    mv.from, mv.to, e
                );
                continue;
            }
            let mut docs = self.documents_mut();
            if let Some(doc) = docs.remove(&mv.from) {
                docs.insert(mv.to.clone(), doc);
            }
            drop(docs);
            debug!(
                "converge_case_only_moves: leaf case-rename {} -> {}",
                mv.from, mv.to
            );
        }
    }

    /// Re-materialize a moved document's `.md` at its new path from the local
    /// `<uuid>.loro` (the path-independent content the receiver already holds).
    ///
    /// Marks the new path synced before the write (echo detection), so the local file
    /// watcher recognizes our own materialization and does not re-broadcast it.
    async fn rematerialize_moved_md(&self, mv: &DetectedMove) -> Result<()> {
        let loro_path = Self::doc_content_path(&DocId(mv.uuid));
        // The `.loro` should be present (a move keeps it); if it is genuinely absent
        // (e.g. this device never had the document materialized), there is nothing to
        // render here. The moved `.md` will be absent at the new path on this receiver
        // until boot reconcile / a later full sync backstops it — a user-visible
        // content gap, so warn rather than log quietly.
        if !self.fs().exists(&loro_path).await? {
            warn!(
                "rematerialize_moved_md: no local .loro for {} — moved .md absent at {} until reconcile backstops it",
                mv.uuid, mv.to
            );
            return Ok(());
        }

        let bytes = self.fs().read(&loro_path).await?;
        let doc = ContentDoc::from_bytes(&bytes, self.loro_author())?;
        self.mark_synced(&mv.to);
        self.fs()
            .write(&mv.to, doc.to_markdown().as_bytes())
            .await?;
        self.documents_mut().insert(mv.to.clone(), doc);
        debug!("rematerialize_moved_md: rendered {} at {}", mv.uuid, mv.to);
        Ok(())
    }

    /// Remove a `.md` file at `path`, marking it synced first (echo detection).
    async fn remove_md_file(&self, path: &str) {
        if self.fs().exists(path).await.unwrap_or(false) {
            debug!("apply_index_updates: deleting {}", path);
            self.mark_synced(path);
            if let Err(e) = self.fs().delete(path).await {
                warn!("Failed to delete {}: {}", path, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::conflict::StructuralOp;
    use crate::fs::{FileSystem, InMemoryFs};
    use crate::index::IndexError;
    use crate::vault::Vault;
    use std::sync::Arc;

    /// S1 fail-loud (defense-in-depth): a structural move whose target is occupied by
    /// a FOLDER must NOT silently land the file onto the folder path — it must hit the
    /// deadlock arm and return a loud `MoveTargetExists` error.
    ///
    /// The resolver never produces such a plan (its fixpoint relocates a file INSIDE an
    /// occupying folder rather than onto it), so this hand-crafts the plan and calls the
    /// apply path directly. It guards a future resolver bug: `node_for_path` sees only
    /// FILE nodes, so without also checking `find_folder_node`, the move-target-free test
    /// would treat a folder-occupied path as free and silently move a file onto it —
    /// leaving a file AND a folder at one display path (a tree inconsistency). The fix
    /// classifies a folder occupant as occupied → the move can't be placed → S1 error.
    ///
    /// The target is a `.md`-suffixed folder path (`Notes.md` as a folder — the real
    /// file-vs-folder shape), because every conflict rename/relocate targets a `.md`
    /// path: that is what makes the un-fixed path a SILENT `Ok` corruption (the inner
    /// `validate_sync_path` passes) rather than an incidental `InvalidPath` rejection.
    #[tokio::test]
    async fn structural_move_onto_folder_occupied_target_fails_loud() {
        let fs = Arc::new(InMemoryFs::new());
        let vault = Vault::init(Arc::clone(&fs), 1).await.unwrap();

        // A loose file we will try to (mis-)relocate, and a FOLDER occupying the move
        // target. Indexing `Notes.md/inner.md` auto-creates a `Notes.md` FOLDER node —
        // folders are absent from the path cache, which is exactly why a file-only target
        // check would miss this occupant.
        fs.write("loose.md", b"# Loose\n\nbody").await.unwrap();
        vault.on_file_changed("loose.md").await.unwrap();
        fs.write("Notes.md/inner.md", b"# Inner\n\nbody")
            .await
            .unwrap();
        vault.on_file_changed("Notes.md/inner.md").await.unwrap();

        let loose_uuid = vault
            .index()
            .node_uuid(&vault.index().node_for_path("loose.md").unwrap())
            .expect("loose.md has an indexed UUID");
        assert!(
            vault.index().node_for_path("Notes.md").is_none(),
            "the `Notes.md` folder is invisible to the FILE-only path cache — the exact gap"
        );
        assert!(
            vault.index().find_folder_node("Notes.md").is_some(),
            "but it IS a real folder node the structural check must treat as occupied"
        );

        // Hand-craft the bad plan: relocate `loose.md` onto the folder path `Notes.md`.
        let plan = vec![StructuralOp::RelocateFile {
            uuid: loose_uuid,
            to: "Notes.md".to_string(),
        }];
        let result = vault.apply_structural_ops(plan).await;

        assert!(
            matches!(result, Err(IndexError::MoveTargetExists(_))),
            "a move onto a folder-occupied target must fail loud (S1), got {result:?}"
        );
    }
}
