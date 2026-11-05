//! Tree operations on the Index: the `LoroTree` mutations that register, move,
//! and delete file nodes, the lookups that back them, and the cache rebuild and
//! path validation they rely on.
//!
//! Carried from the legacy sync crate's `vault/registry_tree.rs` as an `impl Index` block.
//! Every operation here is an in-memory CRDT mutation — there is no filesystem
//! coupling. Three deliberate departures from the port:
//! - **UUID identity:** a file node carries a `uuid` meta (the document's minted
//!   identity) instead of the old path-hash `doc_id`. The UUID is never
//!   recomputed. (The `content_version` fingerprint is NOT node meta — it lives
//!   in the Index's local-only `content_versions` table, keyed by UUID.)
//! - **Two-way caches:** the rebuild fills both `path_to_node` and the inverse
//!   `uuid_to_node` in its single walk over alive file nodes.
//! - **Pure-structural move:** `move_node` does only `tree.mov` + meta updates +
//!   cache updates. The old `rename_file`'s content-`.loro` relocation dance is
//!   gone (the content file is `docs/<uuid>.loro`, path-independent), and its
//!   fs-level recovery fallback (reading/writing `.md` when the source isn't in the
//!   tree) belongs to the public handle's flows, not the catalog.

use super::{INDEX_TREE, Index, TREE_META_NAME, TREE_META_PATH, TREE_META_TYPE, TREE_META_UUID};
use crate::index::state::{IndexError, Result};
use loro::{LoroTree, TreeID, TreeParentId};
use std::collections::HashSet;
use uuid::Uuid;

/// One alive node in the merged tree, paired with its display path — the raw input
/// the conflict resolver's view ([`crate::conflict::StructuralView`]) is built from.
///
/// Deliberately content-free: the resolver needs a file node's [`ContentSummary`]
/// (which requires an async content-doc load), but the Index is fs-agnostic, so the
/// scan returns only the node's identity and the Vault enriches files with their
/// summary in the apply path. A folder carries only its `TreeID` (folders have no
/// content-UUID — their survivor key is their tree-node identity).
///
/// [`ContentSummary`]: crate::hash::ContentSummary
#[derive(Debug, Clone)]
pub enum StructuralNode {
    /// An alive file node at `path`, identified by its content `uuid`.
    File { path: String, uuid: Uuid },
    /// An alive folder node at `path`, identified by its loro `tree_id`.
    Folder { path: String, tree_id: TreeID },
}

impl StructuralNode {
    /// The node's display path — for grouping nodes by path to detect collisions.
    pub fn path(&self) -> &str {
        match self {
            StructuralNode::File { path, .. } | StructuralNode::Folder { path, .. } => path,
        }
    }
}

/// A file node that a peer's folder delete SWEPT (tombstoned by inheriting an
/// ancestor folder's deletion) without it ever being explicitly deleted — the
/// concurrent-add the reactive rescue (`rescue_swept_orphans`, EC-7/OQ-6) recovers.
///
/// Classified PURELY from merged-tree state: a dead file node whose OWN immediate
/// `parent` is still `Node(_)` was only swept (a genuine per-file delete reads
/// `parent == Deleted`). Its `original_path` is read from the denormalized
/// `TREE_META_PATH` meta — NOT a parent-chain walk, which severs at the first
/// `Deleted` ancestor (an ancestor folder deleted alongside the sweep), so
/// `get_node_path` would drop those ancestors and under-report the path.
#[derive(Debug, Clone)]
pub struct SweptOrphan {
    /// The swept file node's tree id (used to `mov` it back under a revived parent).
    pub node: TreeID,
    /// The document UUID the node carries (its stable content identity).
    pub uuid: Uuid,
    /// The node's original display path, recovered from its `path` meta (reliable on
    /// a tombstoned node; the parent-chain walk is not when ancestors are also dead).
    pub original_path: String,
}

/// One folder node's display path and tombstone state — what the empty-folder
/// materialization pass (INV-1.5a) needs to decide mkdir vs rmdir.
///
/// A folder carries no UUID and no content, so its display path plus whether it is
/// deleted is the entire materialization-relevant state. Produced by
/// [`Index::folder_paths`].
#[derive(Debug, Clone)]
pub struct FolderEntry {
    /// The folder's vault-relative display path (e.g. `proj/sub`).
    pub path: String,
    /// Whether the folder node is tombstoned in the merged state.
    pub is_deleted: bool,
}

impl Index {
    // ========== File tree operations (LoroTree) ==========

    /// Get the file tree from the index document.
    pub(crate) fn index_tree(&self) -> LoroTree {
        self.index().get_tree(INDEX_TREE)
    }

    /// Rebuild both lookup caches from the current tree state.
    ///
    /// Call this after applying sync updates (or on load). It walks every alive
    /// file node once, filling `path_to_node` and the inverse `uuid_to_node`, and
    /// rederives the deleted-paths guard from the tombstoned nodes' `path` meta.
    pub fn rebuild_caches(&self) {
        self.path_to_node_mut().clear();
        self.uuid_to_node_mut().clear();
        let tree = self.index_tree();

        // Deleted file paths recovered from node meta. A deleted node's real path is
        // not walkable (Loro reports its parent as `Deleted`), so we read the `path`
        // meta written at registration.
        let mut deleted_paths: HashSet<String> = HashSet::new();

        for node_id in tree.nodes() {
            let is_deleted = tree.is_node_deleted(&node_id).unwrap_or(true);

            let Ok(meta) = tree.get_meta(node_id) else {
                continue;
            };

            let node_type = Self::tree_meta_string(&meta, TREE_META_TYPE);

            // Only file nodes participate in the caches and the deleted-paths guard.
            if node_type.as_deref() != Some("file") {
                continue;
            }

            if is_deleted {
                if let Some(path) = Self::tree_meta_string(&meta, TREE_META_PATH) {
                    deleted_paths.insert(path);
                }
            } else if let Some(path) = self.get_node_path(&node_id) {
                // Fill the path cache. Also fill the inverse uuid cache when the
                // node carries a parseable UUID (every node registered through
                // `register_document` does).
                if let Some(uuid) = Self::tree_meta_string(&meta, TREE_META_UUID)
                    .and_then(|s| Uuid::parse_str(&s).ok())
                {
                    self.uuid_to_node_mut().insert(uuid, node_id);
                }
                self.path_to_node_mut().insert(path, node_id);
            }
        }

        // Alive wins: a path occupied by any alive node is not deleted, regardless
        // of a re-create-after-delete that left an old deleted node carrying the
        // same `path` meta.
        for alive_path in self.path_to_node().keys() {
            deleted_paths.remove(alive_path);
        }

        self.sync_state.replace_deleted_paths(deleted_paths);

        tracing::debug!(
            "Rebuilt index caches: {} path entries, {} uuid entries",
            self.path_to_node().len(),
            self.uuid_to_node().len()
        );
    }

    /// Scan every **alive** node in the merged tree into a flat list of
    /// `(path, identity)` records — the raw input for the conflict resolver's view.
    ///
    /// Unlike the caches (which key uniquely per path, so a collision's *loser* has
    /// no slot, and which omit folders entirely), this walks `tree.nodes()` directly,
    /// so it surfaces EVERY alive node at a contested path — both files at a file
    /// collision, both folders at a folder collision, and a file-and-folder pair at a
    /// file-vs-folder collision. That completeness is exactly what the cascade needs:
    /// the rebuilt caches can't see the nodes it must resolve.
    ///
    /// A file node yields its `uuid`; a folder yields its `TreeID`. The Vault enriches
    /// each file with its [`ContentSummary`] (an async content-doc load it owns) to
    /// build the full [`StructuralView`]. Nodes whose path or identity can't be read
    /// are skipped (defensive — a malformed node can't participate in resolution).
    ///
    /// [`ContentSummary`]: crate::hash::ContentSummary
    /// [`StructuralView`]: crate::conflict::StructuralView
    pub fn scan_structural_nodes(&self) -> Vec<StructuralNode> {
        let tree = self.index_tree();
        let mut nodes = Vec::new();

        // Cross-replica determinism relies on `tree.nodes()` returning a STABLE order for
        // identical CRDT state: two replicas at the same merged tree must build the same
        // view so their `find_map`/iteration over it (and thus the resolver's plan) match.
        // That stable iteration is the CRDT property the conflict cascade's determinism
        // (INV-5.3(c)) is anchored on.
        for node_id in tree.nodes() {
            if tree.is_node_deleted(&node_id).unwrap_or(true) {
                continue;
            }
            let Ok(meta) = tree.get_meta(node_id) else {
                continue;
            };
            let Some(path) = self.get_node_path(&node_id) else {
                continue;
            };
            match Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() {
                Some("file") => {
                    if let Some(uuid) = Self::tree_meta_string(&meta, TREE_META_UUID)
                        .and_then(|s| Uuid::parse_str(&s).ok())
                    {
                        nodes.push(StructuralNode::File { path, uuid });
                    }
                }
                Some("folder") => {
                    nodes.push(StructuralNode::Folder {
                        path,
                        tree_id: node_id,
                    });
                }
                _ => {}
            }
        }

        nodes
    }

    /// Every FOLDER node's display path paired with whether it is tombstoned — the
    /// input the empty-folder materialization pass (INV-1.5a) walks.
    ///
    /// Folders are invisible to `list_files` (it yields only `.md` files) and to the
    /// caches (which hold file nodes only), so the materialization pass — which
    /// creates a real directory for every alive folder node and removes the directory
    /// of a tombstoned one — has no other way to see them. This walks `tree.nodes()`
    /// (which includes tombstoned nodes), keeps the folder nodes, and derives each
    /// one's path via `get_node_path` (whose name-chain walk is valid for a tombstoned
    /// node too — its `name` meta and parent links persist). Both an alive AND a
    /// tombstoned folder are reported; the caller's mkdir/rmdir decision turns on the
    /// `is_deleted` flag.
    ///
    /// A folder whose path can't be derived (a malformed node) is skipped — it can't
    /// be materialized without a path.
    pub fn folder_paths(&self) -> Vec<FolderEntry> {
        let tree = self.index_tree();
        let mut folders = Vec::new();

        for node_id in tree.nodes() {
            let Ok(meta) = tree.get_meta(node_id) else {
                continue;
            };
            if Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() != Some("folder") {
                continue;
            }
            let Some(path) = self.get_node_path(&node_id) else {
                continue;
            };
            let is_deleted = tree.is_node_deleted(&node_id).unwrap_or(true);
            folders.push(FolderEntry { path, is_deleted });
        }

        folders
    }

    /// Create an empty FOLDER node at `path` (and any missing parent folders),
    /// returning its `TreeID` — the public, pure-structural empty-folder create.
    ///
    /// First-class empty folders (INV-1.5a) are normally created by the daemon when a
    /// user makes an empty directory; this is the catalog-level primitive behind that
    /// (and the handle tests use it directly to stage empty-folder state without a P4
    /// daemon). It mirrors the implicit folder creation `register_document` does for a
    /// file's parents, but as a standalone operation with no file. Idempotent: an
    /// existing folder at `path` is returned unchanged. In-memory only — the caller
    /// persists via `save_index`.
    pub fn create_folder(&self, path: &str) -> Result<TreeID> {
        let mut parent = TreeParentId::Root;
        for segment in path.split('/') {
            parent = self.get_or_create_folder(parent, segment)?;
        }
        match parent {
            TreeParentId::Node(id) => Ok(id),
            // `path` is non-empty (split always yields ≥1 segment), so the loop ran at
            // least once and `parent` is a Node — Root is unreachable here.
            _ => Err(IndexError::TreeOperation(format!(
                "create_folder produced no folder node for {path}"
            ))),
        }
    }

    /// Tombstone the FOLDER node at `path` — the library counterpart to "the user
    /// removed a directory" (Model II, EC-7/OQ-6).
    ///
    /// Returns `true` if a live folder node was tombstoned, `false` if no folder node
    /// exists at `path` (an idempotent no-op). The folder analogue of
    /// [`Self::delete_node`], which is file-only: `delete_node` resolves through
    /// `path_to_node` (file nodes only), so it physically cannot tombstone a folder.
    ///
    /// **Folder-node only — the caller per-file-deletes the genuine contents FIRST, and
    /// this REFUSES a folder that still has any alive child (S2).** This `tree.delete`s
    /// ONLY the folder node, not its descendants. A whole-folder delete is driven as:
    /// `delete_node` each removed file (stamping it `parent == Deleted`, the HONOR marker
    /// the rescue pass keys on), THEN `delete_folder` the now-empty folder node. Deleting
    /// the folder while a genuine file child is still alive would SWEEP that deletion —
    /// the swept node reads `parent == Node`, the RESCUE marker, so the reactive rescue
    /// would resurrect a file the user meant to delete (silent data loss). To make that
    /// impossible, this returns `Err` if the folder has any alive child at call time,
    /// forcing the per-file-first contract loudly rather than corrupting silently. The
    /// per-file-then-folder ordering is what makes the parent-pointer signal sound.
    ///
    /// A truly concurrent add (a peer added a child this replica had NOT yet seen when the
    /// local folder delete ran) is NOT an alive child here — it merges in LATER, and the
    /// sweep happens at MERGE time (`import_updates`), where the reactive rescue
    /// (`rescue_swept_orphans`) recovers it, keyed on its surviving `parent == Node(folder)`
    /// edge. Conversely, an add that HAD already merged in before the delete is a child the
    /// user is visibly deleting too — the daemon must per-file-`delete_node` it (HONOR), so
    /// this guard refusing is correct: the rescue is for never-seen concurrent adds, not for
    /// files present at delete time.
    ///
    /// Returns `true` if a live folder node was tombstoned, `false` if no folder node
    /// exists at `path` (an idempotent no-op). The folder analogue of [`Self::delete_node`],
    /// which is file-only: `delete_node` resolves through `path_to_node` (file nodes only),
    /// so it physically cannot tombstone a folder.
    ///
    /// In-memory only — the caller persists via [`Self::save_index`]. Rebuilds the caches
    /// from the now-current tree (a swept descendant's `path` meta re-derives the
    /// deleted-paths guard correctly).
    pub fn delete_folder(&self, path: &str) -> Result<bool> {
        let Some(folder_node) = self.find_folder_node(path) else {
            tracing::debug!("delete_folder: no folder node for '{}' — no-op", path);
            return Ok(false);
        };

        let tree = self.index_tree();
        if tree.is_node_deleted(&folder_node).unwrap_or(true) {
            tracing::debug!(
                "delete_folder: folder '{}' already tombstoned — no-op",
                path
            );
            return Ok(false);
        }

        // S2 (fail-loud): refuse a folder that still has any ALIVE child. The contract is
        // per-file-`delete_node` the contents FIRST (each → `parent == Deleted`, HONORED),
        // then `delete_folder` the now-empty node. Tombstoning a folder with a live child
        // would sweep that child (it inherits the folder's tombstone) into the RESCUE
        // class (`parent == Node`), so the reactive rescue would resurrect a file the user
        // meant to delete. `tree.children` returns DIRECT children and excludes tombstoned
        // nodes in loro 1.13.1, so a non-empty result is exactly an alive direct child.
        let live_children = tree.children(folder_node).unwrap_or_default();
        if let Some(child) = live_children
            .iter()
            .find(|c| !tree.is_node_deleted(c).unwrap_or(true))
        {
            return Err(IndexError::TreeOperation(format!(
                "delete_folder: folder '{path}' still has an alive child ({child:?}) — \
                 per-file-delete the contents first (the per-file-then-folder contract); \
                 refusing to sweep it into a silent rescue-resurrection"
            )));
        }

        tree.delete(folder_node).map_err(|e| {
            IndexError::TreeOperation(format!("Failed to delete folder node: {}", e))
        })?;

        // Re-derive both caches + the deleted-paths guard from the now-current tree: the
        // folder node left the live set, and any descendant it swept must drop out of the
        // path cache (and into the deleted-paths set via its `path` meta).
        self.rebuild_caches();

        tracing::info!(
            "Deleted folder node from index: {} ({:?})",
            path,
            folder_node
        );
        Ok(true)
    }

    /// Enumerate the file nodes a peer's folder delete SWEPT — dead nodes whose own
    /// immediate parent is still a (dead) folder NODE, never explicitly deleted — the
    /// orphans the reactive rescue (EC-7/OQ-6) recovers.
    ///
    /// The classification is a PURE function of merged-tree state (PROVEN deterministic,
    /// depth-independent, order-independent — see the `loro_liveness_probe` module): a
    /// dead file node reads `parent == Deleted` iff it was an explicit `tree.delete`
    /// target (HONOR — a genuine per-file delete), and `parent == Node(_)` iff it was
    /// only swept by an ancestor folder's deletion (RESCUE — a concurrent add the user
    /// never deleted). Every replica computes the same partition over the same converged
    /// tree, so the rescue's effect is replica-identical.
    ///
    /// Reads each orphan's `original_path` from its denormalized `TREE_META_PATH` meta
    /// (set at register, kept current by `move_node`/`move_subtree`), which survives on a
    /// tombstoned node — unlike a `get_node_path` parent-walk, which stops at the first
    /// `Deleted` ancestor (a folder deleted in the same sweep) and so would under-report
    /// the path. An orphan with no parseable `uuid` or no `path` meta is skipped (it
    /// cannot be re-homed without both).
    pub fn swept_orphan_files(&self) -> Vec<SweptOrphan> {
        let tree = self.index_tree();
        let mut orphans = Vec::new();

        for node_id in tree.nodes() {
            // Only DEAD nodes are candidates — a live node is at its path, nothing to do.
            if !tree.is_node_deleted(&node_id).unwrap_or(false) {
                continue;
            }
            // RESCUE iff the node's OWN parent is still a folder node. `Deleted` =
            // explicitly deleted (HONOR); `Root`/absent = a deleted root-level file
            // (HONOR — it was explicitly deleted, never swept).
            if !matches!(tree.parent(node_id), Some(TreeParentId::Node(_))) {
                continue;
            }
            let Ok(meta) = tree.get_meta(node_id) else {
                continue;
            };
            // Only FILE nodes are rescued; a swept sub-folder is revived implicitly when
            // an orphan inside it has its parent chain re-created (`create_folder`).
            if Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() != Some("file") {
                continue;
            }
            let Some(uuid) = Self::tree_meta_string(&meta, TREE_META_UUID)
                .and_then(|s| Uuid::parse_str(&s).ok())
            else {
                continue;
            };
            let Some(original_path) = Self::tree_meta_string(&meta, TREE_META_PATH) else {
                continue;
            };
            orphans.push(SweptOrphan {
                node: node_id,
                uuid,
                original_path,
            });
        }

        orphans
    }

    /// Re-home a swept orphan back to its original path by REVIVING the folder chain to
    /// LIVE nodes and `mov`-ing the node under the live parent — the apply half of the
    /// reactive rescue.
    ///
    /// Re-homing under a still-dead ancestor does NOT un-delete the node (PROVEN — see
    /// `loro_liveness_probe`), so the parent directory is first materialized as a LIVE
    /// folder chain via [`Self::create_folder`] (which never reuses a tombstoned folder
    /// node — `tree.children` excludes tombstoned nodes, so it mints a fresh live one),
    /// then `tree.mov` re-parents the orphan under it. The `mov` un-deletes the orphan;
    /// its `path` meta is already its original path (the node itself was never moved, only
    /// swept), so no meta rewrite is needed. Caches are rebuilt by the caller after the
    /// batch (not per-orphan).
    ///
    /// When two replicas each rescue independently they mint DISTINCT live folder nodes
    /// at one path — a folder collision the structural cascade (folder-merge, INV-1.5c)
    /// collapses to a single survivor afterward, so the caller fires the cascade after
    /// the rescue batch.
    ///
    /// In-memory only — the caller persists via [`Self::save_index`] and rebuilds caches.
    pub fn rescue_orphan(&self, orphan: &SweptOrphan) -> Result<()> {
        // The orphan's original path splits into its parent directory (the folder chain
        // to revive) and its file name. A root-level orphan (no '/') has no folder chain
        // — re-home it directly under Root.
        let live_parent = match orphan.original_path.rsplit_once('/') {
            Some((parent_dir, _name)) => {
                let folder_node = self.create_folder(parent_dir)?;
                TreeParentId::Node(folder_node)
            }
            None => TreeParentId::Root,
        };

        let tree = self.index_tree();
        // `mov` un-deletes the orphan and re-parents it under the revived live folder. No
        // `path`/`name` meta rewrite follows: `orphan.original_path` IS the node's current
        // `TREE_META_PATH` by construction (`swept_orphan_files` reads `original_path` from
        // that exact meta), so re-homing under a same-named live parent leaves the path
        // unchanged. This is also why a MOVE-then-swept orphan re-homes to where it was
        // moved TO: a concurrent `move_node`/`move_subtree` already rewrote `TREE_META_PATH`
        // to the destination before the sweep, so `original_path` carries the new path.
        tree.mov(orphan.node, live_parent).map_err(|e| {
            IndexError::TreeOperation(format!(
                "Failed to re-home swept orphan {} to {}: {}",
                orphan.uuid, orphan.original_path, e
            ))
        })?;

        tracing::info!(
            "Rescued swept orphan {} back to {}",
            orphan.uuid,
            orphan.original_path
        );
        Ok(())
    }

    /// Read a string-valued tree node meta field, or `None` if absent / not a string.
    pub(crate) fn tree_meta_string(meta: &loro::LoroMap, key: &str) -> Option<String> {
        meta.get(key).and_then(|v| {
            if let loro::ValueOrContainer::Value(val) = v {
                val.as_string().map(|s| s.to_string())
            } else {
                None
            }
        })
    }

    /// Get the full path for a node by walking up the tree.
    pub(crate) fn get_node_path(&self, node_id: &TreeID) -> Option<String> {
        let tree = self.index_tree();
        let mut parts = vec![];
        let mut current = *node_id;

        loop {
            let meta = tree.get_meta(current).ok()?;
            let name = Self::tree_meta_string(&meta, TREE_META_NAME)?;
            parts.push(name);

            match tree.parent(current) {
                Some(TreeParentId::Node(parent_id)) => {
                    current = parent_id;
                }
                Some(TreeParentId::Root) | None => break,
                _ => break,
            }
        }

        parts.reverse();
        Some(parts.join("/"))
    }

    /// Find a node by path using the cache.
    fn find_node_by_path(&self, path: &str) -> Option<TreeID> {
        self.path_to_node().get(path).copied()
    }

    /// Find the node currently at `path`, if any (a forward-cache lookup).
    ///
    /// The public read for "does the index have a live document at this path?" — the
    /// resolution a filesystem watcher event needs.
    pub fn node_for_path(&self, path: &str) -> Option<TreeID> {
        self.find_node_by_path(path)
    }

    /// Find a node by document UUID using the inverse cache.
    ///
    /// The resolution an inbound wire `DocUpdate{uuid}` needs: which node (and
    /// thence which current path) does this document live at?
    pub fn find_node_by_uuid(&self, uuid: &Uuid) -> Option<TreeID> {
        self.uuid_to_node().get(uuid).copied()
    }

    /// The current vault-relative path of a node (walks the tree up to the root).
    ///
    /// Pairs with [`Self::find_node_by_uuid`] to complete the inbound-wire
    /// resolution `uuid → node → path`.
    pub fn path_for_node(&self, node_id: &TreeID) -> Option<String> {
        self.get_node_path(node_id)
    }

    /// The document UUID stored on a node, if it is a file node with a parseable
    /// `uuid` meta.
    pub fn node_uuid(&self, node_id: &TreeID) -> Option<Uuid> {
        let meta = self.index_tree().get_meta(*node_id).ok()?;
        Self::tree_meta_string(&meta, TREE_META_UUID).and_then(|s| Uuid::parse_str(&s).ok())
    }

    /// The path a *tombstoned* file node carrying `uuid` was deleted at — the
    /// orphan's last-known location, recovered for the native-move adopt (OQ-3).
    ///
    /// A move arrives at boot as a `delete(old)` + `create(new)`: the old node is
    /// tombstoned but its `path` meta still reads `old` and its `uuid` meta still
    /// reads the moved document's UUID. The live caches deliberately exclude
    /// tombstoned nodes, so this walks the tree to find the deleted node. It is the
    /// replacement for the deleted `_meta.path` in the content doc — the content doc
    /// is now location-agnostic, so the move's old path is recovered HERE (from the
    /// Index), not from the content doc.
    ///
    /// Returns `None` when no tombstoned file node carries that UUID (e.g. an
    /// orphaned content doc whose node never existed — the fs↔loro divergence case,
    /// which is healed by the divergence-adopt arm rather than the move-adopt arm).
    pub fn deleted_node_path_for_uuid(&self, uuid: &Uuid) -> Option<String> {
        let tree = self.index_tree();
        for node_id in tree.nodes() {
            // Only a tombstoned node is a candidate — a live node carrying this UUID
            // is resolvable through the caches and is not an orphan.
            if !tree.is_node_deleted(&node_id).unwrap_or(true) {
                continue;
            }
            let Ok(meta) = tree.get_meta(node_id) else {
                continue;
            };
            if Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() != Some("file") {
                continue;
            }
            let matches_uuid = Self::tree_meta_string(&meta, TREE_META_UUID)
                .and_then(|s| Uuid::parse_str(&s).ok())
                .as_ref()
                == Some(uuid);
            if matches_uuid {
                return Self::tree_meta_string(&meta, TREE_META_PATH);
            }
        }
        None
    }

    /// The denormalized `content_version` fingerprint for a node, if present.
    ///
    /// Reads the LOCAL `content_versions` table (resolving `node → uuid`), NOT the
    /// synced Index CRDT — the digest must read a per-replica-local value no peer can
    /// overwrite (see the `content_versions` field doc). A node with no parseable UUID
    /// or no table entry returns `None` (the digest's `filter_map` tolerates it),
    /// matching the prior "no meta" outcome.
    pub fn node_content_version(&self, node_id: &TreeID) -> Option<[u8; 32]> {
        let uuid = self.node_uuid(node_id)?;
        self.content_versions().get(&uuid).copied()
    }

    /// Refresh a node's `content_version` fingerprint after a local content change —
    /// the SINGLE writer of the local `content_versions` table.
    ///
    /// Every steady-state maintenance path (registration, local edit, inbound merge,
    /// boot rebuild) routes the fingerprint through here, so "maintained ONLY from
    /// local content" is a single-method invariant. Resolves `node → uuid` and writes
    /// the LOCAL table keyed by UUID — the fingerprint is per-replica-local state, NOT
    /// synced Index meta (see the `content_versions` field doc).
    ///
    /// A node with no parseable UUID (unexpected — registration writes the UUID meta
    /// first) is warned-and-skipped: the table is UUID-keyed, so there is no honest key
    /// to write under, and skipping leaves the digest's `filter_map` to tolerate the
    /// absent entry rather than poisoning the table. In-memory only — no persistence
    /// (the table is transient, rebuilt from disk on every boot).
    pub fn set_content_version(&self, node_id: &TreeID, content_version: &[u8; 32]) -> Result<()> {
        match self.node_uuid(node_id) {
            Some(uuid) => {
                self.content_versions_mut().insert(uuid, *content_version);
            }
            None => {
                tracing::warn!(
                    "set_content_version: node {:?} has no parseable UUID — skipping the \
                     local content_version table write",
                    node_id
                );
            }
        }
        Ok(())
    }

    /// Validate a sync path for security.
    fn validate_sync_path(path: &str) -> Result<()> {
        // Empty path
        if path.is_empty() {
            return Err(IndexError::InvalidPath("Empty path not allowed".into()));
        }
        // Path traversal
        if path.contains("..") {
            return Err(IndexError::InvalidPath("Path traversal not allowed".into()));
        }
        // Empty segments (a//b.md)
        if path.contains("//") {
            return Err(IndexError::InvalidPath(
                "Empty path segment not allowed".into(),
            ));
        }
        // Absolute paths (Unix)
        if path.starts_with('/') {
            return Err(IndexError::InvalidPath("Absolute path not allowed".into()));
        }
        // Absolute paths (Windows - drive letter)
        if path.len() >= 2 && path.chars().nth(1) == Some(':') {
            return Err(IndexError::InvalidPath(
                "Windows absolute path not allowed".into(),
            ));
        }
        // Backslash
        if path.contains('\\') {
            return Err(IndexError::InvalidPath(
                "Backslash in path not allowed".into(),
            ));
        }
        // Null bytes
        if path.contains('\0') {
            return Err(IndexError::InvalidPath(
                "Null byte in path not allowed".into(),
            ));
        }
        // Must be .md
        if !path.ends_with(".md") {
            return Err(IndexError::InvalidPath(
                "Only markdown files allowed".into(),
            ));
        }
        // Control characters
        if path.chars().any(|c| c.is_control()) {
            return Err(IndexError::InvalidPath(
                "Control character in path not allowed".into(),
            ));
        }
        // Path length limit (filesystem safety)
        if path.len() > 1024 {
            return Err(IndexError::InvalidPath("Path too long".into()));
        }
        Ok(())
    }

    /// Register a document in the index (creating parent folders as needed).
    /// Returns the `TreeID` of the file node.
    ///
    /// The caller supplies the document's `uuid` (its minted identity, read from
    /// the content doc's `_meta.doc_id`) and a `content_version` fingerprint of the
    /// content doc's current version vector. The Index stores both verbatim — it
    /// does not open content docs. The UUID becomes the node's permanent identity.
    ///
    /// In-memory only: the caller is responsible for persisting via `save_index`
    /// once the mutation reaches a consistent state.
    pub fn register_document(
        &self,
        path: &str,
        uuid: &Uuid,
        content_version: &[u8; 32],
    ) -> Result<TreeID> {
        Self::validate_sync_path(path)?;

        // Already registered at this path → idempotent return of the existing node.
        if let Some(existing_id) = self.find_node_by_path(path) {
            return Ok(existing_id);
        }

        let parts: Vec<&str> = path.split('/').collect();
        let (folders, file_name) = parts.split_at(parts.len() - 1);

        // Ensure parent folders exist.
        let mut parent_id = TreeParentId::Root;
        for folder_name in folders {
            parent_id = self.get_or_create_folder(parent_id, folder_name)?;
        }

        // Create the file node.
        let tree = self.index_tree();
        let node_id = tree
            .create(parent_id)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to create file node: {}", e)))?;

        let meta = tree
            .get_meta(node_id)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to get file meta: {}", e)))?;
        meta.insert(TREE_META_TYPE, "file")
            .map_err(|e| IndexError::TreeOperation(format!("Failed to set file type: {}", e)))?;
        meta.insert(TREE_META_NAME, file_name[0])
            .map_err(|e| IndexError::TreeOperation(format!("Failed to set file name: {}", e)))?;
        // The document's UUID identity — written once, never recomputed (a move
        // does not touch it). This is what keeps `docs/<uuid>.loro` stable across
        // renames.
        meta.insert(TREE_META_UUID, uuid.to_string())
            .map_err(|e| IndexError::TreeOperation(format!("Failed to set uuid: {}", e)))?;
        // Store the full path so the node's path is recoverable after deletion.
        meta.insert(TREE_META_PATH, path)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to set path meta: {}", e)))?;

        // Update both caches and the local content_version table (keyed by UUID —
        // this is the registration write of the per-replica-local fingerprint the
        // digest reads).
        self.path_to_node_mut().insert(path.to_string(), node_id);
        self.uuid_to_node_mut().insert(*uuid, node_id);
        self.content_versions_mut().insert(*uuid, *content_version);

        tracing::debug!("Registered document in index: {} ({})", path, uuid);
        Ok(node_id)
    }

    /// Delete a node from the index (a CRDT tombstone — tracked, reversible).
    ///
    /// Returns `true` if a live tree node was tombstoned, `false` if the path was
    /// already absent (an idempotent no-op). Callers can gate broadcasts on this
    /// bool. In-memory only — the caller persists via `save_index`; the content
    /// `.loro` cleanup (if any) is a flow concern, not the catalog's.
    pub fn delete_node(&self, path: &str) -> Result<bool> {
        Self::validate_sync_path(path)?;

        if let Some(node_id) = self.find_node_by_path(path) {
            let tree = self.index_tree();
            tree.delete(node_id).map_err(|e| {
                IndexError::TreeOperation(format!("Failed to delete file node: {}", e))
            })?;

            // Mark the path deleted synchronously so an inbound document update
            // arriving before the next index sync doesn't resurrect it.
            self.sync_state.mark_path_deleted(path);

            // Remove from both caches. The uuid entry is removed by node id since we
            // resolved the node from the path.
            self.path_to_node_mut().remove(path);
            self.uuid_to_node_mut().retain(|_, id| *id != node_id);

            tracing::info!("Deleted node from index: {}", path);
            Ok(true)
        } else if self.sync_state.is_path_deleted_in_index(path) {
            // A tombstone already covers this path — a redundant echo for a delete
            // that already recorded its tombstone. Nothing is lost.
            tracing::debug!(
                "delete_node: '{}' already tombstoned — redundant delete, no-op",
                path
            );
            Ok(false)
        } else {
            // Genuinely unknown path: no node at all, so no tombstone is recorded
            // and nothing propagates to peers.
            tracing::warn!(
                "delete_node: no index node for '{}' — no tombstone recorded",
                path
            );
            Ok(false)
        }
    }

    /// Tombstone a SPECIFIC node by its `TreeID` — the conflict cascade's collapse
    /// primitive.
    ///
    /// Unlike [`Self::delete_node`] (which resolves the node from `path_to_node`), this
    /// targets an exact node id, because the cascade operates precisely when TWO file
    /// nodes share one display path: there the path cache holds only ONE of them, so a
    /// path-keyed delete could tombstone the wrong one (the survivor instead of the
    /// loser). The caller resolves `uuid → TreeID` via [`Self::find_node_by_uuid`]
    /// (which keys correctly per UUID) and passes that id here.
    ///
    /// `path` is the node's current display path (the caller already resolved it),
    /// used to arm the deleted-paths guard and to patch the caches. In-memory only —
    /// the caller persists via `save_index` and typically rebuilds caches afterward.
    pub fn delete_node_by_id(&self, node_id: TreeID, path: &str) -> Result<()> {
        let tree = self.index_tree();
        tree.delete(node_id).map_err(|e| {
            IndexError::TreeOperation(format!("Failed to delete file node by id: {}", e))
        })?;

        // Arm the deleted-paths guard for this path so an in-flight document update
        // doesn't resurrect it. Note: a `rebuild_caches` with alive-wins will lift this
        // again if the SURVIVOR still occupies the same path — which is exactly right
        // (the path is not deleted, just the losing node).
        self.sync_state.mark_path_deleted(path);

        // Drop this node from both caches by id (the path cache may currently point at
        // the survivor's node, so remove by VALUE, not by the path key).
        self.path_to_node_mut().retain(|_, id| *id != node_id);
        self.uuid_to_node_mut().retain(|_, id| *id != node_id);

        tracing::info!("Deleted node from index by id: {} ({:?})", path, node_id);
        Ok(())
    }

    /// Move a SPECIFIC node by its `TreeID` to `new_path` — the conflict cascade's
    /// rename primitive.
    ///
    /// Unlike [`Self::move_node`] (which resolves the source from `path_to_node`), this
    /// targets an exact node id, because the cascade renames a loser whose OLD path is
    /// shared with the survivor — a path-keyed move could relocate the survivor
    /// instead. The caller resolves `uuid → TreeID` (the correct per-UUID lookup) and
    /// passes the id plus the node's current `old_path`.
    ///
    /// Errors with `MoveTargetExists` if `new_path` already has a node — the caller
    /// (the cascade) treats that as a resolver bug and fails loudly (S1). The node's
    /// `uuid` is untouched (identity is stable across the rename). In-memory only.
    pub fn move_node_by_id(&self, node_id: TreeID, old_path: &str, new_path: &str) -> Result<()> {
        Self::validate_sync_path(new_path)?;
        if old_path == new_path {
            return Ok(());
        }
        if self.find_node_by_path(new_path).is_some() {
            return Err(IndexError::MoveTargetExists(format!(
                "Target already exists: {}",
                new_path
            )));
        }

        let new_parts: Vec<&str> = new_path.split('/').collect();
        let (new_folders, new_name) = new_parts.split_at(new_parts.len() - 1);

        let mut new_parent = TreeParentId::Root;
        for folder_name in new_folders {
            new_parent = self.get_or_create_folder(new_parent, folder_name)?;
        }

        let tree = self.index_tree();
        tree.mov(node_id, new_parent).map_err(|e| {
            IndexError::TreeOperation(format!("Failed to move file node by id: {}", e))
        })?;

        let meta = tree
            .get_meta(node_id)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to get file meta: {}", e)))?;
        meta.insert(TREE_META_NAME, new_name[0])
            .map_err(|e| IndexError::TreeOperation(format!("Failed to update file name: {}", e)))?;
        meta.insert(TREE_META_PATH, new_path)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to update path meta: {}", e)))?;

        // Patch the path cache: drop the old key only if it points at THIS node (the
        // survivor may own it), and point the new key at this node.
        self.path_to_node_mut().retain(|_, id| *id != node_id);
        self.path_to_node_mut()
            .insert(new_path.to_string(), node_id);

        tracing::info!(
            "Moved node in index by id: {} -> {} ({:?})",
            old_path,
            new_path,
            node_id
        );
        Ok(())
    }

    /// Move/rename a node in the index — a pure-structural CRDT tree move.
    ///
    /// Does `tree.mov` + `name`/`path` meta updates + both-cache updates, and
    /// **nothing to the content `.loro`**: the content file is `docs/<uuid>.loro`,
    /// path-independent, so a move re-transfers zero content (INV-1). The node's
    /// `uuid` is NOT touched — identity is stable across a move.
    ///
    /// In-memory only — the caller persists via `save_index`. If the source path
    /// has no node, this errors; the fs-level recovery the old `rename_file` did
    /// (renaming a `.md` that exists on disk but not in the tree) is a flow concern
    /// the public handle owns, not the catalog.
    pub fn move_node(&self, old_path: &str, new_path: &str) -> Result<()> {
        Self::validate_sync_path(old_path)?;
        Self::validate_sync_path(new_path)?;

        // No-op if paths are identical.
        if old_path == new_path {
            return Ok(());
        }

        let Some(node_id) = self.find_node_by_path(old_path) else {
            return Err(IndexError::MoveSourceMissing(format!(
                "Source node not found: {}",
                old_path
            )));
        };

        // Target must not already exist.
        if self.find_node_by_path(new_path).is_some() {
            return Err(IndexError::MoveTargetExists(format!(
                "Target already exists: {}",
                new_path
            )));
        }

        let new_parts: Vec<&str> = new_path.split('/').collect();
        let (new_folders, new_name) = new_parts.split_at(new_parts.len() - 1);

        // Ensure the new parent folders exist.
        let mut new_parent = TreeParentId::Root;
        for folder_name in new_folders {
            new_parent = self.get_or_create_folder(new_parent, folder_name)?;
        }

        let tree = self.index_tree();

        // Move the node to its new parent (Loro API is `mov`).
        tree.mov(node_id, new_parent)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to move file node: {}", e)))?;

        // Update the name and path meta. The UUID and content_version are untouched
        // — a move changes location, not identity or content.
        let meta = tree
            .get_meta(node_id)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to get file meta: {}", e)))?;
        meta.insert(TREE_META_NAME, new_name[0])
            .map_err(|e| IndexError::TreeOperation(format!("Failed to update file name: {}", e)))?;
        meta.insert(TREE_META_PATH, new_path)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to update path meta: {}", e)))?;

        // Update the path cache (the uuid cache's node id is unchanged by a move).
        self.path_to_node_mut().remove(old_path);
        self.path_to_node_mut()
            .insert(new_path.to_string(), node_id);

        tracing::info!("Moved node in index: {} -> {}", old_path, new_path);
        Ok(())
    }

    /// Move/rename a whole FOLDER and everything under it — the folder-safe move
    /// primitive (INV-1.5b) that the per-file [`Self::move_node`] is not.
    ///
    /// A folder move is ONE `tree.mov` on the folder node: loro's movable tree
    /// carries every descendant structurally for free, so no content `.loro` is
    /// touched and every descendant keeps its UUID — that is what makes "a folder
    /// move re-transfers zero content" (INV-1) structurally true.
    ///
    /// The reason this can't just be `move_node` on the folder: `move_node` rewrites
    /// only the single moved node's denormalized `path`/`name` meta and patches one
    /// cache entry. After re-parenting a folder, every DESCENDANT file node's `path`
    /// meta and `path_to_node` entry is stale — its tree position moved but its
    /// stored path string didn't. That denormalized `path` meta is load-bearing: it's
    /// the only way a tombstoned node's location is recovered
    /// ([`Self::deleted_node_path_for_uuid`] + the deleted-paths re-derivation in
    /// [`Self::rebuild_caches`]). So this rewrites every descendant file node's
    /// `path` meta to its new location, then rebuilds the caches from index truth.
    ///
    /// Correctness-first: it does the single `tree.mov`, rewrites descendant `path`
    /// meta, then a full [`Self::rebuild_caches`] (already O(nodes) on every load), in
    /// that order. Folder descendants carry only `type`+`name` meta (no `path`), so
    /// they need no rewrite — the rebuild re-derives their paths from the tree.
    ///
    /// This is a clean STRUCTURAL mover: it errors with [`IndexError::MoveTargetExists`]
    /// if `new_prefix` is already occupied by a folder or a file. Collision POLICY
    /// (folder-merge, file-vs-folder) lives in the conflict resolver, not here. In-memory
    /// only — the caller persists via [`Self::save_index`].
    pub fn move_subtree(&self, old_prefix: &str, new_prefix: &str) -> Result<()> {
        // No-op if the prefixes are identical (mirrors `move_node`).
        if old_prefix == new_prefix {
            return Ok(());
        }

        // Resolve the folder node at `old_prefix`. Folders are not in `path_to_node`,
        // so descend the prefix's segments from the root, requiring each to be an
        // existing folder. A missing folder node means there is nothing to move.
        let Some(folder_node) = self.find_folder_node(old_prefix) else {
            return Err(IndexError::MoveSourceMissing(format!(
                "Source folder not found: {}",
                old_prefix
            )));
        };

        // The raw primitive refuses an occupied target — `new_prefix` already taken by
        // a folder (folder-MERGE territory) or a file (file-vs-folder). Collision
        // policy is the resolver's job (P2d); here it's a hard error. Checked BEFORE
        // creating the destination parent chain so a refused move leaves no spurious
        // folders behind.
        if self.find_folder_node(new_prefix).is_some()
            || self.find_node_by_path(new_prefix).is_some()
        {
            return Err(IndexError::MoveTargetExists(format!(
                "Target already exists: {}",
                new_prefix
            )));
        }

        // Ensure the destination PARENT chain exists (everything above the folder's
        // own new name), then re-parent the folder node there in one move.
        let new_parts: Vec<&str> = new_prefix.split('/').collect();
        let (new_parent_folders, new_name) = new_parts.split_at(new_parts.len() - 1);
        let mut new_parent = TreeParentId::Root;
        for folder_name in new_parent_folders {
            new_parent = self.get_or_create_folder(new_parent, folder_name)?;
        }

        let tree = self.index_tree();
        tree.mov(folder_node, new_parent)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to move folder node: {}", e)))?;

        // Rename the folder node itself to the new last segment. Descendants follow
        // structurally, so their `name` meta is unchanged — only the moved folder's
        // own name changes.
        let folder_meta = tree
            .get_meta(folder_node)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to get folder meta: {}", e)))?;
        folder_meta
            .insert(TREE_META_NAME, new_name[0])
            .map_err(|e| {
                IndexError::TreeOperation(format!("Failed to update folder name: {}", e))
            })?;

        // Rewrite each descendant FILE node's denormalized `path` meta to its new
        // location. The tree is already re-parented, so `get_node_path` walks up
        // through the renamed folder and yields the correct new path — the same
        // derivation `rebuild_caches` uses, keeping the meta and the tree in lock-step.
        for descendant in self.descendant_file_nodes(folder_node) {
            let Some(new_path) = self.get_node_path(&descendant) else {
                continue;
            };
            let meta = tree.get_meta(descendant).map_err(|e| {
                IndexError::TreeOperation(format!("Failed to get descendant meta: {}", e))
            })?;
            meta.insert(TREE_META_PATH, new_path.as_str())
                .map_err(|e| {
                    IndexError::TreeOperation(format!(
                        "Failed to update descendant path meta: {}",
                        e
                    ))
                })?;
        }

        // Rebuild both caches + the deleted-paths guard from the now-consistent tree.
        self.rebuild_caches();

        tracing::info!(
            "Moved folder subtree in index: {} -> {}",
            old_prefix,
            new_prefix
        );
        Ok(())
    }

    /// Merge a loser folder node into a survivor folder node — the conflict resolver's
    /// folder-merge primitive (INV-1.5c), keyed by `TreeID` not by path.
    ///
    /// The resolver emits a `MergeFolder { survivor, loser }` only for two folder nodes
    /// at the SAME display path; this re-parents every ALIVE child of the loser under
    /// the survivor, then tombstones the now-emptied loser. Because both folders sit at
    /// the same display path, re-parenting a child from loser to survivor leaves that
    /// child's display path UNCHANGED (`P/child` either way) — so, unlike
    /// [`Self::move_subtree`], no descendant `path` meta needs rewriting; the caller's
    /// [`Self::rebuild_caches`] re-derives the (unchanged) paths from the tree.
    ///
    /// **Only ALIVE children move** (INV-3 / EC-7): a tombstoned child of the loser
    /// stays tombstoned and is not resurrected. Re-parenting all alive children OUT
    /// first is required before the delete — loro's `tree.delete` makes a deleted node's
    /// remaining children "not appear in the state", so any alive child left under the
    /// loser would be lost.
    ///
    /// Keyed by `TreeID` because folder-merge runs precisely when two folder nodes share
    /// one path, where a path-keyed lookup can't tell them apart. Same-name child
    /// collisions the union surfaces (two files at `P/Notes.md`, or two sub-folders at
    /// `P/sub`) are resolved by the resolver's other cases in the same plan — they are
    /// NOT this primitive's concern. In-memory only — the caller persists via
    /// [`Self::save_index`] and rebuilds caches.
    pub fn merge_folder_into(&self, survivor: TreeID, loser: TreeID) -> Result<()> {
        let tree = self.index_tree();

        // Defensive: a loser already tombstoned (or gone) is a no-op — the resolver
        // emits one merge per loser, so this should not happen, but skipping rather than
        // erroring keeps a redundant replay safe.
        if tree.is_node_deleted(&loser).unwrap_or(true) {
            tracing::debug!("merge_folder_into: loser {:?} already gone — no-op", loser);
            return Ok(());
        }

        // Re-parent every ALIVE direct child of the loser under the survivor. A
        // tombstoned child is left in the Deleted set (INV-3) — it must NOT move.
        // `tree.children(loser)` returns DIRECT children only; grandchildren follow
        // their re-parented parent structurally (loro carries a whole subtree on a
        // single `tree.mov`), so re-parenting the direct children is sufficient — the
        // whole subtree follows without per-node moves.
        let children = tree.children(loser).unwrap_or_default();
        for child in children {
            if tree.is_node_deleted(&child).unwrap_or(true) {
                continue;
            }
            tree.mov(child, TreeParentId::Node(survivor)).map_err(|e| {
                IndexError::TreeOperation(format!("Failed to re-parent folder child: {}", e))
            })?;
        }

        // Tombstone the emptied loser folder node.
        tree.delete(loser).map_err(|e| {
            IndexError::TreeOperation(format!("Failed to delete merged folder node: {}", e))
        })?;

        tracing::info!(
            "Merged folder node into survivor: {:?} <- {:?}",
            survivor,
            loser
        );
        Ok(())
    }

    /// Resolve a FOLDER node by its full prefix path (lookup-only — never creates).
    ///
    /// Folders are absent from `path_to_node` (only file nodes are cached), so this
    /// descends the prefix's segments from the root, requiring each to be an existing
    /// folder. Returns `None` if any segment is missing or is not a folder.
    ///
    /// `pub(crate)` so the structural-apply path (`protocol::apply_index`) can ask
    /// "is this move target occupied by a FOLDER?" — `node_for_path` only sees file
    /// nodes, so the apply path pairs it with this to treat a folder occupant as
    /// occupied (S1 fail-loud), never silently move a file onto a folder path.
    pub(crate) fn find_folder_node(&self, prefix: &str) -> Option<TreeID> {
        let mut parent = TreeParentId::Root;
        let mut found = None;
        for segment in prefix.split('/') {
            let child = self.find_folder_child(&parent, segment)?;
            parent = TreeParentId::Node(child);
            found = Some(child);
        }
        found
    }

    /// Find an existing FOLDER child named `name` under `parent` (lookup-only).
    ///
    /// The non-creating half of [`Self::get_or_create_folder`], used to resolve a
    /// folder path without the create side-effect.
    fn find_folder_child(&self, parent: &TreeParentId, name: &str) -> Option<TreeID> {
        let tree = self.index_tree();
        let children = match parent {
            TreeParentId::Root => tree.roots(),
            TreeParentId::Node(parent_id) => tree.children(parent_id).unwrap_or_default(),
            _ => vec![],
        };
        for child_id in children {
            let Ok(meta) = tree.get_meta(child_id) else {
                continue;
            };
            let is_folder =
                Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() == Some("folder");
            let child_name = Self::tree_meta_string(&meta, TREE_META_NAME);
            if is_folder && child_name.as_deref() == Some(name) {
                return Some(child_id);
            }
        }
        None
    }

    /// Collect every alive FILE node beneath `folder_node` at any depth — the set
    /// whose denormalized `path` meta a subtree move must rewrite.
    ///
    /// Recurses the moved folder's `children`, descending into sub-folders and
    /// collecting file nodes. Folder nodes themselves are not collected (they carry no
    /// `path` meta to rewrite).
    fn descendant_file_nodes(&self, folder_node: TreeID) -> Vec<TreeID> {
        let tree = self.index_tree();
        let mut files = Vec::new();
        let mut stack = tree.children(folder_node).unwrap_or_default();
        while let Some(node_id) = stack.pop() {
            let Ok(meta) = tree.get_meta(node_id) else {
                continue;
            };
            match Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() {
                Some("file") => files.push(node_id),
                Some("folder") => stack.extend(tree.children(node_id).unwrap_or_default()),
                _ => {}
            }
        }
        files
    }

    /// Whether a path's node is deleted in the tree (or absent entirely).
    pub fn is_node_deleted(&self, path: &str) -> bool {
        match self.find_node_by_path(path) {
            Some(node_id) => {
                let tree = self.index_tree();
                tree.is_node_deleted(&node_id).unwrap_or(true)
            }
            None => true, // Not in tree = effectively deleted.
        }
    }

    /// Whether the deleted-paths resurrection guard currently covers `path`.
    ///
    /// The public read an inbound document-update flow consults before
    /// materializing a file: a path known-deleted in the index (set synchronously
    /// by `delete_node` and rederived on every `rebuild_caches`) must not be
    /// resurrected. Distinct from [`Self::is_node_deleted`], which queries live tree
    /// state for a path that still has a node.
    pub fn is_path_deleted(&self, path: &str) -> bool {
        self.sync_state.is_path_deleted_in_index(path)
    }

    /// Get or create a folder node under `parent` with the given `name`.
    fn get_or_create_folder(&self, parent: TreeParentId, name: &str) -> Result<TreeParentId> {
        let tree = self.index_tree();

        // Look for an existing folder with this name under the parent.
        let children = match &parent {
            TreeParentId::Root => tree.roots(),
            TreeParentId::Node(parent_id) => tree.children(parent_id).unwrap_or_default(),
            _ => vec![],
        };

        for child_id in children {
            if let Ok(meta) = tree.get_meta(child_id) {
                let is_folder =
                    Self::tree_meta_string(&meta, TREE_META_TYPE).as_deref() == Some("folder");
                let child_name = Self::tree_meta_string(&meta, TREE_META_NAME);

                // S3 (defensive): never reuse a TOMBSTONED folder node. Re-homing a rescued
                // orphan under a dead folder does NOT un-delete it (proven by the
                // `loro_liveness_probe` tests), so the rescue depends on this minting a FRESH
                // live folder when the only same-name node is tombstoned. `tree.children` /
                // `tree.roots` exclude tombstoned nodes in loro 1.13.1, so this skip is
                // belt-and-suspenders today — but making the aliveness check explicit keeps
                // the "never reuse a tombstoned folder" invariant robust to a loro change.
                let is_alive = !tree.is_node_deleted(&child_id).unwrap_or(true);

                if is_folder && is_alive && child_name.as_deref() == Some(name) {
                    return Ok(TreeParentId::Node(child_id));
                }
            }
        }

        // Create a new folder node.
        let node_id = tree.create(parent).map_err(|e| {
            IndexError::TreeOperation(format!("Failed to create folder node: {}", e))
        })?;

        let meta = tree
            .get_meta(node_id)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to get folder meta: {}", e)))?;
        meta.insert(TREE_META_TYPE, "folder")
            .map_err(|e| IndexError::TreeOperation(format!("Failed to set folder type: {}", e)))?;
        meta.insert(TREE_META_NAME, name)
            .map_err(|e| IndexError::TreeOperation(format!("Failed to set folder name: {}", e)))?;

        Ok(TreeParentId::Node(node_id))
    }
}

#[cfg(test)]
mod loro_liveness_probe {
    //! DP-3 — how does loro's movable tree treat a node when an ANCESTOR folder is
    //! deleted, and how does that interact with a peer's concurrent add under it?
    //!
    //! These probes pin the loro facts the folder-delete + reactive-rescue model (P2e,
    //! Model II) is built on. Two facts matter:
    //!
    //! 1. **Per-file delete vs sweep are distinguishable by the node's own parent edge.**
    //!    The library deletes a removed directory's contents file-by-file
    //!    ([`Index::delete_node`] → `tree.delete(file_node)`), which stamps each
    //!    explicitly-deleted file `parent == Deleted`; a child a peer concurrently added
    //!    keeps its real `parent == Node(folder)`. That parent-pointer split is the pure
    //!    merged-state signal the rescue pass classifies on (HONOR vs RESCUE).
    //! 2. **A swept child is tombstoned and re-homing it under a still-dead ancestor does
    //!    NOT un-delete it.** When a folder NODE is deleted (the Model-II whole-folder
    //!    delete) any concurrently-added child under it inherits the tombstone, so the
    //!    rescue must revive the folder chain to LIVE nodes before `mov`-ing the orphan
    //!    back — these probes are why.
    //!
    //! Kept as regressions so the rescue design's premises can't silently drift if we
    //! ever bump the loro version.

    use super::*;
    use crate::index::Index;

    /// Merge B's full state into A and A's into B (snapshot exchange — loro merges
    /// either direction), so both replicas reach the identical converged tree.
    fn converge(a: &Index, b: &Index) {
        let a_snap = a.export_snapshot().unwrap();
        let b_snap = b.export_snapshot().unwrap();
        a.import_updates(&b_snap).unwrap();
        b.import_updates(&a_snap).unwrap();
        a.rebuild_caches();
        b.rebuild_caches();
    }

    /// The denormalized fingerprint a registration needs (content is irrelevant here —
    /// the probe is about tree structure, not document content).
    const FP: [u8; 32] = [0u8; 32];

    /// Register `proj/keep.md` + `proj/gone.md` on a fresh replica, then seed an
    /// independent peer from its snapshot so both share the `proj/` folder TreeID.
    fn two_replicas_sharing_proj() -> (Index, Index, TreeID) {
        let a = Index::new(1);
        a.register_document("proj/keep.md", &Uuid::from_u128(1), &FP)
            .unwrap();
        a.register_document("proj/gone.md", &Uuid::from_u128(2), &FP)
            .unwrap();
        let b = Index::new(2);
        let seed = a.export_snapshot().unwrap();
        b.import_updates(&seed).unwrap();
        b.rebuild_caches();

        let proj_a = a.find_folder_node("proj").expect("A has proj/ folder");
        let proj_b = b.find_folder_node("proj").expect("B has proj/ folder");
        assert_eq!(
            proj_a, proj_b,
            "both replicas share the proj/ folder TreeID"
        );
        (a, b, proj_a)
    }

    /// **The decisive probe** for the realistic folder-delete shape: A deletes the
    /// folder's CHILD FILES (the way the daemon's per-file delete-detection tombstones a
    /// removed directory's contents — NOT a subtree `tree.delete` on the folder node),
    /// while B concurrently creates a live file under the SAME `proj/` folder node. After
    /// merge, does the `proj/` folder node read `is_node_deleted`, and does B's live child
    /// survive?
    ///
    /// This is the EC-7 shape the revive case must handle. The folder node here is NOT
    /// explicitly deleted by A — only its files are. Whether the folder reads deleted in
    /// the merged state, and whether revive must run, is exactly the gating question.
    #[test]
    fn folder_files_deleted_with_live_concurrent_child() {
        let (a, b, proj) = two_replicas_sharing_proj();

        // A deletes both child FILES (per-file deletes — the realistic folder-removal
        // path). It does NOT touch the folder node itself.
        a.delete_node("proj/keep.md").unwrap();
        a.delete_node("proj/gone.md").unwrap();
        a.rebuild_caches();

        // B concurrently creates a live file UNDER the shared proj/ folder.
        let new_uuid = Uuid::from_u128(99);
        let new_node = b.register_document("proj/new.md", &new_uuid, &FP).unwrap();

        converge(&a, &b);

        let tree_a = a.index_tree();
        let child_alive = !tree_a.is_node_deleted(&new_node).unwrap_or(true);
        let ancestor_deleted = tree_a.is_node_deleted(&proj).unwrap_or(true);
        let child_path = a.get_node_path(&new_node);
        println!(
            "DP-3 PROBE [files-deleted]: child_alive={child_alive}, \
             ancestor_deleted={ancestor_deleted}, child_path={child_path:?}, proj={proj:?}, \
             new_node={new_node:?}"
        );

        // The child survives (its create is concurrent with only the SIBLINGS' deletes,
        // never an ancestor subtree-delete), and the folder node was never deleted →
        // ancestor stays ALIVE. In this shape revive is moot; the live child keeps proj/
        // alive because the folder node itself is alive.
        assert!(
            child_alive,
            "the concurrently-created child survives (INV-3)"
        );
        assert!(
            !ancestor_deleted,
            "PROBE: deleting only the child files leaves the proj/ folder node ALIVE — so a \
             live child under it needs no revive (the folder is not tombstoned)"
        );
    }

    /// A explicitly deletes the `proj/` FOLDER NODE itself (a `tree.delete` on the folder
    /// — the Model-II whole-folder delete that makes the directory disappear), while B
    /// concurrently adds a live child under it. Pins the loro property the reactive rescue
    /// must compensate for: deleting the folder node tombstones the concurrently-added
    /// child too.
    ///
    /// **Outcome (pinned):** loro leaves the folder TOMBSTONED after merge AND the
    /// concurrently-created child is ALSO tombstoned (it inherits the folder's tombstone).
    /// This is precisely the orphan the reactive rescue recovers: the swept child reads
    /// `is_node_deleted` yet its OWN `parent` is still `Node(proj)` (not `Deleted`) — the
    /// RESCUE classification — so the rescue pass revives the `proj/` chain to live and
    /// `mov`s the child back. (The genuine per-file deletes, by contrast, each read
    /// `parent == Deleted` and are HONORED.) Re-homing the child under the STILL-dead
    /// folder would not un-delete it, which is why the rescue revives the chain to live
    /// first (see `empty_folder_node_deleted_after_files_subsumes_concurrent_child`).
    #[test]
    fn folder_node_subtree_delete_subsumes_concurrent_child() {
        let (a, b, proj) = two_replicas_sharing_proj();

        // A deletes the folder NODE — a subtree delete (tombstones proj/ + its files in
        // one op).
        {
            let tree = a.index_tree();
            tree.delete(proj).unwrap();
        }
        a.rebuild_caches();
        assert!(
            a.index_tree().is_node_deleted(&proj).unwrap(),
            "A tombstoned its proj/ folder node"
        );

        // B concurrently creates a live file under the shared proj/ folder.
        let new_uuid = Uuid::from_u128(99);
        let new_node = b.register_document("proj/new.md", &new_uuid, &FP).unwrap();
        assert!(
            !b.index_tree().is_node_deleted(&new_node).unwrap(),
            "B's child is alive pre-merge"
        );

        converge(&a, &b);

        let tree_a = a.index_tree();
        let child_alive = !tree_a.is_node_deleted(&new_node).unwrap_or(true);
        let ancestor_deleted = tree_a.is_node_deleted(&proj).unwrap_or(true);
        println!(
            "DP-3 PROBE [node-subtree-delete]: child_alive={child_alive}, \
             ancestor_deleted={ancestor_deleted}, proj={proj:?}, new_node={new_node:?}"
        );

        // Pinned outcomes (a loro-version bump that changes either must re-trigger this
        // design review):
        assert!(
            ancestor_deleted,
            "loro keeps the folder node tombstoned after merge — it does NOT auto-revive"
        );
        assert!(
            !child_alive,
            "deleting the folder node TOMBSTONES the concurrently-added child too — it \
             inherits the folder's tombstone. This is the orphan the reactive rescue \
             recovers: the swept child reads deleted yet its own parent is still \
             Node(proj), the RESCUE classification (a genuine per-file delete would read \
             parent == Deleted and be honored)."
        );
    }

    /// The Model-II whole-folder delete (the shape [`Index::delete_folder`] drives): A
    /// per-file-deletes the children AND THEN tombstones the now-EMPTY `proj/` folder
    /// node, while B concurrently adds a live child. Pins that, even with the folder empty
    /// at A's delete-moment, B's concurrently-added child still inherits the folder's
    /// tombstone.
    ///
    /// **`child_alive=false`:** the delete targets the folder NODE, and in the merged
    /// state B's child is parented to that tombstoned node, so it reads deleted — the
    /// emptiness-at-delete-moment on A is irrelevant. WITHOUT the reactive rescue this
    /// would be the silent loss EC-7 forbids; WITH it, the child is recovered (its own
    /// parent is still `Node(proj)`, the RESCUE classification, so the rescue revives the
    /// `proj/` chain to live and `mov`s the child back to its original path). The genuine
    /// per-file deletes read `parent == Deleted` and are honored. The rescue must revive
    /// the chain to LIVE before the `mov`, because re-homing under the still-dead folder
    /// would leave the child deleted — this probe is the proof of that necessity.
    #[test]
    fn empty_folder_node_deleted_after_files_subsumes_concurrent_child() {
        let (a, b, proj) = two_replicas_sharing_proj();

        // A removes the directory: delete both files, THEN tombstone the now-empty folder
        // node. At A's delete-moment the folder has no alive children.
        a.delete_node("proj/keep.md").unwrap();
        a.delete_node("proj/gone.md").unwrap();
        a.rebuild_caches();
        {
            let tree = a.index_tree();
            tree.delete(proj).unwrap();
        }
        a.rebuild_caches();

        // B concurrently creates a live file under the shared proj/ folder.
        let new_uuid = Uuid::from_u128(99);
        let new_node = b.register_document("proj/new.md", &new_uuid, &FP).unwrap();

        converge(&a, &b);

        let tree_a = a.index_tree();
        let child_alive = !tree_a.is_node_deleted(&new_node).unwrap_or(true);
        let ancestor_deleted = tree_a.is_node_deleted(&proj).unwrap_or(true);
        let child_path = a.get_node_path(&new_node);
        println!(
            "DP-3 PROBE [empty-folder-then-deleted]: child_alive={child_alive}, \
             ancestor_deleted={ancestor_deleted}, child_path={child_path:?}, proj={proj:?}, \
             new_node={new_node:?}"
        );

        // Pinned: the child is swept (reads deleted) and the folder reads tombstoned. The
        // reactive rescue is what turns this from silent loss into recovery — keyed on the
        // child's own parent still being Node(proj), not Deleted.
        assert!(
            !child_alive,
            "loro tombstones the concurrently-created child even when the folder was empty \
             at A's delete-moment — emptiness-at-delete does not save a concurrent add; the \
             reactive rescue recovers it"
        );
        assert!(ancestor_deleted, "the proj/ folder node stays tombstoned");
    }

    /// EC-7's MOVE leg (vs the CREATE leg above): A deletes the `proj/` folder node while
    /// B MOVES an ALREADY-EXISTING live node (`loose.md` at root) INTO `proj/`. Unlike a
    /// create-under-the-subtree, the moved node pre-exists outside the deleted subtree, so
    /// the question is whether a `tree.mov` INTO a concurrently-deleted folder leaves the
    /// moved node alive (its identity was never under the tombstoned subtree).
    ///
    /// This is the strongest candidate for producing a genuine "live node under a dead
    /// folder" — and the probe records which way loro resolves it.
    #[test]
    fn move_into_concurrently_deleted_folder() {
        let (a, b, proj) = two_replicas_sharing_proj();
        // Add a loose root file to the shared state so both replicas have its node.
        a.register_document("loose.md", &Uuid::from_u128(50), &FP)
            .unwrap();
        let loose_node = a.find_node_by_uuid(&Uuid::from_u128(50)).unwrap();
        // Re-seed B with loose.md so both share it.
        {
            let delta = a.export_snapshot().unwrap();
            b.import_updates(&delta).unwrap();
            b.rebuild_caches();
        }

        // A deletes the proj/ folder NODE (subtree delete).
        {
            let tree = a.index_tree();
            tree.delete(proj).unwrap();
        }
        a.rebuild_caches();

        // B concurrently MOVES the pre-existing loose.md INTO proj/.
        b.move_node("loose.md", "proj/loose.md").unwrap();
        assert!(
            !b.index_tree().is_node_deleted(&loose_node).unwrap(),
            "the moved node is alive on B pre-merge"
        );

        converge(&a, &b);

        let tree_a = a.index_tree();
        let moved_alive = !tree_a.is_node_deleted(&loose_node).unwrap_or(true);
        let ancestor_deleted = tree_a.is_node_deleted(&proj).unwrap_or(true);
        let moved_path = a.get_node_path(&loose_node);
        println!(
            "DP-3 PROBE [move-into-deleted-folder]: moved_alive={moved_alive}, \
             ancestor_deleted={ancestor_deleted}, moved_path={moved_path:?}, proj={proj:?}, \
             loose_node={loose_node:?}"
        );

        // The moved node's survival is the INV-3 question. Whatever loro does, pin it:
        // a move INTO a tombstoned folder is subsumed by the subtree delete just like a
        // create is (the node's parent is the tombstoned folder in the merged state).
        assert!(
            !moved_alive,
            "loro subsumes a node moved INTO a concurrently-subtree-deleted folder too — \
             the moved node reads deleted in the merged state (same as the create leg)"
        );
        assert!(ancestor_deleted, "the folder node stays tombstoned");
    }
}
