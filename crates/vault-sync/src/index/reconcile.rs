//! Boot reconciliation: bring the Index back in sync with the filesystem before
//! the vault opens to remote sync (INV-7, FR-7).
//!
//! Carried from the legacy sync crate's `vault/reconcile.rs`, re-keyed on UUID. Despite living
//! under `index/`, this is an `impl<F: FileSystem> Vault<F>` block: reconcile needs
//! the filesystem, the document cache, and the Index together — all of which the
//! public [`Vault`] handle owns. The Index alone is fs-agnostic.
//!
//! ## What reconcile owns (the fs↔loro divergence heal)
//!
//! The sync engine is purely event-driven, so any gap between disk and the Index is
//! permanent without a reconciliation pass. On every load `reconcile`:
//!
//! - **Adopts** an orphaned content doc (`docs/<uuid>.loro` with no live node) by
//!   registering a node for it, healing the divergence class where a peer's content
//!   landed on disk without its Index node. Under UUID keying the content doc is
//!   location-agnostic (OQ-3 deleted its stored `_meta.path`), so the orphan is
//!   paired to its on-disk `.md` by content hash, and the adopted node's path is the
//!   `.md`'s path.
//! - **Re-attaches a native move's lineage** (EC-8/S1): a `delete(old)` + `create(new)`
//!   on disk leaves the old node tombstoned (carrying `path == old`, `uuid == U`) and
//!   `docs/U.loro` orphaned. The new `.md`, when its content matches the orphan and
//!   the orphan's tombstoned path differs from the new path, re-attaches U at the new
//!   path — a zero-content move (the content `.loro` is never rewritten).
//! - **Quarantines** a tombstoned disk orphan (a `.md` still on disk at a tombstoned
//!   path) to `.trash/` — NEVER resurrecting a user's deletion.
//! - **Reports** an alive node whose backing `.md` is gone (REPORT-ONLY: it neither
//!   recreates the file nor tombstones the node — both are data-loss classes).
//! - **Rebuilds** the LOCAL `content_version` table: the fingerprint is a per-replica
//!   transient cache (no peer can write it), rebuilt from each content doc's actual
//!   `state_vv()` on every boot so the compare digest (P3) reads a correct local value.
//!
//! ## Boot order (INV-7 — load-bearing)
//!
//! `Vault::load` loads the Index (hard-failing on corruption — EC-9), rebuilds the
//! caches, runs `reconcile` (documenting local fs into the Index), and ONLY THEN
//! opens to remote (`process_message`). Local state is fully captured before any
//! remote delta integrates — "commit before pull."

use crate::content_doc::ContentDoc;
use crate::fs::{FileSystem, FsError};
use crate::hash::{content_hash, content_version_fingerprint};
use crate::index::{
    DOCS_DIR, FileMove, IndexError, ReconcileReport, Result, TRASH_DIR, content_doc_path,
};
use crate::vault::Vault;

use std::collections::HashSet;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// One journaled native-move DELETE the boot re-stitch may re-attach. The daemon
/// builds this from its deserialized pending-move records (delete records only; a
/// create record has no UUID to re-attach and is finalized standalone). A plain
/// value type so vault-sync never sees the journal's on-disk serde form — the
/// hex↔bytes conversion lives once at the daemon boundary.
#[derive(Debug, Clone)]
pub struct JournalReStitch {
    /// The move's original document UUID — the lineage to re-attach at `new_path`.
    pub uuid: Uuid,
    /// The path the document was at when its delete buffered (the still-live node).
    pub old_path: String,
    /// The content hash of the buffered document, SAME domain as reconcile's own
    /// `content_hash(&ContentDoc::from_markdown(..))` — `[u8; 32]`, NOT hex. The
    /// daemon decodes its hex `content_hash` back to `[u8; 32]` at the boundary.
    pub content_hash: [u8; 32],
}

impl<F: FileSystem> Vault<F> {
    /// Reconcile the filesystem with the Index — the boot pass that documents local
    /// fs state into the catalog before the vault opens to remote sync.
    ///
    /// The filesystem (the materialized `.md`) is the source of truth for local
    /// state. The pass is structured so the dangerous outcomes (resurrecting a
    /// deletion, deleting a live file) cannot happen:
    /// - orphaned content docs are adopted or their move-lineage re-attached, never
    ///   silently dropped (INV-3);
    /// - tombstoned disk orphans are quarantined, never resurrected;
    /// - alive nodes with no `.md` are reported only, never recreated or tombstoned.
    ///
    /// Per-file failures never abort the pass (a file race-deleted mid-scan must not
    /// fail `Vault::load`); a corrupt single content doc is contained and skipped
    /// (NFR-6). The Index is persisted once at the end if anything changed.
    ///
    /// `journaled` carries the move-coalescer's crash-recovery journal (the daemon's
    /// pending native-move deletes, P4f-2b). When `Some`, a journaled delete whose
    /// original UUID still has a live node AND whose content matches an orphaned
    /// on-disk `.md` is re-stitched (the UUID re-attached at the new path) BEFORE the
    /// per-file loop would mint a fresh node for that orphan — a pure `move_node` on a
    /// still-live source. `None` is byte-identical to the pre-journal behavior.
    pub async fn reconcile(
        &self,
        journaled: Option<&[JournalReStitch]>,
    ) -> Result<ReconcileReport> {
        let mut report = ReconcileReport::default();

        // Recover any stray `*.casemv-tmp` directory a crash left between the two steps
        // of a case-only directory rename (Fix-3 Facet A). This MUST run before the
        // listing below: a stray dir's children look like brand-new files to the
        // per-file loop, which would mint ghost UUIDs (the exact pollution this change
        // fixes). The sweep completes the interrupted rename so the children survive at a
        // real tracked path — never losing them.
        self.sweep_stray_casemv_tmp().await;

        // Every markdown file on disk, and every document UUID with a content `.loro`.
        let md_files: HashSet<String> = self.list_files().await?.into_iter().collect();
        let doc_uuids = self.list_content_docs().await?;

        // First pass: adopt orphaned content docs (no live node) by content-pairing
        // them to new `.md` files — native-move lineage re-attach or divergence heal.
        // Returns the `.md` paths it matched so the per-file loop skips them.
        let matched = self
            .adopt_orphans(&md_files, &doc_uuids, &mut report)
            .await?;

        // Crash-recovery re-stitch (P4f-2b): re-attach the original UUID of each
        // journaled native-move delete whose old node is STILL live and whose content
        // matches an orphaned on-disk `.md`. Runs AFTER `adopt_orphans` but BEFORE the
        // per-file loop, so `new_path` has no node yet and the re-attach is a single
        // `move_node` on a live source — never a delete/tombstone, which is what makes
        // the `{old alive, new tombstoned}` crash-intermediate structurally
        // unconstructable. Disjoint from `adopt_orphans`: that pass acts on UUIDs with
        // NO live node, this one only on UUIDs WITH a live node.
        let restitched = match journaled {
            Some(records) if !records.is_empty() => {
                self.re_stitch_journaled(&md_files, records, &matched, &mut report)
                    .await?
            }
            _ => HashSet::new(),
        };

        // Second pass: reconcile each remaining `.md` against its Index/content state.
        //
        // Each file is reconciled in isolation and a per-file error never aborts the
        // pass: reconcile runs inside `Vault::load`, so propagating a per-file fs error
        // would abort startup over a single file (e.g. one race-deleted between the
        // directory scan and this loop). NotFound (a vanished file) is benign and
        // debug-logged; other errors warn.
        for path in &md_files {
            if matched.contains(path) || restitched.contains(path) {
                continue;
            }
            match self.reconcile_one_file(path, &mut report).await {
                Ok(()) => {}
                Err(IndexError::Fs(FsError::NotFound(_))) => {
                    debug!("Skipping race-deleted file during reconcile: {}", path);
                }
                Err(e) => {
                    warn!("Failed to reconcile {}: {}", path, e);
                }
            }
        }

        // Inverse divergence: alive Index nodes whose backing `.md` is gone from disk.
        // REPORT-ONLY — never recreate the file (resurrection) or tombstone the node
        // (deletion-propagation), both data-loss classes.
        self.report_missing_files(&md_files, &mut report).await;

        // Rebuild the LOCAL `content_version` table from each content doc's actual
        // `state_vv()`, so the compare digest (P3) reads a correct local cache. The
        // table is transient (rebuilt every boot) and lives in memory, so this mutates
        // no synced CRDT state and never affects the save gate below.
        self.rebuild_content_versions().await?;

        // Recover any folder-swept orphan (EC-7/OQ-6) that persisted across this load: a
        // concurrent add a peer's folder delete tombstoned, whose own parent is still a
        // dead folder node. The apply path rescues these inline, but boot reconcile is the
        // backstop for one that survived a restart (e.g. the apply was interrupted before
        // the rescue, or the orphan's content arrived only after the apply that swept it).
        // It revives + re-homes + re-materializes + persists internally when it acts.
        self.rescue_swept_orphans().await?;

        // Collapse any structural collision (folder-merge / file cascade) the merged tree
        // holds — B1. The apply path always fires this cascade after its rescue (so a
        // rescue's freshly-minted folder node that collides with another at the same path
        // is collapsed within the same `process_message`), but boot reconcile has its own
        // rescue above and must mirror that. Two replicas can independently rescue the
        // same swept orphan, each minting a DISTINCT live folder node at one path; once
        // their Indexes merge, that two-folder collision persists across a restart. Without
        // this call reconcile would leave two live folder nodes (and two directories) at
        // one path until the next sync. Cheap when there is no collision (a tree-scan gate,
        // no content load); at boot the `.loro` files are on disk, so any surfaced file
        // collision resolves here too. Runs BEFORE `materialize_folders` so the on-disk
        // directories reflect the collapsed (single-survivor) folder set.
        self.resolve_structural_conflicts().await?;

        // Materialize the folder set from the Index (INV-1.5a): a fresh clone / `reload`
        // re-creates each tracked empty directory and removes a tombstoned folder's empty
        // directory. Folders are invisible to the file passes above (which see only
        // `.md` files), so without this an empty folder never appears on a freshly-loaded
        // vault. fs-only (no Index mutation), so it runs regardless of the save gate.
        self.materialize_folders().await?;

        // Persist the Index mutations made during this pass — batched here (not per
        // adopt/register) to avoid O(n) snapshot writes when many files are indexed at
        // startup. `adopted`/`moved` register nodes that only live in memory until
        // saved. Without the save the heal is illusory (it re-runs on every restart,
        // never persisting). The content_version rebuild touches no CRDT state (it fills
        // only the in-memory table), so it contributes nothing to this gate.
        if report.has_changes() {
            self.save_index().await?;
        }

        Ok(report)
    }

    /// Boot-time recovery for a crash BETWEEN the two steps of a case-only directory
    /// rename (Fix-3 Facet A: `Foo` → `Foo.casemv-tmp` → `foo`). Step 1 leaves a stray
    /// `Foo.casemv-tmp/` on disk that is NOT in the index; reconcile would otherwise see
    /// its children as brand-new files and mint ghost UUIDs (the case-dup pollution this
    /// change exists to prevent — P4f-2's journal does NOT cover Facet A's `fs.rename`).
    ///
    /// For each stray directory the sweep completes the interrupted rename to an
    /// **unambiguous** target: the base name with the suffix stripped (`Foo`), preferring
    /// the casing of an existing live folder node that matches it case-insensitively (so
    /// recovery lands on the path the index already tracks). If the target is occupied by
    /// a DIFFERENT real directory (an ambiguous collision), it LOUDLY warns and leaves the
    /// stray for manual handling — NEVER silently losing the children. Best-effort: a
    /// rename failure is logged, never propagated (it must not fail `Vault::load`).
    ///
    /// Once recovered to a tracked path, the case rename re-propagates through normal
    /// sync (the sender's sweep re-detects the drift), so no convergence is lost.
    async fn sweep_stray_casemv_tmp(&self) {
        let suffix = Vault::<F>::CASEMV_TMP_SUFFIX;
        let strays = match self.list_dirs_with_suffix(suffix).await {
            Ok(dirs) => dirs,
            Err(e) => {
                warn!(
                    "sweep_stray_casemv_tmp: failed to scan for stray dirs: {}",
                    e
                );
                return;
            }
        };

        for stray in strays {
            let base = &stray[..stray.len() - suffix.len()];
            // Prefer the casing of a live folder node that matches the base
            // case-insensitively (recover toward what the index tracks); else the base.
            let target = self
                .index()
                .folder_paths()
                .into_iter()
                .find(|f| !f.is_deleted && f.path.eq_ignore_ascii_case(base))
                .map(|f| f.path)
                .unwrap_or_else(|| base.to_string());

            // Ambiguity guard: if the target already exists as a directory whose casing
            // is NOT the stray's (a genuine other directory occupies it), do not clobber
            // it — warn loudly and leave the stray for manual resolution.
            match self.fs().exists(&target).await {
                Ok(true) => {
                    warn!(
                        "sweep_stray_casemv_tmp: stray {} cannot be completed — target {} \
                         already exists; leaving stray for manual handling (children NOT lost)",
                        stray, target
                    );
                    continue;
                }
                Ok(false) => {}
                Err(e) => {
                    warn!(
                        "sweep_stray_casemv_tmp: cannot stat target {} for stray {}: {} — \
                         leaving stray",
                        target, stray, e
                    );
                    continue;
                }
            }

            if let Err(e) = self.fs().rename(&stray, &target).await {
                warn!(
                    "sweep_stray_casemv_tmp: failed to complete {} -> {}: {} — leaving stray \
                     for manual handling (children NOT lost)",
                    stray, target, e
                );
            } else {
                info!(
                    "sweep_stray_casemv_tmp: recovered stray {} -> {} (crash mid case-rename)",
                    stray, target
                );
            }
        }
    }

    /// List every directory (at any depth, excluding `.sync`/hidden) whose final path
    /// segment ends with `suffix`. Used by [`Self::sweep_stray_casemv_tmp`].
    async fn list_dirs_with_suffix(&self, suffix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut dirs_to_visit = vec![String::new()];

        while let Some(dir) = dirs_to_visit.pop() {
            let entries = self.fs().list(&dir).await?;
            for entry in entries {
                if !entry.is_dir {
                    continue;
                }
                let path = if dir.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{}/{}", dir, entry.name)
                };
                if path.starts_with(".sync") || path.starts_with('.') {
                    continue;
                }
                if entry.name.ends_with(suffix) {
                    out.push(path.clone());
                }
                dirs_to_visit.push(path);
            }
        }
        Ok(out)
    }

    /// Adopt orphaned content docs — the fs↔loro divergence heal and the native-move
    /// lineage re-attach. Returns the set of `.md` paths that were adopted (matched to
    /// an orphan) so the per-file loop skips them.
    ///
    /// An orphan is a `docs/<uuid>.loro` whose UUID has no *live* node. Each orphan is
    /// content-paired (the only link, since OQ-3 removed the content doc's stored path)
    /// to an unmatched new `.md`:
    /// - If the orphan's UUID has a **tombstoned node** at some `old` (the move's
    ///   source), it re-attaches at a content-matching new `.md` whose path differs
    ///   from `old` (a move — a same-path `.md` at `old` is a strand, left for the
    ///   per-file loop to quarantine). Zero content re-transfers: the content `.loro`
    ///   is never rewritten, so a synced peer already holds it (S1, INV-1).
    /// - Otherwise (no node ever — pure divergence) it adopts at any content-matching
    ///   new `.md`, preserving the orphan's lineage UUID rather than minting (a rebuild
    ///   from `.md` would mint a fresh UUID and look independently-created on the next
    ///   sync, diverging instead of merging).
    ///
    /// "Window bound" for the move case is structural: only an *orphaned* `.loro`
    /// (UUID with no live node) is a candidate, so a new `.md` whose content merely
    /// coincides with a still-live document is never adopted onto that document — it
    /// is a fresh file with a fresh UUID.
    async fn adopt_orphans(
        &self,
        md_files: &HashSet<String>,
        doc_uuids: &HashSet<Uuid>,
        report: &mut ReconcileReport,
    ) -> Result<HashSet<String>> {
        let mut matched: HashSet<String> = HashSet::new();

        // Orphans: a content `.loro` on disk whose UUID has no live node.
        let orphans: Vec<Uuid> = doc_uuids
            .iter()
            .filter(|uuid| self.index().find_node_by_uuid(uuid).is_none())
            .copied()
            .collect();
        if orphans.is_empty() {
            return Ok(matched);
        }

        // New `.md` files (no live node) with their content hashes, computed once.
        // A bad file is skipped (it cannot be a move/divergence target if unreadable).
        let mut new_files: Vec<(String, [u8; 32])> = Vec::new();
        for path in md_files {
            if self.index().node_for_path(path).is_some() {
                continue;
            }
            if let Ok(bytes) = self.fs().read(path).await {
                let content = String::from_utf8_lossy(&bytes);
                if let Ok(doc) = ContentDoc::from_markdown(&content, self.loro_author()) {
                    new_files.push((path.clone(), content_hash(&doc)));
                }
            }
        }

        for uuid in orphans {
            let loro_path = content_doc_path(&uuid);
            // A corrupt single content doc is contained: skip it and keep going
            // (NFR-6 — one bad doc never aborts reconcile or drops the rest).
            let Ok(bytes) = self.fs().read(&loro_path).await else {
                continue;
            };
            let Ok(orphan_doc) = ContentDoc::from_bytes(&bytes, self.loro_author()) else {
                warn!("Skipping corrupt orphaned content doc: {}", loro_path);
                continue;
            };
            let orphan_hash = content_hash(&orphan_doc);

            // The orphan's last-known path, recovered from a tombstoned node (OQ-3).
            // `Some(old)` ⇒ a delete happened (the native-move source); `None` ⇒ the
            // UUID never had a node (pure fs↔loro divergence).
            let old_path = self.index().deleted_node_path_for_uuid(&uuid);

            // Pick a content-matching, unmatched new `.md`. For a move source, exclude
            // the orphan's own tombstoned path: a `.md` sitting AT the tombstoned path
            // is a strand to quarantine, not a move target (adopting it there would
            // resurrect the deletion).
            let candidate = match &old_path {
                Some(old) => new_files
                    .iter()
                    .find(|(p, h)| *h == orphan_hash && p != old && !matched.contains(p)),
                None => new_files
                    .iter()
                    .find(|(p, h)| *h == orphan_hash && !matched.contains(p)),
            };

            let Some((new_path, _)) = candidate else {
                // No `.md` to adopt this orphan to → report it (a deleted doc, or a
                // strand whose `.md` the per-file loop will quarantine). Label with the
                // recovered old path when available, else the UUID.
                let label = old_path.unwrap_or_else(|| uuid.to_string());
                warn!("Orphaned content doc (no matching .md): {}", label);
                report.orphaned.push(label);
                continue;
            };
            let new_path = new_path.clone();

            // Register a node at the new path under the orphan's UUID. The content
            // `.loro` is already correctly named `docs/<uuid>.loro` — nothing is
            // relocated or rewritten, which is what keeps the move zero-content.
            let fingerprint = content_version_fingerprint(&orphan_doc.version());
            self.index()
                .register_document(&new_path, &uuid, &fingerprint)?;
            self.documents_mut().insert(new_path.clone(), orphan_doc);

            match old_path {
                Some(old) => {
                    info!("Native-move adopt: {} -> {} ({})", old, new_path, uuid);
                    report.moved.push(FileMove {
                        from: old,
                        to: new_path.clone(),
                    });
                }
                None => {
                    info!(
                        "Adopting orphaned content doc (no node): {} ({})",
                        new_path, uuid
                    );
                    report.adopted.push(new_path.clone());
                }
            }
            matched.insert(new_path);
        }

        Ok(matched)
    }

    /// Re-stitch the move-coalescer's journaled native-move deletes (P4f-2b crash
    /// recovery): re-attach the original UUID of each in-flight move whose delete
    /// buffered but never committed, so a crash mid-move does not degrade the move into
    /// a fresh-UUID delete+create. Returns the new `.md` paths it re-stitched so the
    /// per-file loop skips them.
    ///
    /// Runs AFTER `adopt_orphans` but BEFORE the per-file loop — the load-bearing
    /// ordering. At this point reconcile has NOT reached `new_path` in the per-file
    /// pass, so `new_path` has no node and the re-attach is a single `move_node` on a
    /// still-LIVE source: no `MoveTargetExists`, hence NO `delete_file(new_path)` is
    /// ever needed. Because the recovery contains no tombstone of `new_path`, the
    /// `{old alive, new tombstoned}` crash-intermediate (the data-loss window of the
    /// rejected post-load reclaim) is structurally unconstructable by this path.
    ///
    /// Disjoint from `adopt_orphans`: that pass acts on UUIDs with NO live node (an
    /// orphaned `.loro`); this one acts ONLY on UUIDs WITH a live node (step 1's
    /// guard) — the crash class where the delete buffered, so the node is still live
    /// and its `.loro` was never an orphan candidate. The same UUID can never satisfy
    /// both passes.
    ///
    /// Idempotent: on a second boot an already-re-stitched record's UUID resolves to a
    /// node now at `new_path != old_path`, so the live-node guard skips it; a record
    /// whose delete was finalized standalone (2b-ii) has a tombstoned UUID that
    /// `find_node_by_uuid` excludes → also skipped. Every branch is a no-op the second
    /// time.
    ///
    /// The lib pass only ever does the SAFE `move_node`. A record with no content
    /// matching on-disk `.md` (the create half never landed) is left untouched for the
    /// daemon's standalone-finalize (2b-ii) to commit — the lib never tombstones.
    async fn re_stitch_journaled(
        &self,
        md_files: &HashSet<String>,
        journaled: &[JournalReStitch],
        matched: &HashSet<String>,
        report: &mut ReconcileReport,
    ) -> Result<HashSet<String>> {
        let mut restitched: HashSet<String> = HashSet::new();

        // Hash every orphaned new `.md` once (no live node, not already adopted by
        // `adopt_orphans`), exactly as `adopt_orphans` does its candidate scan: read
        // each once; an unreadable/un-parseable file is skipped (it cannot be a
        // re-stitch target). Computed up front so multiple records don't re-read disk.
        let mut candidates: Vec<(String, [u8; 32])> = Vec::new();
        for path in md_files {
            if matched.contains(path) || self.index().node_for_path(path).is_some() {
                continue;
            }
            if let Ok(bytes) = self.fs().read(path).await {
                let content = String::from_utf8_lossy(&bytes);
                if let Ok(doc) = ContentDoc::from_markdown(&content, self.loro_author()) {
                    candidates.push((path.clone(), content_hash(&doc)));
                }
            }
        }

        for record in journaled {
            let JournalReStitch {
                uuid,
                old_path,
                content_hash: hash,
            } = record;

            // Step 1 — live-node guard (the distinguishing input). `find_node_by_uuid`
            // reads the live cache (excludes tombstones), so a hit is a genuinely live
            // node. `None` ⇒ the UUID has no live node (already re-stitched on a prior
            // boot, or its delete already committed) → skip (the idempotency arm).
            let Some(node) = self.index().find_node_by_uuid(uuid) else {
                continue;
            };
            // Defensive: the live node must still sit at `old_path`. If it has already
            // moved (a prior boot re-stitched it), skip — this is the primary
            // idempotency guard on a second run.
            if self.index().path_for_node(&node).as_deref() != Some(old_path.as_str()) {
                continue;
            }

            // Step 2 — find a content-matching orphaned `.md` at a DIFFERENT path that
            // no other record has already claimed this pass.
            //
            // Accepted limitation: if two orphans share this content hash, `.find` takes
            // the FIRST from an unordered scan (HashSet iteration order) — a
            // non-deterministic winner. This deliberately MIRRORS `adopt_orphans`'s own
            // content-pairing; a deterministic sort here would diverge from that
            // adopt-consistent behavior, so it is left as-is.
            let target = candidates
                .iter()
                .find(|(p, h)| h == hash && p != old_path && !restitched.contains(p.as_str()));
            let Some((new_path, _)) = target else {
                // Step 3 — no disk evidence the move completed (the create half never
                // landed, or its content diverged). The lib pass does NOTHING: the old
                // node stays live, no fresh mint, no tombstone. The daemon's
                // standalone-finalize (2b-ii) decides whether to commit the delete.
                continue;
            };
            let new_path = new_path.clone();

            // Step 4 — re-stitch: re-parent the SAME live node to the new path. UUID and
            // content `.loro` are untouched (INV-1, zero content), exactly as the live
            // move path's `move_node` does. Under the in-reconcile ordering neither
            // `move_node` error can fire (the source is live — step 1; `new_path` has no
            // node yet — the per-file loop hasn't run). If one DOES fire it is a logic
            // bug: log loudly and skip the record (reconcile's log-and-continue
            // discipline), but do NOT attempt a delete-then-retry — that would
            // reintroduce the two-sub-step data-loss window this design exists to kill.
            match self.index().move_node(old_path, &new_path) {
                Ok(()) => {
                    info!("Journal re-stitch: {} -> {} ({})", old_path, new_path, uuid);
                    report.moved.push(FileMove {
                        from: old_path.clone(),
                        to: new_path.clone(),
                    });
                    restitched.insert(new_path);
                }
                Err(e) => {
                    error!(
                        "Journal re-stitch move_node failed ({} -> {}, {}): {} — \
                         skipping this record, deferring to the daemon finalize",
                        old_path, new_path, uuid, e
                    );
                }
            }
        }

        Ok(restitched)
    }

    /// Reconcile a single `.md` file against its Index/content state.
    ///
    /// Classifies the file by the three axes the decision turns on, then `match`es
    /// over the tuple so every case is enumerable and the dangerous ones fall out of
    /// exhaustiveness rather than a hand-written guard — this IS INV-7's mechanism.
    ///
    /// Axes:
    /// - `has_loro`:   the path's live node has its `docs/<uuid>.loro` on disk.
    /// - `has_node`:   an *alive* Index node exists for this path.
    /// - `tombstoned`: the Index marks this path deleted (alive-wins: a path with an
    ///   alive node is never tombstoned after `rebuild_caches`).
    ///
    /// Under UUID keying `has_loro ⟹ has_node` (a content `.loro` is located via its
    /// node's UUID), so the `(loro, no node)` adopt arm that the legacy sync crate's per-file
    /// loop carried does not apply here — the orphan-adopt is content-paired in
    /// `adopt_orphans` (OQ-3 removed the per-path stored path it would have keyed on).
    async fn reconcile_one_file(&self, path: &str, report: &mut ReconcileReport) -> Result<()> {
        let has_node = self.index().node_for_path(path).is_some();
        let has_loro = match self.uuid_for_path(path) {
            Some(uuid) => self.fs().exists(&content_doc_path(&uuid)).await?,
            None => false,
        };
        let tombstoned = self.index().is_path_deleted(path);

        match (has_loro, has_node, tombstoned) {
            // Tombstoned (with or without a `.loro`) → quarantine; NEVER resurrect.
            // A `.md` whose Index state is tombstoned is an untracked strand (a
            // historical-bug or offline-window leftover), not a new file: move it to
            // `.trash/` rather than re-minting a node. quarantine_orphan's own A1 guard
            // short-circuits if an alive node occupies the path, so the stale-armed
            // window is safe. Inner log-and-continue: one orphan failing to quarantine
            // must not block the load, and its specific message is more useful than the
            // caller's generic one.
            (_, _, true) => {
                if let Err(e) = self.quarantine_orphan(path).await {
                    warn!("Failed to quarantine orphan {}: {}", path, e);
                } else {
                    report.quarantined.push(path.to_string());
                }
            }

            // Live node + its `.loro` present → reconcile the content: if the `.md`
            // changed externally, diff-merge it into the doc. on_file_changed is the
            // proven Flow-1 path (it loads the doc from `docs/<uuid>.loro`, diff-merges,
            // rewrites only on a real change, and bumps `content_version`), so the
            // reindex is a delegation rather than a re-implementation of needs_reindex.
            (true, true, false) => {
                if self.on_file_changed(path).await? {
                    info!("File modified externally, re-indexing: {}", path);
                    report.reindexed.push(path.to_string());
                }
            }

            // Live node but its `.loro` is gone (a content file deleted out from under a
            // node — a crash/corruption edge). Non-destructive: leave it for the next
            // sync to backfill from the peer that still holds the document. Recreating
            // it from the `.md` would mint a fresh UUID into the content doc that no
            // longer matches the node's UUID, so reconcile does not fabricate it.
            (false, true, false) => {
                warn!(
                    "Live node at {} has no content doc on disk; recovering on next sync",
                    path
                );
            }

            // No live node, not tombstoned → a brand-new file. on_file_changed creates
            // its content doc and registers the node (Flow-1). `has_loro` cannot be true
            // here (it requires a node), so this single arm covers both `(_, false,
            // false)` tuples.
            (_, false, false) => {
                info!("New file detected, indexing: {}", path);
                self.on_file_changed(path).await?;
                report.indexed.push(path.to_string());
            }
        }

        Ok(())
    }

    /// Record alive Index file nodes that have no backing `.md` on disk
    /// (`report.missing_files`) — the inverse of the adopt direction.
    ///
    /// REPORT-ONLY: reconcile does NOT recreate the file from its content doc
    /// (resurrection) and does NOT tombstone the node (deletion-propagation) — both
    /// are data-loss classes. `path_to_node` holds exactly the alive file-node paths,
    /// so it is the cheap source of truth here; the already-scanned `md_files` set is
    /// consulted first to skip an fs probe for the common (present) case.
    async fn report_missing_files(&self, md_files: &HashSet<String>, report: &mut ReconcileReport) {
        // Collect the alive paths up front (releasing the cache borrow before the fs
        // probes — never hold a lock guard across an await).
        let alive_paths: Vec<String> = self.index().path_to_node().keys().cloned().collect();

        for path in alive_paths {
            if md_files.contains(&path) {
                continue;
            }
            if !self.fs().exists(&path).await.unwrap_or(false) {
                info!("Index node with no backing file (report-only): {}", path);
                report.missing_files.push(path);
            }
        }
    }

    /// Rebuild the LOCAL `content_version` table from disk (the boot table-builder).
    ///
    /// The fingerprint is a per-replica-LOCAL transient cache (the content doc's
    /// `state_vv()` is authoritative). It lives in an in-memory table that no peer can
    /// write, rebuilt from each content doc's `.loro` on every boot — so there is no
    /// persisted fingerprint to go stale. This clears the table, then re-fills it for
    /// every alive doc from the doc's actual `state_vv()`, so the compare digest (P3)
    /// reads a fingerprint that matches the content. Runs after the per-file reindex
    /// pass, so a doc whose `.md` changed externally is already current.
    ///
    /// Mutates NO synced CRDT state — it fills only the in-memory table, so it never
    /// dirties the Index and contributes nothing to the `reconcile()` save gate.
    ///
    /// Ordering note (S1): `rescue_swept_orphans` runs AFTER this rebuild, so a revived
    /// node's fingerprint is not in the table until the next boot. That is benign — the
    /// `catalog_digest`'s `filter_map` over `node_content_version` simply skips a node
    /// with no entry, and the rescued node's content syncs normally; the entry is healed
    /// on the next boot.
    async fn rebuild_content_versions(&self) -> Result<()> {
        // Clear-then-fill: the table is transient; a stale entry from a prior Index
        // instance must not survive. In practice `load_index` constructs a fresh Index
        // (empty table), so this is defensive and matches `rebuild_caches`'s discipline.
        self.index().content_versions_mut().clear();

        // Snapshot the alive (path, node) pairs before the awaits.
        let alive: Vec<(String, loro::TreeID)> = self
            .index()
            .path_to_node()
            .iter()
            .map(|(p, id)| (p.clone(), *id))
            .collect();

        for (path, node) in alive {
            let Some(uuid) = self.index().node_uuid(&node) else {
                continue;
            };
            let loro_path = content_doc_path(&uuid);
            // No content doc on disk → nothing to recompute against (the node-with-no-
            // `.loro` edge handled above); skip rather than fabricate.
            let Ok(bytes) = self.fs().read(&loro_path).await else {
                continue;
            };
            let Ok(doc) = ContentDoc::from_bytes(&bytes, self.loro_author()) else {
                continue;
            };

            let actual = content_version_fingerprint(&doc.version());

            // Fill the transient table from local on-disk content (every live doc).
            self.index().content_versions_mut().insert(uuid, actual);
            debug!("Rebuilt content_version table entry for {}", path);
        }

        Ok(())
    }

    /// Move a tombstoned disk orphan to `.trash/<path>`.
    ///
    /// A disk orphan is a `.md` on disk whose Index state is tombstoned
    /// (`is_path_deleted`). Quarantining is reversible (the file is preserved under
    /// `.trash/`, which `list_files` excludes) and touches only disk — never the Index
    /// tree — so it cannot fight the resurrection guard or a delete, which read the
    /// same deleted-paths set.
    ///
    /// A1 guard: if a live node currently occupies the path, the file is NOT an orphan
    /// no matter how the deleted-paths set looks (a delete inserts synchronously and a
    /// local re-create does not clear it, so a path can be stale-tombstoned while
    /// carrying a freshly re-created live node). The alive-node check closes that
    /// data-loss path in every calling context.
    pub(crate) async fn quarantine_orphan(&self, path: &str) -> Result<()> {
        // A1: an alive node at the path means this is not an orphan — never quarantine.
        if self.index().node_for_path(path).is_some() {
            return Ok(());
        }

        let bytes = self.fs().read(path).await?;

        // Pick the trash destination. Crash-idempotency: if a prior quarantine wrote
        // `.trash/<path>` but failed to delete the original, the orphan sits at BOTH
        // paths. On the next pass we must reuse that identical trash copy and just
        // retry the delete — NOT allocate a new collision suffix, which would let
        // `.trash/<path>.N` grow without bound under a persistent delete failure. A new
        // suffix is only for a genuinely distinct orphan that shares a name.
        let base_dest = format!("{}/{}", TRASH_DIR, path);
        let dest = match self.fs().read(&base_dest).await {
            // Already-trashed identical content → reuse it, skip the write, retry delete.
            Ok(existing) if existing == bytes => base_dest,
            // Occupied by different content → suffix to avoid clobbering a distinct file.
            Ok(_) => {
                let mut n = 1;
                loop {
                    let candidate = format!("{}/{}.{}", TRASH_DIR, path, n);
                    match self.fs().read(&candidate).await {
                        Ok(existing) if existing == bytes => break candidate,
                        Ok(_) => n += 1,
                        Err(_) => break candidate,
                    }
                }
            }
            // Nothing there yet → use the base destination.
            Err(_) => base_dest,
        };

        // Write only when the destination doesn't already hold our content (the
        // crash-idempotency reuse case above skips it). atomic_write (write-temp +
        // rename) keeps the trash copy from being torn if the process crashes mid-write,
        // then delete the original. `write` creates parent dirs (FileSystem contract),
        // so `.trash/` is created on demand.
        if !self.fs().exists(&dest).await? {
            self.fs().atomic_write(&dest, &bytes).await?;
        }

        // The write→delete sequence is non-atomic: if delete fails here, the copy is
        // safely in trash but the original remains. Surface that distinct partial state
        // so the next pass's idempotent reuse (above) is the recovery, not data loss.
        if let Err(e) = self.fs().delete(path).await {
            warn!(
                "Quarantine partially succeeded for {}: copy is in {} but the original \
                 could not be removed ({}); it will be retried on the next reconcile",
                path, dest, e
            );
            return Err(e.into());
        }

        info!("Quarantined disk orphan {} -> {}", path, dest);
        Ok(())
    }

    /// List every document UUID with a content `.loro` in `.sync/docs/`.
    ///
    /// The filename IS the UUID (`<uuid>.loro`), so the parse is the identity read.
    /// A filename that does not parse as a UUID is skipped (foreign debris).
    async fn list_content_docs(&self) -> Result<HashSet<Uuid>> {
        let mut uuids = HashSet::new();

        if !self.fs().exists(DOCS_DIR).await? {
            return Ok(uuids);
        }

        for entry in self.fs().list(DOCS_DIR).await? {
            if entry.is_dir || !entry.name.ends_with(".loro") {
                continue;
            }
            let stem = entry.name.trim_end_matches(".loro");
            if let Ok(uuid) = Uuid::parse_str(stem) {
                uuids.insert(uuid);
            }
        }

        Ok(uuids)
    }
}

#[cfg(test)]
mod b1_boot_cascade {
    //! B1: boot reconcile must run the structural-conflict cascade after its swept-orphan
    //! rescue, so a persisted two-folder collision is collapsed on load — not left as two
    //! live folder nodes (and two on-disk directories) at one path until the next sync.
    //!
    //! How the collision is reached in the field: a peer's whole-folder delete sweeps a
    //! concurrently-added child (EC-7). TWO replicas can each rescue that same swept orphan
    //! independently, and `Index::rescue_orphan` mints a FRESH live folder node for the
    //! revived parent chain (it never reuses the tombstoned one). Once those two replicas'
    //! Indexes merge, two DISTINCT live folder nodes sit at one path. The apply path always
    //! fires `resolve_structural_conflicts` right after its rescue, collapsing this within
    //! the same `process_message`; boot reconcile must mirror that. This test stages exactly
    //! that merged collision in a vault's persisted Index and asserts a `Vault::load` (which
    //! runs reconcile) collapses it to ONE folder node.
    //!
    //! Built from the `pub(crate)` Index seam (`import_updates`/`index_tree`) because the
    //! public `Vault` API cannot construct an *un-collapsed* collision — every inbound apply
    //! collapses it. It is therefore an in-crate test of the reconcile boot invariant, with
    //! a clear user-facing effect (two folders at one path on disk after a restart).

    use crate::content_doc::ContentDoc;
    use crate::fs::{FileSystem, InMemoryFs};
    use crate::hash::content_version_fingerprint;
    use crate::index::{Index, content_doc_path};
    use crate::vault::Vault;
    use std::sync::Arc;
    use uuid::Uuid;

    const AUTHOR_A: u64 = 0x0101_0101_0101_0101;
    const AUTHOR_B: u64 = 0x0202_0202_0202_0202;

    /// Count the ALIVE folder nodes whose display path equals `path` in `index`.
    fn alive_folder_nodes_at(index: &Index, path: &str) -> usize {
        index
            .scan_structural_nodes()
            .iter()
            .filter(
                |n| matches!(n, crate::index::StructuralNode::Folder { path: p, .. } if p == path),
            )
            .count()
    }

    /// Independently rescue every swept orphan on `index` (each mints its own fresh live
    /// folder node for the revived parent chain) and rebuild caches — one replica's half
    /// of the two-independent-rescue race.
    fn rescue_all(index: &Index) {
        for orphan in index.swept_orphan_files() {
            index.rescue_orphan(&orphan).unwrap();
        }
        index.rebuild_caches();
    }

    /// Stage a vault whose PERSISTED Index holds an un-collapsed two-folder collision at
    /// `proj/` (two distinct live folder nodes), reached via two independent EC-7 rescues
    /// of the same swept orphan, then merged. Returns the retained filesystem so the caller
    /// can `Vault::load` it. The orphan's content `.loro` + `.md` are on disk so the file
    /// survives reconcile intact.
    async fn stage_persisted_two_folder_collision() -> Arc<InMemoryFs> {
        let fs = Arc::new(InMemoryFs::new());
        let vault = Vault::init(Arc::clone(&fs), AUTHOR_A).await.unwrap();

        // V (= replica A) starts with proj/seed.md — this creates the shared proj/ folder.
        fs.write("proj/seed.md", b"# seed\n\nseed body")
            .await
            .unwrap();
        vault.on_file_changed("proj/seed.md").await.unwrap();
        vault.save_index().await.unwrap();

        // The concurrent add proj/new.md: mint its content doc (so its `.loro` carries the
        // matching doc_id) and stage it on disk. Its UUID is the lineage we register under.
        let new_doc = ContentDoc::from_markdown("# new\n\nNEW BODY important", AUTHOR_B).unwrap();
        let new_uuid = Uuid::parse_str(&new_doc.doc_id().unwrap()).unwrap();
        let new_fp = content_version_fingerprint(&new_doc.version());
        fs.atomic_write(
            &content_doc_path(&new_uuid),
            &new_doc.export_snapshot().unwrap(),
        )
        .await
        .unwrap();
        fs.write("proj/new.md", new_doc.to_markdown().as_bytes())
            .await
            .unwrap();

        // Replica B: seed from V so both share the proj/ folder node, then B adds proj/new.md.
        let b = Index::new(AUTHOR_B);
        b.import_updates(&vault.index().export_snapshot().unwrap())
            .unwrap();
        b.rebuild_caches();
        b.register_document("proj/new.md", &new_uuid, &new_fp)
            .unwrap();

        // A (= V's Index) deletes the whole proj/ folder Model-II style: the file, then the
        // now-empty folder node.
        let proj_node = vault.index().find_folder_node("proj").unwrap();
        vault.index().delete_node("proj/seed.md").unwrap();
        vault.index().rebuild_caches();
        {
            let tree = vault.index().index_tree();
            tree.delete(proj_node).unwrap();
        }
        vault.index().rebuild_caches();

        // Merge B's add into A and A's delete into B (Index level, NO cascade) — now B's add
        // is SWEPT on both (dead file, parent still the dead proj/ folder node).
        let a_snap = vault.index().export_snapshot().unwrap();
        let b_snap = b.export_snapshot().unwrap();
        vault.index().import_updates(&b_snap).unwrap();
        b.import_updates(&a_snap).unwrap();
        vault.index().rebuild_caches();
        b.rebuild_caches();

        // Each replica rescues the swept orphan INDEPENDENTLY — each mints its OWN fresh
        // live proj/ folder node (distinct TreeIDs).
        rescue_all(vault.index());
        rescue_all(&b);

        // Merge B's independently-rescued state into V → two distinct live proj/ folder
        // nodes now coexist in V's Index: the un-collapsed collision, persisted below.
        vault
            .index()
            .import_updates(&b.export_snapshot().unwrap())
            .unwrap();
        vault.index().rebuild_caches();
        assert_eq!(
            alive_folder_nodes_at(vault.index(), "proj"),
            2,
            "precondition: the persisted Index holds the un-collapsed two-folder collision"
        );

        vault.save_index().await.unwrap();
        fs
    }

    /// Boot reconcile collapses a persisted two-folder collision to a SINGLE survivor folder
    /// node — the structural cascade fires on load, after the swept-orphan rescue.
    ///
    /// Without the `resolve_structural_conflicts()` call in `reconcile`, the two live `proj/`
    /// folder nodes survive the load (the rescue alone does not collapse them) and this fails
    /// with two folder nodes — the B1 RED. The concurrent add `proj/new.md` must survive
    /// either way (the collision is between FOLDER nodes; the file is not lost).
    #[tokio::test]
    async fn boot_reconcile_collapses_persisted_folder_collision() {
        let fs = stage_persisted_two_folder_collision().await;

        let reloaded = Vault::load(Arc::clone(&fs), AUTHOR_A).await.unwrap();

        assert_eq!(
            alive_folder_nodes_at(reloaded.index(), "proj"),
            1,
            "boot reconcile collapses the two-folder collision to exactly one survivor node"
        );
        // The rescued concurrent add survives the collapse (the collision was folder-vs-
        // folder; merging the folders never drops the file living under them).
        assert!(
            reloaded.index().node_for_path("proj/new.md").is_some(),
            "the concurrently-added proj/new.md survives the folder-collision collapse"
        );
    }
}
