//! Acceptance tests for the Index in isolation — the UUID-keyed catalog.
//!
//! These drive the Index's public API the way the future `Vault<F>` handle (and
//! the compare protocol) will: register a document with its minted UUID, move it,
//! delete it, and round-trip the catalog through a save+load. The headline
//! property under test is UUID identity — a node's UUID is stable across a move,
//! and a move touches no content (its `docs/<uuid>.loro` path never changes).

use uuid::Uuid;
use vault_sync::{ContentDoc, InMemoryFs, Index, content_version_fingerprint};

/// A device's Loro author id for the index doc.
const AUTHOR: u64 = 0x0101_0101_0101_0101;

/// Build a content doc from markdown and return it alongside its minted UUID and
/// the version fingerprint the Index will denormalize — the trio a caller hands to
/// `register_document`.
fn doc_with_identity(markdown: &str) -> (ContentDoc, Uuid, [u8; 32]) {
    let doc = ContentDoc::from_markdown(markdown, AUTHOR).unwrap();
    let uuid = Uuid::parse_str(&doc.doc_id().expect("a fresh doc mints a UUID")).unwrap();
    let fingerprint = content_version_fingerprint(&doc.version());
    (doc, uuid, fingerprint)
}

mod register_document {
    use super::*;

    /// Registering a document creates a file node carrying its `uuid` and `path`
    /// meta, records the document's `content_version` in the Index's local table, and
    /// populates BOTH lookup caches (forward path→node and inverse uuid→node).
    #[test]
    fn writes_uuid_and_path_meta_records_content_version_and_fills_both_caches() {
        let index = Index::new(AUTHOR);
        let (_doc, uuid, fingerprint) = doc_with_identity("# Title\n\nBody.");

        let node = index
            .register_document("notes/topic.md", &uuid, &fingerprint)
            .unwrap();

        // Identity meta landed (uuid + recoverable path) and the content_version
        // fingerprint is recorded in the local table (read back via the node).
        assert_eq!(
            index.node_uuid(&node),
            Some(uuid),
            "node carries its UUID meta"
        );
        assert_eq!(
            index.node_content_version(&node),
            Some(fingerprint),
            "the node's content_version fingerprint is recorded in the local table"
        );
        assert_eq!(
            index.path_for_node(&node).as_deref(),
            Some("notes/topic.md"),
            "node resolves to its registered path"
        );

        // Forward cache: a path resolves to the node.
        assert_eq!(
            index.node_for_path("notes/topic.md"),
            Some(node),
            "forward path→node cache is populated"
        );
        // Inverse cache: the UUID resolves to the node.
        assert_eq!(
            index.find_node_by_uuid(&uuid),
            Some(node),
            "inverse uuid→node cache is populated"
        );
    }

    /// The `content_version` fingerprint is LOCAL-only: it must NEVER reach the synced
    /// Index CRDT. A peer's value LWW-merging in ahead of the content it fingerprints is
    /// the convergence trap Resolution #2 closes (the digest would falsely report "in
    /// sync"), so the fingerprint lives in a per-replica-local table, not node meta.
    ///
    /// This pins the absence structurally: the only meta keys the Index serializes are
    /// `type`/`name`/`path`/`uuid`, so the literal key string `content_version` must NOT
    /// appear anywhere in an exported Index snapshot. (It WOULD appear on the pre-C2
    /// dual-write code, which `meta.insert`ed the key — this is that removal's red guard.)
    #[test]
    fn content_version_is_absent_from_the_synced_index_snapshot() {
        let index = Index::new(AUTHOR);
        let (_doc, uuid, fingerprint) = doc_with_identity("# Title\n\nBody.");
        index
            .register_document("notes/topic.md", &uuid, &fingerprint)
            .unwrap();

        let snapshot = index.export_snapshot().unwrap();

        // The meta key is a UTF-8 string in the serialized tree; if `register_document`
        // (or any path) wrote it as node meta, the key bytes would be present here.
        let key_bytes = b"content_version";
        let contains_key = snapshot
            .windows(key_bytes.len())
            .any(|window| window == key_bytes);
        assert!(
            !contains_key,
            "the content_version meta key must not appear in a synced Index snapshot — \
             the fingerprint is per-replica-local, never CRDT state"
        );
    }

    /// Re-registering the same path is idempotent: it returns the existing node
    /// rather than creating a duplicate.
    #[test]
    fn re_registering_same_path_returns_existing_node() {
        let index = Index::new(AUTHOR);
        let (_doc, uuid, fingerprint) = doc_with_identity("body");

        let first = index
            .register_document("a.md", &uuid, &fingerprint)
            .unwrap();
        let second = index
            .register_document("a.md", &uuid, &fingerprint)
            .unwrap();

        assert_eq!(first, second, "the second register returns the same node");
    }
}

mod move_node {
    use super::*;

    /// Moving a node is pure-structural: the tree move fires (the node now resolves
    /// to the new path, not the old), the caches follow, and a move touches neither
    /// the node's UUID identity nor its denormalized `content_version` — the two
    /// content-bearing facts on the node. A move that mutated either (e.g. by
    /// recomputing identity from the new path) would fail here.
    ///
    /// This guards the Index-level half of INV-1 (a move is structure-only). The
    /// end-to-end *zero-content-bytes* guarantee — that a move re-transfers no
    /// document payload — is proven by AC-INV-1 in Chunk 1f, which byte-counts the
    /// sync payloads on a move. (The other half of "no content" here is structural:
    /// the Index holds no filesystem, so a tree op cannot touch a content `.loro`.)
    #[test]
    fn relocates_node_keeps_uuid_and_content_version_stable() {
        let index = Index::new(AUTHOR);
        let (_doc, uuid, fingerprint) = doc_with_identity("# Note\n\nText.");
        let node = index
            .register_document("old/place.md", &uuid, &fingerprint)
            .unwrap();

        // Capture the denormalized content fingerprint before the move so we can
        // assert the structural move leaves it untouched.
        let content_version_before = index
            .node_content_version(&node)
            .expect("a registered node carries a content_version");

        index.move_node("old/place.md", "new/home.md").unwrap();

        // The same node now lives at the new path; the old path is gone.
        assert_eq!(
            index.node_for_path("new/home.md"),
            Some(node),
            "the node resolves at its new path"
        );
        assert_eq!(
            index.node_for_path("old/place.md"),
            None,
            "the node no longer resolves at its old path"
        );
        assert_eq!(
            index.path_for_node(&node).as_deref(),
            Some("new/home.md"),
            "walking the tree yields the new path"
        );

        // UUID identity is stable across the move — same node, same UUID.
        assert_eq!(
            index.node_uuid(&node),
            Some(uuid),
            "the node's UUID is unchanged by the move"
        );
        assert_eq!(
            index.find_node_by_uuid(&uuid),
            Some(node),
            "the inverse cache still resolves the UUID to the same node"
        );

        // No content-side effect: a structural move must not touch the denormalized
        // content fingerprint (the content doc's state is unchanged, so its derived
        // cache must be too). This genuinely fails if `move_node` ever mutates
        // `content_version`.
        assert_eq!(
            index.node_content_version(&node),
            Some(content_version_before),
            "the node's content_version is unchanged by a move"
        );
    }

    /// A move whose target path already has a node is rejected (no clobbering).
    #[test]
    fn rejects_move_onto_an_occupied_target() {
        let index = Index::new(AUTHOR);
        let (_a, uuid_a, fp_a) = doc_with_identity("a");
        let (_b, uuid_b, fp_b) = doc_with_identity("b");
        index.register_document("a.md", &uuid_a, &fp_a).unwrap();
        index.register_document("b.md", &uuid_b, &fp_b).unwrap();

        let result = index.move_node("a.md", "b.md");
        assert!(result.is_err(), "moving onto an occupied target must fail");
    }

    /// A move whose source has no node is rejected — the catalog does not perform
    /// the fs-level recovery the old `rename_file` did (that is a flow concern).
    #[test]
    fn rejects_move_with_missing_source() {
        let index = Index::new(AUTHOR);
        let result = index.move_node("ghost.md", "elsewhere.md");
        assert!(result.is_err(), "moving a non-existent source must fail");
    }
}

mod delete_node {
    use super::*;

    /// Deleting a node tombstones it (it no longer resolves) and arms the
    /// deleted-paths guard so an inbound update can't resurrect it before the next
    /// sync.
    #[test]
    fn tombstones_node_and_arms_deleted_paths_guard() {
        let index = Index::new(AUTHOR);
        let (_doc, uuid, fingerprint) = doc_with_identity("body");
        index
            .register_document("doomed.md", &uuid, &fingerprint)
            .unwrap();

        let tombstoned = index.delete_node("doomed.md").unwrap();

        assert!(tombstoned, "deleting a live node reports a tombstone");
        assert_eq!(
            index.node_for_path("doomed.md"),
            None,
            "the path no longer resolves to a node"
        );
        assert!(
            index.is_node_deleted("doomed.md"),
            "the node reads as deleted"
        );
        // The resurrection guard is armed synchronously.
        assert!(
            index.is_path_deleted("doomed.md"),
            "the deleted-paths guard is armed for the deleted path"
        );
    }

    /// Deleting an unknown path is an idempotent no-op (reports `false`, records no
    /// tombstone).
    #[test]
    fn deleting_unknown_path_is_a_noop() {
        let index = Index::new(AUTHOR);
        let tombstoned = index.delete_node("never-existed.md").unwrap();
        assert!(!tombstoned, "deleting an unknown path reports no tombstone");
    }
}

mod validate_sync_path {
    use super::*;

    /// Registration rejects unsafe or non-markdown paths — the security gate that
    /// keeps the catalog confined to vault-relative markdown.
    #[test]
    fn register_rejects_unsafe_and_non_markdown_paths() {
        let index = Index::new(AUTHOR);
        let (_doc, uuid, fp) = doc_with_identity("body");

        // Path traversal.
        assert!(
            index.register_document("../escape.md", &uuid, &fp).is_err(),
            "path traversal is rejected"
        );
        // Absolute path.
        assert!(
            index
                .register_document("/etc/passwd.md", &uuid, &fp)
                .is_err(),
            "absolute path is rejected"
        );
        // Non-markdown.
        assert!(
            index
                .register_document("notes/data.json", &uuid, &fp)
                .is_err(),
            "non-markdown path is rejected"
        );
        // Empty.
        assert!(
            index.register_document("", &uuid, &fp).is_err(),
            "empty path is rejected"
        );
    }

    #[test]
    fn adjacent_dots_in_names_do_not_count_as_parent_directory_segments() {
        let index = Index::new(AUTHOR);

        for path in [
            "knowledge/Prime Curation Log — W34 (2026-08-20..24).md",
            "a..b/notes...md",
        ] {
            let (_doc, uuid, fp) = doc_with_identity(path);
            let node = index
                .register_document(path, &uuid, &fp)
                .unwrap_or_else(|error| panic!("legitimate adjacent-dot path {path:?} was rejected: {error}"));
            assert_eq!(index.node_for_path(path), Some(node));
        }

        for path in ["../x.md", "a/../x.md", "a/../../x.md", "a/..", "a/../"] {
            let (_doc, uuid, fp) = doc_with_identity(path);
            let error = index.register_document(path, &uuid, &fp).unwrap_err();
            assert_eq!(error.to_string(), "Path traversal not allowed", "{path:?}");
        }

        for path in [r"..\x.md", r"a/..\x.md"] {
            let (_doc, uuid, fp) = doc_with_identity(path);
            let error = index.register_document(path, &uuid, &fp).unwrap_err();
            assert_eq!(error.to_string(), "Backslash in path not allowed", "{path:?}");
        }

        let (_doc, uuid, fp) = doc_with_identity("a//x.md");
        let error = index.register_document("a//x.md", &uuid, &fp).unwrap_err();
        assert_eq!(error.to_string(), "Empty path segment not allowed");
    }
}

mod rebuild_caches {
    use super::*;

    /// After a fresh save + load (which starts with empty caches and rebuilds them
    /// from the persisted tree), BOTH lookup directions round-trip: every alive
    /// document resolves forward (path→node) and inverse (uuid→node), and a
    /// tombstoned path is recovered into the deleted-paths guard.
    #[tokio::test]
    async fn round_trips_both_directions_after_save_and_load() {
        let fs = InMemoryFs::new();

        // Build a catalog: two live docs (one moved) and one deleted doc.
        let (_a, uuid_a, fp_a) = doc_with_identity("alpha");
        let (_b, uuid_b, fp_b) = doc_with_identity("bravo");
        let (_c, uuid_c, fp_c) = doc_with_identity("charlie");
        {
            let index = Index::new(AUTHOR);
            index
                .register_document("dir/alpha.md", &uuid_a, &fp_a)
                .unwrap();
            index.register_document("bravo.md", &uuid_b, &fp_b).unwrap();
            index
                .register_document("charlie.md", &uuid_c, &fp_c)
                .unwrap();
            // Move one and delete another so the rebuild exercises both the
            // relocated-path and the deleted-path derivations.
            index.move_node("bravo.md", "moved/bravo.md").unwrap();
            index.delete_node("charlie.md").unwrap();
            index.save_index(&fs).await.unwrap();
        }

        // Load into a fresh Index with empty caches — rebuild_caches runs on load.
        let reloaded = Index::load_index(&fs, AUTHOR).await.unwrap();

        // Forward direction: alive docs resolve at their current paths.
        let node_a = reloaded
            .node_for_path("dir/alpha.md")
            .expect("alpha resolves forward after reload");
        let node_b = reloaded
            .node_for_path("moved/bravo.md")
            .expect("the moved bravo resolves at its new path after reload");

        // Inverse direction: the same nodes resolve from their UUIDs.
        assert_eq!(
            reloaded.find_node_by_uuid(&uuid_a),
            Some(node_a),
            "alpha resolves inverse (uuid→node) after reload"
        );
        assert_eq!(
            reloaded.find_node_by_uuid(&uuid_b),
            Some(node_b),
            "the moved bravo resolves inverse (uuid→node) after reload"
        );

        // The deleted doc is absent from both caches, and its path is recovered into
        // the deleted-paths guard so it can't be resurrected.
        assert_eq!(
            reloaded.node_for_path("charlie.md"),
            None,
            "the deleted path does not resolve forward after reload"
        );
        assert_eq!(
            reloaded.find_node_by_uuid(&uuid_c),
            None,
            "the deleted doc's UUID does not resolve after reload"
        );
        assert!(
            reloaded.is_path_deleted("charlie.md"),
            "the deleted path is recovered into the deleted-paths guard on rebuild"
        );

        // The moved node kept its UUID across persistence.
        assert_eq!(
            reloaded.node_uuid(&node_b),
            Some(uuid_b),
            "the moved doc's UUID survives save+load"
        );
    }
}
