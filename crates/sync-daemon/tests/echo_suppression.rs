/// Tests for the echo-safe Flow-1 vault primitives (`on_file_changed` / `delete_file`).
///
/// Echo suppression is now a CONTENT-DIFF, not a path-keyed sync flag. The daemon's
/// `on_file_modified`/`on_file_deleted` no longer early-return on any flag; they always
/// apply the vault primitive and gate the broadcast on its returned bool:
/// - `Vault::on_file_changed` returns `Result<bool>` — `true` only when the body/frontmatter
///   actually changed, so re-applying identical content (the watcher echo after an inbound
///   sync write) returns `false` and suppresses a re-broadcast.
/// - `Vault::delete_file` returns `Result<bool>` — `true` only when a live node was
///   tombstoned (idempotent: a no-op delete returns `false`).
///
/// These tests cover that content-diff echo-safety and the persist/tombstone properties
/// directly. They use NativeFs on a `tempfile::tempdir` to match the production codepath
/// (InMemoryFs masks the class of bug the prior ENOENT fix addressed).
mod echo_suppression {
    use std::sync::Arc;

    use p2p_core::PeerId;
    use sync_daemon::NativeFs;
    use tempfile::tempdir;
    use vault_sync::Vault;
    use vault_sync::fs::FileSystem;

    /// Deterministic author seed. vault-sync authors Loro ops under a bare u64.
    fn test_author() -> u64 {
        PeerId::from_secret_bytes([99u8; 32]).as_u64()
    }

    // ── on_file_changed return value ─────────────────────────────────────────

    /// `on_file_changed` returns `true` when the file body has changed.
    #[tokio::test]
    async fn on_file_changed_returns_true_when_body_changes() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        // Create and index the file with initial content.
        fs.write("note.md", b"# Original").await.expect("write");
        let first = vault
            .on_file_changed("note.md")
            .await
            .expect("first on_file_changed");
        // New document creation always counts as a change.
        assert!(
            first,
            "on_file_changed should return true for a new document"
        );

        // Now update the file content on disk and call on_file_changed again.
        fs.write("note.md", b"# Modified").await.expect("overwrite");
        let second = vault
            .on_file_changed("note.md")
            .await
            .expect("second on_file_changed");
        assert!(
            second,
            "on_file_changed should return true when body changes"
        );
    }

    /// Calling `on_file_changed` twice with identical content returns `false`
    /// the second time — the echo-safe diff detects no delta.
    ///
    /// This is what makes the new broadcast-gating safe: if the OS watcher fires
    /// a spurious event after sync writes the file, the second call is a no-op
    /// and `changed = false` suppresses the broadcast.
    #[tokio::test]
    async fn on_file_changed_returns_false_for_unchanged_echo() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        fs.write("note.md", b"# Stable content")
            .await
            .expect("write");

        // Index the file for the first time.
        let first = vault
            .on_file_changed("note.md")
            .await
            .expect("first on_file_changed");
        assert!(
            first,
            "on_file_changed should return true for a new document"
        );

        // Call again with identical disk content — no diff → false.
        let second = vault
            .on_file_changed("note.md")
            .await
            .expect("second on_file_changed with same content");
        assert!(
            !second,
            "on_file_changed should return false when content is unchanged (echo suppression)"
        );
    }

    /// A real edit is applied and its `.loro` snapshot persisted to disk.
    ///
    /// Echo-safety is now a content-diff inside `on_file_changed` (it returns `false`
    /// only when the body is unchanged — see `on_file_changed_returns_false_for_unchanged_echo`),
    /// not a path-keyed sync flag. There is no flag to "arm" anymore, so this test proves
    /// the property the old flag-based variant ultimately guarded: a genuine edit returns
    /// `true` and the new content reaches the on-disk `.loro`.
    ///
    /// The on-disk assertion uses a cold `Vault::load` over the same tempdir — this
    /// exercises the real NativeFs read path and confirms the .loro snapshot persisted,
    /// not just that the in-memory cache advanced.
    #[tokio::test]
    async fn real_edit_is_applied_and_persisted_to_loro() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        // Index the file with initial content.
        fs.write("note.md", b"# Initial")
            .await
            .expect("write initial");
        vault
            .on_file_changed("note.md")
            .await
            .expect("index initial");

        // Edit the file on disk.
        fs.write("note.md", b"# Edited after sync")
            .await
            .expect("write edit");

        // A real content change is applied.
        let changed = vault
            .on_file_changed("note.md")
            .await
            .expect("on_file_changed after edit");
        assert!(changed, "on_file_changed must return true for a real edit");

        // Drop the first vault and open a cold second vault over the same tempdir.
        // This proves the .loro snapshot was actually persisted to disk — not just
        // that the in-memory document cache advanced.
        drop(vault);
        let cold_vault = Vault::load(fs, test_author())
            .await
            .expect("cold vault load");
        let doc = cold_vault
            .get_document("note.md")
            .await
            .expect("get document from cold vault");
        let body_text = doc.body().to_string();
        assert!(
            body_text.contains("Edited after sync"),
            "cold vault must see the edited content — .loro snapshot must have been persisted; got: {:?}",
            body_text
        );
    }

    // ── delete_file return value ───────────────────────────────────────────────

    /// `delete_file` returns `true` when a live tree node is tombstoned.
    #[tokio::test]
    async fn delete_file_returns_true_for_live_node() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        fs.write("to-delete.md", b"# Delete me")
            .await
            .expect("write");
        vault.on_file_changed("to-delete.md").await.expect("index");

        let deleted = vault
            .delete_file("to-delete.md")
            .await
            .expect("delete_file");
        assert!(
            deleted,
            "delete_file should return true when deleting a live node"
        );
    }

    /// `delete_file` returns `false` when called a second time on the same path
    /// (the node is already tombstoned — idempotent no-op).
    #[tokio::test]
    async fn delete_file_returns_false_for_already_deleted() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        fs.write("to-delete.md", b"# Delete me")
            .await
            .expect("write");
        vault.on_file_changed("to-delete.md").await.expect("index");

        let first_delete = vault
            .delete_file("to-delete.md")
            .await
            .expect("first delete_file");
        assert!(first_delete, "first delete_file should return true");

        let second_delete = vault
            .delete_file("to-delete.md")
            .await
            .expect("second delete_file (idempotent)");
        assert!(
            !second_delete,
            "second delete_file should return false (already-deleted no-op)"
        );
    }

    /// A real delete tombstones the registry entry.
    ///
    /// Like the edit case above, the path-keyed sync flag this test once "armed" is gone
    /// (echo-safety moved into the `on_file_changed`/`delete_file` content-diff), so this
    /// proves the property directly: a genuine delete returns `true` and records the
    /// tombstone in the Index tree.
    #[tokio::test]
    async fn real_delete_tombstones_the_node() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        fs.write("flagged.md", b"# Will be deleted")
            .await
            .expect("write");
        vault.on_file_changed("flagged.md").await.expect("index");

        // A real local delete tombstones the node.
        let deleted = vault.delete_file("flagged.md").await.expect("delete_file");
        assert!(deleted, "delete_file must return true for a real delete");

        // The vault must now report the file as deleted (the tombstone is recorded
        // in the Index tree).
        assert!(
            vault.index().is_path_deleted("flagged.md"),
            "vault must report the file as deleted after delete_file"
        );
    }
}
