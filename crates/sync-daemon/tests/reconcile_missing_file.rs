/// Integration test: boot reconcile is resilient to a per-file failure on a real fs.
///
/// Background (the legacy sync crate lineage of this guard): `NativeFs` once mapped every IO
/// error to `FsError::Io`, but the skip-deleted-file guard only matches
/// `FsError::NotFound`. On native vaults a `NotFound` that surfaced as `Io` aborted the
/// whole pass — taking unrelated files down with the one that failed. `InMemoryFs`
/// returned proper `NotFound`, so the bug was invisible in tests (test/prod divergence),
/// which is why the fs conformance suite now pins the NotFound contract for both
/// implementations (`native_fs_passes_conformance_suite`).
///
/// In vault-sync the per-file-error-must-not-abort-the-pass guard lives in boot reconcile
/// (`Vault::reconcile`, run during `Vault::load`): a file that fails mid-pass (e.g.
/// race-deleted, or a tombstoned strand that fails to quarantine) is logged and skipped,
/// and the rest of the pass still runs. (The inbound `process_message` path has no
/// per-file reconcile loop — its `ensure_consistency` hook is a no-op — so this guard is
/// reachable only through boot reconcile.) This test exercises that on a real NativeFs
/// vault: a mixed pass with one file that needs special handling (a tombstoned disk
/// strand to quarantine) plus an unrelated valid file must process BOTH — the strand's
/// handling never aborts the valid file's reconcile.
mod reconcile_missing_file {
    use std::sync::Arc;

    use p2p_core::PeerId;
    use sync_daemon::NativeFs;
    use tempfile::tempdir;
    use vault_sync::Vault;
    use vault_sync::fs::FileSystem;

    /// Deterministic author seed. vault-sync authors Loro ops under a bare u64.
    fn test_author() -> u64 {
        PeerId::from_secret_bytes([42u8; 32]).as_u64()
    }

    /// A boot-reconcile pass over a mix of (a tombstoned disk strand that must be
    /// quarantined) and (an unrelated valid indexed file) processes BOTH — one file's
    /// special handling never aborts the rest of the pass.
    ///
    /// Steps:
    /// 1. Init the vault on a real tempdir; create `keep.md` and `victim.md`, index both
    ///    so each has a registry node + `.loro` doc.
    /// 2. Delete `victim.md` (tombstones its node, removes its `.loro`, persists), then
    ///    recreate `victim.md` on disk directly — a tombstoned disk strand.
    /// 3. `Vault::load` a fresh vault on the same tempdir → boot reconcile runs.
    /// 4. Assert the pass completed: `victim.md` was quarantined to `.trash/` (and is gone
    ///    from its original path), AND `keep.md` survived intact — proving the strand's
    ///    quarantine did not abort `keep.md`'s reconcile.
    #[tokio::test]
    async fn boot_reconcile_processes_siblings_when_one_file_needs_quarantine() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");

        // Create both files and index them so each has a .loro doc on disk.
        fs.write("keep.md", b"# Keep").await.expect("write keep.md");
        fs.write("victim.md", b"# Victim")
            .await
            .expect("write victim.md");
        vault
            .on_file_changed("keep.md")
            .await
            .expect("index keep.md");
        vault
            .on_file_changed("victim.md")
            .await
            .expect("index victim.md");

        // Tombstone victim.md (removes its node + .loro, persists), then recreate it on
        // disk directly — a tombstoned disk strand that boot reconcile must quarantine.
        std::fs::remove_file(dir.path().join("victim.md")).expect("remove victim.md from disk");
        vault
            .delete_file("victim.md")
            .await
            .expect("delete victim.md");
        drop(vault);
        std::fs::write(dir.path().join("victim.md"), b"# Victim")
            .expect("recreate the tombstoned strand");

        // Load fresh — boot reconcile runs during load and must process the whole pass.
        let reloaded = Vault::load(fs.clone(), test_author())
            .await
            .expect("vault load must succeed even though one file needs quarantine");

        // The tombstoned strand was quarantined (its special handling ran)...
        assert!(
            !dir.path().join("victim.md").exists(),
            "the tombstoned strand must be removed from its original path"
        );
        assert!(
            dir.path().join(".trash/victim.md").exists(),
            "the tombstoned strand must be quarantined under .trash/"
        );

        // ...AND the unrelated valid file was NOT collateral damage — it survived with a
        // live node, proving the strand's handling did not abort the rest of the pass.
        assert!(
            reloaded.index().node_for_path("keep.md").is_some(),
            "keep.md must still have a live node after the reconcile pass"
        );
        assert!(
            reloaded
                .list_files()
                .await
                .unwrap()
                .contains(&"keep.md".to_string()),
            "keep.md must remain in the consumer-visible file set"
        );
    }
}
