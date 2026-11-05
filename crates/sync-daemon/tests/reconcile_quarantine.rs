/// Integration test: reconcile quarantines tombstoned disk orphans on a real fs.
///
/// Background: untracked disk orphans — `.md` files on disk whose registry state
/// is tombstoned — accrue from historical sync bugs and offline windows. Startup
/// reconcile moves them to `.trash/<path>` instead of resurrecting them as new
/// file nodes.
///
/// This exercises the NativeFs layer specifically: `InMemoryFs` and `NativeFs`
/// have diverged before (the ENOENT/NotFound mapping bug), which is why the fs
/// conformance suite exists. The quarantine path does real read/write/delete +
/// parent-dir creation under `.trash/`, so a NativeFs-on-tempdir test catches any
/// real-fs divergence at the layer boundary.
mod reconcile_quarantine {
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

    /// A historical orphan strand — a `.md` file on disk whose registry node was
    /// tombstoned — must be quarantined to `.trash/` on `Vault::load`, and a second
    /// load must be idempotent (no re-quarantine, no nested `.trash/.trash/`).
    ///
    /// Steps:
    /// 1. Init the vault on a real tempdir, create `dupe.md`, index it (so it has a
    ///    registry node + `.loro` doc).
    /// 2. `delete_file("dupe.md")` — tombstones the node and removes the `.loro`,
    ///    persisting the tombstone to the registry.
    /// 3. Write `dupe.md` back to disk directly — the orphan strand.
    /// 4. `Vault::load` a fresh vault on the same tempdir → reconcile runs → assert
    ///    `dupe.md` is gone from the vault root and present under `.trash/dupe.md`,
    ///    and the registry has no alive node for it.
    /// 5. `Vault::load` a second time → assert idempotent: `.trash/dupe.md` exists
    ///    exactly once and `.trash/.trash/dupe.md` does NOT exist.
    #[tokio::test]
    async fn reconcile_quarantines_tombstoned_orphan_on_native_fs() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));

        // 1. Init + index dupe.md.
        let vault = Vault::init(fs.clone(), test_author())
            .await
            .expect("vault init");
        fs.write("dupe.md", b"# Dupe").await.expect("write dupe.md");
        vault
            .on_file_changed("dupe.md")
            .await
            .expect("index dupe.md");

        // 2. Delete it — tombstones the node and removes the .loro; persists.
        std::fs::remove_file(dir.path().join("dupe.md")).expect("remove dupe.md from disk");
        vault.delete_file("dupe.md").await.expect("delete dupe.md");
        drop(vault);

        // 3. Recreate the orphan strand directly on disk.
        std::fs::write(dir.path().join("dupe.md"), b"# Dupe").expect("recreate orphan strand");

        // 4. Load fresh — reconcile runs during load and quarantines the orphan.
        let vault = Vault::load(fs.clone(), test_author())
            .await
            .expect("vault load");

        assert!(
            !dir.path().join("dupe.md").exists(),
            "the orphan must be removed from its original vault path"
        );
        assert!(
            dir.path().join(".trash/dupe.md").exists(),
            "the orphan must be quarantined under .trash/"
        );
        // list_files is a filesystem walk that skips dot-directories, so the orphan
        // (now under .trash/) is absent — confirming the consumer-visible file set no
        // longer includes it.
        assert!(
            !vault
                .list_files()
                .await
                .unwrap()
                .contains(&"dupe.md".to_string()),
            "the quarantined orphan must not appear in list_files"
        );
        // Registry-truth check: quarantine never resurrected a node, so the path has
        // no live node in the Index tree (delete_file tombstoned it; quarantine never
        // creates one).
        assert!(
            vault.index().node_for_path("dupe.md").is_none(),
            "the quarantined orphan must have no live node in the registry"
        );
        drop(vault);

        // 5. Second load — must be idempotent.
        let _vault = Vault::load(fs.clone(), test_author())
            .await
            .expect("second vault load");

        assert!(
            dir.path().join(".trash/dupe.md").exists(),
            ".trash/dupe.md must still exist after the second load"
        );
        assert!(
            !dir.path().join(".trash/.trash/dupe.md").exists(),
            "trash contents must never be re-quarantined into a nested .trash/.trash/"
        );

        // The single trash entry must be exactly one file — no collision-suffixed
        // duplicate (e.g. .trash/dupe.md.1) created by a redundant second pass.
        let trash_entries: Vec<_> = std::fs::read_dir(dir.path().join(".trash"))
            .expect("read .trash dir")
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            trash_entries,
            vec!["dupe.md".to_string()],
            ".trash/ must contain exactly one entry (dupe.md), got: {:?}",
            trash_entries
        );
    }
}
