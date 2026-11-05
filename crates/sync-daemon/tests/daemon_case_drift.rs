//! Integration tests for the Daemon event loop — case-drift sweep (Bug 1) plus
//! the daemon-config identity check.
//!
//! Real `Daemon` instances with real iroh nodes, in-memory filesystems, and
//! injected file events. Carved from the former `daemon_integration.rs` monolith;
//! the universal harness lives in `common`, while this file carries the case-drift
//! `build_daemon_no_loop`/`alive_file_node_count` helpers file-locally. These
//! tests assert synchronously, so no file-local `wait_until` is needed.
mod common;

mod daemon_case_drift {
    use std::sync::Arc;

    use tokio::sync::{Mutex, mpsc};
    use tokio_util::sync::CancellationToken;

    use p2p_core::allowlist::InMemoryAllowlist;
    use vault_sync::Vault;
    use vault_sync::fs::{FileSystem, InMemoryFs};
    use vault_sync::network::VaultGossipExt;

    use sync_daemon::daemon::Daemon;
    use sync_daemon::watcher::FileEvent;

    use super::common::{build_node, shared_vault_id, uuid_of};

    // ── case-drift sweep (Bug 1) ────────────────────────────────────────────────
    //
    // A folder case-rename (`Plans/ → plans/`) on a case-INSENSITIVE filesystem
    // fires no `Deleted` watcher event, so the move-coalescer never sees it. The
    // daemon's case-drift sweep is the reliable detection: it lists the vault
    // case-sensitively, compares against the (case-sensitive) index, and re-homes
    // the folder via ONE `move_subtree`. These tests drive the real daemon sweep
    // method (NOT `move_subtree` directly) and assert the folder move is tracked
    // with descendant UUIDs preserved and NO orphaned source-folder node — the
    // anti-ping-pong guarantee.

    /// Build a `Daemon` WITHOUT spawning its event loop, so a test can call
    /// `sweep_case_drift` (and other handlers) directly and inspect the vault
    /// afterward. Joins gossip solo (no peer) so `broadcast_change` is gated off
    /// (`alive_count() == 0`) and the sweep's structural effect is isolated.
    async fn build_daemon_no_loop(
        seed_byte: u8,
    ) -> (
        Daemon<Arc<InMemoryFs>, InMemoryAllowlist>,
        Arc<Mutex<Vault<Arc<InMemoryFs>>>>,
        Arc<InMemoryFs>,
    ) {
        let node = build_node(seed_byte).await.expect("build node");
        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await
            .expect("solo gossip join");

        let (_file_event_tx, file_event_rx) = mpsc::unbounded_channel::<FileEvent>();
        let shutdown = CancellationToken::new();
        let vault = node.vault.clone();
        let fs = node.fs.clone();

        let mut daemon = Daemon::new(
            vault.clone(),
            node.sync_node,
            gossip,
            file_event_rx,
            None,
            node.allowlist.clone(),
            "test-device".to_string(),
            None,
            "/test-vault".into(),
            shutdown,
        );
        daemon.set_inbound_seen_rx(node.inbound_seen_rx);
        daemon.set_fs(Arc::new(fs.clone()));
        (daemon, vault, fs)
    }

    /// Count alive FILE nodes in the index — the ghost-mint guard (a subtree move
    /// re-homes existing nodes, so this is unchanged; a fresh-UUID re-create would
    /// bump it).
    async fn alive_file_node_count(vault: &Arc<Mutex<Vault<Arc<InMemoryFs>>>>) -> usize {
        vault
            .lock()
            .await
            .index()
            .scan_structural_nodes()
            .iter()
            .filter(|n| matches!(n, vault_sync::index::StructuralNode::File { .. }))
            .count()
    }

    /// Index `Plans/a.md` + `Plans/b.md`, then case-rename the on-disk directory
    /// to `plans/` while the index keeps the `Plans/` casing — exactly the state a
    /// case-insensitive-fs folder rename leaves (the index never updated because no
    /// `Deleted` event fired). `InMemoryFs` is case-SENSITIVE, so writing the
    /// lowercase paths + deleting the uppercase ones makes `list_files` return the
    /// new casing while the index retains the old — reproducing the drift directly.
    #[tokio::test]
    async fn case_drift_sweep_tracks_folder_rename_with_no_orphan_folder_node() {
        let (mut daemon, vault, fs) = build_daemon_no_loop(60).await;

        // Index the two files at the UPPERCASE folder casing, capture their UUIDs.
        for (path, content) in [("Plans/a.md", b"# A".as_slice()), ("Plans/b.md", b"# B")] {
            fs.write(path, content).await.unwrap();
            vault.lock().await.on_file_changed(path).await.unwrap();
        }
        let uuid_a = uuid_of(&vault, "Plans/a.md").await.expect("a indexed");
        let uuid_b = uuid_of(&vault, "Plans/b.md").await.expect("b indexed");
        let file_count_before = alive_file_node_count(&vault).await;

        // Disk now reads `plans/` (lowercase): write the new paths, remove the old.
        // The index still holds `Plans/*` — the drift the sweep must heal.
        fs.write("plans/a.md", b"# A").await.unwrap();
        fs.write("plans/b.md", b"# B").await.unwrap();
        fs.delete("Plans/a.md").await.unwrap();
        fs.delete("Plans/b.md").await.unwrap();

        daemon.sweep_case_drift().await;

        // The folder move is TRACKED: both files now resolve at the lowercase path
        // under their ORIGINAL UUIDs (one `move_subtree`, not fresh-UUID re-creates).
        assert_eq!(
            uuid_of(&vault, "plans/a.md").await.as_deref(),
            Some(uuid_a.as_str()),
            "a's UUID is preserved at the re-homed lowercase path"
        );
        assert_eq!(
            uuid_of(&vault, "plans/b.md").await.as_deref(),
            Some(uuid_b.as_str()),
            "b's UUID is preserved at the re-homed lowercase path"
        );
        // The stale uppercase paths have no live node anymore.
        assert!(
            uuid_of(&vault, "Plans/a.md").await.is_none(),
            "the stale uppercase path is vacated"
        );

        // No ghost mint: the file count is unchanged (a subtree move, not re-creates).
        assert_eq!(
            alive_file_node_count(&vault).await,
            file_count_before,
            "a subtree move re-homes existing nodes — no new file nodes minted"
        );

        // THE anti-ping-pong guarantee: NO live `Plans/` folder node remains, so
        // `materialize_folders` cannot re-mkdir the stale casing.
        let folders = vault.lock().await.index().folder_paths();
        let stale_plans_alive = folders.iter().any(|f| f.path == "Plans" && !f.is_deleted);
        assert!(
            !stale_plans_alive,
            "the source folder node was re-homed by move_subtree — no orphaned live `Plans/` node"
        );
        let lowercase_plans_alive = folders.iter().any(|f| f.path == "plans" && !f.is_deleted);
        assert!(
            lowercase_plans_alive,
            "the re-homed folder node lives at the lowercase casing"
        );
    }

    /// A second sweep after the casing has converged is a no-op: no further moves,
    /// no further broadcasts — the sweep is idempotent (the property that keeps it
    /// safe to run on a steady tick).
    #[tokio::test]
    async fn case_drift_sweep_is_idempotent_after_convergence() {
        let (mut daemon, vault, fs) = build_daemon_no_loop(61).await;

        fs.write("Plans/a.md", b"# A").await.unwrap();
        vault
            .lock()
            .await
            .on_file_changed("Plans/a.md")
            .await
            .unwrap();
        fs.write("plans/a.md", b"# A").await.unwrap();
        fs.delete("Plans/a.md").await.unwrap();

        daemon.sweep_case_drift().await;
        let uuid_after_first = uuid_of(&vault, "plans/a.md").await;

        // Second sweep: disk and index now agree → detect_case_drift returns empty,
        // so the index is untouched (same UUID, same path).
        daemon.sweep_case_drift().await;
        assert_eq!(
            uuid_of(&vault, "plans/a.md").await,
            uuid_after_first,
            "a converged casing produces no further moves on re-sweep"
        );
    }

    /// The daemon's persisted PeerId must equal the identity key's PeerId after
    /// `load_or_generate` — `DaemonConfig` derives its `peer_id` from the identity
    /// key, and a divergence would let the daemon advertise an identity it can't
    /// authenticate with. This invariant was asserted by the IdentityKey unit
    /// tests until those moved to p2p-core, where `DaemonConfig` isn't visible;
    /// this re-pins the daemon-config↔identity seam on the daemon side (Inc0-2
    /// foundation-review carry-forward S1).
    #[tokio::test]
    async fn daemon_config_peer_id_matches_identity_key() -> anyhow::Result<()> {
        use sync_daemon::persistence::DaemonConfig;
        use tempfile::TempDir;

        // Fresh vault with no existing daemon.key → load_or_generate mints a new
        // identity and writes the config from it.
        let vault_dir = TempDir::new()?;
        let (config, identity) = DaemonConfig::load_or_generate(vault_dir.path(), None).await?;

        assert_eq!(
            config.peer_id,
            identity.peer_id(),
            "DaemonConfig.peer_id must match the identity key it was generated from"
        );

        Ok(())
    }
}
