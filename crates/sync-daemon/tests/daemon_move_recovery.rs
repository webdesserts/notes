//! Integration tests for the Daemon event loop — native-move coalescing (P4f-1),
//! boot move crash-recovery (P4f-2b-ii), and graceful-shutdown drain (P4f-2c).
//!
//! The inbound-sync freshness signal (S2) test was peeled into
//! `daemon_inbound_freshness.rs` to keep this file under the line-size target.
//!
//! Real `Daemon` instances with real iroh nodes, in-memory filesystems, and
//! injected file events. Carved from the former `daemon_integration.rs` monolith;
//! the universal build/connect/spawn harness lives in `common`, while this file
//! carries the move-coalescer/journal/stage/restitch helper cluster file-locally
//! (it is crash-recovery-specific, used only by these groups). The 2-arg
//! `wait_until` is file-local (name-collides with the relay `common::wait_until`).
mod common;

mod daemon_move_recovery {
    use std::sync::Arc;
    use std::time::Duration;

    use p2p_core::MemoryLookup;
    use tokio::sync::{Mutex, mpsc};
    use tokio_util::sync::CancellationToken;

    use p2p_core::PeerId;
    use p2p_core::allowlist::InMemoryAllowlist;
    use uuid::Uuid;
    use vault_sync::fs::{FileSystem, InMemoryFs};
    use vault_sync::network::{SyncNode, SyncNodeSeam, VaultGossipExt};
    use vault_sync::{ContentDoc, Vault, content_hash};

    use sync_daemon::daemon::Daemon;
    // Boot-recovery helpers shared with `startup_inner` (P4f-2b-ii) — the
    // `spawn_daemon_from_loaded` harness runs the SAME sequence as production.
    use sync_daemon::daemon::{clear_pending_journal, read_pending_journal, restitch_inputs};
    use sync_daemon::move_coalescer::{
        JournaledMove, PENDING_MOVES_VERSION, PendingKind, PendingMovesFile, hex_lower,
    };
    use sync_daemon::watcher::FileEvent;

    use super::common::{
        TestDaemon, build_node, connect_nodes, inject_deleted, inject_modified, shared_vault_id,
        spawn_daemon, uuid_of,
    };

    /// Poll until `predicate` returns true or 10 seconds elapse.
    ///
    /// Checks every 50ms. Panics on timeout.
    async fn wait_until<F, Fut>(description: &str, predicate: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if predicate().await {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for: {}", description);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ── native-move coalescing (P4f-1) ──────────────────────────────────────
    //
    // A native rename surfaces to the daemon as an unlinked `Deleted(old)` +
    // `Modified(new)` (the watcher has no rename linkage — see `watcher.rs`).
    // These tests drive that pair through the live event loop and assert the
    // coalescer collapses it into ONE same-UUID move instead of a tombstone plus a
    // fresh-UUID create. They use the same two-daemon convergence harness as the
    // sync tests above, with simulated `FileEvent`s and in-memory vaults.

    /// Simulate a native rename on a daemon's filesystem: the OS atomically moves
    /// `old` → `new`, so afterward `new` holds the content and `old` is gone. The
    /// caller injects the resulting `Deleted(old)` + `Modified(new)` events.
    async fn rename_on_fs(daemon: &TestDaemon, old: &str, new: &str, content: &[u8]) {
        daemon.fs.write(new, content).await.unwrap();
        daemon.fs.delete(old).await.unwrap();
    }

    /// A native rename converges to B with the SAME UUID (delete arrives first —
    /// the common case). The move re-parents the existing node; B detects the
    /// `tree.mov`, re-materializes at the new path, and removes the old one — all
    /// under the document's original UUID, re-transferring zero content.
    #[tokio::test]
    async fn native_rename_delete_first_converges_same_uuid() -> anyhow::Result<()> {
        let node_a = build_node(120).await?;
        let node_b = build_node(121).await?;
        connect_nodes(&node_a, &node_b).await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // Sync the original file A → B.
        daemon_a.fs.write("notes/old.md", b"# Movable").await?;
        inject_modified(&daemon_a, "notes/old.md");
        wait_until("B has notes/old.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/old.md".to_string())
            }
        })
        .await;

        // Capture the original UUID on both replicas — they must match end-to-end.
        let uuid_before = uuid_of(&daemon_a.vault, "notes/old.md").await;
        assert!(uuid_before.is_some(), "A should have a node for old.md");
        assert_eq!(
            uuid_of(&daemon_b.vault, "notes/old.md").await,
            uuid_before,
            "B's pre-rename UUID should equal A's"
        );

        // Rename on A's fs, then inject the delete-then-create halves of the move.
        rename_on_fs(&daemon_a, "notes/old.md", "notes/new.md", b"# Movable").await;
        inject_deleted(&daemon_a, "notes/old.md");
        inject_modified(&daemon_a, "notes/new.md");

        // B converges: new path present, old path gone.
        wait_until("B has notes/new.md and not notes/old.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let files = vault.lock().await.list_files().await.unwrap_or_default();
                files.contains(&"notes/new.md".to_string())
                    && !files.contains(&"notes/old.md".to_string())
            }
        })
        .await;

        // The headline property: the UUID is preserved across the move on both
        // sides — a coalesced rename, NOT a delete + fresh-UUID create.
        assert_eq!(
            uuid_of(&daemon_a.vault, "notes/new.md").await,
            uuid_before,
            "A must keep the original UUID at the new path (move, not re-create)"
        );
        assert_eq!(
            uuid_of(&daemon_b.vault, "notes/new.md").await,
            uuid_before,
            "B must converge to the SAME UUID at the new path"
        );

        // Content intact on B.
        assert_eq!(daemon_b.fs.read("notes/new.md").await?, b"# Movable");

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// The symmetry proof: the create half can arrive BEFORE the delete half and
    /// still coalesce into the same same-UUID move. This is the case the old
    /// (delete-keyed) design could not express.
    #[tokio::test]
    async fn native_rename_create_first_converges_same_uuid() -> anyhow::Result<()> {
        let node_a = build_node(122).await?;
        let node_b = build_node(123).await?;
        connect_nodes(&node_a, &node_b).await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        daemon_a.fs.write("notes/before.md", b"# Reordered").await?;
        inject_modified(&daemon_a, "notes/before.md");
        wait_until("B has notes/before.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/before.md".to_string())
            }
        })
        .await;

        let uuid_before = uuid_of(&daemon_a.vault, "notes/before.md").await;
        assert!(uuid_before.is_some());

        // Rename on fs, then inject the CREATE half first, then the delete half.
        rename_on_fs(
            &daemon_a,
            "notes/before.md",
            "notes/after.md",
            b"# Reordered",
        )
        .await;
        inject_modified(&daemon_a, "notes/after.md");
        inject_deleted(&daemon_a, "notes/before.md");

        wait_until("B has notes/after.md and not notes/before.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let files = vault.lock().await.list_files().await.unwrap_or_default();
                files.contains(&"notes/after.md".to_string())
                    && !files.contains(&"notes/before.md".to_string())
            }
        })
        .await;

        assert_eq!(
            uuid_of(&daemon_a.vault, "notes/after.md").await,
            uuid_before,
            "create-first must still coalesce to a same-UUID move on A"
        );
        assert_eq!(
            uuid_of(&daemon_b.vault, "notes/after.md").await,
            uuid_before,
            "create-first must converge to the same UUID on B"
        );

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// A `Deleted` with no content-matching create within the window is a REAL
    /// deletion: on window expiry the sweep commits a tombstone and broadcasts, so
    /// B drops the file. Proves the coalescer never swallows a genuine delete.
    ///
    /// The file is seeded to B via the NeighborUp full sync rather than a
    /// `Modified` event, so the deletion's gossip `ChangeNotification{path}` is the
    /// FIRST notification broadcast for that path. (iroh-gossip suppresses a
    /// byte-identical message within a 90s window; a create-notification followed
    /// by a same-path delete-notification collides on its content-derived id — a
    /// pre-existing notification-layer fragility, unrelated to coalescing, that
    /// Issue 2's anti-entropy layer addresses. Seeding via full sync sidesteps it
    /// so this test isolates the coalescer's lone-delete-expiry behavior.)
    #[tokio::test]
    async fn lone_delete_expires_to_real_deletion() -> anyhow::Result<()> {
        let node_a = build_node(124).await?;
        let node_b = build_node(125).await?;
        connect_nodes(&node_a, &node_b).await?;

        // Seed the file into A's vault BEFORE gossip forms, so B receives it via the
        // NeighborUp full sync (no create `ChangeNotification` is broadcast).
        node_a.fs.write("notes/doomed.md", b"goodbye").await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/doomed.md").await?;
        }

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        wait_until("B has notes/doomed.md via full sync", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/doomed.md".to_string())
            }
        })
        .await;

        // Delete with no partner create. The sweep expires it to a tombstone after
        // the window, and the deletion propagates to B.
        daemon_a.fs.delete("notes/doomed.md").await?;
        inject_deleted(&daemon_a, "notes/doomed.md");

        wait_until("B no longer has notes/doomed.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                !vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/doomed.md".to_string())
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// A `Modified` at a fresh path with no content-matching delete is a REAL new
    /// document: on window expiry the sweep mints a fresh UUID and broadcasts, so
    /// B gains the new file. Proves an unpaired create is never lost.
    #[tokio::test]
    async fn lone_create_expires_to_new_document() -> anyhow::Result<()> {
        let node_a = build_node(126).await?;
        let node_b = build_node(127).await?;
        connect_nodes(&node_a, &node_b).await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // A genuinely new file (no prior delete in the window).
        daemon_a.fs.write("notes/brand-new.md", b"# Hello").await?;
        inject_modified(&daemon_a, "notes/brand-new.md");

        wait_until("B has notes/brand-new.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/brand-new.md".to_string())
            }
        })
        .await;

        // It materialized as a normal new document with a real UUID.
        assert!(
            uuid_of(&daemon_a.vault, "notes/brand-new.md")
                .await
                .is_some(),
            "an unpaired create must commit as a real new document with a UUID"
        );
        assert_eq!(daemon_b.fs.read("notes/brand-new.md").await?, b"# Hello");

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// The EC-8 ancient-coincidence non-adopt: a same-content file appearing LONG
    /// after a deletion (its window already expired) is a NEW document, NOT an
    /// adoption of the dead lineage. Drives the window expiry through the live loop.
    #[tokio::test]
    async fn same_content_after_window_is_new_document_not_adopt() -> anyhow::Result<()> {
        let node_a = build_node(128).await?;
        let node_b = build_node(129).await?;
        connect_nodes(&node_a, &node_b).await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        daemon_a
            .fs
            .write("notes/original.md", b"identical body")
            .await?;
        inject_modified(&daemon_a, "notes/original.md");
        wait_until("B has notes/original.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/original.md".to_string())
            }
        })
        .await;
        let dead_uuid = uuid_of(&daemon_a.vault, "notes/original.md").await;
        assert!(dead_uuid.is_some());

        // Delete it and let the window fully expire (tombstone commits).
        daemon_a.fs.delete("notes/original.md").await?;
        inject_deleted(&daemon_a, "notes/original.md");
        wait_until("A tombstoned notes/original.md", || {
            let vault = daemon_a.vault.clone();
            async move { uuid_of(&vault, "notes/original.md").await.is_none() }
        })
        .await;

        // A same-content file appears under a different name well after the window.
        // Nothing remains pending to adopt onto → it is a fresh-UUID new document.
        daemon_a
            .fs
            .write("notes/coincidence.md", b"identical body")
            .await?;
        inject_modified(&daemon_a, "notes/coincidence.md");
        wait_until("A has notes/coincidence.md", || {
            let vault = daemon_a.vault.clone();
            async move { uuid_of(&vault, "notes/coincidence.md").await.is_some() }
        })
        .await;

        let new_uuid = uuid_of(&daemon_a.vault, "notes/coincidence.md").await;
        assert!(new_uuid.is_some());
        assert_ne!(
            new_uuid, dead_uuid,
            "a same-content file after the window must be a NEW UUID, not an adoption of the dead lineage"
        );

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// An in-place edit of an already-tracked file is NOT coalesced — it dispatches
    /// immediately, before any window elapses.
    ///
    /// The proof is A-side and timing-tight: an edit applies SYNCHRONOUSLY in
    /// `on_file_modified`, so A's own document content reflects the edit within a
    /// budget far below the 500ms coalescing window. A buffered edit could not — it
    /// would sit in the window until the sweep (~500-750ms) before applying. Asserting
    /// A's local state (rather than B's converged state) isolates the no-latency
    /// property from QUIC round-trip time, which can itself exceed a few hundred ms.
    #[tokio::test]
    async fn edit_of_tracked_file_is_not_buffered() -> anyhow::Result<()> {
        let node_a = build_node(130).await?;
        let node_b = build_node(131).await?;
        connect_nodes(&node_a, &node_b).await?;

        // Seed v1 into A's vault before gossip forms (no create `ChangeNotification`).
        node_a.fs.write("notes/live.md", b"# v1").await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/live.md").await?;
        }
        let uuid_v1 = uuid_of(&node_a.vault, "notes/live.md").await;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // Edit in place (the path keeps a live node). This must skip the coalescer and
        // apply on A immediately.
        daemon_a.fs.write("notes/live.md", b"# v2 edited").await?;
        inject_modified(&daemon_a, "notes/live.md");

        // A's own document must reflect the edit well inside one coalescing window. A
        // buffered edit would not apply until the sweep — 250ms is a deliberately tight
        // budget below the 500ms window, and A applies with no network in the path.
        let applied = tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if let Ok(doc) = daemon_a
                    .vault
                    .lock()
                    .await
                    .get_document("notes/live.md")
                    .await
                    && doc.body().to_string().contains("v2 edited")
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            applied.is_ok(),
            "an in-place edit must apply immediately on A (not wait for the coalescing window)"
        );

        // Same document — the edit did not mint a new identity (it was never treated as
        // a create-candidate).
        assert_eq!(
            uuid_of(&daemon_a.vault, "notes/live.md").await,
            uuid_v1,
            "an edit keeps the document's UUID"
        );

        // And it still converges to B (the edit's notification is the first for this
        // path, so it is not deduped — see `test_local_edit_propagates_to_peer`).
        wait_until("B has the edited content of notes/live.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .get_document("notes/live.md")
                    .await
                    .map(|d| d.body().to_string().contains("v2 edited"))
                    .unwrap_or(false)
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// OQ-D — a re-create at a JUST-deleted path with identical content. The delete
    /// is still buffered (the node not yet tombstoned), so the matching create
    /// resolves to a same-path move: a no-op that leaves the document alive under
    /// its original UUID, NOT a tombstone-then-fresh-create.
    #[tokio::test]
    async fn recreate_at_deleted_path_keeps_uuid_alive() -> anyhow::Result<()> {
        let node_a = build_node(132).await?;
        let node_b = build_node(133).await?;
        connect_nodes(&node_a, &node_b).await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        daemon_a
            .fs
            .write("notes/churn.md", b"steady content")
            .await?;
        inject_modified(&daemon_a, "notes/churn.md");
        wait_until("B has notes/churn.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/churn.md".to_string())
            }
        })
        .await;
        let uuid_before = uuid_of(&daemon_a.vault, "notes/churn.md").await;
        assert!(uuid_before.is_some());

        // Delete then immediately re-create the SAME path with the SAME content —
        // within the window, so the delete is still buffered when the create lands.
        daemon_a.fs.delete("notes/churn.md").await?;
        inject_deleted(&daemon_a, "notes/churn.md");
        daemon_a
            .fs
            .write("notes/churn.md", b"steady content")
            .await?;
        inject_modified(&daemon_a, "notes/churn.md");

        // Let the window pass so a (wrongly) un-cancelled delete would have
        // tombstoned by now — then assert the doc is still alive with its UUID.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            uuid_of(&daemon_a.vault, "notes/churn.md").await,
            uuid_before,
            "a same-path re-create must cancel the buffered delete and keep the document alive under its UUID"
        );
        assert!(
            daemon_a
                .vault
                .lock()
                .await
                .list_files()
                .await
                .unwrap_or_default()
                .contains(&"notes/churn.md".to_string()),
            "the re-created path must still be present (not tombstoned)"
        );

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;
        Ok(())
    }

    /// A same-path create-then-delete both push in real time — the create
    /// notification does not dedup-suppress the follow-up delete notification.
    ///
    /// This is the OPPOSITE of the three reseeded tests (`test_file_deletion_propagates`,
    /// `test_local_edit_propagates_to_peer`, `lone_delete_expires_to_real_deletion`),
    /// which seed the file via the NeighborUp full sync so the delete/edit notification
    /// is the FIRST for its path. Here BOTH notifications go over LIVE gossip in one
    /// seen-cache window: A creates `notes/ping.md` (broadcasts a create-notif), then
    /// deletes the SAME path (broadcasts a delete-notif). On the un-nonced wire format
    /// both `GossipMessage::ChangeNotification` envelopes serialize byte-identically, so
    /// iroh-gossip's content-derived MessageId suppresses the delete-notif within its
    /// ~90s window and B never pulls the deletion (Issue 2). The per-notification nonce
    /// (`salt ^ seq`, distinct per broadcast) makes the bytes differ, so the delete-notif
    /// survives dedup and B drops the file via real-time push — no full-sync reseed.
    ///
    /// Determinism anchor: we `wait_until` B HAS the file before A deletes it. That
    /// guarantees the create-notif reached B's seen-cache, so the delete-notif is
    /// genuinely tested against a populated dedup cache rather than racing ahead and
    /// passing for the wrong reason.
    ///
    /// Seeds 140/141 reserved.
    #[tokio::test]
    async fn same_path_create_then_delete_pushes_in_realtime() -> anyhow::Result<()> {
        let node_a = build_node(140).await?;
        let node_b = build_node(141).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Join gossip LIVE first (A empty-bootstrap, B off A) and do NOT pre-seed the
        // file — the create-notif must travel over live gossip so its MessageId enters
        // iroh-gossip's seen-cache, which is what the delete-notif later collides with.
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // A creates the file → broadcasts the create-notif (nonce = salt ^ 0).
        daemon_a.fs.write("notes/ping.md", b"v1").await?;
        inject_modified(&daemon_a, "notes/ping.md");

        // Determinism anchor: B must actually have the file before A deletes it, which
        // proves the create-notif landed in B's seen-cache.
        wait_until("B has notes/ping.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/ping.md".to_string())
            }
        })
        .await;

        // A deletes the SAME path → broadcasts the delete-notif (nonce = salt ^ 1).
        // Un-nonced this serializes identically to the create-notif and is suppressed;
        // with the nonce the bytes differ and B receives it.
        inject_deleted(&daemon_a, "notes/ping.md");

        // The real-time assertion: B drops the file via gossip push, with NO full-sync
        // reseed preceding it — a pass REQUIRES the delete-notif to have survived dedup.
        wait_until("B no longer has notes/ping.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                !vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/ping.md".to_string())
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// The move-coalescer's crash-recovery journal path, relative to the vault
    /// root. Kept as a literal here (rather than importing the daemon's `pub(crate)`
    /// const, which an external integration-test crate can't see) so the test
    /// documents the exact on-disk location it asserts against.
    const PENDING_MOVES_JOURNAL: &str = ".sync/pending-moves.json";

    /// Read the crash-recovery journal back from the in-memory fs the daemon writes
    /// to, and parse it as untyped JSON. The journal's record types are `pub(crate)`,
    /// so the assertions inspect JSON fields rather than the typed struct; what
    /// matters is that these are the bytes the daemon actually persisted, not
    /// in-memory coalescer state. `None` when the file is absent (a clean,
    /// fully-pruned journal leaves no file). Takes a bare fs handle so the
    /// graceful-drain tests, whose post-quit assertions outlive the `TestDaemon`
    /// (awaiting `loop_handle` consumes that field), can read it after teardown.
    async fn read_journal_on_fs(fs: &Arc<InMemoryFs>) -> Option<serde_json::Value> {
        let bytes = fs.read(PENDING_MOVES_JOURNAL).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// The `pending` array of the journal, or empty when the file is absent — the
    /// shape the prune assertions check (a committed record leaves no trace).
    async fn journal_pending(daemon: &TestDaemon) -> Vec<serde_json::Value> {
        journal_pending_on_fs(&daemon.fs).await
    }

    /// `journal_pending` against a bare fs handle (see [`read_journal_on_fs`]).
    async fn journal_pending_on_fs(fs: &Arc<InMemoryFs>) -> Vec<serde_json::Value> {
        match read_journal_on_fs(fs).await {
            Some(value) => value
                .get("pending")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    /// A buffered move is journaled to `.sync/pending-moves.json` (carrying the
    /// moved doc's UUID, the lineage crash recovery re-stitches) while it sits in the
    /// window, and the record is PRUNED the moment the move commits — proving the
    /// persist-after-commit / prune-from-snapshot contract against the actually
    /// persisted bytes, read back through the daemon's fs.
    #[tokio::test]
    async fn pending_move_is_journaled_then_pruned() -> anyhow::Result<()> {
        let node = build_node(142).await?;
        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let daemon = spawn_daemon(node, gossip);

        // Track a file so the delete half resolves to a live node (and a UUID).
        daemon.fs.write("notes/old.md", b"# Movable").await?;
        inject_modified(&daemon, "notes/old.md");
        wait_until("notes/old.md is tracked", || {
            let vault = daemon.vault.clone();
            async move { uuid_of(&vault, "notes/old.md").await.is_some() }
        })
        .await;
        let uuid_before = uuid_of(&daemon.vault, "notes/old.md")
            .await
            .expect("old.md should have a UUID");

        // Rename on the fs, then inject only the DELETE half — it buffers, awaiting
        // its partner create for the window.
        rename_on_fs(&daemon, "notes/old.md", "notes/new.md", b"# Movable").await;
        inject_deleted(&daemon, "notes/old.md");

        // The buffered delete is journaled, carrying the doc's UUID.
        wait_until("the buffered delete is journaled with its UUID", || {
            let daemon = &daemon;
            let uuid_before = uuid_before.clone();
            async move {
                let pending = journal_pending(daemon).await;
                pending.len() == 1
                    && pending[0]["kind"] == "delete"
                    && pending[0]["path"] == "notes/old.md"
                    && pending[0]["uuid"] == serde_json::json!(uuid_before)
            }
        })
        .await;

        // Inject the create half — it pairs into a move, committing the record.
        inject_modified(&daemon, "notes/new.md");

        // The committed record is pruned: the journal's pending set is empty.
        wait_until("the committed move is pruned from the journal", || {
            let daemon = &daemon;
            async move { journal_pending(daemon).await.is_empty() }
        })
        .await;

        // The move preserved the UUID at the new path (not a fresh-UUID re-create).
        assert_eq!(
            uuid_of(&daemon.vault, "notes/new.md").await,
            Some(uuid_before),
            "the coalesced move must keep the original UUID at the new path"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    /// A lone delete (no partner create) is journaled while it waits, then PRUNED
    /// once its window expires and the sweep commits it standalone (a real
    /// tombstone). Proves the sweep path also persists the post-commit snapshot, so
    /// a standalone-committed record never lingers in the journal to be re-committed
    /// on boot.
    #[tokio::test]
    async fn journal_survives_lone_delete_until_expiry_then_prunes() -> anyhow::Result<()> {
        let node = build_node(143).await?;
        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let daemon = spawn_daemon(node, gossip);

        daemon.fs.write("notes/doomed.md", b"goodbye").await?;
        inject_modified(&daemon, "notes/doomed.md");
        wait_until("notes/doomed.md is tracked", || {
            let vault = daemon.vault.clone();
            async move { uuid_of(&vault, "notes/doomed.md").await.is_some() }
        })
        .await;

        // Delete with no partner create — it buffers and is journaled.
        daemon.fs.delete("notes/doomed.md").await?;
        inject_deleted(&daemon, "notes/doomed.md");
        wait_until("the lone delete is journaled", || {
            let daemon = &daemon;
            async move {
                let pending = journal_pending(daemon).await;
                pending.len() == 1 && pending[0]["path"] == "notes/doomed.md"
            }
        })
        .await;

        // The window expires; the sweep commits the delete standalone (the node is
        // tombstoned) AND rewrites the journal from the now-empty snapshot.
        wait_until("the expired delete is pruned from the journal", || {
            let daemon = &daemon;
            async move { journal_pending(daemon).await.is_empty() }
        })
        .await;
        assert!(
            uuid_of(&daemon.vault, "notes/doomed.md").await.is_none(),
            "the lone delete must commit as a real tombstone once its window expires"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    // ── P4f-2b-ii: boot move crash-recovery ─────────────────────────────────────
    //
    // The recovery path runs at BOOT, inside `startup_inner`, before the event loop.
    // `build_node`/`spawn_daemon` have no boot-from-disk path (they `Vault::init` a
    // fresh fs), so these tests need a harness that boots a daemon from a pre-staged
    // `InMemoryFs`. `spawn_daemon_from_loaded` runs the EXACT recovery sequence
    // `startup_inner` runs — the same free helpers (`read_pending_journal`,
    // `restitch_inputs`, `clear_pending_journal`) and the same
    // `Daemon::finalize_recovered_journal` — so production and test cannot diverge.
    // The only thing it omits is the network scaffolding (relay/mDNS/watcher) the
    // recovery logic never touches.

    /// Boot a daemon from a PRE-STAGED `InMemoryFs`, running the production recovery
    /// sequence: read journal → build re-stitch inputs → `Vault::load_with_journal`
    /// → `Daemon::new` → `finalize_recovered_journal` → `clear_pending_journal`. The
    /// caller stages `.sync/` + `.loro` + journal + disk into `fs` before calling.
    /// This is `startup_inner`'s recovery path minus relay/mDNS/watcher.
    async fn spawn_daemon_from_loaded(seed_byte: u8, fs: Arc<InMemoryFs>) -> TestDaemon {
        let author = PeerId::from_secret_bytes(super::common::seed(seed_byte));

        // The recovery sequence, identical to `startup_inner`.
        let records = read_pending_journal(fs.as_ref()).await;
        let restitch = restitch_inputs(&records);
        let vault = Vault::load_with_journal(fs.clone(), author.as_u64(), Some(&restitch))
            .await
            .expect("load_with_journal should boot the staged vault");
        let vault = Arc::new(Mutex::new(vault));

        // Build a real SyncNode + gossip exactly as `build_node` does, so the daemon
        // is production-shaped (the recovery broadcast is a real no-op at boot).
        let allowlist = Arc::new(InMemoryAllowlist::new());
        let (inbound_seen_tx, inbound_seen_rx) = mpsc::unbounded_channel();
        let sync_handler = sync_daemon::daemon::PumpedSyncHandler::new(
            vault.clone(),
            allowlist.clone(),
            inbound_seen_tx,
        );
        let sync_node = SyncNode::new_with_sync_handler(
            super::common::seed(seed_byte),
            &[],
            allowlist.clone(),
            sync_handler,
        )
        .await
        .expect("sync node should build");
        let memory_lookup = MemoryLookup::new();
        sync_node
            .endpoint_for_test()
            .address_lookup()
            .unwrap()
            .add(memory_lookup);

        let gossip = sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await
            .expect("gossip join should succeed");

        let (file_event_tx, file_event_rx) = mpsc::unbounded_channel::<FileEvent>();
        let shutdown = CancellationToken::new();

        let mut daemon = Daemon::new(
            vault.clone(),
            sync_node,
            gossip,
            file_event_rx,
            None, // no mDNS discovery in tests
            allowlist.clone(),
            "test-device".to_string(),
            None,
            "/test-vault".into(),
            shutdown.clone(),
        );
        daemon.set_inbound_seen_rx(inbound_seen_rx);
        // The daemon's journal fs MUST be the same stateful `InMemoryFs` the vault
        // uses, so the clear lands where `read_pending_journal` read from.
        daemon.set_fs(Arc::new(fs.clone()));

        // Finish recovery before the loop spawns — finalize the unmatched remainder,
        // then clear the journal in one write (the production ordering).
        daemon.finalize_recovered_journal(&records).await;
        clear_pending_journal(fs.as_ref()).await;

        let loop_handle = tokio::spawn(async move {
            daemon.run_loop().await;
        });

        TestDaemon {
            vault,
            fs,
            allowlist,
            file_event_tx,
            shutdown,
            loop_handle,
        }
    }

    /// Write a crash-recovery journal carrying `records` to a staged `InMemoryFs`,
    /// simulating "the daemon crashed with these moves still buffered."
    async fn stage_crash_journal(fs: &Arc<InMemoryFs>, records: Vec<JournaledMove>) {
        let file = PendingMovesFile {
            version: PENDING_MOVES_VERSION,
            pending: records,
        };
        let bytes = serde_json::to_vec(&file).expect("journal serializes");
        fs.atomic_write(PENDING_MOVES_JOURNAL, &bytes)
            .await
            .expect("stage journal write");
    }

    /// Stage the on-disk shape of a native move that crashed mid-window: track `old`
    /// (minting a node + its `<uuid>.loro`), then rename it on disk so `new` holds the
    /// content with NO node, the OLD node is still LIVE in the persisted index, and
    /// `docs/<uuid>.loro` is present. Crucially does NOT tombstone the old node — the
    /// crash window has the delete buffered, not committed. Returns the doc's UUID.
    /// This is the integration sibling of 2b-i's `stage_buffered_move_crash`.
    async fn stage_tracked_then_renamed_on_disk(
        fs: &Arc<InMemoryFs>,
        author: u64,
        old: &str,
        new: &str,
        content: &[u8],
    ) -> Uuid {
        let vault = Vault::init(fs.clone(), author)
            .await
            .expect("staging vault init");
        fs.write(old, content).await.expect("write old");
        vault.on_file_changed(old).await.expect("index old");
        let node = vault
            .index()
            .node_for_path(old)
            .expect("old should have a node after indexing");
        let uuid = vault
            .index()
            .node_uuid(&node)
            .expect("old's node should have a UUID");
        // The disk rename: `new` gets the content, `old` is removed.
        fs.write(new, content).await.expect("write new");
        fs.delete(old).await.expect("delete old");
        // Persist the index so the loaded vault sees the still-live old node.
        vault.save_index().await.expect("save index");
        uuid
    }

    /// Build a fresh `JournaledMove` DELETE record for a staged crash journal, using
    /// the journal's own `hex_lower` encoder so the persisted `content_hash` exactly
    /// matches what the daemon wrote pre-crash (and what the re-stitch will decode).
    fn delete_record(uuid: Uuid, path: &str, hash: [u8; 32]) -> JournaledMove {
        JournaledMove {
            kind: PendingKind::Delete,
            content_hash: hex_lower(&hash),
            path: path.to_string(),
            uuid: Some(uuid.to_string()),
        }
    }

    /// The content hash a move's `.md` lands in reconcile's hash domain under — the
    /// SAME domain the re-stitch matches against (`content_hash(&ContentDoc::from_markdown)`).
    /// The headline test's re-stitch match depends on this exact domain.
    fn hash_of(content: &str, author: u64) -> [u8; 32] {
        content_hash(&ContentDoc::from_markdown(content, author).expect("content doc"))
    }

    /// The HEADLINE end-to-end proof: a native move buffered when the daemon crashed
    /// is re-stitched on the next boot. The boot path reads the journal, feeds the
    /// DELETE record's lineage into load, re-attaches the original UUID at the new
    /// path (NOT a fresh-UUID re-create), and empties the journal.
    #[tokio::test]
    async fn crash_recovery_restitches_buffered_move_on_boot() -> anyhow::Result<()> {
        let fs = Arc::new(InMemoryFs::new());
        let author = PeerId::from_secret_bytes(super::common::seed(200)).as_u64();

        let uuid = stage_tracked_then_renamed_on_disk(
            &fs,
            author,
            "notes/old.md",
            "notes/new.md",
            b"# Movable",
        )
        .await;
        stage_crash_journal(
            &fs,
            vec![delete_record(
                uuid,
                "notes/old.md",
                hash_of("# Movable", author),
            )],
        )
        .await;

        let daemon = spawn_daemon_from_loaded(200, fs).await;

        // Re-stitched: the original UUID is now at the new path, not a fresh mint.
        assert_eq!(
            uuid_of(&daemon.vault, "notes/new.md").await,
            Some(uuid.to_string()),
            "the buffered move must re-stitch the original UUID at the new path"
        );
        assert!(
            uuid_of(&daemon.vault, "notes/old.md").await.is_none(),
            "the old path must be vacated by the re-stitch"
        );
        // The boot path emptied the journal after recovery.
        assert!(
            journal_pending(&daemon).await.is_empty(),
            "the journal must be cleared after boot recovery"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    /// An unmatched DELETE — a REAL deletion with no content-matching `.md` anywhere —
    /// is finalized as a tombstone by `finalize_recovered_journal`. Reconcile leaves
    /// the still-live old node alone (it never tombstones), so the daemon must commit
    /// the deletion via `on_file_deleted`.
    #[tokio::test]
    async fn boot_finalizes_unmatched_delete_as_tombstone() -> anyhow::Result<()> {
        let fs = Arc::new(InMemoryFs::new());
        let author = PeerId::from_secret_bytes(super::common::seed(201)).as_u64();

        // Track `doomed`, then delete it with NO matching new path — a real deletion.
        let vault = Vault::init(fs.clone(), author).await?;
        fs.write("notes/doomed.md", b"goodbye").await?;
        vault.on_file_changed("notes/doomed.md").await?;
        let uuid = vault
            .index()
            .node_uuid(&vault.index().node_for_path("notes/doomed.md").unwrap())
            .unwrap();
        fs.delete("notes/doomed.md").await?;
        vault.save_index().await?;
        drop(vault);

        stage_crash_journal(
            &fs,
            vec![delete_record(
                uuid,
                "notes/doomed.md",
                hash_of("goodbye", author),
            )],
        )
        .await;

        let daemon = spawn_daemon_from_loaded(201, fs).await;

        assert!(
            uuid_of(&daemon.vault, "notes/doomed.md").await.is_none(),
            "the unmatched delete's still-live node must be tombstoned on boot"
        );
        assert!(
            journal_pending(&daemon).await.is_empty(),
            "the journal must be cleared after boot recovery"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    /// An unmatched CREATE — an orphaned `.md` with no node and ONLY a create record
    /// (uuid `None`) in the journal — is minted a fresh-UUID node by reconcile's
    /// per-file loop, NOT by any daemon finalize. Proves creates are reconcile's job
    /// (§1): the daemon ignores create records after building the re-stitch inputs.
    #[tokio::test]
    async fn boot_finalizes_unmatched_create_as_new_doc() -> anyhow::Result<()> {
        let fs = Arc::new(InMemoryFs::new());
        let author = PeerId::from_secret_bytes(super::common::seed(202)).as_u64();

        // A vault that exists but has an orphaned `.md` on disk with no node.
        let vault = Vault::init(fs.clone(), author).await?;
        vault.save_index().await?;
        drop(vault);
        fs.write("notes/fresh.md", b"# Fresh").await?;

        // Journal carries ONLY a create record (uuid None — its partner delete never
        // arrived). The daemon should take no explicit action for it.
        stage_crash_journal(
            &fs,
            vec![JournaledMove {
                kind: PendingKind::Create,
                content_hash: hex_lower(&hash_of("# Fresh", author)),
                path: "notes/fresh.md".to_string(),
                uuid: None,
            }],
        )
        .await;

        let daemon = spawn_daemon_from_loaded(202, fs).await;

        assert!(
            uuid_of(&daemon.vault, "notes/fresh.md").await.is_some(),
            "reconcile must mint a fresh-UUID node for the unmatched create"
        );
        assert!(
            journal_pending(&daemon).await.is_empty(),
            "the journal must be cleared after boot recovery"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    /// A corrupt journal must NOT abort boot. `read_pending_journal` is tolerant —
    /// garbage parses to an empty set — so the daemon boots, a normal file reconciles
    /// to a live node, and the clear rewrites the garbage to a clean empty journal.
    #[tokio::test]
    async fn corrupt_journal_does_not_abort_boot() -> anyhow::Result<()> {
        let fs = Arc::new(InMemoryFs::new());
        let author = PeerId::from_secret_bytes(super::common::seed(203)).as_u64();

        // A normal trackable file plus a garbage journal.
        let vault = Vault::init(fs.clone(), author).await?;
        fs.write("notes/keeper.md", b"# Keeper").await?;
        vault.on_file_changed("notes/keeper.md").await?;
        vault.save_index().await?;
        drop(vault);
        fs.write(PENDING_MOVES_JOURNAL, b"{ this is not json ]]")
            .await?;

        // Boots without panic despite the garbage journal.
        let daemon = spawn_daemon_from_loaded(203, fs).await;

        assert!(
            uuid_of(&daemon.vault, "notes/keeper.md").await.is_some(),
            "the normal file must reconcile to a live node despite a corrupt journal"
        );
        assert!(
            journal_pending(&daemon).await.is_empty(),
            "the clear must rewrite the garbage journal to a clean empty file"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    /// An absent journal (clean boot) is a no-op: the daemon boots, the tracked file
    /// is unchanged, and no recovery action is taken. The clear still writes an empty
    /// journal even when none existed (harmless).
    #[tokio::test]
    async fn clean_boot_empty_journal_is_noop() -> anyhow::Result<()> {
        let fs = Arc::new(InMemoryFs::new());
        let author = PeerId::from_secret_bytes(super::common::seed(204)).as_u64();

        let vault = Vault::init(fs.clone(), author).await?;
        fs.write("notes/stable.md", b"# Stable").await?;
        vault.on_file_changed("notes/stable.md").await?;
        let uuid_before = vault
            .index()
            .node_for_path("notes/stable.md")
            .and_then(|node| vault.index().node_uuid(&node))
            .map(|u| u.to_string());
        vault.save_index().await?;
        drop(vault);
        // NO journal staged at all.

        let daemon = spawn_daemon_from_loaded(204, fs).await;

        assert_eq!(
            uuid_of(&daemon.vault, "notes/stable.md").await,
            uuid_before,
            "a clean boot must leave the tracked file's UUID unchanged"
        );
        assert!(
            journal_pending(&daemon).await.is_empty(),
            "a clean boot leaves an empty journal"
        );

        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;
        Ok(())
    }

    /// Crash-after-restitch-before-clear convergence: a second boot over the same fs
    /// with the SAME delete record re-staged is a clean no-op. The first boot
    /// re-stitched and cleared; re-staging the journal simulates "crashed after
    /// re-stitch, before the clear." The second boot's `finalize_recovered_journal`
    /// finds `old_path` already vacated (no live node) and skips — no double-move, no
    /// error (the §3.1 finalize idempotency).
    #[tokio::test]
    async fn boot_twice_recovery_is_idempotent() -> anyhow::Result<()> {
        let fs = Arc::new(InMemoryFs::new());
        let author = PeerId::from_secret_bytes(super::common::seed(205)).as_u64();

        let uuid = stage_tracked_then_renamed_on_disk(
            &fs,
            author,
            "notes/old.md",
            "notes/new.md",
            b"# Movable",
        )
        .await;
        let record = delete_record(uuid, "notes/old.md", hash_of("# Movable", author));
        stage_crash_journal(&fs, vec![record.clone()]).await;

        // First boot: re-stitches AND clears the journal.
        let daemon1 = spawn_daemon_from_loaded(205, fs.clone()).await;
        assert_eq!(
            uuid_of(&daemon1.vault, "notes/new.md").await,
            Some(uuid.to_string()),
            "first boot re-stitches the move"
        );
        daemon1.shutdown.cancel();
        let _ = daemon1.loop_handle.await;

        // Simulate "crashed after re-stitch, before clear": re-stage the SAME record
        // onto the now-mutated fs (the node already sits at new.md).
        stage_crash_journal(&fs, vec![record]).await;

        // Second boot over the same fs: a clean no-op.
        let daemon2 = spawn_daemon_from_loaded(205, fs).await;
        assert_eq!(
            uuid_of(&daemon2.vault, "notes/new.md").await,
            Some(uuid.to_string()),
            "second boot must NOT double-move — the UUID stays at the new path"
        );
        assert!(
            uuid_of(&daemon2.vault, "notes/old.md").await.is_none(),
            "old path stays vacated on the second boot"
        );
        assert!(
            journal_pending(&daemon2).await.is_empty(),
            "the journal is cleared again after the idempotent second boot"
        );

        daemon2.shutdown.cancel();
        let _ = daemon2.loop_handle.await;
        Ok(())
    }

    // ── P4f-2c: graceful-shutdown drain ─────────────────────────────────────────
    //
    // On a clean quit the run-loop's shutdown arm drains the move-coalescer buffer
    // BEFORE breaking: every buffered record commits standalone (lone delete →
    // tombstone, lone create → fresh-mint doc) and the journal is rewritten empty.
    // The drain is wait-free — pairing is eager, so the buffer holds only unpaired
    // singletons at shutdown, with nothing to wait for. These tests drive a live
    // daemon through `shutdown.cancel()` + `loop_handle.await`; because the drain
    // runs in the shutdown arm before `break`, awaiting the loop guarantees the
    // drain finished, so post-await assertions observe the drained end state.

    /// A clean quit force-expires EVERY buffered record standalone and empties the
    /// journal. Buffer a lone delete (tombstone-bound) and a lone create
    /// (fresh-mint-bound), confirm both are journaled-but-uncommitted, then cancel:
    /// after the loop joins, the delete's node is gone, the create's node is minted,
    /// and the journal is empty. The end-to-end graceful-drain proof (P4f-2c).
    #[tokio::test]
    async fn graceful_shutdown_drains_buffered_records() -> anyhow::Result<()> {
        let node = build_node(160).await?;
        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let daemon = spawn_daemon(node, gossip);

        // Track a file so the lone delete resolves to a live node + UUID.
        daemon.fs.write("notes/keep-delete.md", b"goodbye").await?;
        inject_modified(&daemon, "notes/keep-delete.md");
        wait_until("notes/keep-delete.md is tracked", || {
            let vault = daemon.vault.clone();
            async move { uuid_of(&vault, "notes/keep-delete.md").await.is_some() }
        })
        .await;

        // Buffer a LONE DELETE (no partner create injected) — bound to commit as a
        // standalone tombstone at drain.
        daemon.fs.delete("notes/keep-delete.md").await?;
        inject_deleted(&daemon, "notes/keep-delete.md");

        // Buffer a LONE CREATE for a path with no live node — bound to commit as a
        // standalone fresh-UUID document at drain.
        daemon
            .fs
            .write("notes/lone-create.md", b"# Brand new")
            .await?;
        inject_modified(&daemon, "notes/lone-create.md");

        // Both singletons are journaled (buffered, NOT yet committed) before we quit.
        wait_until("both lone records are journaled", || {
            let daemon = &daemon;
            async move { journal_pending(daemon).await.len() == 2 }
        })
        .await;

        // Hold the vault + fs handles past the await on `loop_handle` (which consumes
        // that field), so the post-drain assertions can inspect the drained state.
        let vault = daemon.vault.clone();
        let fs = daemon.fs.clone();

        // Clean quit: the shutdown arm drains the buffer before breaking. Awaiting
        // the loop guarantees the drain (and journal clear) completed.
        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;

        // The lone delete committed as a real tombstone — the node is gone.
        assert!(
            uuid_of(&vault, "notes/keep-delete.md").await.is_none(),
            "the graceful drain must commit the lone delete as a standalone tombstone"
        );
        // The lone create committed as a fresh-UUID new document.
        assert!(
            uuid_of(&vault, "notes/lone-create.md").await.is_some(),
            "the graceful drain must commit the lone create as a standalone new doc"
        );
        // The journal is empty — the next boot has zero recovery work.
        assert!(
            journal_pending_on_fs(&fs).await.is_empty(),
            "the graceful drain must leave the journal empty"
        );
        Ok(())
    }

    /// The drain is wait-free: with a lone-delete buffered, the shutdown drain
    /// commits it AND the loop joins well within a generous timeout. A drain that
    /// blocked (waiting on a partner that never arrives) would hit the timeout.
    /// Asserting the drain's EFFECT (the delete is tombstoned + journal emptied) on
    /// top of the timeout is what makes this a wait-free-DRAIN proof and not merely a
    /// "the loop exited" check — the loop joins fast even with no drain, so the
    /// effect assertions are what pin the drain to actually having run.
    #[tokio::test]
    async fn graceful_shutdown_drain_is_wait_free() -> anyhow::Result<()> {
        let node = build_node(161).await?;
        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let daemon = spawn_daemon(node, gossip);

        daemon.fs.write("notes/doomed.md", b"goodbye").await?;
        inject_modified(&daemon, "notes/doomed.md");
        wait_until("notes/doomed.md is tracked", || {
            let vault = daemon.vault.clone();
            async move { uuid_of(&vault, "notes/doomed.md").await.is_some() }
        })
        .await;

        // A single buffered lone-delete — the leanest drainable state.
        daemon.fs.delete("notes/doomed.md").await?;
        inject_deleted(&daemon, "notes/doomed.md");
        wait_until("the lone delete is journaled", || {
            let daemon = &daemon;
            async move { journal_pending(daemon).await.len() == 1 }
        })
        .await;

        // Hold the handles past the await on `loop_handle`, which the timeout consumes.
        let vault = daemon.vault.clone();
        let fs = daemon.fs.clone();
        let loop_handle = daemon.loop_handle;
        daemon.shutdown.cancel();

        // The loop must join well within 5s — the drain force-expires the singleton
        // immediately, with no partner-wait to block on. A blocking drain would
        // instead leave the loop running until this timeout fires.
        let joined = tokio::time::timeout(Duration::from_secs(5), loop_handle).await;
        assert!(
            joined.is_ok(),
            "the graceful drain must return without blocking (the loop joined within 5s)"
        );

        // The drain actually RAN within that window: the lone delete is committed as a
        // standalone tombstone and the journal is empty. These pin the wait-free
        // claim to a real, completed drain (the timeout alone passes even with no
        // drain, since the loop exits fast regardless).
        assert!(
            uuid_of(&vault, "notes/doomed.md").await.is_none(),
            "the wait-free drain must have committed the lone delete as a tombstone"
        );
        assert!(
            journal_pending_on_fs(&fs).await.is_empty(),
            "the wait-free drain must have emptied the journal"
        );
        Ok(())
    }

    /// After a graceful drain leaves the journal empty, the NEXT boot over the same
    /// fs does zero recovery work: the drained standalone state stands, and the
    /// journal stays empty. Proves the §0 guarantee — a clean quit leaves the next
    /// boot nothing to recover.
    #[tokio::test]
    async fn boot_after_graceful_drain_finds_empty_journal() -> anyhow::Result<()> {
        let node = build_node(162).await?;
        // Hold a handle to the SAME fs the daemon persists into, to re-boot over it.
        let fs = node.fs.clone();
        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let daemon = spawn_daemon(node, gossip);

        // Buffer a lone create, drain it on a clean quit (it commits standalone).
        daemon.fs.write("notes/drained-create.md", b"# New").await?;
        inject_modified(&daemon, "notes/drained-create.md");
        wait_until("the lone create is journaled", || {
            let daemon = &daemon;
            async move { journal_pending(daemon).await.len() == 1 }
        })
        .await;
        // Hold the first daemon's vault past the await on `loop_handle`.
        let vault1 = daemon.vault.clone();
        daemon.shutdown.cancel();
        let _ = daemon.loop_handle.await;

        let drained_uuid = uuid_of(&vault1, "notes/drained-create.md").await;
        assert!(
            drained_uuid.is_some(),
            "the drain should have minted the lone create's node before quit"
        );

        // Re-boot a SECOND daemon over the SAME fs (same seed = re-opening the same
        // vault after a restart). The drained empty journal means no recovery work.
        let daemon2 = spawn_daemon_from_loaded(162, fs).await;
        assert_eq!(
            uuid_of(&daemon2.vault, "notes/drained-create.md").await,
            drained_uuid,
            "the next boot keeps the drained standalone create — no recovery churn"
        );
        assert!(
            journal_pending(&daemon2).await.is_empty(),
            "a clean quit leaves the next boot an empty journal"
        );

        daemon2.shutdown.cancel();
        let _ = daemon2.loop_handle.await;
        Ok(())
    }
}
