//! Integration tests for the Daemon event loop — sync propagation / full-sync.
//!
//! Real `Daemon` instances with real iroh nodes, in-memory filesystems, and
//! injected file events: the `tokio::select!` loop routing file changes, gossip
//! notifications, and inbound QUIC sync through to vault state. Carved from the
//! former `daemon_integration.rs` monolith; harness lives in `common`, the 2-arg
//! `wait_until` is file-local (name-collides with the relay `common::wait_until`).
//!
//! Seeds 20+ avoid collisions with sync_workflow.rs (seeds 1–10).
mod common;

mod daemon_sync {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use p2p_core::PeerId;
    use vault_sync::SyncMetadata;
    use vault_sync::fs::FileSystem;
    use vault_sync::network::VaultGossipExt;

    use sync_daemon::daemon::Daemon;
    use sync_daemon::watcher::FileEvent;

    use super::common::{
        build_node, connect_nodes, inject_deleted, inject_modified, shared_vault_id, spawn_daemon,
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

    // ── tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_file_change_propagates_to_peer() -> anyhow::Result<()> {
        let node_a = build_node(20).await?;
        let node_b = build_node(21).await?;

        connect_nodes(&node_a, &node_b).await?;

        // A subscribes first (empty bootstrap), B subscribes bootstrapping off A.
        // Each node joins gossip exactly once to avoid non-deterministic delivery.
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

        // Write file to A's filesystem, then inject the modification event.
        daemon_a
            .fs
            .write("notes/hello.md", b"# Hello World")
            .await?;
        inject_modified(&daemon_a, "notes/hello.md");

        // Wait for B to receive the file via gossip broadcast + QUIC pull.
        wait_until("B has notes/hello.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/hello.md".to_string())
            }
        })
        .await;

        let content_b = daemon_b.fs.read("notes/hello.md").await?;
        assert_eq!(content_b, b"# Hello World");

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// Pre-existing files sync when a new peer joins the gossip swarm.
    ///
    /// A has a file before gossip forms. When the swarm forms, `NeighborUp` fires
    /// on both sides and the daemon's `on_neighbor_up` handler initiates a full QUIC
    /// sync exchange. B ends up with A's pre-existing files.
    #[tokio::test]
    async fn test_neighbor_up_triggers_full_sync() -> anyhow::Result<()> {
        let node_a = build_node(22).await?;
        let node_b = build_node(23).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Write A's file directly into the vault before spawning the event loops.
        node_a
            .fs
            .write("notes/offline-edit.md", b"# Written offline")
            .await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/offline-edit.md").await?;
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

        // When gossip forms, NeighborUp fires and both daemons initiate full syncs.
        // B's on_neighbor_up handler pulls A's pre-existing file.
        wait_until("B has notes/offline-edit.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/offline-edit.md".to_string())
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// The daemon drives the variable-length vault-sync handshake to termination
    /// over a single QUIC bi-stream — the pumped exchange (X), not a one-shot
    /// request/reply/half-close.
    ///
    /// Two scenarios in one test (shared two-daemon setup):
    ///
    /// 1. **Diverged pair converges through the full pump.** A holds a note B
    ///    lacks, so the digests differ. B's `NeighborUp` opens a sync to A; the
    ///    exchange pumps `SyncRequest → DigestMismatch → SyncExchange →
    ///    SyncResponse`. We assert BOTH sides end byte-identical — not just that B
    ///    received A's note, but that the pumped responder converged in-exchange
    ///    too (A and B share the same `.md` set and the same content at each path).
    ///
    /// 2. **Converged pair settles in a no-op without corruption.** After step 1
    ///    the vaults are identical. A fresh NeighborUp (driven by a spurious
    ///    same-content edit on B) exchanges `SyncRequest → InSync` and transfers no
    ///    content — observed as: both vaults keep the identical note set with
    ///    byte-identical content, no spurious churn.
    ///
    /// The byte-level "zero content on a no-op" proof lives in vault-sync's
    /// `ByteCounter`/`full_sync_counting` at the lib layer; this daemon-level test
    /// proves the DAEMON drives the pump to a clean terminus over real QUIC.
    #[tokio::test]
    async fn test_daemon_pumps_variable_length_handshake() -> anyhow::Result<()> {
        let node_a = build_node(30).await?;
        let node_b = build_node(31).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Scenario 1 — diverged: A has two notes B lacks (offline edits).
        for (path, body) in [
            ("notes/alpha.md", b"# Alpha".as_slice()),
            ("notes/beta.md", b"# Beta".as_slice()),
        ] {
            node_a.fs.write(path, body).await?;
            let vault = node_a.vault.lock().await;
            vault.on_file_changed(path).await?;
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

        // The pumped exchange must converge BOTH sides to byte-identical state:
        // same `.md` set + same content at every path. A bidirectional pump (each
        // side initiates on NeighborUp) settles the responder in-exchange too, so
        // we assert full convergence, not merely "B received A's notes".
        wait_until("A and B converge to byte-identical state", || {
            let vault_a = daemon_a.vault.clone();
            let vault_b = daemon_b.vault.clone();
            let fs_a = daemon_a.fs.clone();
            let fs_b = daemon_b.fs.clone();
            async move {
                let files_a = vault_a.lock().await.list_files().await.unwrap_or_default();
                let files_b = vault_b.lock().await.list_files().await.unwrap_or_default();
                let mut a_sorted = files_a.clone();
                let mut b_sorted = files_b.clone();
                a_sorted.sort();
                b_sorted.sort();
                if a_sorted != b_sorted
                    || !a_sorted.contains(&"notes/alpha.md".to_string())
                    || !a_sorted.contains(&"notes/beta.md".to_string())
                {
                    return false;
                }
                // Byte-identical content at every shared path.
                for path in &a_sorted {
                    let ca = fs_a.read(path).await.ok();
                    let cb = fs_b.read(path).await.ok();
                    if ca.is_none() || ca != cb {
                        return false;
                    }
                }
                true
            }
        })
        .await;

        // Snapshot the converged content so scenario 2 can prove the no-op pump
        // left it untouched.
        let alpha_before = daemon_b.fs.read("notes/alpha.md").await?;

        // Scenario 2 — converged no-op: a spurious same-content Modified on B.
        // vault-sync's diff-and-merge yields no change (content matches the stored
        // Loro state), so this re-exercises the digest fast-path on the next
        // exchange. The pumped `SyncRequest → InSync` transfers no content; we
        // observe that the converged state is preserved with no corruption.
        inject_modified(&daemon_b, "notes/alpha.md");

        // Let the spurious event and any resulting exchange settle.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let alpha_after = daemon_b.fs.read("notes/alpha.md").await?;
        assert_eq!(
            alpha_before, alpha_after,
            "a converged-pair no-op exchange must not alter materialized content"
        );
        // A still has both notes unchanged — no spurious content churn from the no-op.
        let files_a = daemon_a
            .vault
            .lock()
            .await
            .list_files()
            .await
            .unwrap_or_default();
        assert!(
            files_a.contains(&"notes/alpha.md".to_string())
                && files_a.contains(&"notes/beta.md".to_string()),
            "A's note set must be intact after the no-op exchange"
        );

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// A pairing initiator adopts the mesh's VaultId, re-joins its gossip topic,
    /// and pulls the mesh's existing notes — the core of feature (b) pair-and-pull.
    ///
    /// A (responder) and B (initiator) start with *distinct* VaultIds, so they
    /// sit on different gossip topics and cannot sync — this is the pre-feature
    /// state. We then drive `Daemon::adopt_and_rejoin` on B with A's VaultId,
    /// which rewrites B's metadata.toml and swaps its gossip subscription onto
    /// A's topic. A `NeighborUp` fires and B's full-sync pulls A's pre-existing
    /// note.
    ///
    /// This test FAILS on pre-feature code: without `adopt_and_rejoin`, B never
    /// leaves its own topic, no NeighborUp fires, and the pull times out.
    ///
    /// Seeds 28/29.
    #[tokio::test]
    async fn test_initiator_adopts_vault_id_and_pulls() -> anyhow::Result<()> {
        let node_a = build_node(28).await?; // responder — owns the note
        let node_b = build_node(29).await?; // initiator — adopts A's VaultId

        connect_nodes(&node_a, &node_b).await?;

        // Precondition: the two vaults start on DIFFERENT VaultIds. If build_node
        // ever shared ids this would fire and the test below would pass trivially.
        let a_vault_id = node_a.vault.lock().await.vault_id();
        let b_vault_id = node_b.vault.lock().await.vault_id();
        assert_ne!(
            a_vault_id, b_vault_id,
            "test requires distinct initial VaultIds"
        );

        // Seed a note into A's vault before any topic forms.
        node_a
            .fs
            .write("notes/from-responder.md", b"# Shared from the mesh")
            .await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/from-responder.md").await?;
        }

        // A joins gossip on A's VaultId. B joins on B's (different) VaultId — at
        // this point they're on separate topics and cannot reach each other.
        let a_node_endpoint = node_a.sync_node.node_id();
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&a_vault_id, vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&b_vault_id, vec![])
            .await?;

        let daemon_a = spawn_daemon(node_a, gossip_a);

        // Build B's daemon inline so we hold `&mut daemon` to drive adoption,
        // mirroring test_status_broadcast_on_neighbor_events.
        let b_vault = node_b.vault.clone();
        let b_fs = node_b.fs.clone();
        let (_b_file_tx, b_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let b_shutdown = CancellationToken::new();
        let mut daemon_b = Daemon::new(
            b_vault.clone(),
            node_b.sync_node,
            gossip_b,
            b_file_rx,
            None,
            node_b.allowlist.clone(),
            "device-b".to_string(),
            None,
            "/test-vault-b".into(),
            b_shutdown.clone(),
        );

        // Adopt A's VaultId and re-join its gossip topic, bootstrapping off A.
        // `adopt_and_rejoin` speaks the iroh layer's `vault_sync::VaultId`; bridge
        // A's vault-sync VaultId through `u64`.
        daemon_b
            .adopt_and_rejoin(
                a_vault_id,
                vec![PeerId::from_bytes(*a_node_endpoint.as_bytes())],
            )
            .await?;

        // In-memory id reflects the adoption immediately.
        assert_eq!(
            b_vault.lock().await.vault_id(),
            a_vault_id,
            "B should have adopted A's VaultId in memory"
        );

        // Now spawn B's event loop — it's subscribed to A's topic, so NeighborUp
        // fires and B pulls A's pre-existing note.
        let b_loop = tokio::spawn(async move {
            daemon_b.run_loop().await;
        });

        // Poll for the pull rather than asserting immediately: A's gossip→B sync
        // may fire (and warn) before B's loop is ready, then B initiates its own
        // sync from A — benign timing. wait_until rides through it.
        wait_until("B pulled notes/from-responder.md", || {
            let vault = b_vault.clone();
            async move {
                let vault = vault.lock().await;
                vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/from-responder.md".to_string())
            }
        })
        .await;

        // The pulled content matches what A wrote.
        let pulled = b_fs.read("notes/from-responder.md").await?;
        assert_eq!(
            String::from_utf8_lossy(&pulled),
            "# Shared from the mesh",
            "pulled note content should match A's"
        );

        // Persistence regression guard: B's metadata.toml on disk reflects the
        // adopted id, not just the in-memory field.
        let meta_bytes = b_fs.read(".sync/metadata.toml").await?;
        let meta: SyncMetadata = toml::from_str(&String::from_utf8(meta_bytes.to_vec())?)?;
        assert_eq!(
            meta.vault_id, a_vault_id,
            "B's metadata.toml should persist the adopted VaultId"
        );

        b_shutdown.cancel();
        daemon_a.shutdown.cancel();
        let _ = b_loop.await;
        let _ = daemon_a.loop_handle.await;

        Ok(())
    }

    /// A file deletion on daemon A propagates to daemon B.
    ///
    /// A file is first synced to B, then A injects a `FileEvent::Deleted`. The
    /// daemon's `on_file_deleted` handler calls `vault.delete_file()` and broadcasts
    /// via gossip. B receives the notification, pulls the updated registry state
    /// from A, and removes the file from its own vault.
    ///
    /// The file is seeded via the NeighborUp full sync rather than a `Modified`
    /// event so the deletion's gossip `ChangeNotification{path}` is the FIRST
    /// notification for that path. A `Modified`-seeded create broadcasts an
    /// IDENTICAL `ChangeNotification{path}`, and iroh-gossip suppresses a
    /// byte-identical message within a 90s window by its content-derived id — so a
    /// create-notification followed by a same-path delete-notification collides and
    /// the deletion is dropped at the receiver. That fragility is pre-existing and
    /// orthogonal to coalescing (a manual ~500ms delay before a `Modified`-seeded
    /// create + delete reproduces it on the pre-coalescer engine); the move-coalescer's
    /// intended buffering delay (P4f-1) merely makes the collision window reliable
    /// rather than timing-lucky. Seeding via full sync isolates the delete-propagation
    /// contract this test owns from that notification-layer issue (Issue 2 / anti-entropy).
    #[tokio::test]
    async fn test_file_deletion_propagates() -> anyhow::Result<()> {
        let node_a = build_node(24).await?;
        let node_b = build_node(25).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Seed the file into A's vault BEFORE gossip forms so B receives it via the
        // NeighborUp full sync (no create `ChangeNotification` broadcast).
        node_a
            .fs
            .write("notes/delete-me.md", b"to be deleted")
            .await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/delete-me.md").await?;
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

        wait_until("B has notes/delete-me.md before deletion", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/delete-me.md".to_string())
            }
        })
        .await;

        // Step 2: Delete from A. The daemon's on_file_deleted handler removes the
        // file from the vault and broadcasts the change via gossip. B pulls the
        // updated state and removes the file from its vault.
        inject_deleted(&daemon_a, "notes/delete-me.md");

        wait_until("B no longer has notes/delete-me.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                !vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/delete-me.md".to_string())
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// An inbound-sync write does not trigger a re-broadcast when the OS watcher
    /// fires a spurious `Modified` event for the same path.
    ///
    /// After receiving a file via inbound sync, the file watcher may fire `Modified`
    /// for the path the daemon just wrote. Because `on_file_changed` is echo-safe
    /// (diff-and-merge returns `false` when the disk content matches the stored Loro
    /// state), the daemon sees `changed = false` and skips broadcasting.
    ///
    /// We verify by injecting a spurious `Modified` on B after A→B sync completes
    /// and asserting B's content is unchanged — no corruption from re-broadcasting
    /// stale local state.
    #[tokio::test]
    async fn test_inbound_sync_does_not_rebroadcast() -> anyhow::Result<()> {
        let node_a = build_node(26).await?;
        let node_b = build_node(27).await?;

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

        // Sync a file from A to B.
        daemon_a
            .fs
            .write("notes/synced.md", b"synced content")
            .await?;
        inject_modified(&daemon_a, "notes/synced.md");

        wait_until("B has notes/synced.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                vault
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/synced.md".to_string())
            }
        })
        .await;

        // Inject a spurious Modified on B — simulating what the OS watcher would see
        // after the daemon writes the synced file to disk. The sync flag was consumed
        // during the inbound sync, so this event goes through normally without corruption.
        inject_modified(&daemon_b, "notes/synced.md");

        // Give the event time to process.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // B's file content should be unchanged.
        let content = daemon_b.fs.read("notes/synced.md").await?;
        assert_eq!(
            content, b"synced content",
            "B's file content should be unchanged after spurious Modified event"
        );

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }
}
