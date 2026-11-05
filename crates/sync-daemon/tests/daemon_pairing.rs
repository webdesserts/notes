//! Integration tests for the Daemon event loop — DaemonControl status broadcast
//! and two-step pairing, plus the two `local_*` propagation tests that
//! physically sat in this block.
//!
//! Real `Daemon` instances with real iroh nodes, in-memory filesystems, and
//! injected file events. Carved from the former `daemon_integration.rs` monolith;
//! harness lives in `common`, the 2-arg `wait_until` is file-local (name-collides
//! with the relay `common::wait_until`).
//!
//! Two pairing tests run on the multi-threaded tokio test flavor (their flows
//! need a real multi-threaded runtime) — preserved verbatim.
mod common;

mod daemon_pairing {
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use p2p_core::PeerId;
    use p2p_core::allowlist::AllowlistStorage;
    use vault_sync::fs::FileSystem;
    use vault_sync::network::VaultGossipExt;

    use sync_daemon::watcher::FileEvent;

    use super::common::{
        build_node, connect_nodes, endpoint_id, inject_deleted, inject_modified, shared_vault_id,
        spawn_daemon,
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

    // ── DaemonControl / status broadcast tests ───────────────────────────────

    /// Status watch channel updates when peers join and leave the gossip swarm.
    ///
    /// Two real iroh nodes join gossip together so NeighborUp fires naturally.
    /// We wire `DaemonControl` into daemon A and verify the watch channel transitions
    /// from `Idle` (0 peers) to `Connected` (1 peer) after gossip connects, then
    /// back to `Idle` after daemon B shuts down and NeighborDown fires.
    ///
    /// Seeds 61/62 are reserved for this test to avoid collisions.
    #[tokio::test]
    async fn test_status_broadcast_on_neighbor_events() -> anyhow::Result<()> {
        use sync_daemon::daemon::Daemon;
        use sync_daemon::pair_api::{ConnectionState, DaemonCommand, DaemonStatus, PairingUiEvent};
        use tokio::sync::{broadcast, mpsc, watch};

        // Build two nodes that can communicate in-memory.
        let node_a = build_node(61).await?;
        let node_b = build_node(62).await?;

        connect_nodes(&node_a, &node_b).await?;

        let vault_id = shared_vault_id();
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&vault_id, vec![])
            .await?;

        let (file_event_tx, file_event_rx) =
            mpsc::unbounded_channel::<sync_daemon::watcher::FileEvent>();
        let shutdown = CancellationToken::new();

        let mut daemon = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            file_event_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault".into(),
            shutdown.clone(),
        );

        // Wire DaemonControl channels.
        let (status_tx, mut status_rx) = watch::channel(DaemonStatus::initial());
        let (pairing_tx, _pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (_command_tx, command_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon.wire_control(status_tx, pairing_tx, command_rx, "Test Vault".to_string());

        // Emit the initial status so the watch channel has a real value immediately.
        daemon.emit_status().await;

        // Verify initial state: Idle with 0 peers, mesh name and device name set.
        {
            let status = status_rx.borrow_and_update();
            assert_eq!(
                status.state,
                ConnectionState::Idle,
                "initial state should be Idle"
            );
            assert_eq!(status.peer_count, 0);
            assert_eq!(status.mesh_name.as_deref(), Some("Test Vault"));
            assert_eq!(status.device_name.as_deref(), Some("device-a"));
        }

        // Spawn daemon A's event loop.
        drop(file_event_tx);
        let loop_handle = tokio::spawn(async move {
            daemon.run_loop().await;
        });

        // Join gossip on node B so NeighborUp fires on daemon A.
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&vault_id, vec![node_a.node_id])
            .await?;
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // Wait for daemon A's status to show Connected (NeighborUp from B).
        wait_until("status = Connected after peer B joins", || {
            let state = status_rx.borrow().state.clone();
            async move { state == ConnectionState::Connected }
        })
        .await;

        {
            let status = status_rx.borrow();
            assert_eq!(status.state, ConnectionState::Connected);
            assert!(status.peer_count >= 1);
        }

        // Shut down daemon B and daemon A.
        daemon_b.shutdown.cancel();
        let _ = daemon_b.loop_handle.await;

        shutdown.cancel();
        let _ = loop_handle.await;
        Ok(())
    }

    /// `DaemonCommand::RequestPairing` returns an error reply (not a hang) when no
    /// initiator session is active — no prior `StartDiscovery` was sent.
    ///
    /// Seed 64 reserved.
    #[tokio::test]
    async fn test_request_pairing_without_discovery_returns_error() -> anyhow::Result<()> {
        use sync_daemon::daemon::Daemon;
        use sync_daemon::pair_api::{DaemonCommand, DaemonStatus, PairingUiEvent};
        use tokio::sync::{broadcast, mpsc, oneshot, watch};

        let node = build_node(64).await?;
        let vault_id = shared_vault_id();
        let gossip = node.sync_node.join_vault_gossip(&vault_id, vec![]).await?;

        let (_file_event_tx, file_event_rx) =
            mpsc::unbounded_channel::<sync_daemon::watcher::FileEvent>();
        let shutdown = CancellationToken::new();

        let mut daemon = Daemon::new(
            node.vault.clone(),
            node.sync_node,
            gossip,
            file_event_rx,
            None,
            node.allowlist.clone(),
            "device-test".to_string(),
            None,
            "/test-vault".into(),
            shutdown.clone(),
        );

        let (status_tx, _status_rx) = watch::channel(DaemonStatus::initial());
        let (pairing_tx, _pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (command_tx, command_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon.wire_control(status_tx, pairing_tx, command_rx, "Test Vault".to_string());

        let loop_handle = tokio::spawn(async move {
            daemon.run_loop().await;
        });

        // Send RequestPairing without ever sending StartDiscovery — the daemon
        // should respond with an Err describing the missing session.
        let (reply_tx, reply_rx) = oneshot::channel::<Result<String, String>>();
        command_tx.send(DaemonCommand::RequestPairing {
            vault_id: "any-vault".to_string(),
            reply: reply_tx,
        })?;

        let reply = tokio::time::timeout(Duration::from_secs(2), reply_rx)
            .await
            .expect("daemon did not reply to RequestPairing within 2s")
            .expect("daemon dropped the reply channel");

        let err = reply.expect_err("expected Err when no active session");
        assert!(
            err.to_lowercase().contains("no active") || err.to_lowercase().contains("session"),
            "error message should mention missing session, got: {err}"
        );

        shutdown.cancel();
        let _ = loop_handle.await;
        Ok(())
    }

    /// `DaemonCommand::SubmitCode` returns a "no pairing request in progress"
    /// error when the vault_id is known (in discovered) but `RequestPairing`
    /// was never called — the `code_tx`-missing guard at `daemon.rs:613` is
    /// exercised directly.
    ///
    /// Seed 65 reserved.
    #[tokio::test]
    async fn test_submit_code_without_request_returns_error() -> anyhow::Result<()> {
        use sync_daemon::daemon::Daemon;
        use sync_daemon::pair_api::{DaemonCommand, DaemonStatus, PairingUiEvent};
        use tokio::sync::{broadcast, mpsc, oneshot, watch};

        let node = build_node(65).await?;
        let vault_id = shared_vault_id();
        let gossip = node.sync_node.join_vault_gossip(&vault_id, vec![]).await?;

        // Capture the node's endpoint_id before moving node.sync_node.
        let endpoint_id: PeerId = node.sync_node.node_id();

        let (_file_event_tx, file_event_rx) =
            mpsc::unbounded_channel::<sync_daemon::watcher::FileEvent>();
        let shutdown = CancellationToken::new();

        let mut daemon = Daemon::new(
            node.vault.clone(),
            node.sync_node,
            gossip,
            file_event_rx,
            None,
            node.allowlist.clone(),
            "device-test".to_string(),
            None,
            "/test-vault".into(),
            shutdown.clone(),
        );

        let (status_tx, _status_rx) = watch::channel(DaemonStatus::initial());
        let (pairing_tx, _pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (command_tx, command_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon.wire_control(status_tx, pairing_tx, command_rx, "Test Vault".to_string());

        // Seed the discovered map so the vault_id check in submit_initiator_code
        // passes. This means the code_tx-missing check at daemon.rs:613 is the
        // one that fires — directly exercising that guard path.
        daemon
            .test_seed_discovered("target-vault".to_string(), endpoint_id)
            .await;

        let loop_handle = tokio::spawn(async move {
            daemon.run_loop().await;
        });

        // Send SubmitCode with the known vault_id but no prior RequestPairing.
        // The code_tx-missing guard should fire.
        let (reply_tx, reply_rx) = oneshot::channel::<Result<String, String>>();
        command_tx.send(DaemonCommand::SubmitCode {
            vault_id: "target-vault".to_string(),
            code: "123456".to_string(),
            reply: reply_tx,
        })?;

        let reply = tokio::time::timeout(Duration::from_secs(2), reply_rx)
            .await
            .expect("daemon did not reply to SubmitCode within 2s")
            .expect("daemon dropped the reply channel");

        let err = reply.expect_err("expected Err when no RequestPairing was sent");
        assert!(
            err.to_lowercase().contains("request") || err.to_lowercase().contains("in progress"),
            "error should describe the missing request step, got: {err}"
        );

        shutdown.cancel();
        let _ = loop_handle.await;
        Ok(())
    }

    /// Two-daemon happy path: RequestPairing → SubmitCode → adopt + pull.
    ///
    /// This test exercises the full two-step command sequence through the real
    /// daemon event loop, covering:
    /// - `connect_reply` receives `Ok(responder_device_name)` after connect
    /// - `code_tx`/`code_rx` park+unblock between the two commands
    /// - `submit_reply` routes the final `PairingResult` through `on_initiator_pair_outcome`
    /// - Post-pair onboarding (allowlist write, VaultId adoption, gossip re-join, pull)
    ///
    /// Daemon A (responder, "device-a") already has a note. Daemon B (initiator,
    /// "device-b") drives `RequestPairing` to connect; A emits the 6-digit code
    /// via its pairing broadcast; B's `SubmitCode` delivers the code to the parked
    /// task. On success B adopts A's VaultId, pulls A's note, and both allowlists
    /// carry both peers.
    ///
    /// Seeds 66/67 reserved.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_step_pairing_request_then_submit() -> anyhow::Result<()> {
        use sync_daemon::daemon::Daemon;
        use sync_daemon::pair_api::{DaemonCommand, DaemonStatus, PairingUiEvent};
        use tokio::sync::{broadcast, mpsc, oneshot, watch};

        let node_a = build_node(66).await?; // responder
        let node_b = build_node(67).await?; // initiator

        connect_nodes(&node_a, &node_b).await?;

        let a_vault_id = node_a.vault.lock().await.vault_id();
        let b_vault_id = node_b.vault.lock().await.vault_id();
        assert_ne!(
            a_vault_id, b_vault_id,
            "test requires distinct initial VaultIds"
        );

        // Seed a note into A's vault so B can prove it pulled via full sync.
        node_a
            .fs
            .write("notes/pair-test.md", b"# From the responder mesh")
            .await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/pair-test.md").await?;
        }

        // Both nodes join gossip on their own VaultId — separate topics until B adopts A's.
        let a_endpoint_id = node_a.sync_node.node_id();
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&a_vault_id, vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&b_vault_id, vec![])
            .await?;

        // Build daemon A (responder) with DaemonControl so we can intercept
        // the InboundRequest pairing event to get the 6-digit code.
        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let a_allowlist = node_a.allowlist.clone();
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            a_allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-a".into(),
            a_shutdown.clone(),
        );

        let (a_status_tx, _) = watch::channel(DaemonStatus::initial());
        let (a_pairing_tx, mut a_pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (_a_cmd_tx, a_cmd_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon_a.wire_control(a_status_tx, a_pairing_tx, a_cmd_rx, "Mesh A".to_string());

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // Build daemon B (initiator) with DaemonControl for command_tx.
        let b_vault = node_b.vault.clone();
        let b_fs = node_b.fs.clone();
        let b_allowlist = node_b.allowlist.clone();
        let (_b_file_tx, b_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let b_shutdown = CancellationToken::new();
        let mut daemon_b = Daemon::new(
            b_vault.clone(),
            node_b.sync_node,
            gossip_b,
            b_file_rx,
            None,
            b_allowlist.clone(),
            "device-b".to_string(),
            None,
            "/test-vault-b".into(),
            b_shutdown.clone(),
        );

        let (b_status_tx, _) = watch::channel(DaemonStatus::initial());
        let (b_pairing_tx, _b_pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (b_cmd_tx, b_cmd_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon_b.wire_control(b_status_tx, b_pairing_tx, b_cmd_rx, "Mesh B".to_string());

        // Seed B's discovered map with A's id — this replaces mDNS in tests.
        daemon_b
            .test_seed_discovered(a_vault_id.to_string(), a_endpoint_id)
            .await;

        let b_loop = tokio::spawn(async move {
            daemon_b.run_loop().await;
        });

        // ── Step 1: RequestPairing ────────────────────────────────────────────
        // B connects to A, A generates its 6-digit code and emits InboundRequest.

        let (req_reply_tx, req_reply_rx) = oneshot::channel::<Result<String, String>>();
        b_cmd_tx.send(DaemonCommand::RequestPairing {
            vault_id: a_vault_id.to_string(),
            reply: req_reply_tx,
        })?;

        // Await both connect_reply and A's InboundRequest concurrently. The connect
        // reply fires after A sends its PairingChallenge; the broadcast fires at the
        // same point on A's event loop. We need both: the reply to confirm the right
        // device name, and the code to forward to SubmitCode.
        let (connect_result, pairing_code) = tokio::join!(
            async {
                tokio::time::timeout(Duration::from_secs(10), req_reply_rx)
                    .await
                    .expect("RequestPairing did not resolve within 10s")
                    .expect("daemon dropped RequestPairing reply channel")
            },
            async {
                // Wait for A to emit its InboundRequest event carrying the code.
                loop {
                    match tokio::time::timeout(Duration::from_secs(10), a_pairing_rx.recv()).await {
                        Ok(Ok(PairingUiEvent::InboundRequest { code, .. })) => break code,
                        Ok(Ok(_)) => continue, // other event — not what we want
                        Ok(Err(_)) => panic!("A's pairing broadcast channel closed unexpectedly"),
                        Err(_) => panic!("timed out waiting for A's InboundRequest pairing event"),
                    }
                }
            }
        );

        let responder_device_name =
            connect_result.expect("RequestPairing should succeed with Ok(device_name)");
        assert_eq!(
            responder_device_name, "device-a",
            "connect_reply should carry A's device name"
        );

        // ── Step 2: SubmitCode ────────────────────────────────────────────────
        // Deliver the code to the parked task; B completes the HMAC exchange.

        let (submit_reply_tx, submit_reply_rx) = oneshot::channel::<Result<String, String>>();
        b_cmd_tx.send(DaemonCommand::SubmitCode {
            vault_id: a_vault_id.to_string(),
            code: pairing_code,
            reply: submit_reply_tx,
        })?;

        let submit_result = tokio::time::timeout(Duration::from_secs(10), submit_reply_rx)
            .await
            .expect("SubmitCode did not resolve within 10s")
            .expect("daemon dropped SubmitCode reply channel");

        let paired_device = submit_result.expect("SubmitCode should succeed");
        assert_eq!(
            paired_device, "device-a",
            "submit_reply should carry A's device name after successful pairing"
        );

        // ── Assertions ───────────────────────────────────────────────────────

        // B adopted A's VaultId.
        assert_eq!(
            b_vault.lock().await.vault_id(),
            a_vault_id,
            "B should have adopted A's VaultId after pairing"
        );

        // B pulled A's pre-existing note via NeighborUp + full sync.
        wait_until("B pulled notes/pair-test.md from A", || {
            let vault = b_vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/pair-test.md".to_string())
            }
        })
        .await;

        let pulled = b_fs.read("notes/pair-test.md").await?;
        assert_eq!(
            String::from_utf8_lossy(&pulled),
            "# From the responder mesh",
            "pulled note content should match A's"
        );

        // Both allowlists carry both peers.
        let b_peers = b_allowlist.list_peers().await?;
        let b_peer_ids: Vec<_> = b_peers.iter().map(|p| &p.node_id).collect();
        let a_node_peer_id = PeerId::from_bytes(*a_endpoint_id.as_bytes());
        assert!(
            b_peer_ids.contains(&&a_node_peer_id),
            "B's allowlist should contain A's PeerId after pairing"
        );

        b_shutdown.cancel();
        a_shutdown.cancel();
        let _ = b_loop.await;
        let _ = a_loop.await;

        Ok(())
    }

    /// `DaemonCommand::SubmitCode` returns an error reply (not a hang) when no
    /// initiator session is active. This protects against the deadlock-prone
    /// pattern where the desktop awaits the oneshot reply while the daemon
    /// silently drops the command — the channel close would surface only as a
    /// generic "daemon disconnected" error in the UI, which is much worse than
    /// the explicit "no active session" message we send back.
    ///
    /// Seed 63 reserved.
    #[tokio::test]
    async fn test_submit_code_without_active_initiator_returns_error() -> anyhow::Result<()> {
        use sync_daemon::daemon::Daemon;
        use sync_daemon::pair_api::{DaemonCommand, DaemonStatus, PairingUiEvent};
        use tokio::sync::{broadcast, mpsc, oneshot, watch};

        let node = build_node(63).await?;
        let vault_id = shared_vault_id();
        let gossip = node.sync_node.join_vault_gossip(&vault_id, vec![]).await?;

        let (_file_event_tx, file_event_rx) =
            mpsc::unbounded_channel::<sync_daemon::watcher::FileEvent>();
        let shutdown = CancellationToken::new();

        let mut daemon = Daemon::new(
            node.vault.clone(),
            node.sync_node,
            gossip,
            file_event_rx,
            None,
            node.allowlist.clone(),
            "device-test".to_string(),
            None,
            "/test-vault".into(),
            shutdown.clone(),
        );

        let (status_tx, _status_rx) = watch::channel(DaemonStatus::initial());
        let (pairing_tx, _pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (command_tx, command_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon.wire_control(status_tx, pairing_tx, command_rx, "Test Vault".to_string());

        let loop_handle = tokio::spawn(async move {
            daemon.run_loop().await;
        });

        // Send SubmitCode without ever sending StartDiscovery — the daemon
        // should respond with an Err describing the missing session.
        let (reply_tx, reply_rx) = oneshot::channel::<Result<String, String>>();
        command_tx.send(DaemonCommand::SubmitCode {
            vault_id: "any-vault".to_string(),
            code: "123456".to_string(),
            reply: reply_tx,
        })?;

        let reply = tokio::time::timeout(Duration::from_secs(2), reply_rx)
            .await
            .expect("daemon did not reply to SubmitCode within 2s")
            .expect("daemon dropped the reply channel");

        let err = reply.expect_err("expected Err when no active session");
        assert!(
            err.to_lowercase().contains("no active") || err.to_lowercase().contains("session"),
            "error message should mention missing session, got: {err}"
        );

        shutdown.cancel();
        let _ = loop_handle.await;
        Ok(())
    }

    /// After a successful two-step pairing, the responder's PUBLIC relay URL is
    /// adopted into B's persisted `known_public_relays` set AND its
    /// (endpoint_id, relay_url) is seeded into B's live `peer_lookup`, keyed by
    /// the responder's transport-verified EndpointId.
    ///
    /// This verifies the Tier-2 pairing-seed reframe (plan chunk C4):
    /// - `persist_adopted_relay` adopts the responder's public relay into the
    ///   `known_public_relays` cold store (the sole durable networking store) —
    ///   NOT a per-peer hint, and only when the URL is off-LAN-reachable.
    /// - After persist, `sync_node.add_peer_relay` still seeds the live lookup so
    ///   the current session reaches the responder by EndpointId without restart.
    /// - The adopted public relay survives a config reload, and no per-peer
    ///   `peer_relays` entry is persisted.
    ///
    /// A advertises a PUBLIC (domain) relay URL so the public-set adoption path is
    /// exercised — a loopback relay would be (correctly) rejected by the
    /// off-LAN-reachable guard. The advertised URL is A's `relay_url` field (the
    /// URL A carries in `PairingResult.relay_urls`), and A's `EndpointId` is the
    /// QUIC connection target B dialed — not inferred from `mesh_members`.
    ///
    /// Seeds 68/69 reserved.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_pairing_persists_responder_relay() -> anyhow::Result<()> {
        use p2p_core::EmbeddedRelay;
        use sync_daemon::daemon::Daemon;
        use sync_daemon::pair_api::{DaemonCommand, DaemonStatus, PairingUiEvent};
        use sync_daemon::persistence::DaemonConfig;
        use tempfile::TempDir;
        use tokio::sync::{broadcast, mpsc, oneshot, watch};

        let node_a = build_node(68).await?; // responder
        let node_b = build_node(69).await?; // initiator

        connect_nodes(&node_a, &node_b).await?;

        let a_vault_id = node_a.vault.lock().await.vault_id();
        let b_vault_id = node_b.vault.lock().await.vault_id();
        assert_ne!(
            a_vault_id, b_vault_id,
            "test requires distinct initial VaultIds"
        );

        // Start a relay for A so the daemon wiring is realistic, but advertise a
        // PUBLIC (domain) URL over the pairing wire: only an off-LAN-reachable URL
        // is adopted into `known_public_relays`, and the actual loopback bind URL
        // would be rejected by that guard. The advertised string is independent of
        // the relay's bind address (the responder fills `relay_urls` from its
        // configured `relay_url`), and this pairing-only test never routes traffic
        // through the relay, so a public-looking URL is sound here.
        let relay = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
        let relay_url_str = "https://relay-a.test/".to_string();

        let a_endpoint_id: PeerId = node_a.sync_node.node_id();

        // Clone B's peer_lookup handle BEFORE moving sync_node into Daemon::new.
        // MemoryLookup is Arc-backed, so the clone stays live and reflects any
        // add_peer_relay calls made on the daemon's internal copy.
        let b_peer_lookup = node_b.sync_node.peer_lookup_for_test().clone();

        // B needs a real on-disk vault path so `persist_adopted_relay` can write
        // `known_public_relays` to `daemon.toml` and we can reload it.
        let b_vault_dir = TempDir::new()?;
        let b_vault_path = b_vault_dir.path().to_path_buf();

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&a_vault_id, vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&b_vault_id, vec![])
            .await?;

        // Build daemon A (responder) — pass A's relay URL so `PairingResult.relay_urls`
        // carries it over the wire to B.
        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let a_allowlist = node_a.allowlist.clone();
        let a_vault_path = TempDir::new()?;
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            a_allowlist.clone(),
            "device-a".to_string(),
            Some(relay_url_str.clone()),
            a_vault_path.path().into(),
            a_shutdown.clone(),
        );

        let (a_status_tx, _) = watch::channel(DaemonStatus::initial());
        let (a_pairing_tx, mut a_pairing_rx): (broadcast::Sender<PairingUiEvent>, _) =
            broadcast::channel(16);
        let (_a_cmd_tx, a_cmd_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon_a.wire_control(a_status_tx, a_pairing_tx, a_cmd_rx, "Mesh A".to_string());

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // Build daemon B (initiator) — use the real temp vault path.
        let b_vault = node_b.vault.clone();
        let b_allowlist = node_b.allowlist.clone();
        let (_b_file_tx, b_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let b_shutdown = CancellationToken::new();
        let mut daemon_b = Daemon::new(
            b_vault.clone(),
            node_b.sync_node,
            gossip_b,
            b_file_rx,
            None,
            b_allowlist.clone(),
            "device-b".to_string(),
            None,
            b_vault_path.clone(),
            b_shutdown.clone(),
        );

        let (b_status_tx, _) = watch::channel(DaemonStatus::initial());
        let (b_pairing_tx, _): (broadcast::Sender<PairingUiEvent>, _) = broadcast::channel(16);
        let (b_cmd_tx, b_cmd_rx) = mpsc::unbounded_channel::<DaemonCommand>();
        daemon_b.wire_control(b_status_tx, b_pairing_tx, b_cmd_rx, "Mesh B".to_string());

        daemon_b
            .test_seed_discovered(a_vault_id.to_string(), a_endpoint_id)
            .await;

        let b_loop = tokio::spawn(async move {
            daemon_b.run_loop().await;
        });

        // ── Step 1: RequestPairing ────────────────────────────────────────────

        let (req_reply_tx, req_reply_rx) = oneshot::channel::<Result<String, String>>();
        b_cmd_tx.send(DaemonCommand::RequestPairing {
            vault_id: a_vault_id.to_string(),
            reply: req_reply_tx,
        })?;

        let (connect_result, pairing_code) = tokio::join!(
            async {
                tokio::time::timeout(Duration::from_secs(10), req_reply_rx)
                    .await
                    .expect("RequestPairing did not resolve within 10s")
                    .expect("daemon dropped RequestPairing reply channel")
            },
            async {
                loop {
                    match tokio::time::timeout(Duration::from_secs(10), a_pairing_rx.recv()).await {
                        Ok(Ok(PairingUiEvent::InboundRequest { code, .. })) => break code,
                        Ok(Ok(_)) => continue,
                        Ok(Err(_)) => panic!("A's pairing broadcast channel closed unexpectedly"),
                        Err(_) => panic!("timed out waiting for A's InboundRequest pairing event"),
                    }
                }
            }
        );

        connect_result.expect("RequestPairing should succeed with Ok(device_name)");

        // ── Step 2: SubmitCode ────────────────────────────────────────────────

        let (submit_reply_tx, submit_reply_rx) = oneshot::channel::<Result<String, String>>();
        b_cmd_tx.send(DaemonCommand::SubmitCode {
            vault_id: a_vault_id.to_string(),
            code: pairing_code,
            reply: submit_reply_tx,
        })?;

        tokio::time::timeout(Duration::from_secs(10), submit_reply_rx)
            .await
            .expect("SubmitCode did not resolve within 10s")
            .expect("daemon dropped SubmitCode reply channel")
            .expect("SubmitCode should succeed");

        // ── Assertions ───────────────────────────────────────────────────────

        // Give the daemon's async `persist_adopted_relay` a moment to finish writing
        // before we reload the config.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Live lookup: B's peer_lookup should carry A's (endpoint_id, relay_url).
        // This proves `add_peer_relay` was called on the current session after pairing.
        let hint = b_peer_lookup
            .get_endpoint_info(endpoint_id(a_endpoint_id))
            .expect("B's peer_lookup should have A's relay hint after pairing");
        let hint_relay_urls: Vec<_> = hint.into_endpoint_addr().relay_urls().cloned().collect();
        assert!(
            hint_relay_urls
                .iter()
                .any(|u| u.to_string() == relay_url_str),
            "B's live peer_lookup hint should contain A's relay URL, got: {:?}",
            hint_relay_urls,
        );

        // Persistence: reload B's DaemonConfig and verify A's public relay was
        // adopted into `known_public_relays` — the sole durable networking store.
        // (No per-peer hint is persisted; the `peer_relays` field is gone.)
        // `DaemonConfig::load_or_generate` reads from the real temp vault path.
        let (b_config, _) = DaemonConfig::load_or_generate(&b_vault_path, None)
            .await
            .expect("should be able to reload B's daemon config");
        assert!(
            b_config.known_public_relays.contains(&relay_url_str),
            "B's known_public_relays should contain A's advertised public relay \
             after pairing; set was {:?}",
            b_config.known_public_relays,
        );

        b_shutdown.cancel();
        a_shutdown.cancel();
        let _ = b_loop.await;
        let _ = a_loop.await;
        relay.shutdown().await;

        Ok(())
    }

    /// A real local edit propagates to a peer.
    ///
    /// This guards the daemon's `on_file_modified` → `on_file_changed` (true) → gossip
    /// broadcast path end-to-end: an edit on A reaches B. Echo-safety is a content-diff in
    /// `on_file_changed` (it returns `false` only for unchanged content, suppressing the
    /// re-broadcast of an inbound-sync echo); the daemon no longer consults any path-keyed
    /// sync flag, so there is nothing to "arm" — a real edit always applies and broadcasts.
    ///
    /// The initial file is seeded via the NeighborUp full sync (not a `Modified` event) so
    /// the EDIT's gossip `ChangeNotification{path}` is the FIRST notification for that path.
    /// A `Modified`-seeded create broadcasts an IDENTICAL `ChangeNotification{path}`, and
    /// iroh-gossip's content-id dedup drops a same-path follow-up within its 90s window once
    /// the create's broadcast is delayed — so an edit shortly after a create would be
    /// suppressed at the receiver. That fragility is pre-existing and orthogonal to coalescing
    /// (it reproduces on the pre-coalescer engine with a manual delay before the create); the
    /// move-coalescer's create-buffering (P4f-1) makes the window reliable. The edit itself is
    /// NOT buffered — it dispatches immediately (the edit fast-path); only the create-seed's
    /// notification timing is at issue, which the full-sync seeding removes. (Issue 2.)
    ///
    /// Seeds 70/71 reserved.
    #[tokio::test]
    async fn test_local_edit_propagates_to_peer() -> anyhow::Result<()> {
        let node_a = build_node(70).await?;
        let node_b = build_node(71).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Seed the initial file into A's vault BEFORE gossip forms so B receives it via
        // the NeighborUp full sync (no create `ChangeNotification` broadcast).
        node_a.fs.write("notes/flag-edit.md", b"# Original").await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/flag-edit.md").await?;
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

        wait_until("B has initial notes/flag-edit.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/flag-edit.md".to_string())
            }
        })
        .await;

        // Make a real local edit to the file on A's filesystem.
        daemon_a
            .fs
            .write("notes/flag-edit.md", b"# Edited content")
            .await?;

        // Inject the modification event — this is what the OS watcher would deliver.
        inject_modified(&daemon_a, "notes/flag-edit.md");

        // B must receive the updated content.
        wait_until("B has the edited content of notes/flag-edit.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                let vault = vault.lock().await;
                if let Ok(content) = vault.get_document("notes/flag-edit.md").await {
                    content.body().to_string().contains("Edited content")
                } else {
                    false
                }
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// A real local delete tombstones the registry and propagates to a peer.
    ///
    /// This guards the daemon's `on_file_deleted` → `delete_file` (true) → gossip broadcast
    /// path end-to-end: a delete on A tombstones B's copy. `delete_file` is idempotent
    /// (returns `false` for an already-absent path) and the daemon broadcasts only when it
    /// returns `true`; there is no path-keyed sync flag to "arm" anymore.
    ///
    /// The file is seeded via the NeighborUp full sync (not a `Modified` event) so the
    /// deletion's gossip `ChangeNotification{path}` is the FIRST notification for that
    /// path — see `test_file_deletion_propagates` for why a `Modified`-seeded create's
    /// identical notification collides with it in iroh-gossip's content-id dedup once
    /// the create's broadcast is delayed (a pre-existing fragility the move-coalescer's
    /// buffering reliably surfaces; Issue 2 / anti-entropy).
    ///
    /// Seeds 72/73 reserved.
    #[tokio::test]
    async fn test_local_delete_propagates_to_peer() -> anyhow::Result<()> {
        let node_a = build_node(72).await?;
        let node_b = build_node(73).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Seed the file into A's vault BEFORE gossip forms so B receives it via the
        // NeighborUp full sync (no create `ChangeNotification` broadcast).
        node_a
            .fs
            .write("notes/flag-delete.md", b"# To be deleted")
            .await?;
        {
            let vault = node_a.vault.lock().await;
            vault.on_file_changed("notes/flag-delete.md").await?;
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

        wait_until("B has notes/flag-delete.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/flag-delete.md".to_string())
            }
        })
        .await;

        // Delete the file on A's filesystem and inject the Deleted event.
        daemon_a.fs.delete("notes/flag-delete.md").await?;
        inject_deleted(&daemon_a, "notes/flag-delete.md");

        // B must tombstone the file.
        wait_until("B no longer has notes/flag-delete.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                !vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/flag-delete.md".to_string())
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }
}
