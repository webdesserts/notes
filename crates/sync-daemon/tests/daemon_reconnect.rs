//! Integration tests for the Daemon event loop — reconnect supervisor + roster/
//! relay convergence.
//!
//! Real `Daemon` instances with real iroh nodes, in-memory filesystems, and
//! injected file events. Carved from the former `daemon_integration.rs` monolith;
//! harness lives in `common`, the 2-arg `wait_until` is file-local (name-collides
//! with the relay `common::wait_until`).
mod common;

mod daemon_reconnect {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use p2p_core::PeerId;
    use p2p_core::allowlist::AllowlistStorage;
    use vault_sync::fs::FileSystem;
    use vault_sync::network::VaultGossipExt;

    use sync_daemon::daemon::Daemon;
    use sync_daemon::watcher::{FileEvent, FileEventKind};

    use super::common::{
        build_node, connect_nodes, endpoint_id, now_ms_test, shared_vault_id, spawn_daemon,
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

    // ── reconnect supervisor ──────────────────────────────────────────────────

    /// The reconnect supervisor re-bootstraps gossip from a seeded hint so a
    /// partitioned daemon reconnects without a restart.
    ///
    /// This exercises a COLD START (B never bootstraps to A) rather than a live
    /// partition, but the two are functionally equivalent for recovery: after a
    /// `NeighborDown` the connection close clears iroh's `selected_path` and the
    /// remote-state actor idles out (~60s), so both paths hit the same address
    /// lookup on the supervisor's re-dial. Live post-partition recovery is
    /// validated on the real mesh after merge.
    ///
    /// Setup: A and B are wired for connectivity (`connect_nodes`) and both join
    /// the shared topic, but B does NOT bootstrap off A — so no swarm forms and A
    /// stays at zero neighbors. A's supervisor snapshot carries B's hint. When
    /// the tick fires, A re-bootstraps toward B, `NeighborUp` fires, and A's
    /// full-sync pulls a note B never could have delivered while partitioned.
    ///
    /// Seeds 80/81 reserved.
    #[tokio::test]
    async fn supervisor_rebootstraps_after_zero_neighbors() -> anyhow::Result<()> {
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(80).await?;
        let node_b = build_node(81).await?;

        connect_nodes(&node_a, &node_b).await?;

        // Seed a note into B's vault so A can prove it pulled after reconnecting.
        node_b
            .fs
            .write(
                "notes/from-partitioned-peer.md",
                b"# Delivered after reconnect",
            )
            .await?;
        {
            let vault = node_b.vault.lock().await;
            vault
                .on_file_changed("notes/from-partitioned-peer.md")
                .await?;
        }

        // B's hint, as it would appear in A's persisted snapshot. The relay URL
        // only needs to parse — direct addresses from `connect_nodes` carry the
        // actual dial.
        let b_endpoint_hex = node_b.sync_node.node_id().to_string();
        let b_hint = PeerRelay::new(b_endpoint_hex, "http://example.com:3340/".to_string());

        // A joins with no bootstrap; B joins with no bootstrap (NOT off A). They
        // share a topic but never dial each other — A is partitioned at zero
        // neighbors, the production failure shape.
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;

        // Build A's daemon inline so we can seed its supervisor snapshot and shrink
        // the tick before the loop starts.
        let a_vault = node_a.vault.clone();
        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            a_vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-a".into(),
            a_shutdown.clone(),
        );
        daemon_a.seed_peer_relays_snapshot(vec![b_hint]);
        daemon_a.set_reconnect_interval(Duration::from_millis(200));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // B's daemon just needs to be alive to answer A's sync pull.
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // A's supervisor re-bootstraps toward B; NeighborUp fires; A pulls B's note.
        wait_until(
            "A pulled notes/from-partitioned-peer.md after reconnect",
            || {
                let vault = a_vault.clone();
                async move {
                    vault
                        .lock()
                        .await
                        .list_files()
                        .await
                        .unwrap_or_default()
                        .contains(&"notes/from-partitioned-peer.md".to_string())
                }
            },
        )
        .await;

        a_shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = a_loop.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// When already connected, the supervisor stays idle — it does not churn the
    /// swarm or revert a healthy sync.
    ///
    /// A and B pair up normally (B bootstraps off A) and sync a file. A's
    /// supervisor snapshot still carries B's hint, but because A has a live
    /// neighbor the tick gates out at step 1 every wake. We prove non-interference
    /// by syncing a SECOND file across several tick periods after connection: if
    /// the supervisor were re-bootstrapping or otherwise disturbing the swarm, the
    /// steady-state sync would be at risk.
    ///
    /// Seeds 82/83 reserved.
    #[tokio::test]
    async fn supervisor_idle_when_connected() -> anyhow::Result<()> {
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(82).await?;
        let node_b = build_node(83).await?;

        connect_nodes(&node_a, &node_b).await?;

        let b_hint = PeerRelay::new(
            node_b.sync_node.node_id().to_string(),
            "http://example.com:3340/".to_string(),
        );

        // A and B form a normal swarm (B bootstraps off A).
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        let a_vault = node_a.vault.clone();
        let a_fs = node_a.fs.clone();
        let (a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            a_vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-a".into(),
            a_shutdown.clone(),
        );
        daemon_a.seed_peer_relays_snapshot(vec![b_hint]);
        // Fast tick so several supervisor wakes occur during the test window.
        daemon_a.set_reconnect_interval(Duration::from_millis(100));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        let daemon_b = spawn_daemon(node_b, gossip_b);

        // First sync: proves the swarm formed.
        a_fs.write("notes/first.md", b"# First").await?;
        a_file_tx
            .send(FileEvent {
                path: "notes/first.md".to_string(),
                kind: FileEventKind::Modified,
            })
            .expect("file event channel closed");

        wait_until("B has notes/first.md", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/first.md".to_string())
            }
        })
        .await;

        // Let several supervisor ticks fire while connected — they must gate out.
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Second sync still works: the supervisor did not disturb the swarm.
        a_fs.write("notes/second.md", b"# Second").await?;
        a_file_tx
            .send(FileEvent {
                path: "notes/second.md".to_string(),
                kind: FileEventKind::Modified,
            })
            .expect("file event channel closed");

        wait_until("B has notes/second.md after idle supervisor ticks", || {
            let vault = daemon_b.vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/second.md".to_string())
            }
        })
        .await;

        a_shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = a_loop.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// The supervisor evicts a THROTTLED hint from the address-lookup but leaves
    /// a DUE hint present — the core of the stale-hint fix.
    ///
    /// While partitioned (zero neighbors), `on_reconnect_tick` decides per hint:
    /// a hint inside its backoff window (recent attempt + failures) is removed
    /// from `MemoryLookup` so iroh-gossip can't re-resolve and re-feed the dead
    /// relay; a hint that's due is re-seeded and dialed. We seed two hints into
    /// A's `peer_lookup` up front, run the supervisor with no peer to connect to,
    /// and assert: the throttled one is gone, the due one remains.
    ///
    /// Uses no embedded relay (immune to the live-app `test_sync_through_embedded_relay`
    /// interference). Seeds 84/85/86 reserved (85/86 only supply valid EndpointIds).
    #[tokio::test]
    async fn supervisor_evicts_throttled_hint_keeps_due() -> anyhow::Result<()> {
        use p2p_core::RelayAddr;
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(84).await?;
        // 85/86 exist only to mint two valid, distinct peer EndpointIds.
        let node_throttled = build_node(85).await?;
        let node_due = build_node(86).await?;

        let throttled_id = node_throttled.sync_node.node_id();
        let due_id = node_due.sync_node.node_id();
        let relay_url = RelayAddr::parse("http://example.com:3340/")?;

        // Seed BOTH hints into A's lookup so eviction is observable as a removal.
        node_a.sync_node.set_peer_relay(throttled_id, &relay_url);
        node_a.sync_node.set_peer_relay(due_id, &relay_url);

        // A clone of the lookup shares the same backing store, so it observes the
        // supervisor's mutations from outside the spawned event loop.
        let lookup = node_a.sync_node.peer_lookup_for_test().clone();
        assert!(
            lookup
                .get_endpoint_info(endpoint_id(throttled_id))
                .is_some(),
            "throttled hint should start present in the lookup"
        );
        assert!(
            lookup.get_endpoint_info(endpoint_id(due_id)).is_some(),
            "due hint should start present in the lookup"
        );

        // Snapshot: the throttled hint has failures and a just-now attempt, so it
        // is well inside its backoff window (not due). The due hint has never been
        // attempted, so it is due immediately.
        let now = now_ms_test();
        let mut throttled_hint = PeerRelay::new(
            throttled_id.to_string(),
            "http://example.com:3340/".to_string(),
        );
        throttled_hint.failure_count = 6;
        throttled_hint.last_attempt_ms = Some(now);
        let due_hint = PeerRelay::new(due_id.to_string(), "http://example.com:3340/".to_string());

        // A joins gossip with no bootstrap — it stays partitioned at zero
        // neighbors, so the supervisor acts every tick.
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;

        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            // A non-existent vault path: persisting hint failures will fail
            // gracefully (logged, non-fatal), but the in-memory eviction — what
            // this test asserts — still runs.
            "/test-vault-evict".into(),
            a_shutdown.clone(),
        );
        daemon_a.seed_peer_relays_snapshot(vec![throttled_hint, due_hint]);
        daemon_a.set_reconnect_interval(Duration::from_millis(100));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // The supervisor evicts the throttled hint while keeping the due one.
        // Both hints are `example.com` domains (off-LAN-reachable), so the
        // throttled one is evicted because a *due* alternative exists
        // (offlan_reachable_count == 2 ⇒ it is NOT the sole off-LAN lifeline) —
        // the eviction is driven by the due alternative, not by URL class.
        wait_until("throttled hint evicted from lookup", || {
            let lookup = lookup.clone();
            async move {
                lookup
                    .get_endpoint_info(endpoint_id(throttled_id))
                    .is_none()
            }
        })
        .await;

        assert!(
            lookup.get_endpoint_info(endpoint_id(due_id)).is_some(),
            "due hint must remain in the lookup (re-seeded each due tick)"
        );

        a_shutdown.cancel();
        let _ = a_loop.await;

        Ok(())
    }

    /// While partitioned with its SOLE peer-relay hint, the supervisor never
    /// evicts that hint even on throttled ticks — the relay-reap reconnect fix.
    ///
    /// Bug being guarded: a non-home iroh `ActiveRelayActor` reaps after 60s of
    /// inactivity; once reaped, the supervisor's re-bootstrap can't respawn it,
    /// and the OLD behavior compounded this by `remove_peer_relay`-ing the only
    /// hint on throttled ticks — starving the next due dial of the peer's
    /// address so the partition never heals without a process restart. The fix
    /// throttles the dial FREQUENCY (the existing `hint_attempt_due` gate), not
    /// the address PRESENCE: a sole hint stays resident in the lookup while at
    /// zero neighbors.
    ///
    /// To prove the supervisor loop actually ran (not a pass-by-luck dead loop),
    /// the hint starts ABSENT from the lookup but DUE in the snapshot. The first
    /// supervisor tick re-seeds it (`None` → `Some`, the positive liveness
    /// signal) and stamps `last_attempt_ms`, throwing it into a 120s backoff so
    /// every later tick sees it throttled. The OLD code would then remove it; the
    /// fix retains it. Seed 89 reserved (90 only mints a valid peer EndpointId).
    #[tokio::test]
    async fn supervisor_retains_sole_hint_when_throttled() -> anyhow::Result<()> {
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(89).await?;
        // 90 exists only to mint a valid, distinct peer EndpointId.
        let node_peer = build_node(90).await?;
        let peer_id = node_peer.sync_node.node_id();

        // A clone of the lookup shares the backing store, so it observes the
        // supervisor's mutations from outside the spawned event loop. The hint
        // starts absent: the first due tick is what adds it.
        let lookup = node_a.sync_node.peer_lookup_for_test().clone();
        assert!(
            lookup.get_endpoint_info(endpoint_id(peer_id)).is_none(),
            "hint should start absent — the first supervisor tick adds it"
        );

        // A never-attempted hint is due immediately; the SOLE hint in the snapshot.
        let sole_hint = PeerRelay::new(peer_id.to_string(), "http://example.com:3340/".to_string());

        // A joins gossip with no bootstrap — it stays partitioned at zero
        // neighbors, so the supervisor acts every tick.
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;

        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-sole-hint".into(),
            a_shutdown.clone(),
        );
        daemon_a.seed_peer_relays_snapshot(vec![sole_hint]);
        daemon_a.set_reconnect_interval(Duration::from_millis(50));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // Liveness: the first due tick re-seeds the hint into the lookup. This
        // proves the supervisor loop is running before we assert retention.
        wait_until("sole hint re-seeded by first due tick", || {
            let lookup = lookup.clone();
            async move { lookup.get_endpoint_info(endpoint_id(peer_id)).is_some() }
        })
        .await;

        // The hint is now throttled (120s backoff). Let many throttled ticks fire
        // — the OLD code removes the sole hint on the first of these; the fix
        // keeps it. 750ms at a 50ms interval is ~15 ticks, far more than the one
        // tick the old eviction needed.
        tokio::time::sleep(Duration::from_millis(750)).await;
        assert!(
            lookup.get_endpoint_info(endpoint_id(peer_id)).is_some(),
            "the sole peer-relay hint must remain in the lookup across throttled \
             ticks — evicting it would strand the only address needed to heal the \
             partition"
        );

        a_shutdown.cancel();
        let _ = a_loop.await;

        Ok(())
    }

    /// With TWO throttled hints and none due, BOTH are evicted — the retention
    /// guard fires only when a hint is the sole hint OR the sole off-LAN lifeline.
    ///
    /// The relay-reap fix retains a hint only when it is the lookup's LAST address
    /// (`peer_relays.len() == 1`) or the LAST off-LAN-reachable one. Both hints
    /// here are LAN-IP relays (`192.168.68.52` and `10.0.0.5`), so neither is
    /// off-LAN-reachable: `offlan_reachable_count == 0`, the off-LAN-lifeline guard
    /// never applies, and the `is_sole_hint` guard is also false (len == 2). With
    /// no single lifeline to protect, both are throttled, both get evicted, and
    /// `bootstrap_ids` is empty so the tick early-returns (nothing to dial until
    /// one comes due). This pins the boundary so a future change can't silently
    /// widen the guard to "never evict while partitioned." The URLs are LAN-IPs
    /// (not domains) on purpose — a domain would be off-LAN-reachable and, as the
    /// sole such hint, would be RETAINED, which is the opposite of what this
    /// boundary test asserts.
    ///
    /// Seeds 91 reserved (92/93 only mint valid peer EndpointIds).
    #[tokio::test]
    async fn supervisor_evicts_both_when_two_throttled_none_due() -> anyhow::Result<()> {
        use p2p_core::RelayAddr;
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(91).await?;
        // 92/93 exist only to mint two valid, distinct peer EndpointIds.
        let node_x = build_node(92).await?;
        let node_y = build_node(93).await?;
        let x_id = node_x.sync_node.node_id();
        let y_id = node_y.sync_node.node_id();
        // Distinct LAN-IP relays: both LAN-only, so neither is the off-LAN
        // lifeline the new rule protects. (The seeds only need to parse; the
        // classification reads the snapshot strings below.)
        let x_relay_url = RelayAddr::parse("http://192.168.68.52:3340/")?;
        let y_relay_url = RelayAddr::parse("http://10.0.0.5:3340/")?;

        // Seed BOTH hints into A's lookup so eviction is observable as removal.
        node_a.sync_node.set_peer_relay(x_id, &x_relay_url);
        node_a.sync_node.set_peer_relay(y_id, &y_relay_url);

        let lookup = node_a.sync_node.peer_lookup_for_test().clone();
        assert!(lookup.get_endpoint_info(endpoint_id(x_id)).is_some());
        assert!(lookup.get_endpoint_info(endpoint_id(y_id)).is_some());

        // Both hints are throttled: recent attempt + failures puts each well
        // inside its backoff window, and neither is due.
        let now = now_ms_test();
        let mut hint_x = PeerRelay::new(x_id.to_string(), "http://192.168.68.52:3340/".to_string());
        hint_x.failure_count = 6;
        hint_x.last_attempt_ms = Some(now);
        let mut hint_y = PeerRelay::new(y_id.to_string(), "http://10.0.0.5:3340/".to_string());
        hint_y.failure_count = 6;
        hint_y.last_attempt_ms = Some(now);

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;

        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-two-throttled".into(),
            a_shutdown.clone(),
        );
        daemon_a.seed_peer_relays_snapshot(vec![hint_x, hint_y]);
        daemon_a.set_reconnect_interval(Duration::from_millis(50));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // With no due alternative to protect, both throttled hints are evicted.
        wait_until("both throttled hints evicted from lookup", || {
            let lookup = lookup.clone();
            async move {
                lookup.get_endpoint_info(endpoint_id(x_id)).is_none()
                    && lookup.get_endpoint_info(endpoint_id(y_id)).is_none()
            }
        })
        .await;

        a_shutdown.cancel();
        let _ = a_loop.await;

        Ok(())
    }

    /// With two throttled hints — a domain lifeline and a dead LAN-IP relay — the
    /// supervisor RETAINS the domain hint (the sole off-LAN-reachable route) and
    /// evicts the LAN-IP one. This is the off-LAN regression guard.
    ///
    /// The coffeeshop bug: a laptop that paired with both umbra (public
    /// `umbra.computer` relay) and charon (a `192.168.x` LAN-IP relay) holds two
    /// hints. Off-LAN, charon's LAN-IP relay is unreachable, so umbra's domain
    /// hint is the ONLY real route — yet the old `is_sole_hint` guard (count == 1)
    /// evicted it the moment it was throttled, because a second hint existed. With
    /// n0 DNS removed (commit `2b51540`) there was no longer a parallel resolver
    /// to mask the gap, so off-LAN reconnect silently broke. The fix generalizes
    /// the retention guard from "sole hint" to "sole off-LAN-reachable hint": a
    /// LAN-only alternative is no alternative off-LAN.
    ///
    /// The LAN-IP eviction is the positive liveness proof that the supervisor loop
    /// actually ran (not a pass-by-luck dead loop) — we wait for that removal,
    /// THEN assert the domain hint is still resident. Both hints are throttled
    /// (failure_count 6 + just-now attempt ⇒ well inside backoff), so neither is
    /// due; only the classification decides which is kept.
    ///
    /// FAILS on pre-fix code: the old branch evicts any throttled non-sole hint,
    /// so the domain lifeline would be removed alongside the LAN-IP one.
    ///
    /// Seed 103 reserved (104/105 only mint valid peer EndpointIds).
    #[tokio::test]
    async fn supervisor_retains_throttled_offlan_lifeline_over_lan_hint() -> anyhow::Result<()> {
        use p2p_core::RelayAddr;
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(103).await?;
        // 104/105 exist only to mint two valid, distinct peer EndpointIds.
        let node_lifeline = build_node(104).await?;
        let node_lan = build_node(105).await?;

        let lifeline_id = node_lifeline.sync_node.node_id();
        let lan_id = node_lan.sync_node.node_id();
        // The seeded URL only needs to parse; the classification reads the
        // snapshot `PeerRelay.relay_url` string, so the domain-vs-IP distinction
        // is carried by the snapshot strings below, not by these seeds.
        let lifeline_relay_url = RelayAddr::parse("http://example.com:3340/")?;
        let lan_relay_url = RelayAddr::parse("http://192.168.68.52:3340/")?;

        // Seed BOTH hints into A's lookup so eviction is observable as a removal.
        node_a
            .sync_node
            .set_peer_relay(lifeline_id, &lifeline_relay_url);
        node_a.sync_node.set_peer_relay(lan_id, &lan_relay_url);

        let lookup = node_a.sync_node.peer_lookup_for_test().clone();
        assert!(
            lookup.get_endpoint_info(endpoint_id(lifeline_id)).is_some(),
            "domain lifeline hint should start present in the lookup"
        );
        assert!(
            lookup.get_endpoint_info(endpoint_id(lan_id)).is_some(),
            "LAN-IP hint should start present in the lookup"
        );

        // Both hints are throttled: failures + just-now attempt puts each well
        // inside its backoff window, so neither is due. The domain hint
        // (`example.com`) is off-LAN-reachable; the `192.168.68.52` hint is
        // LAN-only.
        let now = now_ms_test();
        let mut lifeline_hint = PeerRelay::new(
            lifeline_id.to_string(),
            "http://example.com:3340/".to_string(),
        );
        lifeline_hint.failure_count = 6;
        lifeline_hint.last_attempt_ms = Some(now);
        let mut lan_hint =
            PeerRelay::new(lan_id.to_string(), "http://192.168.68.52:3340/".to_string());
        lan_hint.failure_count = 6;
        lan_hint.last_attempt_ms = Some(now);

        // A joins gossip with no bootstrap — it stays partitioned at zero
        // neighbors, so the supervisor acts every tick.
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;

        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-offlan-lifeline".into(),
            a_shutdown.clone(),
        );
        daemon_a.seed_peer_relays_snapshot(vec![lifeline_hint, lan_hint]);
        daemon_a.set_reconnect_interval(Duration::from_millis(50));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });

        // The LAN-IP hint is evicted — the liveness proof the supervisor ran.
        wait_until("LAN-IP hint evicted from lookup", || {
            let lookup = lookup.clone();
            async move { lookup.get_endpoint_info(endpoint_id(lan_id)).is_none() }
        })
        .await;

        // The domain lifeline is the sole off-LAN-reachable hint, so it is
        // RETAINED across throttled ticks even though a (LAN-only) alternative
        // exists. This is the off-LAN regression the fix closes.
        assert!(
            lookup.get_endpoint_info(endpoint_id(lifeline_id)).is_some(),
            "off-LAN domain lifeline must be retained when it is the only \
             off-LAN-reachable hint, even with a LAN-only alternative present"
        );

        a_shutdown.cancel();
        let _ = a_loop.await;

        Ok(())
    }

    /// A network change re-dials a throttled peer and heals the partition without
    /// a restart — the fix for "every wifi switch needs an app restart."
    ///
    /// The reconnect supervisor's per-hint backoff pins a long-unreachable peer at
    /// the 30-min max, and nothing reset it on a network change. Now iroh's
    /// `watch_addr` net-change signal (injected here through the same channel the
    /// startup task feeds) resets the backoff so the hint becomes due and the next
    /// supervisor tick re-dials.
    ///
    /// Mirrors `supervisor_rebootstraps_after_zero_neighbors` — A and B share a
    /// topic but neither bootstraps off the other, so A is partitioned at zero
    /// neighbors and the ONLY route to B is A's supervisor re-dialing its hint.
    /// The single difference: B's hint starts THROTTLED (failure_count 6, recent
    /// attempt ⇒ ~30-min backoff), so the supervisor will NOT dial it on its own
    /// within the test budget. The net change is therefore the SOLE cause of the
    /// reconnect, and A pulling B's note (a durable observable, not a transient
    /// lookup-presence flicker) is the fix's proof. FAILS on pre-fix code: without
    /// the handler, `net_tx.send` goes nowhere, the hint stays throttled, and A
    /// never pulls.
    ///
    /// Seed 101 reserved (102 mints B's EndpointId via the supplier node).
    #[tokio::test]
    async fn net_change_redials_throttled_peer_and_heals_partition() -> anyhow::Result<()> {
        use sync_daemon::persistence::PeerRelay;

        let node_a = build_node(101).await?; // partitioned peer that re-dials
        let node_b = build_node(102).await?; // supplier — answers A's pull after reconnect

        // Direct-address wiring so A's re-dial reaches B once the hint is due.
        // (Direct addresses live in a separate resolver, so they survive the
        // supervisor evicting the throttled hint from `peer_lookup`.)
        connect_nodes(&node_a, &node_b).await?;

        // Seed a note into B's vault so A's pull proves the partition healed.
        node_b
            .fs
            .write("notes/after-net-change.md", b"# Delivered after net change")
            .await?;
        {
            let vault = node_b.vault.lock().await;
            vault.on_file_changed("notes/after-net-change.md").await?;
        }

        // B's hint, THROTTLED: a recent attempt plus a high failure count puts it
        // well inside its ~30-min backoff window (>> the 10s wait budget), so the
        // supervisor will not re-dial it until the net change resets the throttle.
        // The relay URL only needs to parse — `connect_nodes` carries the dial.
        let b_endpoint_hex = node_b.sync_node.node_id().to_string();
        let mut b_hint = PeerRelay::new(b_endpoint_hex, "http://example.com:3340/".to_string());
        b_hint.failure_count = 6;
        b_hint.last_attempt_ms = Some(now_ms_test());

        // Both join with NO bootstrap — they never dial each other, so A stays
        // partitioned at zero neighbors (the production failure shape).
        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;

        let a_vault = node_a.vault.clone();
        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
        // The net-change channel the production startup task feeds; here a test
        // holds the sender so `send(())` simulates a wifi switch with no real net.
        let (net_tx, net_rx) = tokio::sync::mpsc::channel::<()>(1);
        let mut daemon_a = Daemon::new(
            a_vault.clone(),
            node_a.sync_node,
            gossip_a,
            a_file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            "/test-vault-netchange".into(),
            a_shutdown.clone(),
        );
        daemon_a.set_net_change_rx(net_rx);
        daemon_a.seed_peer_relays_snapshot(vec![b_hint]);
        daemon_a.set_reconnect_interval(Duration::from_millis(50));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });
        // B just needs to be alive to answer A's sync pull.
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // Fire the network change. (The throttled hint guarantees A has not yet
        // re-dialed B — its backoff is ~30 min, far beyond the wait budget — so
        // the reconnect below is caused by this signal, not by an early tick.)
        net_tx.send(()).await.unwrap();

        // The fix's proof: the net-change reset makes B's hint due, the next
        // supervisor tick re-dials B, NeighborUp fires, and A pulls B's note.
        wait_until(
            "A pulled notes/after-net-change.md after the net change",
            || {
                let vault = a_vault.clone();
                async move {
                    vault
                        .lock()
                        .await
                        .list_files()
                        .await
                        .unwrap_or_default()
                        .contains(&"notes/after-net-change.md".to_string())
                }
            },
        )
        .await;

        a_shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = a_loop.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// A successful sync exchange resets the peer's IN-MEMORY hint freshness —
    /// the learn-on-exchange behavior that makes stale-hint eviction safe.
    ///
    /// The supervisor carries a THROTTLED hint for B in its in-memory working set
    /// (high failure_count, recorded attempt). A successful exchange with B fires
    /// `on_exchange_learned`, which stamps the hint and zeroes its failure count
    /// so the supervisor stops backing it off. This is a LAN-direct exchange (no
    /// learned relay URL), the path that used to be `mark_peer_relay_success`.
    ///
    /// The supervisor's working set is now runtime-only (`known_public_relays` is
    /// the sole durable networking store), so the reset is observable in memory,
    /// not on disk — we assert it via `peer_relays_snapshot_for_test()`. We drive
    /// the real `on_exchange_learned` handler directly (via the test seam) rather
    /// than through a spawned `run_loop`, so the daemon stays owned by the test
    /// and its in-memory snapshot is readable. The real run-loop supervisor with a
    /// real swarm is exercised by `net_change_redials_throttled_peer_and_heals_partition`
    /// and the off-LAN-lifeline test; this test's unique value is the reset-on-success.
    ///
    /// Seeds 87/88 reserved (88 only mints B's EndpointId).
    #[tokio::test]
    async fn successful_exchange_resets_hint_freshness() -> anyhow::Result<()> {
        use sync_daemon::persistence::PeerRelay;
        use tempfile::TempDir;

        let node_a = build_node(87).await?;
        // B is never a live node here — only its EndpointId matters, as the hint
        // key and the exchange's reported peer.
        let node_b = build_node(88).await?;
        let b_endpoint = node_b.sync_node.node_id();
        let b_hex = b_endpoint.to_string();

        let vault_dir = TempDir::new()?;
        let vault_path = vault_dir.path().to_path_buf();

        let gossip = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let (_file_tx, file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let shutdown = CancellationToken::new();
        let mut daemon_a = Daemon::new(
            node_a.vault.clone(),
            node_a.sync_node,
            gossip,
            file_rx,
            None,
            node_a.allowlist.clone(),
            "device-a".to_string(),
            None,
            vault_path,
            shutdown.clone(),
        );

        // The supervisor's working set starts with a THROTTLED hint for B
        // (failure_count 4, a recorded attempt), as a long-partitioned peer would.
        let mut seeded = PeerRelay::new(b_hex.clone(), "http://example.com:3340/".to_string());
        seeded.failure_count = 4;
        seeded.last_attempt_ms = Some(1_000);
        daemon_a.seed_peer_relays_snapshot(vec![seeded]);

        // A successful LAN-direct exchange with B (no learned relay URL).
        daemon_a
            .apply_exchange_success_for_test(b_endpoint, None)
            .await;

        // The hint's backoff is reset in the supervisor's in-memory snapshot:
        // failure_count back to 0 and a success stamped, so the next tick stops
        // throttling B. The URL is untouched (none was learned).
        let snapshot = daemon_a.peer_relays_snapshot_for_test();
        let hint = snapshot
            .iter()
            .find(|r| r.endpoint_id == b_hex)
            .expect("B's hint should still be present in the supervisor snapshot");
        assert_eq!(
            hint.failure_count, 0,
            "a successful exchange must reset the in-memory failure count"
        );
        assert!(
            hint.last_success_ms.is_some(),
            "a successful exchange must stamp last_success_ms"
        );
        assert_eq!(
            hint.relay_url, "http://example.com:3340/",
            "a LAN-direct exchange (no learned URL) must not change the stored relay URL"
        );

        shutdown.cancel();
        Ok(())
    }

    /// CROWN JEWEL: a peer ABSENT at a pairing's broadcast instant converges to
    /// the full mesh roster after it joins — the exact rhea↔umbra trust-
    /// propagation bug.
    ///
    /// A already trusts a third peer C (the "umbra" A paired with earlier). B (the
    /// "rhea") paired through A but never saw C — its allowlist holds only A. When
    /// B late-joins the swarm, A's `NeighborUp` fires `push_allowlist_roster`,
    /// which broadcasts A's full roster `{B, C}`. B is already trusted by A, so B
    /// merges the roster and learns C with no direct B↔C pairing and no broadcast
    /// at C's original pairing instant.
    ///
    /// The user-facing effect asserted is trust state — B can now sync with C
    /// because C is in B's allowlist — not any internal call.
    ///
    /// Seeds 94/95 for A/B; C = synthetic PeerId from seed 96 (never a live node).
    #[tokio::test]
    async fn allowlist_roster_converges_on_late_join() -> anyhow::Result<()> {
        let node_a = build_node(94).await?;
        let node_b = build_node(95).await?;

        // C is a synthetic third member A already knows — a roster entry, never a
        // real node. Derived from a distinct seed so its PeerId can't collide.
        let c_peer = PeerId::from_secret_bytes(super::common::seed(96));

        // connect_nodes wires A↔B mutual trust → A's allowlist = {B}, B's = {A}.
        connect_nodes(&node_a, &node_b).await?;
        // Pre-seed C into A only: A's allowlist becomes {B, C}; B still has only A.
        // (PeerId is Copy, so `c_peer` stays usable for the assert below.)
        node_a.allowlist.add_peer(c_peer, "peer-c").await?;

        // Sug2 (pin the causal chain): B does NOT know C before the roster push.
        // Asserted before any daemon spawns, so B's allowlist is definitively {A}.
        assert!(
            !node_b.allowlist.is_allowed(&c_peer).await?,
            "precondition: B must not know C until the roster push (else pass-by-luck)"
        );

        // A joins with empty bootstrap; B late-joins off A → NeighborUp fires on A.
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

        // B converges to the full roster {A, C} purely from joining the mesh.
        wait_until("B's allowlist contains C after roster push", || {
            let allowlist = daemon_b.allowlist.clone();
            async move {
                allowlist
                    .list_peers()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .any(|p| p.node_id == c_peer && !p.removed)
            }
        })
        .await;

        daemon_a.shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = daemon_a.loop_handle.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// Connected-path periodic reconcile (convergence mechanism 2): a roster drift
    /// that NeighborUp already missed self-heals on a reconcile tick.
    ///
    /// A and B form a live swarm and sync. THEN — after they are already connected,
    /// so no NeighborUp will fire for it — C is added to A's allowlist directly.
    /// The connected-path reconcile inside the supervisor tick re-pushes A's roster
    /// on its throttled cadence; B picks up C without any new pairing or reconnect.
    ///
    /// The reconcile throttle is shrunk via `set_roster_reconcile_interval` (a seam
    /// on the daemon's own timer) so the reconcile lands within `wait_until`.
    ///
    /// Seeds 97/98 for A/B; C = synthetic PeerId from seed 96.
    #[tokio::test]
    async fn allowlist_roster_reconciles_drift_while_connected() -> anyhow::Result<()> {
        let node_a = build_node(97).await?;
        let node_b = build_node(98).await?;

        let c_peer = PeerId::from_secret_bytes(super::common::seed(96));

        connect_nodes(&node_a, &node_b).await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        // Build A inline so we can shrink its reconcile cadence before run_loop.
        let a_allowlist = node_a.allowlist.clone();
        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
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
        // Fast tick + near-zero reconcile throttle so a reconcile fires promptly
        // once a neighbor is live. (The crown-jewel NeighborUp push happens before
        // C exists, so only the connected-path reconcile can carry C here.)
        daemon_a.set_reconnect_interval(Duration::from_millis(100));
        daemon_a.set_roster_reconcile_interval(Duration::from_millis(0));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // Wait for the swarm to form (B learns A on NeighborUp; A's roster at this
        // instant is just {B}, so B does not yet know C).
        wait_until("B's allowlist contains A (swarm formed)", || {
            let allowlist = daemon_b.allowlist.clone();
            async move { !allowlist.list_peers().await.unwrap_or_default().is_empty() }
        })
        .await;

        // Drift: add C to A AFTER connection — NeighborUp already fired and won't
        // re-fire, so only the periodic reconcile can propagate this.
        a_allowlist.add_peer(c_peer, "peer-c").await?;

        // The connected-path reconcile re-pushes A's roster; B converges on C.
        wait_until("B's allowlist contains C via reconcile", || {
            let allowlist = daemon_b.allowlist.clone();
            async move {
                allowlist
                    .list_peers()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .any(|p| p.node_id == c_peer && !p.removed)
            }
        })
        .await;

        a_shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = a_loop.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// Revocation propagates and WINS over re-add — the B1 enforcement end-to-end.
    ///
    /// A and B both start trusting a third peer C. On A, `remove_peer(C)` writes a
    /// tombstone (no user-facing revoke command exists yet, so this stands in for
    /// what one would do). The connected-path reconcile carries A's roster — which
    /// includes the C tombstone — to B, whose `merge_roster` honors tombstone-
    /// precedence: B stops trusting C.
    ///
    /// Then the negative-resurrection property: B's own reconcile pushes its roster
    /// back to A, and A's C must STAY revoked. `merge_roster` never lets a (stale)
    /// live row resurrect a tombstone, so A's `is_allowed(C)` can never flip back to
    /// true — removal wins over re-add across the mesh.
    ///
    /// Seeds 99/100 for A/B; C = synthetic PeerId from seed 96.
    #[tokio::test]
    async fn allowlist_revocation_propagates_and_wins() -> anyhow::Result<()> {
        let node_a = build_node(99).await?;
        let node_b = build_node(100).await?;

        let c_peer = PeerId::from_secret_bytes(super::common::seed(96));

        connect_nodes(&node_a, &node_b).await?;
        // Both members trust C (the shared roster before revocation).
        node_a.allowlist.add_peer(c_peer, "peer-c").await?;
        node_b.allowlist.add_peer(c_peer, "peer-c").await?;

        let gossip_a = node_a
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let gossip_b = node_b
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![node_a.sync_node.node_id()])
            .await?;

        // Build A inline so its reconcile cadence is fast enough to carry the
        // tombstone within `wait_until` (there's no revoke command to push promptly).
        let a_allowlist = node_a.allowlist.clone();
        let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let a_shutdown = CancellationToken::new();
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
        daemon_a.set_reconnect_interval(Duration::from_millis(100));
        daemon_a.set_roster_reconcile_interval(Duration::from_millis(0));

        let a_loop = tokio::spawn(async move {
            daemon_a.run_loop().await;
        });
        let daemon_b = spawn_daemon(node_b, gossip_b);

        // Swarm forms; both still trust C at this point.
        wait_until("swarm formed (B knows A)", || {
            let allowlist = daemon_b.allowlist.clone();
            async move { !allowlist.list_peers().await.unwrap_or_default().is_empty() }
        })
        .await;

        // Revoke C on A — writes a tombstone that travels in the roster.
        a_allowlist.remove_peer(&c_peer).await?;

        // B honors the revocation: tombstone-precedence flips C to not-trusted.
        wait_until("B no longer trusts C after revocation", || {
            let allowlist = daemon_b.allowlist.clone();
            async move { !allowlist.is_allowed(&c_peer).await.unwrap_or(true) }
        })
        .await;

        // Negative-resurrection: B's roster pushes back to A, but a removed peer can
        // never be resurrected by a stale live row — A's C stays revoked. This can
        // never become true, so a single assertion (post-convergence) is sound.
        // `a_allowlist` is the daemon's own handle (shared Arc), so this is A's view.
        assert!(
            !a_allowlist.is_allowed(&c_peer).await?,
            "A's revocation of C must not be resurrected by B's roster push"
        );

        a_shutdown.cancel();
        daemon_b.shutdown.cancel();
        let _ = a_loop.await;
        let _ = daemon_b.loop_handle.await;

        Ok(())
    }

    /// C6 gossip expansion: a public relay learned from a peer (e.g. a second
    /// server's home relay, discovered once the laptop joins the mesh) is adopted
    /// into the laptop's persisted `known_public_relays` — the cold-bootstrap store
    /// that survives restart, so next launch homes on it too. This is what gives a
    /// laptop failover redundancy across servers it never directly paired with.
    ///
    /// Drives the real learn-on-exchange adoption path with a fabricated learned
    /// relay (the genuine exchange yields a loopback URL, which is correctly
    /// rejected; the cross-network public URL a real second server advertises is
    /// what the adoption path is for), then asserts the persisted config.
    ///
    /// Seed 110 reserved.
    #[tokio::test]
    async fn gossip_learned_public_relay_is_persisted() -> anyhow::Result<()> {
        use sync_daemon::persistence::DaemonConfig;
        use tempfile::TempDir;

        let node = build_node(110).await?;

        let vault_dir = TempDir::new()?;
        let vault_path = vault_dir.path().to_path_buf();

        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let (_file_tx, file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let shutdown = CancellationToken::new();
        let mut daemon = Daemon::new(
            node.vault.clone(),
            node.sync_node,
            gossip,
            file_rx,
            None,
            node.allowlist.clone(),
            "device".to_string(),
            None,
            vault_path.clone(),
            shutdown.clone(),
        );

        let learned = p2p_core::RelayAddr::parse("https://server2.example.com/").unwrap();
        daemon.learn_public_relay_for_test(&learned).await;

        let (config, _) = DaemonConfig::load_or_generate(&vault_path, None).await?;
        assert!(
            config
                .known_public_relays
                .contains(&"https://server2.example.com/".to_string()),
            "learned public relay should be adopted into known_public_relays; set was {:?}",
            config.known_public_relays
        );

        Ok(())
    }

    /// C6 classifier guard: a learned LAN-IP relay is NEVER adopted into the public
    /// set. A private relay is useless to any peer not on that LAN, so homing on it
    /// would leave a laptop unreachable once it leaves — the exact failure the
    /// public-relay model exists to prevent.
    ///
    /// Seed 111 reserved.
    #[tokio::test]
    async fn gossip_learned_private_relay_is_not_persisted() -> anyhow::Result<()> {
        use sync_daemon::persistence::DaemonConfig;
        use tempfile::TempDir;

        let node = build_node(111).await?;

        let vault_dir = TempDir::new()?;
        let vault_path = vault_dir.path().to_path_buf();

        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let (_file_tx, file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let shutdown = CancellationToken::new();
        let mut daemon = Daemon::new(
            node.vault.clone(),
            node.sync_node,
            gossip,
            file_rx,
            None,
            node.allowlist.clone(),
            "device".to_string(),
            None,
            vault_path.clone(),
            shutdown.clone(),
        );

        let learned = p2p_core::RelayAddr::parse("http://192.168.68.52:3340/").unwrap();
        daemon.learn_public_relay_for_test(&learned).await;

        let (config, _) = DaemonConfig::load_or_generate(&vault_path, None).await?;
        assert!(
            config.known_public_relays.is_empty(),
            "a learned private LAN-IP relay must not be adopted; set was {:?}",
            config.known_public_relays
        );

        Ok(())
    }

    /// C6 cross-product refresh: a newly-learned public relay means new
    /// `(allowlist peer) × {new relay}` reconnect targets, so the supervisor's
    /// in-memory snapshot gains a `(peer, new_relay)` entry — without it, the
    /// laptop could home on the new relay but would never DIAL its already-trusted
    /// peers through it. The trusted peer comes from the ALLOWLIST (the trust
    /// anchor); the relay is only ever a transport hint paired with it.
    ///
    /// Asserts the user-facing effect via the persisted store plus the snapshot the
    /// supervisor re-dials from. Seeds 112/113 reserved (113 only mints a valid
    /// peer EndpointId for the allowlist).
    #[tokio::test]
    async fn gossip_learned_public_relay_refreshes_cross_product() -> anyhow::Result<()> {
        use sync_daemon::persistence::DaemonConfig;
        use tempfile::TempDir;

        let node = build_node(112).await?;
        // A trusted peer B already in the allowlist (the trust anchor the
        // cross-product enumerates). B is never a live node here — only its
        // EndpointId matters for the (peer × relay) pairing.
        let b_peer = PeerId::from_secret_bytes(super::common::seed(113));
        node.allowlist.add_peer(b_peer, "peer-b").await?;
        let b_hex = b_peer.to_string();

        let vault_dir = TempDir::new()?;
        let vault_path = vault_dir.path().to_path_buf();

        let gossip = node
            .sync_node
            .join_vault_gossip(&shared_vault_id(), vec![])
            .await?;
        let (_file_tx, file_rx) = mpsc::unbounded_channel::<FileEvent>();
        let shutdown = CancellationToken::new();
        let mut daemon = Daemon::new(
            node.vault.clone(),
            node.sync_node,
            gossip,
            file_rx,
            None,
            node.allowlist.clone(),
            "device".to_string(),
            None,
            vault_path.clone(),
            shutdown.clone(),
        );

        let relay_str = "https://server2.example.com/".to_string();
        let learned = p2p_core::RelayAddr::parse(&relay_str).unwrap();
        daemon.learn_public_relay_for_test(&learned).await;

        // Persisted into the cold store.
        let (config, _) = DaemonConfig::load_or_generate(&vault_path, None).await?;
        assert!(
            config.known_public_relays.contains(&relay_str),
            "learned public relay should be persisted; set was {:?}",
            config.known_public_relays
        );

        // Supervisor snapshot gained the (B, new_relay) reconnect target so a future
        // tick dials B through server2's relay.
        assert!(
            daemon
                .peer_relays_snapshot_for_test()
                .iter()
                .any(|h| h.endpoint_id == b_hex && h.relay_url == relay_str),
            "supervisor snapshot should gain a (peer, new_relay) cross-product entry; \
             snapshot was {:?}",
            daemon.peer_relays_snapshot_for_test()
        );

        Ok(())
    }
}
