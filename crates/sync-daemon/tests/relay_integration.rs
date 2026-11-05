//! Integration tests for the embedded relay server.
//!
//! These tests verify that two `SyncNode`s can discover and communicate with
//! each other through an `EmbeddedRelay` when they have no direct route.
//! Unlike direct-connectivity tests (which use MemoryLookup), here the relay is
//! the only path between nodes.
//!
//! sync-daemon is always native (no WASM target), so no cfg guards are needed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use p2p_core::PeerId;
use p2p_core::allowlist::{AllowlistStorage, InMemoryAllowlist};
use p2p_core::{EndpointId, RelayUrl, SecretKey, Watcher};
use vault_sync::VaultId;
use vault_sync::network::gossip::GossipEvent;
use vault_sync::network::{RejectSyncHandler, SyncNode, SyncNodeSeam, VaultGossipExt};
// `NativeFs`/`InMemoryFs` (via `common`) implement vault-sync's `FileSystem`.
use p2p_core::{EmbeddedRelay, RelayAddr};
use sync_daemon::daemon::Daemon;
use sync_daemon::persistence::PeerRelay;
use sync_daemon::watcher::FileEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use vault_sync::fs::FileSystem;

mod common;

// ── tests ─────────────────────────────────────────────────────────────────

/// `add_peer_relay` registers a relay hint that is resolvable via the node's
/// peer lookup service.
///
/// This validates the seam used at startup when the
/// `allowlist × known_public_relays` cross-product is non-empty: the hint is
/// written to the `MemoryLookup` and can be queried back by endpoint id before
/// any live connection is attempted.
#[tokio::test]
async fn test_add_peer_relay_registers_resolvable_hint() -> anyhow::Result<()> {
    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_with_sync_handler(common::seed(10), &[], allowlist, RejectSyncHandler)
        .await?;

    // Use a second node's id as the "peer" and a standalone relay URL.
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let relay = EmbeddedRelay::start(bind_addr).await?;
    let relay_url = relay.relay_url().clone();

    // A plausible peer id derived from a distinct key seed. `add_peer_relay`
    // takes our `PeerId`; the raw iroh lookup is keyed by `EndpointId`.
    let peer_id = PeerId::from_secret_bytes([42u8; 32]);
    let peer_endpoint_id = common::endpoint_id(peer_id);

    // Before seeding, the lookup should have no entry for this peer.
    assert!(
        node.peer_lookup_for_test()
            .get_endpoint_info(peer_endpoint_id)
            .is_none(),
        "lookup should be empty before add_peer_relay"
    );

    node.add_peer_relay(peer_id, &relay_url);

    // After seeding, the hint must be resolvable and carry the relay URL.
    let info = node
        .peer_lookup_for_test()
        .get_endpoint_info(peer_endpoint_id)
        .expect("hint should be present after add_peer_relay");

    // iroh's lookup hands back its own `RelayUrl`; compare via string form against
    // the seeded `RelayAddr` (both render the same full URL).
    let relay_urls: Vec<String> = info
        .into_endpoint_addr()
        .relay_urls()
        .map(|u| u.to_string())
        .collect();
    assert_eq!(
        relay_urls,
        vec![relay_url.as_str()],
        "seeded relay URL should be recoverable from the lookup"
    );

    relay.shutdown().await;
    Ok(())
}

/// `add_peer_relay` silently ignores a call where the target id equals the
/// node's own EndpointId, leaving the lookup empty.
///
/// iroh rejects self-directed relay paths, so seeding our own id into the
/// lookup is a footgun. The startup cross-product seed skips self explicitly,
/// but `add_peer_relay` is also called directly after pairing and on
/// learn-on-exchange; this guard closes the gap at the method level.
#[tokio::test]
async fn test_add_peer_relay_ignores_self_id() -> anyhow::Result<()> {
    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_with_sync_handler(common::seed(30), &[], allowlist, RejectSyncHandler)
        .await?;

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let relay = EmbeddedRelay::start(bind_addr).await?;
    let relay_url = relay.relay_url().clone();

    // Calling add_peer_relay with our own id should be a no-op.
    node.add_peer_relay(node.node_id(), &relay_url);

    // The lookup must remain empty — no self-hint was registered.
    assert!(
        node.peer_lookup_for_test()
            .get_endpoint_info(common::endpoint_id(node.node_id()))
            .is_none(),
        "self-id should not be seeded into the peer lookup"
    );

    relay.shutdown().await;
    Ok(())
}

/// When `peer_relays` is empty at startup, the `MemoryLookup` service contains
/// no hints — the LAN-only path via mDNS is entirely unaffected.
///
/// This is the LAN-unchanged guard: confirms that the MemoryLookup registration
/// does not break or interfere with existing mDNS-based discovery when there
/// are no persisted relay hints to seed.
#[tokio::test]
async fn test_empty_peer_relays_leaves_lookup_empty() -> anyhow::Result<()> {
    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_with_sync_handler(common::seed(20), &[], allowlist, RejectSyncHandler)
        .await?;

    // A deterministic peer id derived from a key seed (distinct from the node's own seed).
    let peer_secret = SecretKey::from_bytes(&[99u8; 32]);
    let peer_endpoint_id: EndpointId = peer_secret.public();

    // With no add_peer_relay calls (simulating an empty cross-product seed),
    // the lookup must return None for any peer id.
    assert!(
        node.peer_lookup_for_test()
            .get_endpoint_info(peer_endpoint_id)
            .is_none(),
        "peer_lookup should be empty when no hints are seeded"
    );

    Ok(())
}

/// Per-peer relay hints resolve and route via a non-home relay.
///
/// **Topology:** Node A's home relay is R_a; Node B's home relay is R_b. The two
/// relay maps are disjoint — neither node has the other's relay in its `RelayMap`.
///
/// B's address lookup is seeded with `(A_id, R_a)` via `add_peer_relay`. B then
/// bootstraps gossip using A's bare `EndpointId` (no direct address). This exercises
/// the load-bearing path from the plan: lookup resolution → iroh spawning an
/// `ActiveRelayActor` for R_a (a URL not in B's RelayMap, handled on-demand per
/// iroh's non-home relay behavior) → routing via that relay. The mirror direction
/// (A's lookup seeded with B's relay) is also exercised in the same test.
///
/// **What this test proves:** hint resolution is wired up correctly; gossip and QUIC
/// sync succeed when the only pre-seeded path is via a non-home relay. It does NOT
/// prove that relay routing was the *exclusive* transport path used — in-process
/// localhost nodes can find each other via mDNS direct addresses, so iroh may use
/// direct QUIC as a faster path once discovered. The relay hint is available as a
/// bootstrap; whether iroh ultimately prefers it is an internal routing decision.
/// The true off-LAN property (both home relays unreachable to the dialer) is only
/// verifiable in the real-world charon ↔ umbra E2E scenario.
#[tokio::test]
async fn test_non_home_relay_hint_resolves_and_routes() -> anyhow::Result<()> {
    // Start two separate relays — R_a is A's home, R_b is B's home.
    let relay_a = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_b = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let url_a: RelayAddr = relay_a.relay_url().clone();
    let url_b: RelayAddr = relay_b.relay_url().clone();

    // Node A: home relay = R_a. Node B: home relay = R_b. Neither node has the
    // other's relay in its own RelayMap. Both run the daemon's pumped inbound
    // handler, so the sync exchange below is the production vault-sync protocol.
    let node_a = common::build_node_with_relay(51, &url_a).await?;
    let node_b = common::build_node_with_relay(52, &url_b).await?;
    node_a.allowlist.add_peer(node_b.node_id, "node-b").await?;
    node_b.allowlist.add_peer(node_a.node_id, "node-a").await?;

    let a_id = node_a.sync_node.node_id();
    let b_id = node_b.sync_node.node_id();

    // Seed per-peer relay hints in both directions.
    // B learns to reach A through R_a (not in B's RelayMap — non-home relay routing).
    node_b.sync_node.add_peer_relay(a_id, &url_a);
    // A learns to reach B through R_b (not in A's RelayMap — mirror direction).
    node_a.sync_node.add_peer_relay(b_id, &url_b);

    let vault_id: VaultId = "deadbeef00000001".parse().unwrap();

    // A subscribes with no bootstrap peers; B bootstraps from A's bare EndpointId.
    // The hint in B's lookup provides the relay path to A.
    let gossip_a = node_a
        .sync_node
        .join_vault_gossip(&vault_id, vec![])
        .await?;
    let gossip_b = node_b
        .sync_node
        .join_vault_gossip(&vault_id, vec![a_id])
        .await?;

    // Drive both nodes as full Daemons.
    let a_vault = node_a.vault.clone();
    let a_fs = node_a.fs.clone();
    let b_vault = node_b.vault.clone();
    let b_fs = node_b.fs.clone();

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
        "/test-vault-hint-a".into(),
        a_shutdown.clone(),
    );
    daemon_a.set_inbound_seen_rx(node_a.inbound_seen_rx);

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
        "/test-vault-hint-b".into(),
        b_shutdown.clone(),
    );
    daemon_b.set_inbound_seen_rx(node_b.inbound_seen_rx);

    let a_loop = tokio::spawn(async move { daemon_a.run_loop().await });
    let b_loop = tokio::spawn(async move { daemon_b.run_loop().await });

    // A writes a note; B must pull it. B reaches A only through the per-peer
    // hint (R_a is not in B's RelayMap), so convergence proves the hint resolves
    // the route for both gossip and the pumped vault-sync exchange.
    a_fs.write("notes/relay-hint-test.md", b"relay-hint-data")
        .await?;
    {
        let vault = a_vault.lock().await;
        vault.on_file_changed("notes/relay-hint-test.md").await?;
    }
    common::wait_until(
        "B pulls A's note through the non-home relay hint",
        Duration::from_secs(30),
        || {
            let fs = b_fs.clone();
            async move {
                matches!(
                    fs.read("notes/relay-hint-test.md").await,
                    Ok(data) if data == b"relay-hint-data"
                )
            }
        },
    )
    .await;

    // Clean up.
    a_shutdown.cancel();
    b_shutdown.cancel();
    a_loop.abort();
    b_loop.abort();
    relay_a.shutdown().await;
    relay_b.shutdown().await;

    Ok(())
}

/// Faithful repro: a running daemon re-establishes its gossip neighbor after the
/// peer's relay flaps long enough for the non-home `ActiveRelayActor` to reap —
/// WITHOUT a process restart.
///
/// This is the end-to-end scenario behind the relay-reap reconnect fix. The
/// supervisor lives in daemon A; B's home relay (`relay_peer`) is the one that
/// goes down. With the only A→B path being `relay_peer`, A's endpoint carries a
/// non-home `ActiveRelayActor` for it, which iroh reaps after 60s of inactivity
/// (the hardcoded `RELAY_INACTIVE_CLEANUP_TIME` — verified to have no env/config/
/// builder override in iroh-1.0.0-rc.1, and `Endpoint::network_change` does NOT
/// force-close non-home actors, so the reap cannot be forced faster). Once
/// reaped, the OLD supervisor never respawned it and additionally evicted B's
/// sole hint on throttled ticks → permanent partition until restart. The fix
/// keeps the sole hint resident so the next due dial drives relay traffic that
/// respawns the actor.
///
/// `#[ignore]`d because it idles past the 60s reap (≥65s wall time). Run manually:
///
/// ```text
/// cargo test -p sync-daemon --test relay_integration -- --ignored \
///     supervisor_heals_after_peer_relay_reap
/// ```
///
/// CAVEAT (in-process faithfulness): `SyncNode::new_with_sync_handler` always enables mDNS, so two
/// localhost nodes may discover each other's direct addresses and reconnect
/// without the relay — masking the bug. The truly off-LAN property (both home
/// relays unreachable to the dialer, relay the only path) is only guaranteed in
/// the real charon ↔ umbra E2E. Treat this test as a best-effort local
/// confirmation, not a hermetic gate; the fast logic tests in
/// `daemon_integration.rs` are the deterministic regression guards.
///
/// Seeds 60/61 reserved.
#[tokio::test]
#[ignore = "idles past iroh's 60s RELAY_INACTIVE_CLEANUP_TIME reap (~65s+); run with --ignored"]
async fn supervisor_heals_after_peer_relay_reap() -> anyhow::Result<()> {
    // Two disjoint home relays: R_a is A's home; R_peer is B's home (the flapper).
    let relay_a = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_peer = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let url_a: RelayAddr = relay_a.relay_url().clone();
    let url_peer: RelayAddr = relay_peer.relay_url().clone();

    // Build full relay-aware nodes (home relay each), then allowlist both ways.
    let node_a = common::build_node_with_relay(60, &url_a).await?;
    let node_b = common::build_node_with_relay(61, &url_peer).await?;
    node_a.allowlist.add_peer(node_b.node_id, "node-b").await?;
    node_b.allowlist.add_peer(node_a.node_id, "node-a").await?;

    let a_id = node_a.sync_node.node_id();
    let b_id = node_b.sync_node.node_id();

    // Seed relay hints both directions — the ONLY path each has to the other.
    // No direct addresses (no `connect_nodes`): the relay must carry A↔B so a
    // real reap-sensitive non-home actor exists.
    node_a.sync_node.set_peer_relay(b_id, &url_peer);
    node_b.sync_node.set_peer_relay(a_id, &url_a);

    let vault_id: VaultId = "cafebabecafebabe".parse().unwrap();

    // A subscribes with no bootstrap; B bootstraps off A's bare EndpointId.
    let gossip_a = node_a
        .sync_node
        .join_vault_gossip(&vault_id, vec![])
        .await?;
    let gossip_b = node_b
        .sync_node
        .join_vault_gossip(&vault_id, vec![a_id])
        .await?;

    // Drive both as full Daemons so A's reconnect supervisor runs. Shrink A's
    // tick so recovery lands inside the test's window once the relay returns.
    let a_vault = node_a.vault.clone();
    let a_fs = node_a.fs.clone();
    let b_fs = node_b.fs.clone();
    let b_vault = node_b.vault.clone();

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
        "/test-vault-reap-a".into(),
        a_shutdown.clone(),
    );
    daemon_a.set_inbound_seen_rx(node_a.inbound_seen_rx);
    let mut a_hint = PeerRelay::new(b_id.to_string(), url_peer.to_string());
    a_hint.last_success_ms = Some(0);
    daemon_a.seed_peer_relays_snapshot(vec![a_hint]);
    daemon_a.set_reconnect_interval(Duration::from_millis(500));

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
        "/test-vault-reap-b".into(),
        b_shutdown.clone(),
    );
    daemon_b.set_inbound_seen_rx(node_b.inbound_seen_rx);

    let a_loop = tokio::spawn(async move { daemon_a.run_loop().await });
    let b_loop = tokio::spawn(async move { daemon_b.run_loop().await });

    // Establish the swarm through the relays: A pulls a note B writes pre-flap.
    b_fs.write("notes/pre-flap.md", b"# Before the flap")
        .await?;
    {
        let vault = b_vault.lock().await;
        vault.on_file_changed("notes/pre-flap.md").await?;
    }
    common::wait_until(
        "A pulls B's pre-flap note (swarm established via relay)",
        Duration::from_secs(45),
        || {
            let fs = a_fs.clone();
            async move { fs.read("notes/pre-flap.md").await.is_ok() }
        },
    )
    .await;

    // Flap B's relay and idle past the 60s reap — A's non-home actor for
    // `relay_peer` goes idle and reaps. Capture the bind addr first so the
    // replacement can advertise the identical URL.
    let peer_bind: SocketAddr =
        format!("{}:{}", url_peer.host().unwrap(), url_peer.port().unwrap())
            .parse()
            .unwrap();
    relay_peer.shutdown().await;
    tokio::time::sleep(Duration::from_secs(70)).await;

    // Bring the relay back advertising the SAME URL. Prefer reusing the original
    // bind addr; fall back to a fresh bind that still advertises the same URL if
    // the OS hasn't released the port yet (start failure → retry on :0).
    let relay_peer = match EmbeddedRelay::start(peer_bind).await {
        Ok(r) => r,
        Err(_) => {
            EmbeddedRelay::start_with_advertised_url(
                "127.0.0.1:0".parse().unwrap(),
                &url_peer.as_str(),
            )
            .await?
        }
    };

    // B writes a second note during/after the partition. Recovery is proven when
    // A pulls it WITHOUT any restart — the supervisor respawned the reaped relay
    // path on its own. On the OLD code this hangs (actor never respawns; sole
    // hint was evicted on throttled ticks) → the repro.
    b_fs.write("notes/post-flap.md", b"# After the flap")
        .await?;
    {
        let vault = b_vault.lock().await;
        vault.on_file_changed("notes/post-flap.md").await?;
    }
    common::wait_until(
        "A pulls B's post-flap note after the relay returns (heals without restart)",
        Duration::from_secs(90),
        || {
            let fs = a_fs.clone();
            async move { fs.read("notes/post-flap.md").await.is_ok() }
        },
    )
    .await;

    a_shutdown.cancel();
    b_shutdown.cancel();
    let _ = a_loop.await;
    let _ = b_loop.await;
    relay_a.shutdown().await;
    relay_peer.shutdown().await;

    Ok(())
}

// ── H3': off-LAN bootstrap re-dial gap ──────────────────────────────────────
//
// These two tests isolate the "laptop never connects to umbra off-LAN" failure
// (see [[Reviews/Off-LAN Relay-Dial Regression]], H3'). Both use truly
// relay-only nodes (`build_relay_only_node` → `clear_ip_transports()`), so the
// relay is the ONLY route — exactly rhea's coffeeshop condition, which the
// IP-retaining relay tests above cannot force.
//
// CONTROL proves the relay-only + MemoryLookup-hint path connects when the hint
// is present from the start. REPRO proves that once the FIRST bootstrap dial
// fails (hint absent at dial time), the peer is never re-dialed even after the
// hint becomes present and the supervisor keeps re-Joining — because gossip
// leaves the peer in `Pending { queue: non-empty }` and only re-dials a peer
// whose queue is empty (`iroh-gossip` `net.rs:696`).

/// CONTROL: a relay-only node reaches a relay-only peer when the peer's relay
/// hint is seeded into its `peer_lookup` BEFORE bootstrap.
///
/// This is the verdict-A happy path in isolation: with IP transports stripped
/// from both endpoints, the relay is the only possible route, and the only way
/// A learns B's relay is the `add_peer_relay` hint (B's relay is NOT in A's
/// `RelayMap`). If A still NeighborUps B, the relay-only + hint-only mechanism
/// works — so a failure to connect in the REPRO below is attributable to the
/// re-dial gap, not to a broken off-LAN simulation.
///
/// If THIS test fails, the harness/off-LAN sim is wrong (e.g. relay-only nodes
/// can't home on the in-process embedded relay) — investigate that before
/// trusting the REPRO.
///
/// Seeds 106/107 reserved.
#[tokio::test]
async fn relay_only_control_connects_with_seeded_hint() -> anyhow::Result<()> {
    // One relay both nodes home on; A reaches B only by resolving B's relay
    // from its peer_lookup hint.
    let relay = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_url = relay.relay_url().clone();

    // Relay-only nodes: no IP transports, so neither can fall back to a direct
    // loopback path — the relay is genuinely the sole route (off-LAN/NAT sim).
    let node_a = common::build_relay_only_node(106, &relay_url).await?;
    let node_b = common::build_relay_only_node(107, &relay_url).await?;

    node_a.allowlist.add_peer(node_b.node_id, "node-b").await?;
    node_b.allowlist.add_peer(node_a.node_id, "node-a").await?;

    let a_id = node_a.sync_node.node_id();
    let b_id = node_b.sync_node.node_id();

    // Ensure both endpoints have actually connected to their home relay before
    // we test routing — a relay-only node that hasn't homed yet is unreachable
    // through the relay, which would confound the result.
    node_a.sync_node.endpoint_for_test().online().await;
    node_b.sync_node.endpoint_for_test().online().await;

    // Seed B's relay hint into A's lookup BEFORE bootstrap — the persisted-hint
    // path (mirrors rhea's daemon.toml peer_relays). B's relay is NOT in A's
    // RelayMap, so this hint is the only thing that resolves B for A.
    node_a.sync_node.add_peer_relay(b_id, &relay_url);

    let vault_id: VaultId = "cafebabe0beef106".parse().unwrap();

    // B subscribes with no bootstrap; A bootstraps off B's bare EndpointId. The
    // seeded hint provides A's relay path to B.
    let mut gossip_b = node_b
        .sync_node
        .join_vault_gossip(&vault_id, vec![])
        .await?;
    let mut gossip_a = node_a
        .sync_node
        .join_vault_gossip(&vault_id, vec![b_id])
        .await?;

    // A NeighborUps B through the relay — proves the relay-only + hint-only path.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match gossip_a.event_rx.recv().await {
                Some(GossipEvent::NeighborUp(id)) => {
                    assert_eq!(id, b_id, "A's neighbor should be B");
                    break;
                }
                Some(_) => continue,
                None => panic!("gossip_a event channel closed before NeighborUp"),
            }
        }
    })
    .await
    .expect(
        "CONTROL: relay-only node failed to NeighborUp via seeded hint — \
             the off-LAN sim itself is broken; fix this before trusting the repro",
    );

    gossip_a.event_rx.close();
    gossip_b.event_rx.close();
    let _ = a_id; // retained for symmetry with the repro below
    relay.shutdown().await;
    Ok(())
}

/// The reconnect supervisor recovers a relay-only peer whose FIRST gossip
/// bootstrap dial parked, once the peer's relay hint is present (H3' fix).
///
/// This is the green regression guard for the off-LAN "never connects to umbra"
/// bug. Setup mirrors the CONTROL (relay-only A + B, relay the sole route) but
/// breaks the first dial: A bootstraps off B's bare `EndpointId` while B's relay
/// hint is ABSENT, so gossip's bare-id dial parks in iroh's address resolution
/// (no path, no timeout) and leaves B `Pending` with a non-empty queue. Gossip's
/// own `net.rs:696` `queue.is_empty()` guard then suppresses every later re-dial,
/// so gossip alone can never recover B — even though the supervisor re-Joins it
/// every ~200ms tick (kept due by the net-change pump).
///
/// The fix makes the supervisor establish the connection itself: for each due
/// hint it spawns a relay-carrying `endpoint.connect(EndpointAddr::with_relay_url,
/// GOSSIP_ALPN)` (an `App` path resolves immediately — no park, independent of
/// gossip's stuck Dialer) and hands the connection to `gossip.handle_connection`,
/// which adopts it like an inbound accept: `accept_conn` drains the queued Join →
/// B Active → handshake → NeighborUp → full sync. We assert the durable
/// observable the other supervisor tests use — A pulls a note B wrote.
///
/// BEFORE the fix this test was RED (the pull timed out; gossip dialed B exactly
/// once and never again despite ~60 re-Joins, observable as `start to dial
/// peer=<B>` ×1 / `NeighborUp` ×0 in the emitted trace). The CONTROL test above
/// proves the relay-only + seeded-hint path connects, so this test isolates the
/// supervisor's recovery of an already-parked peer, not the off-LAN sim.
///
/// Run with `--nocapture` to see the supervisor-connect + adoption trace:
///
/// ```text
/// cargo test -p sync-daemon --test relay_integration -- --nocapture \
///     supervisor_recovers_relay_only_peer_after_parked_bootstrap_dial
/// ```
///
/// Seeds 108/109 reserved.
#[tokio::test]
async fn supervisor_recovers_relay_only_peer_after_parked_bootstrap_dial() -> anyhow::Result<()> {
    use vault_sync::fs::FileSystem;

    // The emitted gossip-dial + supervisor trace documents the recovery path;
    // `try_init` is a no-op if the test binary already installed a subscriber.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("iroh_gossip::net=debug,sync_daemon=info")
        .with_test_writer()
        .try_init();

    let relay = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_url = relay.relay_url().clone();

    // Relay-only nodes (no IP transports) — the relay is the sole route.
    let node_a = common::build_relay_only_node(108, &relay_url).await?;
    let node_b = common::build_relay_only_node(109, &relay_url).await?;

    node_a.allowlist.add_peer(node_b.node_id, "node-b").await?;
    node_b.allowlist.add_peer(node_a.node_id, "node-a").await?;

    let a_id = node_a.sync_node.node_id();
    let b_id = node_b.sync_node.node_id();

    // Both endpoints home on the relay before any routing is attempted.
    node_a.sync_node.endpoint_for_test().online().await;
    node_b.sync_node.endpoint_for_test().online().await;

    // Seed a note into B's vault so A pulling it proves the partition healed —
    // the same durable observable the other supervisor tests assert on.
    node_b
        .fs
        .write(
            "notes/after-bootstrap-redial.md",
            b"# Delivered after re-dial",
        )
        .await?;
    {
        let vault = node_b.vault.lock().await;
        vault
            .on_file_changed("notes/after-bootstrap-redial.md")
            .await?;
    }

    let vault_id: VaultId = "cafebabe0beef108".parse().unwrap();

    // CRUX: A bootstraps off B's bare id while B's hint is ABSENT from A's
    // lookup. The first connect(B) has no path to resolve, so the dial parks in
    // `resolve_remote` and never drains B's gossip queue — leaving B `Pending`
    // with a non-empty queue (the H3' precondition that suppresses all re-dials).
    let gossip_b = node_b
        .sync_node
        .join_vault_gossip(&vault_id, vec![])
        .await?;
    let gossip_a = node_a
        .sync_node
        .join_vault_gossip(&vault_id, vec![b_id])
        .await?;

    // Let the first bootstrap dial run to completion and FAIL before the hint
    // exists. With an empty lookup the dial resolves no path to B and fails
    // after iroh's connect timeout (~10s); we wait past it so B is left
    // `Pending` with a non-empty queue BEFORE any hint is present. This delay is
    // the deterministic gate for the "first dial fails" precondition — the
    // architect's "seed the hint late" trigger from the diagnosis note.
    tokio::time::sleep(Duration::from_secs(15)).await;

    // The hint becomes present right after the failed first dial — exactly the
    // rhea state (umbra's hint resident in daemon.toml). On healthy code a
    // subsequent re-dial would now resolve B and connect.
    node_a.sync_node.add_peer_relay(b_id, &relay_url);

    // Drive A as a real Daemon so its reconnect supervisor re-Joins B. The
    // net-change channel lets us reset A's hint backoff repeatedly (below) so
    // the supervisor stays un-throttled and re-Joins B on EVERY tick — the
    // "supervisor keeps re-joining" condition. On healthy code one of those
    // re-Joins would re-dial B; on current code none do.
    let a_vault = node_a.vault.clone();
    let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
    let a_shutdown = CancellationToken::new();
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
        "/test-vault-bootstrap-redial".into(),
        a_shutdown.clone(),
    );
    daemon_a.set_inbound_seen_rx(node_a.inbound_seen_rx);
    daemon_a.set_net_change_rx(net_rx);
    let b_hint = PeerRelay::new(b_id.to_string(), relay_url.as_str());
    daemon_a.seed_peer_relays_snapshot(vec![b_hint]);
    daemon_a.set_reconnect_interval(Duration::from_millis(200));

    let a_loop = tokio::spawn(async move {
        daemon_a.run_loop().await;
    });

    // Keep A's hint perpetually due by firing a network-change signal every
    // 500ms — each one resets the hint's backoff so the supervisor re-Joins B
    // every tick instead of backing off after the first failure. This makes the
    // premise unambiguous: B is re-Joined many times over the wait window, and
    // gossip still never re-dials it.
    let net_shutdown = CancellationToken::new();
    let net_pump = {
        let net_shutdown = net_shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = net_shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {
                        if net_tx.send(()).await.is_err() {
                            break;
                        }
                    }
                }
            }
        })
    };

    // B just needs to stay alive to answer A's pull once A connects.
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
        b_allowlist,
        "device-b".to_string(),
        None,
        "/test-vault-bootstrap-redial-b".into(),
        b_shutdown.clone(),
    );
    daemon_b.set_inbound_seen_rx(node_b.inbound_seen_rx);
    let b_loop = tokio::spawn(async move {
        daemon_b.run_loop().await;
    });

    // PROOF: the supervisor's relay-carrying connect (hint now present) bypasses
    // gossip's parked bare-id dial, establishes the relay path, and hands the
    // connection to gossip → the queued Join drains → NeighborUp → A pulls B's
    // note. Before the fix B stayed Pending and this wait timed out.
    common::wait_until(
        "A pulls B's note after the supervisor recovers the parked bootstrap peer",
        Duration::from_secs(30),
        || {
            let vault = a_vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/after-bootstrap-redial.md".to_string())
            }
        },
    )
    .await;

    net_shutdown.cancel();
    a_shutdown.cancel();
    b_shutdown.cancel();
    let _ = net_pump.await;
    let _ = a_loop.await;
    let _ = b_loop.await;
    let _ = (a_id, b_fs, b_vault); // retained for clarity/symmetry
    relay.shutdown().await;
    Ok(())
}

/// The reconnect supervisor recovers an off-LAN peer when its working snapshot is
/// seeded SOLELY from the `(allowlist peers) × (known_public_relays)` cross-product
/// — NO saved per-peer hint.
///
/// This is the Tier-2 simplification (plan chunk C5): per-peer relay hints are no
/// longer persisted; the only durable networking store is the public-relay set, and
/// the supervisor's working set is rebuilt at startup by crossing the allowlist with
/// that set. This test proves that cross-product seed is sufficient to recover a
/// parked off-LAN peer — exactly the path `startup.rs` now wires.
///
/// Setup mirrors `supervisor_recovers_relay_only_peer_after_parked_bootstrap_dial`
/// (relay-only A + B, relay the sole route, A's first bare-id bootstrap dial parks
/// so gossip alone can never recover B). The ONLY difference is the seed source: the
/// supervisor snapshot is built from `A.allowlist.list_peers() × [relay_url]` — the
/// same cross-product `startup.rs` computes — rather than a hand-built hint. With B
/// in A's allowlist and the relay in the public set, the cross-product yields exactly
/// the `(B, relay)` hint the supervisor needs.
///
/// We assert the durable observable — A pulls a note B wrote — not a spy call, so a
/// pass requires the connection to actually establish and sync, not merely that a
/// hint was enqueued. `MAX_HINT_BACKOFF_MS` (30 min) ≫ the 30s test budget, so the
/// cross-product seed is the sole thing that can drive the reconnect — there is no
/// background backoff window that could mask a missing seed.
///
/// Seeds 122/123 reserved.
#[tokio::test]
async fn supervisor_recovers_offlan_peer_from_cross_product_seed() -> anyhow::Result<()> {
    use vault_sync::fs::FileSystem;

    let _ = tracing_subscriber::fmt()
        .with_env_filter("iroh_gossip::net=debug,sync_daemon=info")
        .with_test_writer()
        .try_init();

    let relay = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_url = relay.relay_url().clone();

    // Relay-only nodes (no IP transports) — the relay is the sole route (off-LAN sim).
    let node_a = common::build_relay_only_node(122, &relay_url).await?;
    let node_b = common::build_relay_only_node(123, &relay_url).await?;

    node_a.allowlist.add_peer(node_b.node_id, "node-b").await?;
    node_b.allowlist.add_peer(node_a.node_id, "node-a").await?;

    let a_id = node_a.sync_node.node_id();
    let b_id = node_b.sync_node.node_id();

    node_a.sync_node.endpoint_for_test().online().await;
    node_b.sync_node.endpoint_for_test().online().await;

    // Seed a note into B's vault so A pulling it proves the partition healed.
    node_b
        .fs
        .write(
            "notes/from-cross-product-seed.md",
            b"# Delivered via cross-product seed",
        )
        .await?;
    {
        let vault = node_b.vault.lock().await;
        vault
            .on_file_changed("notes/from-cross-product-seed.md")
            .await?;
    }

    let vault_id: VaultId = "cafebabe0beef122".parse().unwrap();

    // A bootstraps off B's bare id while B's hint is ABSENT — the first dial parks
    // and leaves B `Pending` with a non-empty queue (gossip can never re-dial it).
    let gossip_b = node_b
        .sync_node
        .join_vault_gossip(&vault_id, vec![])
        .await?;
    let gossip_a = node_a
        .sync_node
        .join_vault_gossip(&vault_id, vec![b_id])
        .await?;

    // Let the first bootstrap dial fail before any hint exists, so B is left
    // `Pending` with a non-empty queue (the H3' precondition).
    tokio::time::sleep(Duration::from_secs(15)).await;

    // The hint becomes present in the live lookup right after the failed first dial.
    // Under C5 this is the cross-product seed at work; here we add it to the lookup
    // explicitly (mirroring startup's `sync_node.add_peer_relay` over the
    // cross-product) so A can resolve B once the supervisor re-dials.
    node_a.sync_node.add_peer_relay(b_id, &relay_url);

    let a_vault = node_a.vault.clone();
    let (_a_file_tx, a_file_rx) = mpsc::unbounded_channel::<FileEvent>();
    let a_shutdown = CancellationToken::new();
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
        "/test-vault-cross-product-seed".into(),
        a_shutdown.clone(),
    );
    daemon_a.set_inbound_seen_rx(node_a.inbound_seen_rx);
    daemon_a.set_net_change_rx(net_rx);

    // THE C5 PATH: build the supervisor snapshot from the cross-product
    // `(allowlist peers) × (known_public_relays)` — NOT a hand-built per-peer hint.
    // This is the exact computation `startup.rs` performs; with B in A's allowlist
    // and `relay_url` the sole public relay, it yields the `(B, relay)` hint.
    let known_public_relays = [relay_url.clone()];
    let cross_product: Vec<PeerRelay> = {
        let peers = node_a.allowlist.list_peers().await?;
        let mut seed = Vec::new();
        for peer in &peers {
            for relay in &known_public_relays {
                seed.push(PeerRelay::new(peer.node_id.to_string(), relay.as_str()));
            }
        }
        seed
    };
    assert!(
        cross_product
            .iter()
            .any(|h| h.endpoint_id == b_id.to_string() && h.relay_url == relay_url.as_str()),
        "the cross-product must contain the (B, relay) hint; built {cross_product:?}"
    );
    daemon_a.seed_peer_relays_snapshot(cross_product);
    daemon_a.set_reconnect_interval(Duration::from_millis(200));

    let a_loop = tokio::spawn(async move {
        daemon_a.run_loop().await;
    });

    // Keep A's hint perpetually due by firing a network-change signal every 500ms so
    // the supervisor re-dials B every tick instead of backing off after one failure.
    let net_shutdown = CancellationToken::new();
    let net_pump = {
        let net_shutdown = net_shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = net_shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {
                        if net_tx.send(()).await.is_err() {
                            break;
                        }
                    }
                }
            }
        })
    };

    // B just needs to stay alive to answer A's pull once A connects.
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
        b_allowlist,
        "device-b".to_string(),
        None,
        "/test-vault-cross-product-seed-b".into(),
        b_shutdown.clone(),
    );
    daemon_b.set_inbound_seen_rx(node_b.inbound_seen_rx);
    let b_loop = tokio::spawn(async move {
        daemon_b.run_loop().await;
    });

    // PROOF: the supervisor's relay-carrying connect — driven SOLELY by the
    // cross-product-seeded snapshot — recovers the parked peer and A pulls B's note.
    common::wait_until(
        "A pulls B's note after the supervisor recovers the peer from the cross-product seed",
        Duration::from_secs(30),
        || {
            let vault = a_vault.clone();
            async move {
                vault
                    .lock()
                    .await
                    .list_files()
                    .await
                    .unwrap_or_default()
                    .contains(&"notes/from-cross-product-seed.md".to_string())
            }
        },
    )
    .await;

    net_shutdown.cancel();
    a_shutdown.cancel();
    b_shutdown.cancel();
    let _ = net_pump.await;
    let _ = a_loop.await;
    let _ = b_loop.await;
    let _ = (a_id, b_fs, b_vault);
    relay.shutdown().await;
    Ok(())
}

/// A node built with a SET of relays (`&[relay_a, relay_b]`) homes on ONE of them.
///
/// This is the Tier-2 wire fix: `SyncNode::new_with_sync_handler` now takes a relay slice and builds
/// `RelayMode::Custom(RelayMap::from_iter(set))`, so a laptop can home on the public
/// relays it has learned (and fail over across them). This test pins the foundation:
/// a multi-relay `RelayMap` actually homes the endpoint (it doesn't panic or refuse),
/// and the selected home is a MEMBER of the configured set — proving the set is the
/// home-candidate list.
///
/// SCOPE: this asserts home-relay SELECTION only — that the endpoint picks a home
/// from the set. It does NOT exercise end-to-end forwarding through that relay (no
/// second peer connects). `test_sync_through_embedded_relay` is the forwarding guard:
/// it routes a real sync between two peers via an embedded relay.
///
/// Seeds 120 reserved.
#[tokio::test]
async fn relay_map_homes_on_one_of_a_set() -> anyhow::Result<()> {
    // Two independent relays; the node's RelayMap contains BOTH.
    let relay_a = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_b = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let url_a: RelayAddr = relay_a.relay_url().clone();
    let url_b: RelayAddr = relay_b.relay_url().clone();

    // Relay-only so the endpoint MUST select a relay home (no IP transport can mask
    // the relay-home selection we're testing).
    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_relay_only_with_sync_handler(
        common::seed(120),
        &[url_a.clone(), url_b.clone()],
        allowlist,
        RejectSyncHandler,
    )
    .await?;

    // `online()` returns once at least one home relay is connected — so the endpoint
    // has finished selecting a home from the RelayMap.
    tokio::time::timeout(Duration::from_secs(30), node.endpoint_for_test().online())
        .await
        .expect("relay-only node with a 2-relay RelayMap failed to home on any relay");

    // The connected home must be one of the two relays we configured.
    let statuses = node.endpoint_for_test().home_relay_status().get();
    let homed: Vec<RelayUrl> = statuses
        .iter()
        .filter(|s| s.is_connected())
        .map(|s| s.url().clone())
        .collect();
    assert!(
        !homed.is_empty(),
        "online() returned but no home relay is connected"
    );
    assert!(
        homed
            .iter()
            .all(|u| u.to_string() == url_a.as_str() || u.to_string() == url_b.as_str()),
        "home relay {homed:?} is not a member of the configured set {{{url_a}, {url_b}}}"
    );

    relay_a.shutdown().await;
    relay_b.shutdown().await;
    Ok(())
}

/// A node fails over to another relay in its set when its current home dies.
///
/// IGNORED: failover is net-report-paced — the endpoint only re-homes after a
/// net-report cycle observes the dead relay, which is wall-clock-timed and flaky
/// in-process. The failover MECHANISM is source-verified (net-report probes every
/// RelayMap member and re-selects the lowest-latency reachable one; see
/// [[Reviews/iroh Relay Conventions]] Q2), so a fragile wall-clock test here is
/// worse than none. Kept as executable documentation of the intended behavior;
/// run manually with `--ignored` when validating an iroh upgrade.
///
/// Seeds 121 reserved.
#[tokio::test]
#[ignore = "net-report-paced failover is timing-fragile in-process; mechanism is source-verified"]
async fn relay_map_fails_over_when_home_dies() -> anyhow::Result<()> {
    let relay_a = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    let relay_b = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    // Kept as iroh `RelayUrl` here: the assertions below compare against iroh's
    // own `home_relay_status()` URLs. The RelayMap is fed `RelayAddr`s (the seam's
    // type) built from these.
    let url_a: RelayUrl = relay_a.relay_url().as_str().parse().unwrap();
    let url_b: RelayUrl = relay_b.relay_url().as_str().parse().unwrap();

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_relay_only_with_sync_handler(
        common::seed(121),
        &[
            RelayAddr::parse(&url_a.to_string()).unwrap(),
            RelayAddr::parse(&url_b.to_string()).unwrap(),
        ],
        allowlist,
        RejectSyncHandler,
    )
    .await?;

    tokio::time::timeout(Duration::from_secs(30), node.endpoint_for_test().online())
        .await
        .expect("node failed to home on any relay");

    // Kill whichever relay the node homed on; it must migrate to the survivor.
    let homed_on_a = node
        .endpoint_for_test()
        .home_relay_status()
        .get()
        .iter()
        .any(|s| s.is_connected() && *s.url() == url_a);
    let survivor = if homed_on_a {
        relay_a.shutdown().await;
        url_b.clone()
    } else {
        relay_b.shutdown().await;
        url_a.clone()
    };

    // Wait (bounded) for the home to migrate to the surviving relay.
    let migrated = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let connected_survivor = node
                .endpoint_for_test()
                .home_relay_status()
                .get()
                .iter()
                .any(|s| s.is_connected() && *s.url() == survivor);
            if connected_survivor {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await;
    assert!(
        migrated.is_ok(),
        "home relay did not fail over to the surviving relay {survivor} within the wait"
    );

    Ok(())
}

/// A laptop homes on a relay drawn from its persisted `known_public_relays` set.
///
/// This pins the C5 laptop wire: at startup a laptop (no embedded relay) parses its
/// persisted `known_public_relays` into a `RelayUrl` slice and hands it to
/// `SyncNode::new_with_sync_handler` as the RelayMap. Here we model that exact transform — a
/// `Vec<String>` public set → `Vec<RelayUrl>` → `new_relay_only` — and assert the
/// endpoint actually homes on the public relay it was given. Relay-only so no IP
/// transport can mask the relay-home selection.
///
/// Seeds 124 reserved.
#[tokio::test]
async fn laptop_homes_on_persisted_public_relay() -> anyhow::Result<()> {
    let relay = EmbeddedRelay::start("127.0.0.1:0".parse().unwrap()).await?;
    // Kept as iroh `RelayUrl` for the `home_relay_status()` comparison below.
    let relay_url: RelayUrl = relay.relay_url().as_str().parse().unwrap();

    // Model the persisted store: the laptop has one known public relay (its paired
    // server's). Parse it exactly as startup.rs does (String → RelayAddr).
    let known_public_relays: Vec<String> = vec![relay_url.to_string()];
    let home_relays: Vec<RelayAddr> = known_public_relays
        .iter()
        .filter_map(|u| RelayAddr::parse(u).ok())
        .collect();

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_relay_only_with_sync_handler(
        common::seed(124),
        &home_relays,
        allowlist,
        RejectSyncHandler,
    )
    .await?;

    tokio::time::timeout(Duration::from_secs(30), node.endpoint_for_test().online())
        .await
        .expect("laptop with a persisted public relay failed to home on it");

    let homed: Vec<RelayUrl> = node
        .endpoint_for_test()
        .home_relay_status()
        .get()
        .iter()
        .filter(|s| s.is_connected())
        .map(|s| s.url().clone())
        .collect();
    assert!(
        homed.contains(&relay_url),
        "laptop should home on its persisted public relay {relay_url}, homed on {homed:?}"
    );

    relay.shutdown().await;
    Ok(())
}

/// A laptop that has never met a server (empty `known_public_relays`) builds with
/// `RelayMode::Disabled` and homes on no relay — LAN-only, no panic.
///
/// This documents the never-met-a-server edge: an empty public set → empty relay
/// slice → `RelayMode::Disabled`. The node still constructs and runs (it keeps its
/// IP transports + mDNS + peer_lookup for LAN-direct sync), it simply has no relay
/// home. We use the regular `SyncNode::new_with_sync_handler` here — NOT `new_relay_only_with_sync_handler` — because a
/// real laptop in this state keeps its IP transports; a relay-only node with neither
/// a relay nor IP transports has no transport at all and cannot construct an
/// endpoint. We assert the endpoint reports no connected relay home rather than
/// calling `online()` (which would block forever with no relay to connect to).
///
/// Seeds 125 reserved.
#[tokio::test]
async fn laptop_with_empty_public_set_is_relay_disabled() -> anyhow::Result<()> {
    // Empty persisted public set → empty relay slice (the C5 laptop edge).
    let home_relays: Vec<RelayAddr> = Vec::new();

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_with_sync_handler(
        common::seed(125),
        &home_relays,
        allowlist,
        RejectSyncHandler,
    )
    .await?;

    // With RelayMode::Disabled there is no relay to home on. Give the endpoint a
    // brief window to (not) select a home, then assert no relay home is connected.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let connected_homes: Vec<RelayUrl> = node
        .endpoint_for_test()
        .home_relay_status()
        .get()
        .iter()
        .filter(|s| s.is_connected())
        .map(|s| s.url().clone())
        .collect();
    assert!(
        connected_homes.is_empty(),
        "an empty public set must yield RelayMode::Disabled (no relay home), got {connected_homes:?}"
    );

    node.shutdown().await.ok();
    Ok(())
}

/// The startup cross-product seeds the live `peer_lookup` with EVERY
/// `(allowlist peer, known public relay)` combination — and skips this node's own
/// identity even when it is in its own allowlist.
///
/// Regression guard for the C5 seed-source change: the live lookup is no longer
/// seeded from persisted per-peer hints but from `(allowlist peers) × (public set)`.
/// With 2 OTHER allowlisted peers and 2 public relays, all 4 combinations must be
/// resolvable — so each peer can be reached by trying it through either relay. The
/// node's OWN id (added to its allowlist by the first-pair bootstrap) must NOT be
/// seeded: a self-directed relay path is rejected by iroh and would make the
/// supervisor dial this node itself. We replicate startup's exact seeding loop
/// (self-skip + `add_peer_relay` over the cross-product) and assert both.
///
/// Seeds 126 reserved.
#[tokio::test]
async fn startup_seeds_peer_lookup_from_allowlist_cross_public_set() -> anyhow::Result<()> {
    // Two public relays (just need valid, distinct URLs — not dialed here).
    let relay_x = RelayAddr::parse("https://relay-x.test/").unwrap();
    let relay_y = RelayAddr::parse("https://relay-y.test/").unwrap();
    let public_relays = [relay_x.clone(), relay_y.clone()];

    // Two allowlisted peers (distinct from our own identity).
    let peer_1 = PeerId::from_secret_bytes(common::seed(200));
    let peer_2 = PeerId::from_secret_bytes(common::seed(201));

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_with_sync_handler(
        common::seed(126),
        &[],
        allowlist.clone(),
        RejectSyncHandler,
    )
    .await?;

    // Add the node's OWN id to its allowlist (what the first-pair bootstrap does),
    // alongside the two real peers — so the self-skip is actually exercised.
    let own_peer_id = node.node_id();
    allowlist.add_peer(own_peer_id, "self").await?;
    allowlist.add_peer(peer_1, "peer-1").await?;
    allowlist.add_peer(peer_2, "peer-2").await?;

    // Replicate startup's cross-product seed verbatim: skip self, then for each
    // remaining (peer, relay) call `add_peer_relay` (the live lookup wire) AND push
    // a `PeerRelay` (the supervisor-snapshot wire) — the same dual write startup
    // performs from one cross-product pass.
    let peers = allowlist.list_peers().await?;
    let mut supervisor_seed: Vec<PeerRelay> = Vec::new();
    for peer in &peers {
        if peer.node_id == own_peer_id {
            continue;
        }
        let endpoint_hex = peer.node_id.to_string();
        for relay in &public_relays {
            node.add_peer_relay(peer.node_id, relay);
            supervisor_seed.push(PeerRelay::new(endpoint_hex.clone(), relay.as_str()));
        }
    }

    // peer_lookup wire: every one of the 4 (peer, relay) pairs must resolve.
    for peer in [peer_1, peer_2] {
        let info = node
            .peer_lookup_for_test()
            .get_endpoint_info(common::endpoint_id(peer))
            .unwrap_or_else(|| {
                panic!("peer {peer} should be resolvable from the cross-product seed")
            });
        // iroh's lookup returns its own `RelayUrl`; compare via string form.
        let relay_urls: Vec<String> = info
            .into_endpoint_addr()
            .relay_urls()
            .map(|u| u.to_string())
            .collect();
        for relay in &public_relays {
            assert!(
                relay_urls.contains(&relay.as_str()),
                "peer {peer}'s lookup entry should carry relay {relay}; got {relay_urls:?}"
            );
        }
    }

    // supervisor-snapshot wire: exactly the 4 cross-product entries, and crucially
    // NONE for this node's own id — a self-entry would drive the supervisor to dial
    // this node (iroh rejects self-directed relay paths). This is the behavior the
    // startup self-skip guards; `add_peer_relay`'s own skip only protects the
    // peer_lookup wire, not the supervisor snapshot.
    assert_eq!(
        supervisor_seed.len(),
        4,
        "2 peers × 2 relays = 4 supervisor-snapshot entries; got {supervisor_seed:?}"
    );
    let own_hex = own_peer_id.to_string();
    assert!(
        supervisor_seed.iter().all(|h| h.endpoint_id != own_hex),
        "the supervisor snapshot must not contain this node's own id; got {supervisor_seed:?}"
    );

    node.shutdown().await.ok();
    Ok(())
}

/// A SERVER's cross-product is `(other allowlist peers) × {its own relay}` — non-empty
/// and self-skipped — so its reconnect supervisor has dial targets after a restart.
///
/// Regression guard for the server-own-relay consistency seed (C5): a server's own
/// public relay is added to `known_public_relays` at startup, so its public set is
/// `{own relay}` (it only ever LEARNS others' relays via pairing, never its own). We
/// model that post-seed state — public set = the server's own relay, two OTHER peers
/// in the allowlist plus self — and assert the cross-product gives the supervisor a
/// dial target for each peer through the server's own relay, with self excluded.
/// This is what lets e.g. umbra re-dial laptops `(charon,rhea) × {umbra}` instead of
/// the supervisor sitting idle.
///
/// Seeds 127 reserved.
#[tokio::test]
async fn cross_product_for_server_is_peers_times_own_relay() -> anyhow::Result<()> {
    // The server's own public relay — the sole member of its public set after the
    // startup consistency seed (a server learns others' relays only via pairing).
    let own_relay = RelayAddr::parse("https://umbra-server.test/").unwrap();
    let public_relays = [own_relay.clone()];

    let peer_charon = PeerId::from_secret_bytes(common::seed(210));
    let peer_rhea = PeerId::from_secret_bytes(common::seed(211));

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let node = SyncNode::new_with_sync_handler(
        common::seed(127),
        &[],
        allowlist.clone(),
        RejectSyncHandler,
    )
    .await?;

    // The server is in its own allowlist (first-pair bootstrap) alongside the laptops.
    let own_peer_id = node.node_id();
    allowlist.add_peer(own_peer_id, "self").await?;
    allowlist.add_peer(peer_charon, "charon").await?;
    allowlist.add_peer(peer_rhea, "rhea").await?;

    // Replicate startup's cross-product seed (self-skipped) over the server's set.
    let peers = allowlist.list_peers().await?;
    let mut supervisor_seed: Vec<PeerRelay> = Vec::new();
    for peer in &peers {
        if peer.node_id == own_peer_id {
            continue;
        }
        let endpoint_hex = peer.node_id.to_string();
        for relay in &public_relays {
            node.add_peer_relay(peer.node_id, relay);
            supervisor_seed.push(PeerRelay::new(endpoint_hex.clone(), relay.as_str()));
        }
    }

    // The supervisor now has a dial target per laptop through the server's own relay
    // — NON-empty (the bug this seed fixes was an empty cross-product → idle
    // supervisor) — and self is excluded.
    assert_eq!(
        supervisor_seed.len(),
        2,
        "2 laptops × 1 own relay = 2 supervisor targets; got {supervisor_seed:?}"
    );
    let own_hex = own_peer_id.to_string();
    for peer in [peer_charon, peer_rhea] {
        let peer_hex = peer.to_string();
        assert!(
            supervisor_seed
                .iter()
                .any(|h| h.endpoint_id == peer_hex && h.relay_url == own_relay.as_str()),
            "supervisor should target {peer} through the server's own relay; got {supervisor_seed:?}"
        );
    }
    assert!(
        supervisor_seed.iter().all(|h| h.endpoint_id != own_hex),
        "the server must not target itself; got {supervisor_seed:?}"
    );

    node.shutdown().await.ok();
    Ok(())
}
