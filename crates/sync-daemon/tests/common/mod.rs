//! Shared test helpers for the sync-daemon integration suites.
//!
//! `relay_integration.rs` and the `daemon_*` suites (`daemon_sync`,
//! `daemon_pairing`, `daemon_reconnect`, `daemon_move_recovery`,
//! `daemon_case_drift`) all `mod common`. Keep anything here genuinely
//! cross-suite — suite-private helpers live in their own file.

use std::sync::Arc;
use std::time::Duration;

use p2p_core::PeerId;
use p2p_core::allowlist::{AllowlistStorage, InMemoryAllowlist};
use p2p_core::{EndpointId, MemoryLookup, RelayAddr};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use vault_sync::Vault;
use vault_sync::VaultId;
use vault_sync::fs::InMemoryFs;
use vault_sync::network::{SyncNode, SyncNodeSeam, gossip::VaultGossip};

use sync_daemon::daemon::Daemon;
use sync_daemon::watcher::{FileEvent, FileEventKind};

/// Generate a deterministic 32-byte key seed from a small integer.
///
/// Used by relay_integration and daemon_integration tests to build iroh nodes
/// with repeatable identities. Each unique `n` produces a distinct key.
pub fn seed(n: u8) -> [u8; 32] {
    [n; 32]
}

/// Convert a `PeerId` to iroh's `EndpointId` (same 32 bytes) for tests that drive
/// raw iroh APIs — `MemoryLookup::get_endpoint_info`, `endpoint.connect`, etc. —
/// which still speak iroh's id even though the daemon's public surface is `PeerId`.
#[allow(dead_code)] // see RelayNode — per-binary common compilation
pub fn endpoint_id(peer: PeerId) -> EndpointId {
    EndpointId::from_bytes(peer.as_bytes()).expect("test PeerId is a valid key")
}

/// A relay-aware test node: an iroh `SyncNode` whose home relay is `relay_url`,
/// plus a vault and allowlist, ready to be handed to `Daemon::new`.
///
/// Unlike `daemon_integration.rs`'s `build_node` (which wires direct
/// `MemoryLookup` addresses), this node's only routing path is its home relay —
/// the topology a non-home `ActiveRelayActor` reap repro needs. Peer-relay hints
/// are seeded afterward via `sync_node.set_peer_relay`, which targets the
/// `peer_lookup` `SyncNode::new` already registered.
///
/// `dead_code` allow: Rust compiles `mod common` separately into each
/// integration-test binary, so this is "unused" from `daemon_integration.rs`'s
/// perspective even though `relay_integration.rs` uses it.
#[allow(dead_code)]
pub struct RelayNode {
    pub vault: Arc<Mutex<Vault<Arc<InMemoryFs>>>>,
    pub fs: Arc<InMemoryFs>,
    pub allowlist: Arc<InMemoryAllowlist>,
    pub sync_node: SyncNode,
    pub node_id: PeerId,
    /// The inbound-sync freshness receiver paired with the node's pumped handler.
    /// Wire into the daemon via `set_inbound_seen_rx` so inbound-only peers count
    /// as alive (S2).
    pub inbound_seen_rx: mpsc::UnboundedReceiver<PeerId>,
}

/// Build a [`RelayNode`] whose home relay is `relay_url`.
///
/// Built with the daemon's PUMPED inbound handler (the relay-path convergence
/// tests run a full `Daemon`, so the responder must drive the multi-message
/// vault-sync handshake exactly as production does).
#[allow(dead_code)] // see RelayNode — per-binary common compilation
pub async fn build_node_with_relay(
    seed_byte: u8,
    relay_url: &RelayAddr,
) -> anyhow::Result<RelayNode> {
    let fs = Arc::new(InMemoryFs::new());
    let author = PeerId::from_secret_bytes(seed(seed_byte));
    let vault = Vault::init(fs.clone(), author.as_u64()).await?;
    let vault = Arc::new(Mutex::new(vault));

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let (inbound_seen_tx, inbound_seen_rx) = mpsc::unbounded_channel();
    let sync_handler = sync_daemon::daemon::PumpedSyncHandler::new(
        vault.clone(),
        allowlist.clone(),
        inbound_seen_tx,
    );
    let sync_node = SyncNode::new_with_sync_handler(
        seed(seed_byte),
        std::slice::from_ref(relay_url),
        allowlist.clone(),
        sync_handler,
    )
    .await?;
    let node_id = PeerId::from_bytes(*sync_node.node_id().as_bytes());

    Ok(RelayNode {
        vault,
        fs,
        allowlist,
        sync_node,
        node_id,
        inbound_seen_rx,
    })
}

/// Build a [`RelayNode`] whose home relay is `relay_url` and whose endpoint has
/// **no IP transports** — relay is the only routing path.
///
/// Identical to [`build_node_with_relay`] except it uses
/// [`SyncNode::new_relay_only`], reproducing the off-LAN / behind-NAT condition:
/// two such loopback nodes cannot fall back to direct addresses, so the relay is
/// genuinely the only way they reach each other. Without this, in-process
/// localhost nodes discover each other's direct addresses and bypass the relay,
/// masking relay-path bugs.
#[allow(dead_code)] // see RelayNode — per-binary common compilation
pub async fn build_relay_only_node(
    seed_byte: u8,
    relay_url: &RelayAddr,
) -> anyhow::Result<RelayNode> {
    let fs = Arc::new(InMemoryFs::new());
    let author = PeerId::from_secret_bytes(seed(seed_byte));
    let vault = Vault::init(fs.clone(), author.as_u64()).await?;
    let vault = Arc::new(Mutex::new(vault));

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let (inbound_seen_tx, inbound_seen_rx) = mpsc::unbounded_channel();
    let sync_handler = sync_daemon::daemon::PumpedSyncHandler::new(
        vault.clone(),
        allowlist.clone(),
        inbound_seen_tx,
    );
    let sync_node = SyncNode::new_relay_only_with_sync_handler(
        seed(seed_byte),
        std::slice::from_ref(relay_url),
        allowlist.clone(),
        sync_handler,
    )
    .await?;
    let node_id = PeerId::from_bytes(*sync_node.node_id().as_bytes());

    Ok(RelayNode {
        vault,
        fs,
        allowlist,
        sync_node,
        node_id,
        inbound_seen_rx,
    })
}

/// Poll `predicate` until it returns true or `deadline` elapses, panicking on
/// timeout. Checks every 100ms.
///
/// Takes an explicit deadline rather than a fixed budget because the relay-reap
/// repro must outlast iroh's hardcoded 60s `RELAY_INACTIVE_CLEANUP_TIME` — far
/// longer than `daemon_integration.rs`'s 10s poller allows.
#[allow(dead_code)] // see RelayNode — per-binary common compilation
pub async fn wait_until<F, Fut>(description: &str, deadline: Duration, predicate: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + deadline;
    loop {
        if predicate().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for: {description}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ── daemon-integration universal helpers ────────────────────────────────────
//
// Promoted from `daemon_integration.rs` when it was split into the focused
// `daemon_*` suites. These build/connect/spawn the live `Daemon` harness every
// daemon suite shares. (The daemon suites' 2-arg `wait_until` is NOT here — it
// collides by name with the relay 3-arg `wait_until` above, so each suite keeps
// a file-local copy.)
//
// `dead_code` allows: `mod common` compiles separately into each integration
// binary, so any item a given binary doesn't call looks "unused" there.

/// Shared gossip topic so all test daemons join the same swarm.
///
/// Returns a `vault_sync::VaultId` — the same type the vault's own `vault_id()`
/// returns and the gossip layer (`join_vault_gossip`) takes as its topic key.
#[allow(dead_code)]
pub fn shared_vault_id() -> VaultId {
    "cafebabecafebabe".parse().unwrap()
}

/// A test daemon: vault, filesystem, allowlist, and channels for injecting
/// events and triggering shutdown. The event loop runs in a background task.
#[allow(dead_code)]
pub struct TestDaemon {
    pub vault: Arc<Mutex<Vault<Arc<InMemoryFs>>>>,
    pub fs: Arc<InMemoryFs>,
    pub allowlist: Arc<InMemoryAllowlist>,
    /// Send file events into the daemon's event loop.
    pub file_event_tx: mpsc::UnboundedSender<FileEvent>,
    /// Cancel to stop the daemon's event loop.
    pub shutdown: CancellationToken,
    /// Background task handle for the event loop.
    pub loop_handle: JoinHandle<()>,
}

/// Pre-gossip node state — built first so we can wire connectivity before joining.
#[allow(dead_code)]
pub struct NodeBundle {
    pub vault: Arc<Mutex<Vault<Arc<InMemoryFs>>>>,
    pub fs: Arc<InMemoryFs>,
    pub allowlist: Arc<InMemoryAllowlist>,
    pub sync_node: SyncNode,
    pub node_id: PeerId,
    /// The inbound-sync freshness receiver paired with the pumped handler's
    /// sender. Wired into the daemon (`set_inbound_seen_rx`) by `spawn_daemon`
    /// so inbound-only peers are stamped alive (S2). Tests building a `Daemon`
    /// inline can take it too, or drop it (the handler's `send` then fails
    /// quietly — inert, since those tests don't assert inbound-only liveness).
    pub inbound_seen_rx: mpsc::UnboundedReceiver<PeerId>,
}

/// Build an iroh node with vault and allowlist, but do NOT join gossip yet.
///
/// Gossip is joined separately (after connectivity is wired) so each node
/// has exactly one subscription per topic, matching the daemon's production behavior.
///
/// The node is built with the daemon's PUMPED inbound handler (via
/// `new_with_sync_handler`), not the default one-shot `SyncStreamHandler` —
/// the convergence tests exercise the multi-message vault-sync handshake, so
/// the inbound side must pump exactly as production does.
#[allow(dead_code)]
pub async fn build_node(seed_byte: u8) -> anyhow::Result<NodeBundle> {
    let fs = Arc::new(InMemoryFs::new());
    // Author Loro ops under a per-device PeerId derived from this node's
    // secret seed, so each node is a distinct Loro replica.
    let author = PeerId::from_secret_bytes(seed(seed_byte));
    let vault = Vault::init(fs.clone(), author.as_u64()).await?;
    let vault = Arc::new(Mutex::new(vault));

    let allowlist = Arc::new(InMemoryAllowlist::new());
    let (inbound_seen_tx, inbound_seen_rx) = mpsc::unbounded_channel();
    let sync_handler = sync_daemon::daemon::PumpedSyncHandler::new(
        vault.clone(),
        allowlist.clone(),
        inbound_seen_tx,
    );
    let sync_node =
        SyncNode::new_with_sync_handler(seed(seed_byte), &[], allowlist.clone(), sync_handler)
            .await?;

    let memory_lookup = MemoryLookup::new();
    sync_node
        .endpoint_for_test()
        .address_lookup()?
        .add(memory_lookup);

    let node_id = PeerId::from_bytes(*sync_node.node_id().as_bytes());

    Ok(NodeBundle {
        vault,
        fs,
        allowlist,
        sync_node,
        node_id,
        inbound_seen_rx,
    })
}

/// Wire two nodes for direct `MemoryLookup` connectivity and mutual allowlist access.
#[allow(dead_code)]
pub async fn connect_nodes(a: &NodeBundle, b: &NodeBundle) -> anyhow::Result<()> {
    let addr_a = a.sync_node.endpoint_for_test().addr();
    let addr_b = b.sync_node.endpoint_for_test().addr();

    let lookup_a = MemoryLookup::new();
    lookup_a.add_endpoint_info(addr_b.clone());
    a.sync_node
        .endpoint_for_test()
        .address_lookup()?
        .add(lookup_a);

    let lookup_b = MemoryLookup::new();
    lookup_b.add_endpoint_info(addr_a.clone());
    b.sync_node
        .endpoint_for_test()
        .address_lookup()?
        .add(lookup_b);

    a.allowlist.add_peer(b.node_id, "peer-b").await?;
    b.allowlist.add_peer(a.node_id, "peer-a").await?;

    Ok(())
}

/// Spawn the Daemon event loop from pre-wired components.
///
/// Takes ownership of the `NodeBundle` and the gossip subscription so the
/// daemon owns all components for the duration of the test.
#[allow(dead_code)]
pub fn spawn_daemon(node: NodeBundle, gossip: VaultGossip) -> TestDaemon {
    let (file_event_tx, file_event_rx) = mpsc::unbounded_channel::<FileEvent>();
    let shutdown = CancellationToken::new();

    let vault = node.vault.clone();
    let fs = node.fs.clone();
    let allowlist = node.allowlist.clone();
    let inbound_seen_rx = node.inbound_seen_rx;

    let mut daemon = Daemon::new(
        vault.clone(),
        node.sync_node,
        gossip,
        file_event_rx,
        None, // no mDNS discovery in tests
        allowlist.clone(),
        "test-device".to_string(),
        None,
        "/test-vault".into(),
        shutdown.clone(),
    );
    // Wire the pumped handler's freshness receiver so inbound-only peers are
    // stamped alive (S2) — mirrors `startup.rs`.
    daemon.set_inbound_seen_rx(inbound_seen_rx);
    // Wire the SAME in-memory fs the vault uses so the move-coalescer's
    // crash-recovery journal (`.sync/pending-moves.json`, P4f-2) lands in the
    // same stateful `InMemoryFs` as the vault's `.loro`/`.md`. `InMemoryFs` is
    // stateful, so this MUST be `node.fs`, not a fresh instance. The daemon's
    // `FS` is `Arc<InMemoryFs>`, so the handle is double-Arc'd — harmless, the
    // `FileSystem for Arc<T>` blanket impl makes it a usable filesystem.
    daemon.set_fs(Arc::new(fs.clone()));

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

/// Inject a `FileEvent::Modified` into the daemon's event loop.
///
/// Write the file to `daemon.fs` before calling this — the daemon's
/// `on_file_modified` handler calls `vault.on_file_changed()` which reads
/// from the in-memory filesystem.
#[allow(dead_code)]
pub fn inject_modified(daemon: &TestDaemon, path: &str) {
    daemon
        .file_event_tx
        .send(FileEvent {
            path: path.to_string(),
            kind: FileEventKind::Modified,
        })
        .expect("file event channel unexpectedly closed");
}

/// Inject a `FileEvent::Deleted` into the daemon's event loop.
#[allow(dead_code)]
pub fn inject_deleted(daemon: &TestDaemon, path: &str) {
    daemon
        .file_event_tx
        .send(FileEvent {
            path: path.to_string(),
            kind: FileEventKind::Deleted,
        })
        .expect("file event channel unexpectedly closed");
}

/// The document UUID the index currently records for `path` (as a string), if
/// any. The headline property of a move is that this UUID is unchanged across
/// the rename, so the tests read it before and after. Returned as a `String`
/// purely so the assertions need no `uuid` dependency.
#[allow(dead_code)]
pub async fn uuid_of(vault: &Arc<Mutex<Vault<Arc<InMemoryFs>>>>, path: &str) -> Option<String> {
    let vault = vault.lock().await;
    let node = vault.index().node_for_path(path)?;
    vault.index().node_uuid(&node).map(|u| u.to_string())
}

/// Wall-clock ms helper for tests — mirrors the daemon's `now_ms` so seeded
/// `last_attempt_ms` values land on the same clock the supervisor reads.
#[allow(dead_code)]
pub fn now_ms_test() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
