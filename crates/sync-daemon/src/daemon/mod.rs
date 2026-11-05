//! Daemon event loop: the `Daemon` struct, its fields, and all event handlers.
//!
//! Submodule layout:
//! - `startup` — startup seam (`StartupBundle`, `startup_inner`) and public entry points
//!   (`run_with_shutdown_controlled`, `run_with_shutdown`, `run`).
//! - `initiator` — two-step GUI pairing state machine (`InitiatorSession`,
//!   `InitiatorPairOutcome`, `run_initiator_pairing_parked`, and the five
//!   `impl Daemon` initiator methods).

mod fs_events;
mod initiator;
mod move_recovery;
mod peer_events;
mod reconnect;
mod startup;
mod sync_exchange;
mod sync_stream;

use reconnect::RECONNECT_BASE_MS;

// Re-export public names so `sync_daemon::daemon::X` paths stay byte-identical.
use initiator::{InitiatorPairOutcome, InitiatorSession};
pub use startup::{run, run_with_shutdown, run_with_shutdown_controlled};
use sync_exchange::{SyncExchangeKind, spawn_sync_exchange};
// Re-exported for the integration-test harness, which builds nodes with the same
// pumped inbound handler production uses (see `sync_stream`).
pub use sync_stream::PumpedSyncHandler;
// Re-exported for the boot-recovery integration harness (`spawn_daemon_from_loaded`),
// which runs the SAME journal-read → restitch → clear sequence as `startup_inner` so
// production and test cannot diverge (P4f-2b-ii §0.1).
pub use move_recovery::{clear_pending_journal, read_pending_journal, restitch_inputs};

use crate::move_coalescer::{
    Expired, JournaledMove, MoveCoalescer, MoveDecision, PENDING_MOVES_FILE, PENDING_MOVES_VERSION,
    PendingKind, PendingMovesFile,
};
use crate::pair_api::{ConnectionState, DaemonCommand, DaemonStatus, PairingUiEvent, PeerSummary};
use crate::watcher::{FileEvent, FileEventKind};
use p2p_core::{
    PLACEHOLDER_DEVICE_NAME, PeerAddr, PeerConnType, RelayAddr, relay_is_offlan_reachable,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use p2p_core::PeerId;
use p2p_core::allowlist::{AllowedPeer, AllowlistStorage};
use p2p_core::pairing::{PairingChallenge, PairingSession};
use p2p_core::pairing_handler::{InboundPairingExchange, PairingApproval, PairingEvent};
use p2p_core::time_scale::{scaled, scaled_ms};
use vault_sync::network::peers::{ConnectTransition, PeerRegistry};
use vault_sync::network::{
    GOSSIP_ALPN, SyncNode,
    gossip::{GossipEvent, GossipMessage, MAX_GOSSIP_MESSAGE_SIZE, VaultGossip},
};
// The vault/sync-engine layer and its iroh seam (`vault_sync::network`) are vault-sync.
// `Daemon<FS>` and `Vault<FS>` are both generic over vault-sync's `FileSystem`.
use uuid::Uuid;
use vault_sync::Vault;
use vault_sync::fs::FileSystem;

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How often the connected-path allowlist reconcile re-pushes the membership
/// roster to live neighbors. The reconnect supervisor wakes every
/// [`RECONNECT_BASE_MS`] (5s), but the roster push is throttled to this slower
/// cadence so a healthy mesh re-converges any drift (a missed `NeighborUp`, a
/// restart) without a per-tick gossip broadcast. ~60s is responsive at the
/// stated 3-machine scale and negligible fan-out.
const ROSTER_RECONCILE_MS: u64 = 60_000;

/// How often the case-drift sweep lists the vault and reconciles disk-vs-index
/// folder/file casing (Bug 1). A folder case-rename on a case-INSENSITIVE
/// filesystem fires no watcher delete (the old casing still resolves), so the
/// event path can't see it — this dedicated sweep is the reliable signal. A few
/// seconds keeps the ghost-minting window between a rename and its detection
/// small while staying off the per-event hot path. Deliberately NOT piggybacked
/// on the reconcile/move-sweep timer (it is a distinct, independently-tunable
/// concern), and ALSO triggerable on demand so a controlled fleet rename fires
/// it immediately rather than waiting the interval.
const CASE_DRIFT_SWEEP_MS: u64 = 3_000;

/// Configuration for running the sync daemon.
pub struct DaemonRunConfig {
    pub vault: PathBuf,
    /// Optional path to an alternate identity key file (default: `.sync/daemon.key`).
    pub identity_key: Option<PathBuf>,
    /// If set, serve a `/health` endpoint on this address (e.g. `"127.0.0.1:8081"`).
    pub health_listen: Option<String>,
    /// If set, start an embedded iroh relay server on this address (e.g. `"0.0.0.0:3340"`).
    ///
    /// Relay startup failure is non-fatal — the daemon will log a warning and continue
    /// without relay support rather than refusing to start.
    pub relay_listen: Option<String>,
    /// The URL advertised to peers for the embedded relay.
    ///
    /// When `relay_listen` binds to `0.0.0.0`, the bound address is not dialable
    /// by peers. Set this to the machine's LAN IP so peers can reach the relay.
    /// Defaults to the bound address when `None`.
    pub advertised_relay_url: Option<String>,
}

/// A peer's freshness, observed from a successful background sync exchange.
///
/// `spawn_sync_exchange` runs in a detached task with no `&mut self`, so it
/// can't refresh the supervisor snapshot directly. It sends this back to the
/// run-loop (mirroring the initiator-outcome channel) where `on_exchange_learned`
/// applies it — resetting the hint's throttle the instant a peer is reachable,
/// which is what makes stale-hint eviction safe.
struct ExchangeLearned {
    /// The peer we just synced with.
    endpoint_id: PeerId,
    /// The active relay URL iroh reported for the peer, if it connected through
    /// a relay. `None` for a LAN-direct connection (no relay path) — we still
    /// stamp success, we just don't overwrite the stored URL with nothing.
    relay_url: Option<RelayAddr>,
}

/// Daemon state holding all components.
///
/// Generic over the filesystem (`FS`) and allowlist storage (`AL`) so that
/// integration tests can inject in-memory implementations without touching the
/// real filesystem or spawning a file watcher.
pub struct Daemon<FS: FileSystem, AL> {
    vault: Arc<Mutex<Vault<FS>>>,
    /// Exposed `pub(crate)` so `startup.rs` can call `sync_node.shutdown()`
    /// (which consumes `self`) during graceful teardown without requiring a
    /// separate accessor method.
    pub(crate) sync_node: SyncNode,
    vault_gossip: VaultGossip,
    /// Injected file event channel — replaces the `FileWatcher` field so tests can
    /// push synthetic events without a real OS filesystem watcher.
    file_event_rx: mpsc::UnboundedReceiver<FileEvent>,
    /// Optional mDNS discovery channel. `None` in tests (and on WASM builds).
    discovery_rx: Option<mpsc::Receiver<p2p_core::discovery::DiscoveredMesh>>,
    /// Network-change signal. Each `()` means iroh observed an `EndpointAddr`
    /// change (relay/direct-address delta ⇒ network conditions changed). Drained
    /// in the run_loop select; the handler resets every hint's backoff so the
    /// next supervisor tick re-dials on the new network — the fix for "every net
    /// change needs a restart." `None` in tests that don't exercise reconnect,
    /// and on any build where `watch_addr` isn't wired.
    net_change_rx: Option<mpsc::Receiver<()>>,
    /// Deadline (`now_ms()`) until which per-hint reconnect backoff is floored at
    /// base, set by [`on_network_change`] to `now + NET_CHANGE_GRACE_MS`. Session-
    /// only and NOT persisted — a fresh process simply starts without a grace
    /// window. `None` means no grace window is active (the steady state). See
    /// [`effective_backoff_due`] for how it floors the due-gate.
    net_change_grace_until_ms: Option<u64>,
    /// Inbound-sync freshness signal. The pumped inbound handler
    /// (`sync_stream::PumpedSyncHandler`) processes a peer's sync inline and
    /// fires that peer's id here on completion; the run-loop drains it and stamps
    /// `peer_registry.update_last_seen`. The handler can't touch `peer_registry`
    /// directly — it's built before `Daemon` exists — so this fire-and-forget
    /// channel carries the inbound liveness stamp instead (S2). It is the
    /// inbound-only-peer liveness path that the `alive_count` broadcast gate
    /// reads. `None` (inert) until wired post-construction by `startup.rs` or a
    /// test; an undrained channel is harmless (the handler's `send` fails quietly).
    inbound_seen_rx: Option<mpsc::UnboundedReceiver<PeerId>>,
    /// Tracks liveness state of all peers observed in the gossip swarm.
    ///
    /// Wrapped in `Arc<Mutex<...>>` so spawned tasks (QUIC sync exchanges) can
    /// call `update_last_seen` after a successful sync without borrowing `&mut self`.
    peer_registry: Arc<Mutex<PeerRegistry>>,
    /// Controls which peers are allowed to sync. Empty = deny all (pair first).
    ///
    /// Wrapped in Arc so it can be shared with the gossip connection handler,
    /// which runs in separate tasks and must check the allowlist on each inbound
    /// gossip connection before the gossip protocol runs.
    allowlist: Arc<AL>,
    /// Active pairing session, if any. Only one pairing session at a time.
    active_pairing: Option<PairingSession>,
    /// Human-readable name for this device, advertised during pairing.
    device_name: String,
    /// Relay URL if the embedded relay is running, for distribution to new peers.
    relay_url: Option<String>,
    /// The vault's filesystem path. Used by the post-pair onboarding to persist
    /// the adopted relay URL to `daemon.toml`.
    vault_path: PathBuf,
    /// Cancellation token wired to the process shutdown signal. Calling
    /// `shutdown.cancel()` causes `run_loop` to exit cleanly.
    shutdown: CancellationToken,
    /// Broadcasts live daemon status to the desktop tray. `None` when running
    /// from the CLI (no tray to update).
    status_tx: Option<watch::Sender<DaemonStatus>>,
    /// Broadcasts pairing UI events to the desktop tray. `None` in CLI mode.
    pairing_tx: Option<broadcast::Sender<PairingUiEvent>>,
    /// Receives commands from the desktop tray. `None` in CLI mode.
    command_rx: Option<mpsc::UnboundedReceiver<DaemonCommand>>,
    /// Name of the vault mesh, carried through for status broadcasts.
    mesh_name: Option<String>,
    /// In-flight initiator pairing session. Holds the discovered meshes map so
    /// `SubmitCode` can resolve `vault_id` → `EndpointId` without re-scanning,
    /// and a cancellation token so `CancelInitiate` aborts both the discovery
    /// task and any pairing attempt in progress. `None` between sessions.
    active_initiator: Option<InitiatorSession>,
    /// Sender half of the internal initiator-outcome channel. Cloned into the
    /// spawned pairing task so a completed `PairingResult` can be routed back to
    /// the event loop for `&mut self` onboarding work (adopt + re-join).
    initiator_outcome_tx: mpsc::UnboundedSender<InitiatorPairOutcome>,
    /// Receiver half, drained in a `run_loop` `select!` arm.
    initiator_outcome_rx: mpsc::UnboundedReceiver<InitiatorPairOutcome>,
    /// Sender half of the learn-on-exchange channel. Cloned into each spawned
    /// sync-exchange task so a successful sync routes the peer's observed relay
    /// back to the event loop for `&mut self` snapshot/persist refresh.
    exchange_learn_tx: mpsc::UnboundedSender<ExchangeLearned>,
    /// Receiver half, drained in a `run_loop` `select!` arm.
    exchange_learn_rx: mpsc::UnboundedReceiver<ExchangeLearned>,

    // ── reconnect supervisor ────────────────────────────────────────────────
    /// Steady wake cadence for the reconnect supervisor. Per-hint backoff
    /// (tracked on each `PeerRelay`'s freshness fields) decides whether a given
    /// hint actually attempts a re-dial on a wake — there is no longer a single
    /// global backoff gate.
    reconnect_tick: tokio::time::Interval,
    /// In-memory snapshot of persisted peer relay hints, the supervisor's source
    /// of truth for who to re-dial. Populated at startup from `DaemonConfig` and
    /// updated when pairing learns a peer's relay. Held in memory (not reloaded
    /// from disk per tick) so the test harness — which uses a non-existent vault
    /// path — can seed it directly without writing `daemon.toml`.
    peer_relays: Vec<crate::persistence::PeerRelay>,
    /// EndpointIds with a supervisor-issued bootstrap connect currently in flight.
    ///
    /// When a peer's first gossip bootstrap dial parks (relay-only, off-LAN), the
    /// supervisor establishes the connection itself and hands it to gossip (see
    /// `on_reconnect_tick`). Those connects are spawned tasks that can take up to
    /// the QUIC handshake timeout to fail, while the supervisor keeps ticking
    /// every ~200ms after a net change — so without a guard the same peer would
    /// accumulate dozens of concurrent connects. An id is inserted before its
    /// task spawns and removed when the task finishes; a tick skips any id already
    /// present. Shared `Arc<tokio::sync::Mutex<…>>` because the removal happens
    /// inside the spawned task, which can't borrow `&mut self`. The async mutex
    /// (not `std`) is required: nothing holds the guard across an `.await` today,
    /// but it lives behind the same shared-handle pattern as `peer_registry`.
    bootstrap_connect_inflight: Arc<Mutex<HashSet<PeerId>>>,

    // ── allowlist convergence ───────────────────────────────────────────────
    /// Wall-clock (`now_ms()`) of the last connected-path roster reconcile, or
    /// `None` if one hasn't fired yet. Throttles the roster push inside
    /// `on_reconnect_tick` to [`ROSTER_RECONCILE_MS`] so it doesn't broadcast on
    /// every 5s supervisor wake.
    last_roster_reconcile_ms: Option<u64>,
    /// Reconcile throttle window (defaults to [`ROSTER_RECONCILE_MS`]). A field
    /// rather than a const only so integration tests can shrink it to observe a
    /// reconcile within `wait_until`'s budget — same test-seam rationale as
    /// [`Daemon::set_reconnect_interval`].
    roster_reconcile_interval_ms: u64,

    // ── native-move coalescing (P4f-1) ──────────────────────────────────────
    /// Buffers not-yet-tracked create/delete events for a short window so a native
    /// rename (which the watcher delivers as an unlinked delete+create — see
    /// `move_coalescer`) collapses into one same-UUID move instead of a tombstone
    /// plus a fresh-UUID create. Pure (no vault handle); `on_file_changed` computes
    /// the content hashes and executes the decisions it returns.
    move_coalescer: MoveCoalescer,
    /// Drives the coalescer's window-expiry sweep on a steady cadence — a buffered
    /// event whose partner never arrived commits to its standalone meaning (a real
    /// tombstone or a real new doc) once its window passes.
    move_sweep_tick: tokio::time::Interval,
    /// Drives the case-drift sweep (Bug 1): a folder/file case-rename on a
    /// case-insensitive fs fires no watcher delete, so a periodic disk-vs-index
    /// casing reconcile is the only reliable detection. Dedicated (not the
    /// move-sweep timer) and also reachable on demand via [`Self::sweep_case_drift`].
    case_drift_sweep_tick: tokio::time::Interval,
    /// Filesystem handle for writing the move-coalescer's crash-recovery journal
    /// (`.sync/pending-moves.json`, P4f-2). The daemon writes the journal — NOT the
    /// coalescer (which stays pure) and NOT `persistence.rs` (which writes through
    /// `std::fs` against the real OS path, bypassing the vault's `FileSystem` and so
    /// unusable in the in-memory test harness). Routing through the SAME
    /// `FileSystem` the vault uses keeps the crash/boot tests hermetic — the journal
    /// lands in the same fs as the `.loro`/`.md`. Wired post-construction (mirrors
    /// `inbound_seen_rx` / `net_change_rx`) via [`Daemon::set_fs`]; `None` means
    /// journal persistence is inert (the write logs a `warn!` and skips, degrading
    /// to the crash-tail — never silently no-ops). `FileSystem` is impl'd for
    /// `Arc<T>`, so an `Arc<FS>` is itself a usable filesystem.
    fs: Option<Arc<FS>>,
}

impl<FS: FileSystem + 'static, AL: AllowlistStorage + 'static> Daemon<FS, AL> {
    /// Construct a daemon from pre-built components.
    ///
    /// The `run()` function creates these from real filesystem paths; tests
    /// inject in-memory equivalents directly.
    // The constructor takes each pre-built component by value (a builder/struct
    // would just move the same fields one layer up and obscure the public test
    // harness's call site). Bundling them is churn for no readability gain on a
    // sensitive ctor, so we annotate rather than refactor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vault: Arc<Mutex<Vault<FS>>>,
        sync_node: SyncNode,
        vault_gossip: VaultGossip,
        file_event_rx: mpsc::UnboundedReceiver<FileEvent>,
        discovery_rx: Option<mpsc::Receiver<p2p_core::discovery::DiscoveredMesh>>,
        allowlist: Arc<AL>,
        device_name: String,
        relay_url: Option<String>,
        vault_path: PathBuf,
        shutdown: CancellationToken,
    ) -> Self {
        // The initiator-outcome and learn-on-exchange channels are internal —
        // created here rather than passed in so the public `Daemon::new`
        // signature (used directly by the test harness) stays unchanged.
        let (initiator_outcome_tx, initiator_outcome_rx) = mpsc::unbounded_channel();
        let (exchange_learn_tx, exchange_learn_rx) = mpsc::unbounded_channel();

        // The reconnect supervisor wakes on a steady cadence; backoff (tracked in
        // the fields below) gates whether a wake actually re-dials. `Delay` skips
        // missed ticks rather than bursting to catch up after a slow turn.
        let mut reconnect_tick =
            tokio::time::interval(scaled(std::time::Duration::from_millis(RECONNECT_BASE_MS)));
        reconnect_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // The move-coalescer sweep wakes on a fraction of the pairing window so an
        // unpaired event's standalone commit is caught promptly. `Delay` skips
        // missed ticks rather than bursting after a slow turn.
        let mut move_sweep_tick = tokio::time::interval(MoveCoalescer::sweep_interval());
        move_sweep_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // The case-drift sweep wakes on its own few-second cadence (scaled for
        // tests). `Delay` skips missed ticks rather than bursting after a slow turn.
        let mut case_drift_sweep_tick = tokio::time::interval(scaled(
            std::time::Duration::from_millis(CASE_DRIFT_SWEEP_MS),
        ));
        case_drift_sweep_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Self {
            vault,
            sync_node,
            vault_gossip,
            file_event_rx,
            discovery_rx,
            // Wired post-construction by the startup `watch_addr` task (or by
            // tests via `set_net_change_rx`); `None` means net-change reconnect
            // is inert, exactly like an unwired `discovery_rx`.
            net_change_rx: None,
            // No grace window until the first network change opens one.
            net_change_grace_until_ms: None,
            // Wired post-construction (startup.rs / tests via `set_inbound_seen_rx`);
            // `None` means inbound freshness stamping is inert, like an unwired
            // `net_change_rx`.
            inbound_seen_rx: None,
            peer_registry: Arc::new(Mutex::new(PeerRegistry::new())),
            allowlist,
            active_pairing: None,
            device_name,
            relay_url,
            vault_path,
            shutdown,
            status_tx: None,
            pairing_tx: None,
            command_rx: None,
            mesh_name: None,
            active_initiator: None,
            initiator_outcome_tx,
            initiator_outcome_rx,
            exchange_learn_tx,
            exchange_learn_rx,
            reconnect_tick,
            peer_relays: Vec::new(),
            bootstrap_connect_inflight: Arc::new(Mutex::new(HashSet::new())),
            last_roster_reconcile_ms: None,
            roster_reconcile_interval_ms: scaled_ms(ROSTER_RECONCILE_MS),
            move_coalescer: MoveCoalescer::new(),
            move_sweep_tick,
            case_drift_sweep_tick,
            // Wired post-construction (startup.rs / tests via `set_fs`); `None`
            // means the crash-recovery journal write is inert, like an unwired
            // `inbound_seen_rx`.
            fs: None,
        }
    }

    /// Wire control channels into this daemon, enabling tray integration.
    ///
    /// Called by `run_with_shutdown_controlled` after `Daemon::new()` succeeds
    /// but before `run_loop` starts. Not called by the CLI entry points.
    pub fn wire_control(
        &mut self,
        status_tx: watch::Sender<DaemonStatus>,
        pairing_tx: broadcast::Sender<PairingUiEvent>,
        command_rx: mpsc::UnboundedReceiver<DaemonCommand>,
        mesh_name: String,
    ) {
        self.status_tx = Some(status_tx);
        self.pairing_tx = Some(pairing_tx);
        self.command_rx = Some(command_rx);
        self.mesh_name = Some(mesh_name);
    }

    /// Compute and broadcast the current daemon status on the watch channel.
    ///
    /// No-op when running in CLI mode (no `status_tx`).
    pub async fn emit_status(&self) {
        let Some(ref status_tx) = self.status_tx else {
            return;
        };

        let registry = self.peer_registry.lock().await;
        let alive_peers = registry.get_alive_peers();
        let peer_count = alive_peers.len();
        let state = if peer_count > 0 {
            ConnectionState::Connected
        } else {
            ConnectionState::Idle
        };

        let peers = alive_peers
            .iter()
            .map(|e| PeerSummary {
                device_name: e.device_name.clone(),
                last_seen: e.last_seen,
            })
            .collect();

        let status = DaemonStatus {
            state,
            peer_count,
            peers,
            relay_url: self.relay_url.clone(),
            mesh_name: self.mesh_name.clone(),
            device_name: Some(self.device_name.clone()),
        };

        // send() only fails if all receivers are dropped — not an error condition.
        let _ = status_tx.send(status);
    }

    /// Emit a pairing UI event to the desktop tray.
    ///
    /// No-op when running in CLI mode (no `pairing_tx`).
    fn emit_pairing_event(&self, event: PairingUiEvent) {
        let Some(ref pairing_tx) = self.pairing_tx else {
            return;
        };
        // send() only fails when there are no receivers — not an error condition.
        let _ = pairing_tx.send(event);
    }

    /// Run the daemon event loop until the shutdown token is cancelled.
    ///
    /// This is the `loop { tokio::select! { ... } }` body, extracted so tests can
    /// drive it in a background task and inject synthetic events via channels.
    pub async fn run_loop(&mut self) {
        loop {
            tokio::select! {
                Some(event) = self.file_event_rx.recv() => {
                    self.on_file_changed(event).await;
                }

                Some(gossip_event) = self.vault_gossip.event_rx.recv() => {
                    match gossip_event {
                        GossipEvent::NeighborUp(node_id) => {
                            self.on_neighbor_up(node_id).await;
                        }
                        GossipEvent::NeighborDown(node_id) => {
                            self.on_neighbor_down(node_id).await;
                        }
                        GossipEvent::ChangeReceived { from, notification } => {
                            self.on_change_received(from, notification.path).await;
                        }
                        GossipEvent::AllowlistUpdate { from, peer } => {
                            self.on_allowlist_update_received(from, peer).await;
                        }
                        GossipEvent::AllowlistRoster { from, peers } => {
                            self.on_allowlist_roster_received(from, peers).await;
                        }
                    }
                }

                Some(remote_id) = async {
                    match self.inbound_seen_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => None,
                    }
                } => {
                    // A peer completed an inbound sync (handled inline by the pumped
                    // handler). This proves liveness even when our HyParView never
                    // fired NeighborUp (the edge-trigger bug), so mark it alive and
                    // log the connect line on a true transition — and stamp it so
                    // the broadcast `alive_count` gate counts an inbound-only peer
                    // (S2). The inbound handler can't reach `peer_registry`, so it
                    // routes the signal here.
                    self.on_peer_liveness(remote_id).await;
                }

                Some(pairing_event) = self.sync_node.inbound_pairing_rx.recv() => {
                    match pairing_event {
                        PairingEvent::InboundRequest(exchange) => {
                            self.on_inbound_pairing_request(exchange).await;
                        }
                        PairingEvent::PairingCompleted { peer_id, device_name } => {
                            self.on_pairing_completed(peer_id, device_name).await;
                        }
                        PairingEvent::PairingFailed { peer_id, reason } => {
                            self.on_pairing_failed(peer_id, reason);
                        }
                    }
                }

                Some(mesh) = async {
                    match self.discovery_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => None,
                    }
                } => {
                    info!(
                        mesh = %mesh.mesh_name,
                        vid = %mesh.vault_id,
                        peers = mesh.online_count,
                        "mDNS: discovered mesh on LAN"
                    );
                }

                Some(()) = async {
                    match self.net_change_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => None,
                    }
                } => {
                    self.on_network_change().await;
                }

                Some(cmd) = async {
                    match self.command_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => None,
                    }
                } => {
                    self.on_daemon_command(cmd).await;
                }

                Some(outcome) = self.initiator_outcome_rx.recv() => {
                    self.on_initiator_pair_outcome(outcome).await;
                }

                Some(learned) = self.exchange_learn_rx.recv() => {
                    self.on_exchange_learned(learned).await;
                }

                _ = self.reconnect_tick.tick() => {
                    self.on_reconnect_tick().await;
                }

                _ = self.move_sweep_tick.tick() => {
                    self.sweep_pending_moves().await;
                }

                _ = self.case_drift_sweep_tick.tick() => {
                    self.sweep_case_drift().await;
                }

                _ = self.shutdown.cancelled() => {
                    info!("Shutting down");
                    // Drain the move-coalescer buffer BEFORE break: `select!` never
                    // re-polls `file_event_rx` once this arm runs, so no new event
                    // mutates the buffer after the drain. Runs here (not after the
                    // loop) so the vault + gossip handles are still alive when the
                    // standalone sinks fire (P4f-2c).
                    self.drain_pending_moves().await;
                    break;
                }
            }
        }
    }

    /// Wire the network-change signal that resets reconnect backoff.
    ///
    /// Production passes a receiver fed by `endpoint.watch_addr()` (see
    /// `startup.rs`); tests pass a receiver whose sender they hold, so `send(())`
    /// simulates a net change with no real network. `pub` for integration-test
    /// access — same seam rationale as [`Daemon::set_reconnect_interval`]
    /// (integration-test binaries compile this crate without the `test` cfg, so a
    /// `#[cfg(test)]` gate would break them). Not a public production API.
    pub fn set_net_change_rx(&mut self, rx: mpsc::Receiver<()>) {
        self.net_change_rx = Some(rx);
    }

    /// Wire the inbound-sync freshness receiver — the other half of the channel
    /// the pumped inbound handler sends on (built alongside the `SyncNode` in
    /// `startup.rs` / `build_node`). The run-loop drains it to stamp
    /// `peer_registry.update_last_seen` for peers that synced inbound, so a peer
    /// that only ever connects inbound still counts as alive (S2). `pub` for
    /// integration-test access — same seam rationale as [`Daemon::set_net_change_rx`].
    pub fn set_inbound_seen_rx(&mut self, rx: mpsc::UnboundedReceiver<PeerId>) {
        self.inbound_seen_rx = Some(rx);
    }

    /// Wire the filesystem handle used to write the move-coalescer's crash-recovery
    /// journal (`.sync/pending-moves.json`, P4f-2). Pass the SAME `Arc<FS>` the
    /// vault uses so the journal lands in the same filesystem as the vault's
    /// `.loro`/`.md` — in the in-memory test harness this means sharing the one
    /// stateful `InMemoryFs` instance (a separate instance would not see the
    /// journal, making the crash/boot tests meaningless). Called post-construction
    /// by `startup.rs` and the test harness; `pub` for integration-test access —
    /// same seam rationale as [`Daemon::set_inbound_seen_rx`].
    pub fn set_fs(&mut self, fs: Arc<FS>) {
        self.fs = Some(fs);
    }

    /// Dispatch a `DaemonCommand` from the desktop tray.
    async fn on_daemon_command(&mut self, cmd: DaemonCommand) {
        match cmd {
            DaemonCommand::StartDiscovery { reply } => {
                self.start_initiator_discovery(reply).await;
            }
            DaemonCommand::RequestPairing { vault_id, reply } => {
                self.request_initiator_pairing(vault_id, reply).await;
            }
            DaemonCommand::SubmitCode {
                vault_id,
                code,
                reply,
            } => {
                self.submit_initiator_code(vault_id, code, reply).await;
            }
            DaemonCommand::CancelInitiate { reply } => {
                if let Some(session) = self.active_initiator.take() {
                    session.cancel.cancel();
                    info!("Initiator pairing session cancelled by user");
                }
                let _ = reply.send(());
            }
            DaemonCommand::RejectInbound { reply } => {
                // Drop the active pairing session. This closes reply_tx, which
                // the pairing handler treats as an immediate rejection.
                if self.active_pairing.take().is_some() {
                    info!("Inbound pairing rejected by user");
                    self.emit_pairing_event(PairingUiEvent::InboundFailed {
                        reason: "Rejected by user".to_string(),
                    });
                }
                let _ = reply.send(());
            }
        }
    }
}
