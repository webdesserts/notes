//! In-process daemon control handle for the desktop app.
//!
//! When the desktop app embeds the sync daemon via `run_with_shutdown_controlled`,
//! it receives a `DaemonControl` back immediately after startup succeeds. This
//! handle lets the tray observe daemon status changes and drive pairing without
//! polling.
//!
//! The CLI entry points (`run` / `run_with_shutdown`) are unchanged — they drop
//! the `DaemonControl` internally and are unaffected by this module.

use tokio::sync::{broadcast, mpsc, oneshot, watch};

// Broadcast channel capacity for pairing events. Pairing events are meaningful
// (a missed InboundRequest means a missed UI prompt), so broadcast is the right
// primitive. A capacity of 16 is generous — in practice at most one event is
// in-flight at a time.
pub(crate) const PAIRING_BROADCAST_CAPACITY: usize = 16;

/// High-level connection state of the daemon, suitable for display in the tray.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    /// No peers have joined the gossip swarm yet.
    Idle,
    /// One or more peers are currently in the gossip swarm.
    Connected,
}

/// A summary of a connected peer, for display in the tray peers submenu.
#[derive(Clone, Debug)]
pub struct PeerSummary {
    pub device_name: Option<String>,
    /// Unix timestamp (seconds) of the peer's last activity.
    pub last_seen: u64,
}

/// Live snapshot of the daemon's observable state.
///
/// Sent on a `watch` channel so the tray always sees the latest value without
/// replaying intermediate states when it lags behind.
#[derive(Clone, Debug)]
pub struct DaemonStatus {
    pub state: ConnectionState,
    pub peer_count: usize,
    pub peers: Vec<PeerSummary>,
    /// The embedded relay URL, if the relay is running.
    pub relay_url: Option<String>,
    /// The name of the vault mesh — `None` until `Daemon::new()` succeeds.
    pub mesh_name: Option<String>,
    /// The hostname of this device — `None` until `Daemon::new()` succeeds.
    pub device_name: Option<String>,
}

impl DaemonStatus {
    pub fn initial() -> Self {
        DaemonStatus {
            state: ConnectionState::Idle,
            peer_count: 0,
            peers: vec![],
            relay_url: None,
            mesh_name: None,
            device_name: None,
        }
    }
}

/// Pairing UI events emitted by the daemon to the desktop tray.
///
/// Sent on a `broadcast` channel because each event is meaningful — a missed
/// `InboundRequest` means a missed UI prompt.
#[derive(Clone, Debug)]
pub enum PairingUiEvent {
    /// A remote device has sent a pairing request. Show the responder window.
    InboundRequest {
        device_name: String,
        /// 6-digit numeric code the remote device must enter.
        code: String,
        /// Unix timestamp (milliseconds) when the pairing session expires.
        expires_at_ms: u64,
    },
    /// The remote device submitted the correct code — pairing completed.
    InboundCompleted { device_name: String },
    /// The pairing session failed (bad code, timeout, or explicit rejection).
    InboundFailed { reason: String },
}

/// Commands sent from the desktop tray to the daemon event loop.
pub enum DaemonCommand {
    /// Begin mDNS discovery and stream discovered meshes back via `reply`.
    ///
    /// The daemon runs mDNS for up to 10 seconds, sending each `DiscoveredMesh`
    /// as it appears. The channel is closed when the scan completes.
    StartDiscovery {
        reply: mpsc::UnboundedSender<p2p_core::discovery::DiscoveredMesh>,
    },
    /// Connect to the selected mesh's peer and initiate the pairing handshake,
    /// causing the responder to generate + display the 6-digit code. Parks the
    /// QUIC connection open awaiting a later `SubmitCode`. On successful connect
    /// the reply carries the responder's device name — the UI's cue to show the
    /// code entry step.
    RequestPairing {
        vault_id: String,
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Deliver the typed 6-digit code into the parked pairing request.
    ///
    /// Must be called after `RequestPairing` succeeds. Unblocks the parked QUIC
    /// connection and drives the HMAC exchange. On success the reply carries the
    /// responder's device name; on failure it carries a human-readable error.
    SubmitCode {
        vault_id: String,
        code: String,
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Cancel any active initiator pairing session. Idempotent.
    CancelInitiate { reply: oneshot::Sender<()> },
    /// Reject the currently-active inbound pairing request. Idempotent.
    RejectInbound { reply: oneshot::Sender<()> },
}

/// Handle returned to the desktop app after `run_with_shutdown_controlled` completes
/// startup successfully.
///
/// The handle provides:
/// - `status_rx`: subscribe to live daemon status snapshots (watch channel — only
///   the latest value matters).
/// - `pairing_rx`: subscribe to pairing UI events (broadcast channel — every event
///   must be delivered).
/// - `command_tx`: send commands to the daemon event loop.
pub struct DaemonControl {
    /// Subscribe to daemon status updates. `changed()` fires on every state change.
    pub status_rx: watch::Receiver<DaemonStatus>,
    /// Subscribe to pairing UI events.
    pub pairing_rx: broadcast::Receiver<PairingUiEvent>,
    /// Send commands to the daemon event loop.
    pub command_tx: mpsc::UnboundedSender<DaemonCommand>,
}
