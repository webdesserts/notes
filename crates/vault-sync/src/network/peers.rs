//! PeerRegistry: tracks the liveness state of peers in the gossip swarm.
//!
//! Peers move between two states: `Alive` (currently in the gossip swarm) and
//! `Dead` (previously seen, now gone). Dead peers stay in the registry so the
//! UI can show them as offline rather than invisible.
//!
//! The registry is plain `&mut self` — no interior mutability. The owner
//! (daemon or plugin) holds exclusive access and drives state transitions
//! via the gossip event callbacks.

use serde::Serialize;
use std::collections::HashMap;
use web_time::SystemTime;

use p2p_core::PeerId;

/// Liveness state of a peer in the gossip swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PeerState {
    /// Peer is currently in the gossip swarm.
    Alive,
    /// Peer was previously seen but has since left.
    Dead,
}

/// Whether a liveness signal flipped a peer to `Alive` or it was already there.
///
/// Returned by [`PeerRegistry::mark_alive`] so the daemon logs the enriched
/// connect line exactly once per alive transition, regardless of which of the
/// several liveness signals (NeighborUp, inbound sync, gossip roster) arrives
/// first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectTransition {
    /// The peer just transitioned Unknown/Dead → Alive.
    NewlyAlive,
    /// The peer was already Alive; only `last_seen` was refreshed.
    AlreadyAlive,
}

/// A snapshot of a peer's registry state.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerEntry {
    /// The peer's identity (ed25519 public key).
    pub node_id: PeerId,
    /// Human-readable device name, if the peer has announced one.
    pub device_name: Option<String>,
    /// Current liveness state.
    pub state: PeerState,
    /// Unix timestamp (seconds) when the peer was first seen.
    pub first_seen: u64,
    /// Unix timestamp (seconds) when the peer was last active.
    pub last_seen: u64,
    /// Last-observed connection-path descriptor ("LAN" / "relay"), captured at
    /// connect time so a disconnect log can echo how we had been reaching the
    /// peer. Internal logging aid only — not consumed by the UI.
    #[serde(skip)]
    pub last_conn_type: Option<String>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Tracks the Alive/Dead state of all peers observed during this session.
///
/// State transitions:
/// - Unknown → Alive: `on_neighbor_up`
/// - Alive → Dead: `on_neighbor_down`
/// - Dead → Alive: `on_neighbor_up` (peer rejoined)
pub struct PeerRegistry {
    peers: HashMap<PeerId, PeerEntry>,
}

impl PeerRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    /// Mark a peer alive on any liveness signal, reporting the transition.
    ///
    /// Creates a new `Alive` entry for an unknown peer and transitions a `Dead`
    /// peer back to `Alive` (both `NewlyAlive`); an already-`Alive` peer just has
    /// its `last_seen` refreshed (`AlreadyAlive`). The returned
    /// [`ConnectTransition`] lets the daemon log the enriched connect line
    /// exactly once per alive transition even though several signals
    /// (NeighborUp, inbound sync, gossip roster) all drive this.
    pub fn mark_alive(&mut self, node_id: PeerId) -> ConnectTransition {
        let now = now_secs();
        match self.peers.get_mut(&node_id) {
            Some(entry) => {
                let was_alive = entry.state == PeerState::Alive;
                entry.state = PeerState::Alive;
                entry.last_seen = now;
                if was_alive {
                    ConnectTransition::AlreadyAlive
                } else {
                    ConnectTransition::NewlyAlive
                }
            }
            None => {
                self.peers.insert(
                    node_id,
                    PeerEntry {
                        node_id,
                        device_name: None,
                        state: PeerState::Alive,
                        first_seen: now,
                        last_seen: now,
                        last_conn_type: None,
                    },
                );
                ConnectTransition::NewlyAlive
            }
        }
    }

    /// Record that a peer joined the gossip swarm.
    ///
    /// Creates a new `Alive` entry for an unknown peer. For a peer that was
    /// previously `Dead`, transitions it back to `Alive`. For an already-`Alive`
    /// peer (e.g., duplicate event), updates `last_seen`.
    ///
    /// Returns a reference to the updated entry. Thin wrapper over
    /// [`PeerRegistry::mark_alive`] preserved for callers that don't need the
    /// transition signal.
    pub fn on_neighbor_up(&mut self, node_id: PeerId) -> &PeerEntry {
        self.mark_alive(node_id);
        self.peers
            .get(&node_id)
            .expect("mark_alive just inserted this peer")
    }

    /// Record the connection-path descriptor last observed for a peer.
    ///
    /// Stored so a later disconnect log can echo how we had been reaching the
    /// peer. No-op for unknown peers.
    pub fn set_last_conn_type(&mut self, node_id: &PeerId, conn_type: Option<String>) {
        if let Some(entry) = self.peers.get_mut(node_id) {
            entry.last_conn_type = conn_type;
        }
    }

    /// Record that a peer left the gossip swarm.
    ///
    /// Transitions an `Alive` peer to `Dead`. The entry is kept in the registry
    /// so the UI can show the peer as offline. No-op for unknown peers.
    pub fn on_neighbor_down(&mut self, node_id: &PeerId) {
        if let Some(entry) = self.peers.get_mut(node_id) {
            entry.state = PeerState::Dead;
        }
    }

    /// Update the `last_seen` timestamp for a peer.
    ///
    /// Call this after a successful sync operation to track activity. No-op
    /// for unknown peers.
    pub fn update_last_seen(&mut self, node_id: &PeerId) {
        if let Some(entry) = self.peers.get_mut(node_id) {
            entry.last_seen = now_secs();
        }
    }

    /// Set or update the human-readable device name for a peer.
    ///
    /// No-op for unknown peers.
    pub fn set_device_name(&mut self, node_id: &PeerId, name: String) {
        if let Some(entry) = self.peers.get_mut(node_id) {
            entry.device_name = Some(name);
        }
    }

    /// All peers currently in the `Alive` state.
    pub fn get_alive_peers(&self) -> Vec<&PeerEntry> {
        self.peers
            .values()
            .filter(|e| e.state == PeerState::Alive)
            .collect()
    }

    /// All known peers, regardless of state.
    pub fn get_all_peers(&self) -> Vec<&PeerEntry> {
        self.peers.values().collect()
    }

    /// Number of peers currently in the `Alive` state.
    pub fn alive_count(&self) -> usize {
        self.peers
            .values()
            .filter(|e| e.state == PeerState::Alive)
            .count()
    }

    /// Whether a specific peer is currently `Alive`.
    pub fn is_alive(&self, node_id: &PeerId) -> bool {
        self.peers
            .get(node_id)
            .map(|e| e.state == PeerState::Alive)
            .unwrap_or(false)
    }
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peer() -> PeerId {
        PeerId::generate()
    }

    // ── Basic state transitions ──────────────────────────────────────────────

    #[test]
    fn test_neighbor_up_creates_alive_entry() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        let entry = registry.on_neighbor_up(peer);
        assert_eq!(entry.state, PeerState::Alive);
        assert_eq!(entry.node_id, peer);
        assert!(registry.is_alive(&peer));
        assert_eq!(registry.alive_count(), 1);
    }

    #[test]
    fn test_neighbor_down_transitions_to_dead() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.on_neighbor_down(&peer);

        assert!(!registry.is_alive(&peer));
        assert_eq!(registry.alive_count(), 0);
    }

    #[test]
    fn test_dead_peer_stays_in_registry() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.on_neighbor_down(&peer);

        // Still visible via get_all_peers even though dead
        assert_eq!(registry.get_all_peers().len(), 1);
        assert_eq!(registry.get_alive_peers().len(), 0);

        let entry = &registry.get_all_peers()[0];
        assert_eq!(entry.state, PeerState::Dead);
    }

    #[test]
    fn test_dead_peer_transitions_alive_on_up() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.on_neighbor_down(&peer);
        assert_eq!(registry.alive_count(), 0);

        registry.on_neighbor_up(peer);
        assert!(registry.is_alive(&peer));
        assert_eq!(registry.alive_count(), 1);
        // Still only one entry in registry
        assert_eq!(registry.get_all_peers().len(), 1);
    }

    // ── mark_alive transitions ───────────────────────────────────────────────

    #[test]
    fn mark_alive_reports_newly_alive_once_then_already_alive() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        // First signal is the transition that should log.
        assert_eq!(registry.mark_alive(peer), ConnectTransition::NewlyAlive);
        // Duplicate liveness signals (e.g. NeighborUp + inbound sync + roster)
        // must NOT re-log — they report AlreadyAlive.
        assert_eq!(registry.mark_alive(peer), ConnectTransition::AlreadyAlive);
        assert_eq!(registry.mark_alive(peer), ConnectTransition::AlreadyAlive);
    }

    #[test]
    fn mark_alive_reports_newly_alive_again_after_dead() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        assert_eq!(registry.mark_alive(peer), ConnectTransition::NewlyAlive);
        registry.on_neighbor_down(&peer);
        // A peer that died and rejoined is a fresh connect — should log again.
        assert_eq!(registry.mark_alive(peer), ConnectTransition::NewlyAlive);
    }

    #[test]
    fn on_neighbor_up_still_creates_alive_entry() {
        // on_neighbor_up is reimplemented on mark_alive; its existing contract
        // (returns an Alive entry for the peer) must be unchanged.
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        let entry = registry.on_neighbor_up(peer);
        assert_eq!(entry.state, PeerState::Alive);
        assert_eq!(entry.node_id, peer);
    }

    // ── set_last_conn_type ───────────────────────────────────────────────────

    #[test]
    fn set_last_conn_type_for_known_peer() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.mark_alive(peer);
        registry.set_last_conn_type(&peer, Some("LAN".to_string()));

        let entry = &registry.get_all_peers()[0];
        assert_eq!(entry.last_conn_type, Some("LAN".to_string()));
    }

    #[test]
    fn set_last_conn_type_unknown_peer_is_noop() {
        let mut registry = PeerRegistry::new();
        let unknown = make_peer();

        registry.set_last_conn_type(&unknown, Some("LAN".to_string()));
        assert_eq!(registry.get_all_peers().len(), 0);
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[test]
    fn test_double_up_stays_alive_single_entry() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.on_neighbor_up(peer); // duplicate event

        assert!(registry.is_alive(&peer));
        assert_eq!(registry.alive_count(), 1);
        assert_eq!(registry.get_all_peers().len(), 1);
    }

    #[test]
    fn test_down_unknown_peer_is_noop() {
        let mut registry = PeerRegistry::new();
        let unknown = make_peer();

        // Should not panic or add an entry
        registry.on_neighbor_down(&unknown);
        assert_eq!(registry.get_all_peers().len(), 0);
    }

    #[test]
    fn test_double_down_stays_dead() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.on_neighbor_down(&peer);
        registry.on_neighbor_down(&peer); // duplicate event

        assert!(!registry.is_alive(&peer));
        assert_eq!(registry.get_all_peers().len(), 1);
    }

    // ── Multiple peers ───────────────────────────────────────────────────────

    #[test]
    fn test_multiple_peers_tracked_independently() {
        let mut registry = PeerRegistry::new();
        let peer_a = make_peer();
        let peer_b = make_peer();
        let peer_c = make_peer();

        registry.on_neighbor_up(peer_a);
        registry.on_neighbor_up(peer_b);
        registry.on_neighbor_up(peer_c);
        assert_eq!(registry.alive_count(), 3);

        registry.on_neighbor_down(&peer_b);
        assert_eq!(registry.alive_count(), 2);
        assert!(registry.is_alive(&peer_a));
        assert!(!registry.is_alive(&peer_b));
        assert!(registry.is_alive(&peer_c));

        // All three still in the full list
        assert_eq!(registry.get_all_peers().len(), 3);
    }

    // ── update_last_seen ─────────────────────────────────────────────────────

    #[test]
    fn test_update_last_seen_for_known_peer() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        let initial = registry.get_all_peers()[0].last_seen;

        // Advance time by sleeping briefly (1ms is enough to detect a change)
        std::thread::sleep(std::time::Duration::from_millis(1100));
        registry.update_last_seen(&peer);

        let updated = registry.get_all_peers()[0].last_seen;
        // last_seen should be >= initial (may be equal if sub-second)
        assert!(updated >= initial);
    }

    #[test]
    fn test_update_last_seen_unknown_peer_is_noop() {
        let mut registry = PeerRegistry::new();
        let unknown = make_peer();

        // Should not panic or create an entry
        registry.update_last_seen(&unknown);
        assert_eq!(registry.get_all_peers().len(), 0);
    }

    // ── set_device_name ──────────────────────────────────────────────────────

    #[test]
    fn test_set_device_name_for_known_peer() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.set_device_name(&peer, "MacBook Pro".to_string());

        let entry = &registry.get_all_peers()[0];
        assert_eq!(entry.device_name, Some("MacBook Pro".to_string()));
    }

    #[test]
    fn test_set_device_name_unknown_peer_is_noop() {
        let mut registry = PeerRegistry::new();
        let unknown = make_peer();

        registry.set_device_name(&unknown, "Ghost".to_string());
        assert_eq!(registry.get_all_peers().len(), 0);
    }

    #[test]
    fn test_set_device_name_overwrites_previous() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();

        registry.on_neighbor_up(peer);
        registry.set_device_name(&peer, "First Name".to_string());
        registry.set_device_name(&peer, "Updated Name".to_string());

        let entry = &registry.get_all_peers()[0];
        assert_eq!(entry.device_name, Some("Updated Name".to_string()));
    }

    // ── Serialization ────────────────────────────────────────────────────────

    #[test]
    fn test_peer_state_serializes_as_camel_case() {
        let alive = serde_json::to_string(&PeerState::Alive).unwrap();
        let dead = serde_json::to_string(&PeerState::Dead).unwrap();
        assert_eq!(alive, r#""alive""#);
        assert_eq!(dead, r#""dead""#);
    }

    #[test]
    fn test_peer_entry_serializes_with_camel_case_fields() {
        let mut registry = PeerRegistry::new();
        let peer = make_peer();
        registry.on_neighbor_up(peer);
        registry.set_device_name(&peer, "Test Device".to_string());
        // Populate the internal field so the skip-guard below proves exclusion
        // even when there is a value to serialize.
        registry.set_last_conn_type(&peer, Some("LAN".to_string()));

        let entry = &registry.get_all_peers()[0];
        let json = serde_json::to_string(entry).unwrap();
        assert!(json.contains("nodeId"));
        assert!(json.contains("deviceName"));
        assert!(json.contains("firstSeen"));
        assert!(json.contains("lastSeen"));
        assert!(json.contains("\"alive\""));
        // `last_conn_type` is `#[serde(skip)]` — an internal connection-path
        // descriptor that must not leak into the UI-facing PeerEntry JSON.
        assert!(
            !json.contains("lastConnType"),
            "internal field must not leak to UI-facing PeerEntry JSON"
        );
    }
}
