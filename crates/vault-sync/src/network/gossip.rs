//! Vault-scoped gossip codec over p2p-core's generic byte-transport.
//!
//! Each vault subscribes to its own gossip topic (derived from its VaultId). The
//! transport itself — membership (`NeighborUp`/`NeighborDown`) and opaque
//! broadcast/receive — lives in `p2p_core::GossipSubscription`. This module is the
//! thin VAULT codec on top:
//!
//! - Outbound: serialize a [`GossipMessage`] with bincode and hand the bytes to
//!   the transport (`GossipHandle::broadcast`).
//! - Inbound: a decode pump consumes the transport's generic events, deserializes
//!   `Data` payloads back into [`GossipMessage`], and surfaces decoded vault-level
//!   [`GossipEvent`]s to the daemon.
//!
//! All gossip payloads are wrapped in [`GossipMessage`] so the wire format can
//! carry multiple message types without breaking framing. The bincode encoding of
//! that envelope is the gossip WIRE format — a mixed-version fleet depends on it
//! byte-for-byte, so it is pinned by `golden_change_notification_bytes_are_stable`.
//!
//! Gossip messages must be small (~1KB). Large sync data goes through QUIC streams.

use anyhow::Result;
use bytes::Bytes;
use n0_future::task::{self, AbortOnDropHandle};
use p2p_core::{GossipHandle, GossipSubscription, PeerId, Topic};
use rand::Rng;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use p2p_core::allowlist::AllowedPeer;

/// Maximum size of a gossip message payload in bytes.
///
/// PlumTree's eager push has practical limits around 1KB. Larger messages
/// should be sent through QUIC bi-streams instead.
pub const MAX_GOSSIP_MESSAGE_SIZE: usize = 1024;

/// A notification broadcasted via gossip when a file changes.
///
/// Peers receiving this notification open a QUIC stream to pull updates.
/// Kept small (under 1KB) to stay within gossip size limits.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeNotification {
    /// Path of the modified file
    pub path: String,
    /// Opaque discriminator that makes successive notifications for the same path
    /// serialize to distinct bytes, so iroh-gossip's content-hash MessageId dedup
    /// cannot suppress a same-path follow-up within its seen-cache window. The
    /// receiver MUST treat this as opaque — it carries no ordering or identity
    /// meaning, it exists solely to defeat content-based dedup (Issue 2).
    pub nonce: u64,
}

/// Envelope for all gossip messages.
///
/// Wrapping messages in an enum lets us add new gossip message types without
/// breaking the framing layer — deserialization errors from unknown or corrupt
/// messages are logged and skipped rather than causing a protocol failure.
///
/// NOTE: This is a breaking wire format change relative to pre-v0.5.x versions,
/// which serialized `ChangeNotification` directly. All peers must be upgraded
/// together.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum GossipMessage {
    /// A file changed — receiving peers should pull the update via QUIC.
    ChangeNotification(ChangeNotification),
    /// A peer was added to the allowlist — receiving peers should add them locally.
    AllowlistUpdate(AllowedPeer),
    /// The sender's full membership roster (live peers + tombstones) — receiving
    /// peers merge it by union with tombstone-precedence to converge on one
    /// allowlist. Sent on reconnect/reconcile so trust propagates even to peers
    /// that were offline at a pairing's single-shot `AllowlistUpdate` broadcast.
    AllowlistRoster(Vec<AllowedPeer>),
}

/// Events produced by the gossip subscription loop.
#[derive(Debug)]
pub enum GossipEvent {
    /// A peer joined the gossip swarm for this vault.
    NeighborUp(PeerId),
    /// A peer left the gossip swarm for this vault.
    NeighborDown(PeerId),
    /// A change notification broadcast received from a peer.
    ChangeReceived {
        from: PeerId,
        notification: ChangeNotification,
    },
    /// A peer was added to the allowlist by a trusted mesh member.
    AllowlistUpdate { from: PeerId, peer: AllowedPeer },
    /// A full membership roster received from a trusted mesh member, to be
    /// merged into the local allowlist by union with tombstone-precedence.
    AllowlistRoster {
        from: PeerId,
        peers: Vec<AllowedPeer>,
    },
}

/// Handle for a vault-scoped gossip subscription.
///
/// A thin codec over [`p2p_core::GossipSubscription`]: broadcasts serialize a
/// [`GossipMessage`] and hand the bytes to the transport; a decode pump turns the
/// transport's opaque `Data` events back into decoded [`GossipEvent`]s for the
/// daemon. The transport's membership events pass straight through.
pub struct VaultGossip {
    /// Cloneable transport handle for broadcast + rejoin.
    handle: GossipHandle,
    /// The gossip topic (its `as_bytes()` feeds the pairing approval wire field).
    topic: Topic,
    /// Decoded vault-level events, fed by the decode pump.
    pub event_rx: mpsc::UnboundedReceiver<GossipEvent>,
    /// The decode pump. Abort-on-drop and owner of the MOVED `GossipSubscription`,
    /// so dropping this `VaultGossip` aborts the decode task, which drops the
    /// subscription, which aborts the transport pump and drops the gossip sender —
    /// leaving the topic. A bare `JoinHandle` would DETACH (not abort), leaking
    /// both tasks and never leaving the topic, breaking the `adopt_and_rejoin`
    /// old-topic-leave invariant. See the constructor's teardown-chain note.
    _decode_task: AbortOnDropHandle<()>,
    /// Random session base for change-notification nonces, drawn once per
    /// `VaultGossip`. XORed with `notif_seq` to discriminate successive
    /// same-path notifications (see `ChangeNotification.nonce`). A daemon
    /// restart builds a new `VaultGossip` with a fresh salt, so the counter
    /// resetting to 0 cannot collide with pre-restart nonces.
    notif_salt: u64,
    /// Monotonic per-broadcast counter, 0 at construction. Makes within-session
    /// nonce uniqueness deterministic (XOR with a fixed salt is a bijection).
    notif_seq: u64,
}

impl VaultGossip {
    /// Wrap a p2p-core gossip subscription with the vault `GossipMessage` codec.
    ///
    /// Spawns the decode pump: it consumes the subscription's generic events,
    /// deserializes `Data` payloads into [`GossipMessage`], and forwards decoded
    /// [`GossipEvent`]s on `event_rx`. Membership events pass through; `Lagged` and
    /// decode errors are logged and skipped (so a `Lagged` never reaches the
    /// daemon, preserving today's behavior).
    ///
    /// 🔴 Teardown chain (load-bearing — both tasks are `AbortOnDropHandle`).
    /// The subscription is MOVED into the decode task. Dropping a bare
    /// `tokio`/`n0_future` `JoinHandle` DETACHES rather than aborts, so a bare
    /// handle would leak both this decode pump AND the transport pump, and the
    /// gossip topic would never be left (iroh-gossip leaves only when both split
    /// halves drop). With abort-on-drop the chain is:
    ///
    /// `VaultGossip` drop → `_decode_task` abort → the moved `GossipSubscription`
    /// drops → its transport-pump `AbortOnDropHandle` aborts + its `GossipSender`
    /// drops → both split halves gone → iroh-gossip leaves the topic.
    ///
    /// This is what makes the VaultId-swap (`initiator::adopt_and_rejoin`, which
    /// drops the old `VaultGossip`) leave the abandoned topic, and makes daemon
    /// shutdown leak no tasks/topics.
    pub fn new(subscription: GossipSubscription) -> Self {
        let topic = subscription.topic();
        let handle = subscription.handle();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // The decode pump owns the MOVED subscription (see the teardown chain).
        let _decode_task = AbortOnDropHandle::new(task::spawn(async move {
            let mut sub = subscription;
            while let Some(ev) = sub.recv().await {
                let gossip_event = match ev {
                    p2p_core::GossipEvent::NeighborUp(peer) => GossipEvent::NeighborUp(peer),
                    p2p_core::GossipEvent::NeighborDown(peer) => GossipEvent::NeighborDown(peer),
                    p2p_core::GossipEvent::Data { from, bytes } => {
                        match bincode::deserialize::<GossipMessage>(&bytes) {
                            Ok(GossipMessage::ChangeNotification(notification)) => {
                                debug!(path = %notification.path, %from, "Change notification received");
                                GossipEvent::ChangeReceived { from, notification }
                            }
                            Ok(GossipMessage::AllowlistUpdate(peer)) => {
                                debug!(peer_id = %peer.node_id, %from, "Allowlist update received");
                                GossipEvent::AllowlistUpdate { from, peer }
                            }
                            Ok(GossipMessage::AllowlistRoster(peers)) => {
                                debug!(count = peers.len(), %from, "Allowlist roster received");
                                GossipEvent::AllowlistRoster { from, peers }
                            }
                            Err(e) => {
                                warn!("Failed to deserialize gossip message from {from}: {e}");
                                continue;
                            }
                        }
                    }
                    p2p_core::GossipEvent::Lagged => {
                        // Swallowed at the daemon boundary, matching the prior
                        // single-pump behavior (Lagged was never forwarded).
                        debug!("Gossip lagged (message buffer overflow)");
                        continue;
                    }
                };

                if event_tx.send(gossip_event).is_err() {
                    break; // Receiver dropped
                }
            }
        }));

        Self {
            handle,
            topic,
            event_rx,
            _decode_task,
            notif_salt: rand::rng().random(),
            notif_seq: 0,
        }
    }

    /// The gossip topic. Its `as_bytes()` is read into the pairing approval's
    /// `vault_topic` wire field, so the 32 bytes are byte-identical.
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Broadcast a change notification to all vault peers.
    ///
    /// The notification is small (path only). Peers who receive it will
    /// open a QUIC stream to pull the actual document updates.
    pub async fn broadcast_change(&mut self, path: &str) -> Result<()> {
        // The nonce makes successive same-path notifications serialize to distinct
        // bytes so iroh-gossip's content-hash dedup can't suppress a follow-up (a
        // create-notif followed by a same-path delete-notif within the seen window).
        // `wrapping_add` so a pathological 2^64-broadcast session can't panic in debug.
        let nonce = self.notif_salt ^ self.notif_seq;
        self.notif_seq = self.notif_seq.wrapping_add(1);
        let msg = GossipMessage::ChangeNotification(ChangeNotification {
            path: path.to_string(),
            nonce,
        });
        self.broadcast_message(&msg).await
    }

    /// Broadcast an allowlist update to all vault peers.
    ///
    /// Called after a successful pairing to propagate the new peer to the rest
    /// of the mesh. Peers who receive this add the new peer to their local
    /// allowlist if the sender is trusted.
    pub async fn broadcast_allowlist_update(&mut self, peer: &AllowedPeer) -> Result<()> {
        let msg = GossipMessage::AllowlistUpdate(peer.clone());
        self.broadcast_message(&msg).await
    }

    /// Broadcast the full membership roster to all vault peers.
    ///
    /// Used by the reconnect/reconcile convergence path so a peer that missed a
    /// pairing's single-shot `AllowlistUpdate` still learns the whole roster.
    /// Goes through `broadcast_message`, so it inherits the 1KB size cap — the
    /// caller is responsible for the pre-check and per-peer-delta fallback when a
    /// large roster would exceed it (see the daemon's `push_allowlist_roster`).
    pub async fn broadcast_allowlist_roster(&mut self, peers: &[AllowedPeer]) -> Result<()> {
        let msg = GossipMessage::AllowlistRoster(peers.to_vec());
        self.broadcast_message(&msg).await
    }

    /// Re-bootstrap gossip toward a set of known peers without re-subscribing.
    ///
    /// Tells HyParView to (re-)dial those peers on the already-subscribed topic.
    /// Used by the reconnect supervisor to recover from a partition: after a
    /// `NeighborDown`, neither side dials the other again on its own, so the
    /// supervisor re-seeds relay hints and calls this to re-establish the swarm.
    /// Re-subscribing would churn the swarm; the targeted re-join is cheaper.
    pub async fn rejoin_peers(&self, peers: Vec<PeerId>) -> Result<()> {
        self.handle.rejoin_peers(peers).await
    }

    /// A cloneable handle that can re-join peers from a spawned task.
    ///
    /// `VaultGossip` itself is not `Clone` (it owns the event receiver and the
    /// decode task), but the reconnect supervisor's bootstrap-connect runs in a
    /// spawned task that must re-queue a Join just before adopting the connection.
    /// This hands out the underlying p2p-core gossip handle for that purpose.
    pub fn rejoin_handle(&self) -> GossipHandle {
        self.handle.clone()
    }

    /// Serialize and broadcast a [`GossipMessage`] envelope.
    ///
    /// This is the gossip WIRE-PRODUCTION site: `bincode::serialize` produces the
    /// exact bytes that travel (pinned by the golden-bytes test). The transport
    /// (`GossipHandle::broadcast`) is a pure pass-through — no envelope is added.
    async fn broadcast_message(&self, msg: &GossipMessage) -> Result<()> {
        let bytes: Bytes = bincode::serialize(msg)?.into();
        if bytes.len() > MAX_GOSSIP_MESSAGE_SIZE {
            return Err(anyhow::anyhow!(
                "Gossip message too large: {} bytes (max {})",
                bytes.len(),
                MAX_GOSSIP_MESSAGE_SIZE
            ));
        }
        self.handle.broadcast(bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🔴 Gossip WIRE pin (the fleet anchor). The bincode encoding of a
    /// `GossipMessage` IS the gossip wire format; a mixed-version mesh
    /// (VaultId `cc7727ad30b7b1c7`) partitions silently if it drifts — peers
    /// connect at the gossip layer but every broadcast is undecodable on the other
    /// side. This literal was captured from REAL serialization at the deployed tip
    /// (`c0ced264`), so it is the external anchor against a bincode-config change
    /// (fixint→varint, endianness, a serde attr) — a change the two-node
    /// pass-through test alone would NOT catch (it runs the same bincode on both
    /// ends, so it round-trips green even as it diverges from the fleet's bytes).
    ///
    /// `ChangeNotification` is the highest-traffic path; `nonce: 0` keeps the bytes
    /// deterministic. Layout: variant tag (u32 LE = 0) + string len (u64 LE = 14) +
    /// the 14 UTF-8 bytes of "notes/hello.md" + nonce (u64 LE = 0) = 34 bytes.
    #[test]
    fn golden_change_notification_bytes_are_stable() {
        let msg = GossipMessage::ChangeNotification(ChangeNotification {
            path: "notes/hello.md".into(),
            nonce: 0,
        });
        let bytes = bincode::serialize(&msg).unwrap();
        assert_eq!(
            bytes,
            vec![
                0, 0, 0, 0, // ChangeNotification variant tag (u32 LE)
                14, 0, 0, 0, 0, 0, 0, 0, // path length 14 (u64 LE)
                110, 111, 116, 101, 115, 47, 104, 101, 108, 108, 111, 46, 109,
                100, // "notes/hello.md"
                0, 0, 0, 0, 0, 0, 0, 0, // nonce 0 (u64 LE)
            ],
            "gossip wire encoding drifted from the deployed fleet — a mixed-version \
             mesh would silently partition"
        );
    }
}
