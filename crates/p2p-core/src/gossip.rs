//! Generic gossip byte-transport over iroh-gossip.
//!
//! A [`GossipSubscription`] is the app-agnostic transport for one gossip topic:
//! it broadcasts opaque payloads and yields a stream of generic membership/data
//! events ([`GossipEvent`]). p2p-core does NOT interpret the payload bytes — the
//! app owns its codec. vault-sync layers its `GossipMessage` bincode codec on top
//! via a thin wrapper (`VaultGossip`).
//!
//! ## The pump
//!
//! [`P2pNode::subscribe`](crate::P2pNode::subscribe) splits the iroh-gossip topic
//! into a sender + receiver, then spawns a task (the "pump") that translates
//! iroh-gossip's `Event`s into [`GossipEvent`]s on an mpsc channel. The pump runs
//! for the life of the subscription and is held by an [`AbortOnDropHandle`] so
//! dropping the subscription aborts the pump (which drops the receiver half) and,
//! together with the sender dropping, leaves the gossip topic. See
//! [`GossipSubscription`] for the lifecycle contract.

use anyhow::Result;
use bytes::Bytes;
use iroh_gossip::TopicId;
use iroh_gossip::api::GossipSender;
use n0_future::task::AbortOnDropHandle;
use tokio::sync::mpsc;

use crate::iroh_adapt::try_peer_to_endpoint;
use crate::node::{topic_from_u64, u64_from_topic};
use crate::peer_id::PeerId;

/// A gossip topic identifier.
///
/// Opaque value type wrapping iroh-gossip's `TopicId`. Derived from a `u64` seed
/// (vault-sync derives the seed from a `VaultId`, keeping the `VaultId` type out of
/// p2p-core). [`as_bytes`](Topic::as_bytes) exposes the raw 32 bytes — a wire
/// field for pairing (`PairingApproval.vault_topic`), so the bytes must stay
/// byte-identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Topic(TopicId);

impl Topic {
    /// Derive a deterministic topic from a `u64` seed.
    pub fn from_u64(seed: u64) -> Self {
        Topic(topic_from_u64(seed))
    }

    /// Recover the `u64` seed encoded in this topic's bytes.
    pub fn to_u64(&self) -> u64 {
        u64_from_topic(self.0.as_bytes())
    }

    /// The raw 32-byte topic id. This is a wire field (it feeds the pairing
    /// approval's `vault_topic`), so the bytes are byte-identical to the
    /// underlying `TopicId`.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// The underlying iroh-gossip `TopicId`, for the crate-internal subscribe call.
    pub(crate) fn as_iroh(&self) -> TopicId {
        self.0
    }
}

/// A generic event from a gossip subscription.
///
/// Membership transitions plus opaque data delivery. The `bytes` in
/// [`Data`](GossipEvent::Data) are the exact payload a peer broadcast — p2p-core
/// does not decode them.
#[derive(Debug)]
pub enum GossipEvent {
    /// A peer joined the gossip swarm for this topic.
    NeighborUp(PeerId),
    /// A peer left the gossip swarm for this topic.
    NeighborDown(PeerId),
    /// An opaque payload was received from a peer.
    Data {
        /// The peer that delivered the payload.
        from: PeerId,
        /// The opaque payload bytes, exactly as broadcast.
        bytes: Bytes,
    },
    /// The receive buffer overflowed and messages were dropped.
    Lagged,
}

/// A subscription to one gossip topic — the generic byte-transport.
///
/// Owns the iroh-gossip sender, the topic, an event receiver fed by the pump
/// task, and the pump's [`AbortOnDropHandle`]. Broadcast opaque payloads via
/// [`broadcast`](GossipSubscription::broadcast); read events via
/// [`recv`](GossipSubscription::recv).
///
/// ## Lifecycle (load-bearing)
///
/// The `_task` field is an [`AbortOnDropHandle`], NOT a bare `JoinHandle`:
/// dropping a bare `tokio`/`n0_future` `JoinHandle` DETACHES the task rather than
/// aborting it. iroh-gossip leaves a topic only when BOTH split halves (sender +
/// receiver) drop. The pump holds the receiver half, so a detached pump would
/// keep the receiver alive and the topic would never be left. Aborting the pump
/// on drop (`AbortOnDropHandle`) drops the receiver; dropping the subscription
/// also drops the `sender` — both halves gone, so iroh-gossip leaves the topic.
/// This is what makes "drop the subscription ⇒ leave the topic" hold, which the
/// daemon's VaultId-swap (`adopt_and_rejoin`) and clean shutdown rely on.
pub struct GossipSubscription {
    /// Channel to broadcast opaque payloads to the swarm.
    sender: GossipSender,
    /// The subscribed topic.
    topic: Topic,
    /// Generic events from the swarm, pumped by `_task`.
    event_rx: mpsc::UnboundedReceiver<GossipEvent>,
    /// The pump task. Abort-on-drop so the subscription dropping leaves the topic
    /// (see the struct-level lifecycle note).
    _task: AbortOnDropHandle<()>,
}

impl GossipSubscription {
    /// Build a subscription from a freshly-split iroh-gossip topic handle and the
    /// pump it feeds. Called by [`P2pNode::subscribe`](crate::P2pNode::subscribe).
    pub(crate) fn new(
        sender: GossipSender,
        topic: Topic,
        event_rx: mpsc::UnboundedReceiver<GossipEvent>,
        task: AbortOnDropHandle<()>,
    ) -> Self {
        Self {
            sender,
            topic,
            event_rx,
            _task: task,
        }
    }

    /// The subscribed topic.
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Broadcast an opaque payload to all topic peers.
    ///
    /// Pure pass-through to iroh-gossip's `GossipSender::broadcast` — p2p-core
    /// adds no envelope, length-prefix, or re-framing. The `payload` bytes the
    /// caller passes are exactly what travel on the wire. (Size policy, if any, is
    /// the app's; p2p-core enforces none.)
    pub async fn broadcast(&self, payload: Bytes) -> Result<()> {
        self.sender
            .broadcast(payload)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to broadcast gossip payload: {e}"))
    }

    /// Re-bootstrap gossip toward a set of known peers without re-subscribing.
    ///
    /// Tells HyParView to (re-)dial those peers on the already-subscribed topic.
    /// Used by the reconnect supervisor to recover from a partition. A
    /// legacy/non-curve-point id is dropped (it could never be a reachable target).
    pub async fn rejoin_peers(&self, peers: Vec<PeerId>) -> Result<()> {
        self.sender
            .join_peers(peer_ids_to_endpoints(peers))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to re-join gossip peers: {e}"))
    }

    /// A cloneable handle for broadcast/rejoin from a detached task.
    ///
    /// `GossipSubscription` is not `Clone` (it owns the event receiver and the
    /// pump task), but a spawned task — e.g. the reconnect supervisor's
    /// bootstrap-connect — needs to re-queue a Join. This hands out a lightweight
    /// clone of the underlying gossip sender without exposing the receiver.
    pub fn handle(&self) -> GossipHandle {
        GossipHandle {
            sender: self.sender.clone(),
        }
    }

    /// Receive the next gossip event, or `None` when the pump has stopped.
    pub async fn recv(&mut self) -> Option<GossipEvent> {
        self.event_rx.recv().await
    }
}

/// A cloneable handle for re-joining gossip peers from a detached task.
///
/// Obtained via [`GossipSubscription::handle`]. Carries only a clone of the
/// gossip sender, so it can be moved into a `tokio::spawn`ed task (the reconnect
/// supervisor's bootstrap-connect) where the non-`Clone` subscription cannot go.
#[derive(Clone)]
pub struct GossipHandle {
    sender: GossipSender,
}

impl GossipHandle {
    /// Re-join the given peers — see [`GossipSubscription::rejoin_peers`].
    pub async fn rejoin_peers(&self, peers: Vec<PeerId>) -> Result<()> {
        self.sender
            .join_peers(peer_ids_to_endpoints(peers))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to re-join gossip peers: {e}"))
    }

    /// Broadcast an opaque payload — see [`GossipSubscription::broadcast`].
    ///
    /// Pure pass-through to iroh-gossip; no envelope added. vault-sync's codec
    /// wrapper broadcasts through this handle so it does not need to hold the whole
    /// (non-`Clone`) subscription.
    pub async fn broadcast(&self, payload: Bytes) -> Result<()> {
        self.sender
            .broadcast(payload)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to broadcast gossip payload: {e}"))
    }
}

/// Convert `PeerId`s to iroh `EndpointId`s for a gossip re-join, dropping any
/// legacy/non-curve-point id (it could never be a reachable bootstrap target).
fn peer_ids_to_endpoints(peers: Vec<PeerId>) -> Vec<iroh::EndpointId> {
    peers
        .into_iter()
        .filter_map(|p| try_peer_to_endpoint(p).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Topic::from_u64` and `Topic::to_u64` must be exact inverses so a caller
    /// (vault-sync's `VaultGossipExt`) can recover the seed encoded in a topic.
    /// Mirrors the edge values `node_seam.rs`'s round-trip test uses.
    #[test]
    fn topic_round_trips_through_u64() {
        // Cover edge values: small seeds that zero-pad the high bytes, a full-width
        // seed, and the all-ones seed — the bit patterns most likely to expose a
        // byte-order or width mistake in the encode/decode pair.
        for raw in [1u64, 0xFF, 0x1234, 0xa1b2c3d4e5f67890, u64::MAX] {
            let topic = Topic::from_u64(raw);
            assert_eq!(topic.to_u64(), raw, "round-trip failed for {raw:#x}");
            assert_eq!(topic.as_bytes().len(), 32, "topic bytes must be 32 wide");
        }
    }
}
