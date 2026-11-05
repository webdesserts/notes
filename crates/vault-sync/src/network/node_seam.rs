//! vault-sync's seam onto p2p-core's [`P2pNode`].
//!
//! The node itself is generic (`p2p_core::P2pNode`) — it registers a
//! caller-supplied ALPN + handler and exposes `u64`-keyed gossip topics. This
//! module binds the vault-sync specifics back onto it:
//!
//! - [`SyncNodeSeam`] binds the `SYNC_ALPN` const to a caller-supplied sync
//!   stream handler (the daemon's `PumpedSyncHandler`, or
//!   [`RejectSyncHandler`](crate::network::RejectSyncHandler) for nodes that
//!   serve no sync).
//! - [`VaultGossipExt`] maps vault-sync's `VaultId` onto p2p-core's generic
//!   `u64` topic helpers, keeping `VaultId` out of p2p-core.
//!
//! Both are extension traits on `P2pNode`, so callers write
//! `SyncNode::new_with_sync_handler(...)` and `node.join_vault_gossip(...)`
//! after `use vault_sync::network::{SyncNodeSeam, VaultGossipExt};`.

use std::sync::Arc;

use anyhow::Result;
use p2p_core::{AllowlistStorage, P2pNode, PeerId, RelayAddr, Topic};

use crate::VaultId;
use crate::network::SYNC_ALPN;
use crate::network::gossip::VaultGossip;

/// Vault-sync constructors layered onto the generic [`P2pNode`].
///
/// Every constructor takes a caller-supplied `SYNC_ALPN` handler: the daemon's
/// `PumpedSyncHandler` (which owns its own inbound channel) for real sync, or
/// [`RejectSyncHandler`](crate::network::RejectSyncHandler) for nodes that only
/// need the endpoint (pairing, relay-routing tests).
#[allow(async_fn_in_trait)]
pub trait SyncNodeSeam: Sized {
    /// Create a node whose `SYNC_ALPN` connections are dispatched to
    /// `sync_handler`.
    ///
    /// The daemon supplies a handler that drives the variable-length
    /// vault-sync exchange (digest → mismatch → exchange → response) inline over
    /// one bi-stream.
    async fn new_with_sync_handler<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: p2p_core::ProtocolHandler;

    /// Relay-only [`new_with_sync_handler`](SyncNodeSeam::new_with_sync_handler) —
    /// the endpoint has no IP transports, so the relay is the sole route. The
    /// daemon's relay-path tests need the relay-only topology with a real (or
    /// rejecting) handler. Test-only (behind `test-util`).
    #[cfg(feature = "test-util")]
    async fn new_relay_only_with_sync_handler<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: p2p_core::ProtocolHandler;
}

impl SyncNodeSeam for P2pNode {
    async fn new_with_sync_handler<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: p2p_core::ProtocolHandler,
    {
        P2pNode::with_sync_alpn(secret_key_bytes, relays, allowlist, SYNC_ALPN, sync_handler).await
    }

    #[cfg(feature = "test-util")]
    async fn new_relay_only_with_sync_handler<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: p2p_core::ProtocolHandler,
    {
        P2pNode::relay_only_with_sync_alpn(
            secret_key_bytes,
            relays,
            allowlist,
            SYNC_ALPN,
            sync_handler,
        )
        .await
    }
}

/// Vault-scoped gossip helpers layered onto the generic [`P2pNode`].
///
/// `P2pNode` speaks `u64`-keyed topics (`topic_from_u64`/`u64_from_topic`); this
/// trait maps the `VaultId` type onto them and wraps the raw subscribe handle in
/// a [`VaultGossip`]. Keeping the mapping here is what keeps `VaultId` out of
/// p2p-core.
#[allow(async_fn_in_trait)]
pub trait VaultGossipExt {
    /// Derive a deterministic gossip [`Topic`] from a [`VaultId`].
    ///
    /// Each vault gets its own gossip topic, scoped to peers who share that vault.
    fn vault_topic(vault_id: &VaultId) -> Topic;

    /// Recover the [`VaultId`] encoded in a gossip topic's bytes.
    ///
    /// Inverse of [`vault_topic`](VaultGossipExt::vault_topic). Used when a pairing
    /// initiator must adopt the mesh's VaultId carried in
    /// `PairingResult::vault_topic` — the topic is the only place the VaultId
    /// travels over the wire, so this is the single defined inverse mapping rather
    /// than duplicating the id in a separate field.
    fn vault_id_from_topic(topic: &[u8; 32]) -> VaultId;

    /// Subscribe to gossip for a specific vault.
    ///
    /// `bootstrap_nodes` should be the `PeerId`s of known peers for that vault.
    /// At least one bootstrap node is needed to join the gossip swarm.
    async fn join_vault_gossip(
        &self,
        vault_id: &VaultId,
        bootstrap_nodes: Vec<PeerId>,
    ) -> Result<VaultGossip>;
}

impl VaultGossipExt for P2pNode {
    fn vault_topic(vault_id: &VaultId) -> Topic {
        Topic::from_u64(vault_id.as_u64())
    }

    fn vault_id_from_topic(topic: &[u8; 32]) -> VaultId {
        VaultId::from(p2p_core::u64_from_topic(topic))
    }

    async fn join_vault_gossip(
        &self,
        vault_id: &VaultId,
        bootstrap_nodes: Vec<PeerId>,
    ) -> Result<VaultGossip> {
        let topic = <P2pNode as VaultGossipExt>::vault_topic(vault_id);
        let subscription = self.subscribe(topic, bootstrap_nodes).await?;
        Ok(VaultGossip::new(subscription))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `vault_topic` and `vault_id_from_topic` must be exact inverses so a pairing
    /// initiator can recover the mesh's VaultId from the topic it receives on the wire.
    #[test]
    fn vault_topic_round_trips_through_vault_id_from_topic() {
        // Cover edge values: small ids that zero-pad the high bytes, a full-width
        // id, and the all-ones id — the bit patterns most likely to expose a
        // byte-order or width mistake in the encode/decode pair.
        for raw in [1u64, 0xFF, 0x1234, 0xa1b2c3d4e5f67890, u64::MAX] {
            let vault_id = VaultId::from(raw);
            let topic = <P2pNode as VaultGossipExt>::vault_topic(&vault_id);
            let recovered = <P2pNode as VaultGossipExt>::vault_id_from_topic(topic.as_bytes());
            assert_eq!(recovered, vault_id, "round-trip failed for {raw:#x}");
        }
    }

    /// Freeze the three ALPN strings the Router dispatches on. These are the live
    /// accept/dial contract — a mixed-version fleet partitions if any of them
    /// changes. The test makes a "tidy the ALPN" edit fail at unit-test time
    /// instead of at fleet-partition time. `GOSSIP_ALPN` is iroh-gossip's own
    /// literal (we re-export it via `p2p_core::GOSSIP_ALPN`); pinning it documents
    /// the iroh-gossip version we are wire-compatible with.
    #[test]
    fn alpn_strings_are_stable() {
        assert_eq!(SYNC_ALPN, b"obsidian-memory/sync/1");
        assert_eq!(p2p_core::PAIRING_ALPN, b"obsidian-memory/pair/1");
        assert_eq!(p2p_core::GOSSIP_ALPN, b"/iroh-gossip/1");
    }
}
