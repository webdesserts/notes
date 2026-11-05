//! Shared post-pairing logic used by both the desktop tray and the CLI.
//!
//! When a pairing initiator successfully joins an existing mesh, both paths must
//! perform the same onboarding steps: write the mesh roster into the local
//! allowlist, adopt the mesh's VaultId so the device lands on the right gossip
//! topic, and persist the mesh's relay URL for the next daemon start. These
//! helpers exist so the logic lives in exactly one place rather than being
//! duplicated (and drifting) across the two entry points.

use std::path::Path;

use p2p_core::PeerId;
use p2p_core::RelayAddr;
use tracing::warn;
use vault_sync::VaultId;
use vault_sync::network::{SyncNode, VaultGossipExt};
// The on-disk `.sync/metadata.toml` is owned by the vault-sync engine; `NativeFs`
// implements vault-sync's `FileSystem`, so the metadata round-trip goes through
// `vault_sync::SyncMetadata` (the format is byte-identical to the legacy sync crate's).
use vault_sync::SyncMetadata;

use crate::native_fs::NativeFs;

/// Rewrite `.sync/metadata.toml` to adopt the mesh's VaultId (CLI path).
///
/// The CLI process does not hold a live in-memory `Vault` during pairing, so it
/// adopts by rewriting the on-disk metadata directly. The next `memory sync up`
/// reads the adopted VaultId and joins the correct gossip topic. The tray path
/// instead uses the live `Vault::adopt_vault_id` so its in-memory id stays in
/// sync; this on-disk variant is the equivalent for the exit-then-restart CLI.
pub async fn adopt_vault_id_on_disk(vault_path: &Path, new_id: VaultId) -> anyhow::Result<()> {
    let fs = NativeFs::new(vault_path.to_path_buf());
    let existing = SyncMetadata::load_or_migrate(&fs).await?;
    if existing.vault_id == new_id {
        return Ok(());
    }
    let meta = SyncMetadata {
        version: existing.version,
        vault_id: new_id,
    };
    meta.save(&fs).await?;
    Ok(())
}

/// Adopt the responder's PUBLIC relay into the persisted public-relay set and
/// seed the live address-lookup so the current session can route through their
/// relay without a restart.
///
/// The relay the responder hands over is its own PUBLIC relay (it is a server).
/// Two destinations, only one durable:
/// - **`known_public_relays`** (persisted) — the cold-bootstrap that lets the
///   laptop home on this relay off-LAN after a restart. Only added if the URL is
///   off-LAN-reachable (a laptop-to-laptop pair with no server hands a private
///   LAN-IP relay, which the `add_known_public_relay` guard rejects — see D-2).
/// - **`peer_lookup`** (live/ephemeral) — seeded so the current session reaches
///   the responder by `EndpointId` before gossip warms up (pairing-then-immediate
///   -sync needs it). This is an in-memory write only; per-peer relay hints are
///   no longer persisted (the public-relay set is the sole durable store).
///
/// `responder_id` is the transport-verified identity of the QUIC connection the
/// initiator dialed — not inferred from `PairingResult.mesh_members` — so the
/// binding is correct even if `mesh_members` ordering or content differs.
///
/// `relay_urls` is currently always single-source (the responder's own relay),
/// so taking the first non-empty entry is sound. Mesh-wide relay propagation
/// (other members' relays) is deferred.
///
/// **Asymmetry note:** the responder side does NOT learn the initiator's relay
/// today — the `PairingHello` message does not carry a relay URL. That
/// propagation is deferred to a future phase.
///
/// Loading `DaemonConfig::load_or_generate` on a fresh vault generates the
/// device keypair as a side effect; that is acceptable — a device pairing in
/// needs an identity anyway.
///
/// `self_relay_url` is this device's own advertised relay URL, if it is running
/// one. It is threaded through to the persist helper's clobber-guard so that
/// adopting the responder's relay does NOT drop our own advertised `relay_url`
/// from `daemon.toml`. The CLI pairing flow has no running relay and passes
/// `None`; the live-daemon initiator flow passes its advertised URL.
pub async fn persist_adopted_relay(
    vault_path: &Path,
    responder_id: PeerId,
    relay_urls: &[String],
    self_relay_url: Option<String>,
    sync_node: &SyncNode,
) {
    let Some(url_str) = relay_urls.iter().find(|u| !u.is_empty()).cloned() else {
        return;
    };

    // Persist the responder's public relay into the public-relay set (the cold
    // off-LAN-reach store). `add_known_public_relay` self-guards on
    // off-LAN-reachability, so a same-LAN-only pair (private relay URL) is a
    // no-op here — the live lookup seed below still happens for that session.
    if let Err(e) = p2p_core::persist_config_change(vault_path, self_relay_url, |config| {
        config.add_known_public_relay(&url_str)
    })
    .await
    {
        warn!("Failed to persist adopted public relay URL: {}", e);
        return;
    }

    // Seed the live lookup so gossip can route to the responder through their
    // relay in the current session without waiting for a restart. This is an
    // EPHEMERAL in-memory write — no persisted per-peer hint (the public-relay
    // set above is the only durable networking store).
    if let Ok(relay_addr) = RelayAddr::parse(&url_str) {
        sync_node.add_peer_relay(responder_id, &relay_addr);
    } else {
        warn!(
            url = %url_str,
            "Adopted relay URL is not a valid relay URL — skipping live lookup seed"
        );
    }
}

/// Recover the mesh's VaultId from a `PairingResult`'s gossip topic bytes.
///
/// The VaultId travels over the wire only inside the topic, so this is the
/// single defined way to recover it on the initiator side. Returns `None` when
/// the topic is absent (only on a failed pair, which both callers handle first).
pub fn vault_id_from_pairing_topic(vault_topic: Option<[u8; 32]>) -> Option<VaultId> {
    vault_topic.as_ref().map(SyncNode::vault_id_from_topic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2p_core::allowlist::InMemoryAllowlist;
    use vault_sync::network::{RejectSyncHandler, SyncNodeSeam};

    /// Regression: adopting a pairing relay must NOT clobber the daemon's own
    /// advertised `relay_url` out of `daemon.toml`, and the adopted relay lands
    /// in the persisted PUBLIC-relay set (not a per-peer hint).
    ///
    /// `persist_adopted_relay` does a load → mutate → save round-trip, and
    /// `load_or_generate` deliberately drops `relay_url` (runtime state). Before
    /// the fix, the function never re-stamped it, so an active daemon (running
    /// its own relay) that paired in would silently lose its advertised URL.
    /// Threading `self_relay_url` through to the persist helper's clobber-guard
    /// is the fix; this proves the URL survives the adopt-write while the
    /// responder's public relay is recorded in `known_public_relays`.
    #[tokio::test]
    async fn persist_adopted_relay_preserves_own_relay_url() {
        use crate::persistence::DaemonConfig;
        use std::sync::Arc;
        use tempfile::TempDir;

        let vault_dir = TempDir::new().unwrap();
        let vault_path = vault_dir.path();

        // Simulate our daemon having started its own relay: write our advertised
        // URL to daemon.toml (this is what set_relay_url does on relay start).
        let own_relay_url = "http://my-own-relay:3340/".to_string();
        let (mut config, identity) = DaemonConfig::load_or_generate(vault_path, None)
            .await
            .unwrap();
        config
            .set_relay_url(Some(own_relay_url.clone()), vault_path)
            .unwrap();

        // Build a minimal SyncNode (no relay) just so the live-lookup seed step
        // of persist_adopted_relay has something to call. Use our persisted
        // identity so node_id() matches the config's peer_id.
        let allowlist = Arc::new(InMemoryAllowlist::new());
        let sync_node = SyncNode::new_with_sync_handler(
            identity.secret_key_bytes(),
            &[],
            allowlist,
            RejectSyncHandler,
        )
        .await
        .expect("failed to build SyncNode for test");

        // The responder is a DIFFERENT device, so its id won't trip the self-skip
        // in the live-lookup seed (`add_peer_relay`).
        let responder_id = PeerId::from_secret_bytes([7u8; 32]);
        let responder_relay = "http://responder-relay:3340/".to_string();

        persist_adopted_relay(
            vault_path,
            responder_id,
            std::slice::from_ref(&responder_relay),
            Some(own_relay_url.clone()),
            &sync_node,
        )
        .await;

        let contents = std::fs::read_to_string(vault_path.join(".sync/daemon.toml")).unwrap();

        // The fix: our own advertised relay_url must survive the adopt-write.
        assert!(
            contents.contains(&format!("relay_url = \"{own_relay_url}\"")),
            "the daemon's own relay_url must NOT be clobbered when adopting a \
             pairing relay; daemon.toml was:\n{contents}"
        );

        // And the responder's PUBLIC relay was recorded in the public-relay set
        // (the cold off-LAN store) — the sole durable networking store. (No
        // per-peer hint is persisted; the persisted `peer_relays` field is gone.)
        let (reloaded, _) = DaemonConfig::load_or_generate(vault_path, None)
            .await
            .unwrap();
        assert!(
            reloaded.known_public_relays.contains(&responder_relay),
            "responder's public relay should land in known_public_relays; set was {:?}",
            reloaded.known_public_relays
        );

        sync_node.shutdown().await.ok();
    }
}
