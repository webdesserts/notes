//! P2pNode: top-level iroh networking component.
//!
//! Creates an iroh Endpoint from an ed25519 identity key, registers the gossip
//! and pairing ALPNs (and a caller-supplied sync ALPN + handler), and provides
//! methods for generic gossip topics and LAN mesh discovery.
//!
//! This is the application-agnostic networking node. The vault-sync protocol
//! (`SYNC_ALPN`, `RejectSyncHandler`, and the `VaultId`-typed topic
//! wrappers) lives in `vault_sync::network`, which supplies the sync handler through the
//! seam constructors and wraps the generic topic helpers via `VaultGossipExt`.

use anyhow::{Context, Result};
use ed25519_dalek::SigningKey;
use futures::StreamExt;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayConfig, RelayMap, RelayMode, SecretKey,
    address_lookup::memory::MemoryLookup,
};
use iroh_gossip::api::Event;
use iroh_gossip::net::GOSSIP_ALPN;
use iroh_gossip::{Gossip, TopicId};
use n0_future::task::AbortOnDropHandle;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::allowlist::AllowlistStorage;
use crate::gossip::{GossipEvent, GossipSubscription, Topic};
use crate::iroh_adapt::{endpoint_to_peer, try_peer_to_endpoint};
use crate::mesh_mdns::MeshMdns;
use crate::pairing_handler::{PAIRING_ALPN, PairingEvent, PairingStreamHandler};
use crate::peer_addr::PeerAddr;
use crate::peer_conn::{PeerConnInfo, PeerConnType, classify_remote_info};
use crate::peer_id::PeerId;
use crate::protocol::{Connection, IrohHandlerAdapter};
use crate::relay_addr::RelayAddr;

/// The mDNS service name for obsidian-sync mesh discovery.
///
/// Peers using the same service name can discover each other on the LAN.
/// Peers on a different service name are invisible to each other.
const MDNS_SERVICE_NAME: &str = "obsidian-sync";

/// Derive a deterministic gossip [`TopicId`] from a `u64` seed.
///
/// The seed's value is written into the first 8 bytes of a 32-byte little-endian
/// array to produce a consistent `TopicId`. vault-sync's `VaultGossipExt` derives
/// the seed from a `VaultId`, keeping the `VaultId` type out of p2p-core.
pub fn topic_from_u64(seed: u64) -> TopicId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    TopicId::from_bytes(bytes)
}

/// Recover the `u64` seed encoded in a gossip topic's bytes.
///
/// Inverse of [`topic_from_u64`]: reads the little-endian `u64` back out of
/// `topic[0..8]`.
pub fn u64_from_topic(topic: &[u8; 32]) -> u64 {
    u64::from_le_bytes(topic[..8].try_into().unwrap())
}

/// Wraps iroh-gossip's connection handler with allowlist enforcement.
///
/// Non-allowlisted peers are rejected before the gossip protocol runs.
/// This prevents unpaired devices from joining the gossip swarm and
/// observing which files are changing, even if they can reach the network.
///
/// The pairing ALPN is a separate handler and is NOT affected — pairing
/// must remain open to allow new devices to join.
#[derive(Debug)]
struct AllowlistGossipHandler<A: AllowlistStorage + std::fmt::Debug> {
    gossip: Gossip,
    allowlist: Arc<A>,
}

impl<A: AllowlistStorage + std::fmt::Debug + 'static> iroh::protocol::ProtocolHandler
    for AllowlistGossipHandler<A>
{
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let remote_id = connection.remote_id();
        let peer_id = PeerId::from_bytes(*remote_id.as_bytes());

        let allowed = match self.allowlist.list_peers().await {
            Ok(peers) => peers.iter().any(|p| p.node_id == peer_id),
            Err(e) => {
                warn!(peer = %remote_id, "Failed to read allowlist for gossip accept, denying: {}", e);
                false
            }
        };

        if !allowed {
            warn!(peer = %remote_id, "Gossip connection rejected — peer not in allowlist");
            connection.close(iroh::endpoint::VarInt::from_u32(1), b"not in allowlist");
            return Ok(());
        }

        self.gossip
            .handle_connection(connection)
            .await
            .map_err(iroh::protocol::AcceptError::from_err)
    }
}

/// The iroh-based networking node.
///
/// Owns the QUIC endpoint, gossip instance, and protocol router.
/// One `P2pNode` per daemon instance — not per vault.
/// Vaults get per-topic gossip subscriptions via `join_topic()` (wrapped by
/// vault-sync's `VaultGossipExt::join_vault_gossip`).
pub struct P2pNode {
    /// The underlying QUIC endpoint.
    ///
    /// `pub(crate)`: production reaches the endpoint through p2p-core's own methods
    /// (`connect`, `peer_relay`, `watch_net_changes`, …); integration tests that
    /// need the raw handle use [`P2pNode::endpoint_for_test`] (test-gated).
    pub(crate) endpoint: Endpoint,
    /// The gossip subsystem (HyParView + PlumTree).
    ///
    /// `pub(crate)`: production reaches gossip through `adopt_gossip_connection` /
    /// the gossip-subscription helpers; no test needs the raw handle.
    pub(crate) gossip: Gossip,
    /// Inbound pairing events from new devices.
    ///
    /// Drive this in the daemon event loop to process pairing requests.
    pub inbound_pairing_rx: tokio::sync::mpsc::UnboundedReceiver<PairingEvent>,
    /// The protocol router (dispatches by ALPN).
    router: Router,
    /// mDNS address lookup for LAN mesh discovery.
    mdns: Option<MeshMdns>,
    /// In-memory address lookup seeded with known peer relay hints.
    ///
    /// Populated at startup from the `allowlist × known_public_relays`
    /// cross-product so gossip bootstrap can resolve off-LAN peers through a
    /// public relay before mDNS finds them. Also updated at runtime after pairing
    /// and on learn-on-exchange via `add_peer_relay`.
    ///
    /// `pub(crate)`: integration tests inspect hint registration via
    /// [`P2pNode::peer_lookup_for_test`] (test-gated); the canonical production
    /// verification path is full gossip connectivity (see relay_integration tests).
    pub(crate) peer_lookup: MemoryLookup,
}

impl P2pNode {
    /// Create a `P2pNode` whose `sync_alpn` connections are dispatched to a
    /// caller-supplied `ProtocolHandler`.
    ///
    /// This is the seam `vault_sync::network` uses: it binds its `SYNC_ALPN` const
    /// and a handler (the daemon's pumped multi-message handler, or
    /// `RejectSyncHandler` for pairing-only nodes) and forwards them here. p2p-core stays
    /// protocol-agnostic — it only routes the supplied ALPN to the supplied
    /// handler alongside the always-registered gossip and pairing ALPNs.
    ///
    /// Named distinctly from vault-sync's `SyncNodeSeam::new_with_sync_handler`
    /// (which forwards `SYNC_ALPN` and omits the ALPN parameter) so the two don't
    /// collide on `SyncNode::` associated-function resolution — an inherent fn
    /// silently shadows a same-named trait fn at `Type::func` call sites.
    pub async fn with_sync_alpn<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        sync_alpn: &'static [u8],
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: crate::protocol::ProtocolHandler,
    {
        Self::build(
            secret_key_bytes,
            relays,
            allowlist,
            false,
            sync_alpn,
            sync_handler,
        )
        .await
    }

    /// Create a `P2pNode` whose endpoint has **no IP transports** — relay-only —
    /// with a caller-supplied `sync_alpn` handler.
    ///
    /// Identical to [`P2pNode::with_sync_alpn`] except the iroh endpoint is built
    /// with `clear_ip_transports()`, so it cannot open any direct/loopback QUIC
    /// path and can only reach peers through a relay. This reproduces the off-LAN /
    /// behind-NAT condition where the relay is the sole route — the topology that
    /// in-process localhost tests otherwise can't force, because two loopback nodes
    /// would discover each other's direct addresses and bypass the relay.
    ///
    /// Test-only (behind the `test-util` feature); never used in production.
    #[cfg(feature = "test-util")]
    pub async fn relay_only_with_sync_alpn<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        sync_alpn: &'static [u8],
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: crate::protocol::ProtocolHandler,
    {
        Self::build(
            secret_key_bytes,
            relays,
            allowlist,
            true,
            sync_alpn,
            sync_handler,
        )
        .await
    }

    /// Shared constructor body.
    ///
    /// `relay_only` strips IP transports from the endpoint so the only routing
    /// path is a relay. The production callers always pass `false`; only the
    /// `test-util` relay-only constructor passes `true`.
    async fn build<A, H>(
        secret_key_bytes: [u8; 32],
        relays: &[RelayAddr],
        allowlist: Arc<A>,
        relay_only: bool,
        sync_alpn: &'static [u8],
        sync_handler: H,
    ) -> Result<Self>
    where
        A: AllowlistStorage + std::fmt::Debug + 'static,
        H: crate::protocol::ProtocolHandler,
    {
        let signing_key = SigningKey::from_bytes(&secret_key_bytes);
        let secret_key = SecretKey::from_bytes(&signing_key.to_bytes());

        let relay_mode = if relays.is_empty() {
            // `RelayMode::Disabled` disables ONLY the relay transport. `peer_lookup`
            // (`MemoryLookup`) and mDNS stay registered on the endpoint's
            // `address_lookup()` chain, so LAN-direct sync still works with no relay
            // — a laptop with no known public relays is reachable on the LAN. Do NOT
            // add an "if disabled, skip the lookup" shortcut: that would break the
            // LAN-direct path this preserves.
            RelayMode::Disabled
        } else {
            // Home on this SET of relays: iroh selects the lowest-latency reachable
            // one and fails over across the rest (net-report only probes RelayMap
            // members, so the set IS the home/failover candidate list).
            RelayMode::Custom(RelayMap::from_iter(
                relays.iter().map(|r| r.as_iroh().clone()),
            ))
        };

        // `presets::Minimal`, NOT `presets::N0` — deliberate and load-bearing.
        // Minimal registers NO address lookup: no n0 DNS, no pkarr publish/resolve,
        // no `DnsAddressLookup`, no n0 relay pool. "Zero reliance on n0/corporate
        // infra" is the project's founding principle (see the [[Iroh]] note).
        // Discovery is fully self-hosted: mDNS (LAN) + persisted peer_relays hints
        // (cold path) + gossip's own EndpointAddr propagation (warm path, below).
        // Do NOT switch to `presets::N0` or add `DnsAddressLookup` to "fix" a
        // connectivity bug — that re-adds the n0 dependency we intentionally cut.
        let builder = Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .relay_mode(relay_mode)
            // Trust the OS/system CA store (macOS Keychain, incl. MDM-installed CAs)
            // instead of iroh's bundled webpki roots. Corporate TLS-intercepting
            // proxies re-sign with a CA the OS already trusts via MDM, so this lets
            // the relay client connect through them as well as off-network. The
            // relay's outer TLS is just transport — actual sync traffic is
            // independently end-to-end encrypted via QUIC/TLS1.3 keyed to node
            // identities, so this only affects what a relay-hop MITM can see (peer
            // public keys, connection timing), never note content.
            .ca_roots_config(iroh::tls::CaRootsConfig::system());
        // Relay-only: drop every IP transport so no direct/loopback path exists —
        // the endpoint must route through a relay (the off-LAN/NAT condition).
        let builder = if relay_only {
            builder.clear_ip_transports()
        } else {
            builder
        };
        let endpoint = builder
            .bind()
            .await
            .context("Failed to create iroh endpoint")?;

        info!(node_id = %endpoint.id(), "Iroh endpoint created");

        let mdns = {
            use crate::mesh_mdns::socket_addrs_to_port_addrs;
            let bound = endpoint.bound_sockets();
            let (port, ips) = socket_addrs_to_port_addrs(&bound);
            // Only advertise IPv4 sockets (V4Only mDNS scope).
            let v4_ips: Vec<std::net::IpAddr> = ips.into_iter().filter(|ip| ip.is_ipv4()).collect();
            let rt = tokio::runtime::Handle::current();
            match MeshMdns::new(
                endpoint_to_peer(endpoint.id()),
                MDNS_SERVICE_NAME,
                port,
                v4_ips,
                &rt,
            ) {
                Ok(mdns) => {
                    match endpoint.address_lookup() {
                        Ok(lookup) => {
                            lookup.add(mdns.clone());
                            info!("mDNS mesh discovery enabled (V4Only)");
                        }
                        Err(e) => {
                            warn!("Failed to register mDNS with address lookup: {e}");
                        }
                    }
                    Some(mdns)
                }
                Err(e) => {
                    warn!("Failed to create mDNS address lookup, mesh discovery disabled: {e}");
                    None
                }
            }
        };

        // Register the peer-relay MemoryLookup alongside mDNS so gossip can
        // resolve off-LAN peers through hints seeded from persisted peer_relays.
        // Registration failure is non-fatal — we warn and continue without hints,
        // mirroring the mDNS registration pattern above.
        //
        // This is the COLD-path backstop. Gossip already shares addresses warmly
        // (see `join_topic`), but that book is soft-state (~300s TTL) and is
        // useless to a node that's been silent past the TTL, cold-started, or fully
        // partitioned (no neighbor to learn from). These PERSISTED hints are what a
        // returning off-LAN peer dials to re-enter the mesh. iroh won't persist or
        // seed learned relays across restarts — that gap is exactly why we do.
        let peer_lookup = MemoryLookup::with_provenance("peer_relays");
        match endpoint.address_lookup() {
            Ok(lookup) => {
                lookup.add(peer_lookup.clone());
                info!("Peer relay address lookup registered");
            }
            Err(e) => {
                warn!("Failed to register peer relay address lookup: {e}");
            }
        }

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let (pairing_handler, inbound_pairing_rx) = PairingStreamHandler::new();

        let gossip_handler = AllowlistGossipHandler {
            gossip: gossip.clone(),
            allowlist,
        };

        // Registration order GOSSIP → SYNC → PAIRING is unchanged. Only the
        // app-supplied sync handler is wrapped in `IrohHandlerAdapter` (a transparent
        // pass-through to iroh's trait); the gossip + pairing handlers are p2p-core's
        // own and stay on iroh's trait directly.
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip_handler)
            .accept(sync_alpn, IrohHandlerAdapter(sync_handler))
            .accept(PAIRING_ALPN, pairing_handler)
            .spawn();

        Ok(Self {
            endpoint,
            gossip,
            inbound_pairing_rx,
            router,
            mdns,
            peer_lookup,
        })
    }

    /// This node's [`PeerId`] (matches our ed25519 public key).
    pub fn node_id(&self) -> PeerId {
        endpoint_to_peer(self.endpoint.id())
    }

    /// Snapshot how we are currently reaching `peer`.
    ///
    /// Queries iroh's transport state and classifies it into an app-agnostic
    /// [`PeerConnType`] (LAN / relay / unknown). This is a point-in-time
    /// snapshot — it may report `Unknown` in the instant a peer first connects
    /// (before holepunching settles) and self-corrects on later queries. The
    /// daemon composes the friendly device name on top of this.
    ///
    /// A legacy/non-curve-point `PeerId` (one that did not come from a real key)
    /// has no transport identity, so it degrades to an `Unknown` path rather than
    /// panicking — the daemon's connect-logging relies on this graceful fallback.
    pub async fn peer_conn_info(&self, peer: PeerId) -> PeerConnInfo {
        let Ok(endpoint_id) = try_peer_to_endpoint(peer) else {
            return PeerConnInfo {
                conn_type: PeerConnType::Unknown,
            };
        };
        let conn_type = match self.endpoint.remote_info(endpoint_id).await {
            Some(info) => classify_remote_info(&info),
            None => PeerConnType::Unknown,
        };
        PeerConnInfo { conn_type }
    }

    /// Seed a relay hint for a peer into the address-lookup service.
    ///
    /// After this call, gossip bootstrap with the peer's bare `EndpointId` will
    /// resolve the hint and attempt a connection through their relay. This is
    /// called at startup for each `allowlist × known_public_relays` cross-product
    /// entry, and at runtime after a successful pairing or learn-on-exchange.
    ///
    /// If the relay is unreachable off-LAN, connection attempts through that hint
    /// will simply fail; there is no automatic fallback to mDNS off-LAN because
    /// mDNS does not operate across network boundaries.
    pub fn add_peer_relay(&self, peer: PeerId, relay_url: &RelayAddr) {
        if peer == self.node_id() {
            tracing::warn!(
                "add_peer_relay called with our own PeerId — ignoring to prevent \
                 self-connect (iroh rejects self-directed relay paths)"
            );
            return;
        }
        let Ok(endpoint_id) = try_peer_to_endpoint(peer) else {
            tracing::warn!(%peer, "add_peer_relay skipped — peer id is not a valid key");
            return;
        };
        let addr = EndpointAddr::new(endpoint_id).with_relay_url(relay_url.as_iroh().clone());
        self.peer_lookup.add_endpoint_info(addr);
    }

    /// Add a relay to this node's live `RelayMap` so it can home on / fail over to
    /// it this session, without a restart.
    ///
    /// Used by gossip expansion: when a peer's learned home relay is a new public
    /// relay (e.g. a second server's), adopting it into the RelayMap lets net-report
    /// probe it and fail over to it — net-report only probes relays that are in the
    /// RelayMap, so a relay we merely persist but never insert here is invisible to
    /// failover until the next cold start.
    ///
    /// Idempotent at the iroh layer: `insert_relay` replaces any existing config for
    /// the URL and returns the prior one (ignored here). Returns without effect if
    /// the endpoint is closed (`insert_relay` yields `None`).
    pub async fn add_home_relay(&self, relay_url: &RelayAddr) {
        let iroh_url = relay_url.as_iroh();
        self.endpoint
            .insert_relay(
                iroh_url.clone(),
                Arc::new(RelayConfig::from(iroh_url.clone())),
            )
            .await;
        tracing::info!(relay_url = %relay_url.as_str(), "Added learned public relay to live RelayMap");
    }

    /// Replace a peer's relay hint in the address-lookup service.
    ///
    /// Unlike `add_peer_relay`, which unions the new relay with any existing
    /// addresses for the peer, this completely overwrites the prior entry. Used
    /// when refreshing a hint that may be stale: the reconnect supervisor re-seeds
    /// before re-bootstrapping, and learn-on-exchange replaces a moved peer's relay.
    /// A union would let a stale, dead relay URL linger alongside the fresh one and
    /// keep getting dialed; the overwrite guarantees only the latest hint remains.
    pub fn set_peer_relay(&self, peer: PeerId, relay_url: &RelayAddr) {
        if peer == self.node_id() {
            tracing::warn!(
                "set_peer_relay called with our own PeerId — ignoring to prevent \
                 self-connect (iroh rejects self-directed relay paths)"
            );
            return;
        }
        let Ok(endpoint_id) = try_peer_to_endpoint(peer) else {
            tracing::warn!(%peer, "set_peer_relay skipped — peer id is not a valid key");
            return;
        };
        let addr = EndpointAddr::new(endpoint_id).with_relay_url(relay_url.as_iroh().clone());
        self.peer_lookup.set_endpoint_info(addr);
    }

    /// Evict a peer's relay hint from the address-lookup service.
    ///
    /// Removing the entry from `peer_lookup` (`MemoryLookup`) is the only lever
    /// that stops a dead hint from being re-dialed: merely declining to re-seed
    /// it is insufficient because while the entry is present it keeps getting
    /// re-resolved and re-fed to iroh's relay actor — the gossip Dialer consults
    /// the lookup whenever the state machine emits a dial (RequestJoin / active-
    /// view repair), and iroh's relay-actor / address-lookup machinery churns on
    /// a ~60s cadence (this is iroh's own machinery, NOT a HyParView timer). With
    /// the entry gone, no re-resolution source can revive the relay, and iroh
    /// reaps the idle `ActiveRelayActor` within ~60s — that is what actually
    /// quiets the "No route to host" warn loop.
    ///
    /// The reconnect supervisor calls this to throttle a stale hint, then
    /// re-adds it via `set_peer_relay` on a slow cadence so a genuinely off-LAN
    /// peer that returns is still reached. Removing an absent key is a harmless
    /// no-op.
    pub fn remove_peer_relay(&self, peer: PeerId) {
        if peer == self.node_id() {
            tracing::warn!(
                "remove_peer_relay called with our own PeerId — ignoring for \
                 consistency with add/set_peer_relay (we never seed ourselves)"
            );
            return;
        }
        // A legacy/non-curve-point id was never seeded, so there is nothing to
        // remove — a no-op skip, matching the "removing an absent key" contract.
        let Ok(endpoint_id) = try_peer_to_endpoint(peer) else {
            return;
        };
        self.peer_lookup.remove_endpoint_info(endpoint_id);
    }

    /// Subscribe to a gossip `topic`, bootstrapping toward `bootstrap` peers.
    ///
    /// Returns a generic byte-transport [`GossipSubscription`]: broadcast opaque
    /// payloads + a stream of generic membership/data events. p2p-core does NOT
    /// interpret the payload bytes — the app owns its codec (vault-sync wraps this
    /// in a `VaultGossip` that bincode-codecs its `GossipMessage` on top).
    ///
    /// At least one bootstrap node is needed to join an existing swarm.
    ///
    /// We pass bare `EndpointId`s and attach NO peer data — yet iroh-gossip still
    /// auto-publishes THIS node's own `EndpointAddr` (relay + direct IPs) as
    /// membership peer-data, ships it on join/shuffle, and decodes peers' addresses
    /// into a gossip-owned `AddressLookup` on the endpoint. So gossip IS our warm
    /// address-sharing channel, for free: a connected peer that switches networks
    /// re-announces its new address automatically (driven by iroh's `watch_addr`).
    /// Don't conclude "we have no warm address sharing" — we do. Source trace:
    /// [[Reviews/Gossip Address Propagation]].
    ///
    /// The catch (why the persisted `peer_lookup` hints still exist): gossip's book
    /// is soft-state (~300s TTL) and can't cross a full partition or cold-start.
    /// That COLD path is owned by the relay hints + the reconnect supervisor.
    pub async fn subscribe(
        &self,
        topic: Topic,
        bootstrap: Vec<PeerId>,
    ) -> Result<GossipSubscription> {
        // Convert to iroh ids at the boundary, dropping any legacy/non-curve-point
        // id (it could never be a reachable gossip bootstrap target anyway).
        let bootstrap_ids: Vec<EndpointId> = bootstrap
            .into_iter()
            .filter_map(|p| try_peer_to_endpoint(p).ok())
            .collect();
        let gossip_topic = self
            .gossip
            .subscribe(topic.as_iroh(), bootstrap_ids)
            .await
            .context("Failed to subscribe to gossip topic")?;

        let (sender, mut receiver) = gossip_topic.split();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // The pump: translate iroh-gossip's events into generic `GossipEvent`s.
        // Descended verbatim from the legacy sync crate's `VaultGossip::new` loop, with
        // the ONE change at `Event::Received` — the payload is forwarded OPAQUE
        // (`bytes: msg.content`) rather than decoded. The app decodes downstream.
        let task = AbortOnDropHandle::new(n0_future::task::spawn(async move {
            while let Some(result) = receiver.next().await {
                let event = match result {
                    Ok(e) => e,
                    Err(e) => {
                        debug!("Gossip stream error: {}", e);
                        break;
                    }
                };

                let gossip_event = match event {
                    Event::NeighborUp(node_id) => {
                        // Diagnostic only: the daemon emits the enriched INFO
                        // connect/disconnect notice off the PeerRegistry liveness
                        // transition. This is the raw HyParView membership edge.
                        debug!(peer = %node_id, "Gossip NeighborUp");
                        GossipEvent::NeighborUp(endpoint_to_peer(node_id))
                    }
                    Event::NeighborDown(node_id) => {
                        debug!(peer = %node_id, "Gossip NeighborDown");
                        GossipEvent::NeighborDown(endpoint_to_peer(node_id))
                    }
                    Event::Received(msg) => {
                        // Forward the payload OPAQUE — no decode. iroh-gossip's
                        // `msg.content` is the exact bytes a peer broadcast; the app
                        // (vault-sync's codec wrapper) deserializes downstream.
                        GossipEvent::Data {
                            from: endpoint_to_peer(msg.delivered_from),
                            bytes: msg.content,
                        }
                    }
                    Event::Lagged => {
                        debug!("Gossip lagged (message buffer overflow)");
                        GossipEvent::Lagged
                    }
                };

                if event_tx.send(gossip_event).is_err() {
                    break; // Receiver dropped
                }
            }
        }));

        Ok(GossipSubscription::new(sender, topic, event_rx, task))
    }

    /// Publish this node's mesh metadata via mDNS so LAN peers can discover it.
    ///
    /// Other devices running obsidian-sync on the same LAN will see this
    /// node's `MeshMetadata` in their `subscribe_discovery()` stream.
    /// Devices with the same `VaultId` belong to the same mesh.
    ///
    /// Relay URL is included when provided so discovered peers can connect
    /// even without direct address information.
    pub fn publish_mesh_info(
        &self,
        metadata: &crate::discovery::MeshMetadata,
        relay_url: Option<&RelayAddr>,
    ) {
        use crate::mesh_mdns::socket_addrs_to_port_addrs;

        let Some(ref mdns) = self.mdns else {
            tracing::debug!("mDNS not available, skipping mesh info publish");
            return;
        };

        let json = match serde_json::to_string(metadata) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to serialize MeshMetadata for mDNS: {e}");
                return;
            }
        };

        // Validate and send as user-data TXT attribute (routed through actor for ordering).
        match json.parse::<iroh::address_lookup::UserData>() {
            Ok(_) => mdns.set_user_data(&json),
            Err(e) => {
                tracing::warn!(
                    "MeshMetadata JSON too long for mDNS UserData ({} bytes): {e}",
                    json.len()
                );
                return;
            }
        }

        // Re-advertise current bound addresses (IPv4 only, matching V4Only scope).
        let bound = self.endpoint.bound_sockets();
        let v4_bound: Vec<std::net::SocketAddr> =
            bound.into_iter().filter(|sa| sa.is_ipv4()).collect();
        if !v4_bound.is_empty() {
            let (port, ips) = socket_addrs_to_port_addrs(&v4_bound);
            mdns.republish_addrs(port, ips);
        }

        // Set relay URL TXT attribute (routed through actor, sequenced after addrs).
        let relay_url_str = relay_url.map(|u| u.as_str());
        mdns.set_relay_url(relay_url_str.as_deref());

        info!(mesh = %metadata.mesh, vid = %metadata.vid, "Published mesh info via mDNS");
    }

    /// On a network change, re-advertise our mDNS addresses and restart the
    /// browse so same-LAN re-discovery after a migration is prompt rather than
    /// waiting for the next periodic re-query tick (`SLOW_REQUERY_INTERVAL`).
    ///
    /// This is the address-only subset of [`Self::publish_mesh_info`]: it
    /// re-snapshots the bound IPv4 sockets and restarts the browse, but it
    /// deliberately does NOT re-send user-data or the relay URL — a network move
    /// changes our network position, not our mesh metadata, and the stale
    /// launch-time relay-URL re-advertisement is separate (Tier-2) work.
    ///
    /// Note on addresses: `bound_sockets()` on a wildcard-bound endpoint yields
    /// `0.0.0.0:port`, so the republish is a "re-snapshot and re-announce NOW"
    /// nudge — mdns-sd's `enable_addr_auto()` supplies the real per-interface IPs
    /// at register time. The port is the durable contribution and is stable across
    /// a LAN move, so there is no ordering requirement against iroh's rebind.
    pub fn republish_mdns_on_net_change(&self) {
        use crate::mesh_mdns::socket_addrs_to_port_addrs;

        let Some(ref mdns) = self.mdns else {
            tracing::debug!("mDNS not available, skipping net-change republish");
            return;
        };

        // Field-observability: a firm, unconditional log so a live network switch
        // can be confirmed to have kicked the mDNS path (not just the backoff reset).
        tracing::debug!("Re-publishing mDNS addresses + restarting browse after network change");

        // Re-advertise current bound addresses (IPv4 only, matching V4Only scope).
        // Mirror `publish_mesh_info`'s guard so the two paths don't drift.
        let bound = self.endpoint.bound_sockets();
        let v4_bound: Vec<std::net::SocketAddr> =
            bound.into_iter().filter(|sa| sa.is_ipv4()).collect();
        if !v4_bound.is_empty() {
            let (port, ips) = socket_addrs_to_port_addrs(&v4_bound);
            mdns.republish_addrs(port, ips);
        }

        // Restart the browse UNCONDITIONALLY — it is what re-discovers peers, so
        // it must fire even if the IPv4 set is momentarily empty mid-migration.
        mdns.restart_browse();
    }

    /// Subscribe to mDNS discovery events for nearby meshes.
    ///
    /// Each `DiscoveryEvent::Discovered` event carries a peer's `EndpointInfo`,
    /// including their `UserData` which contains JSON-encoded `MeshMetadata`.
    /// Parse the `UserData` to extract the mesh name and VaultId, then group
    /// peers by VaultId to form `DiscoveredMesh` entries.
    pub async fn subscribe_discovery(
        &self,
    ) -> Option<impl n0_future::Stream<Item = crate::discovery::DiscoveryEvent> + Unpin + use<>>
    {
        let mdns = self.mdns.as_ref()?;
        Some(mdns.subscribe().await)
    }

    /// A cheap-cloneable handle for dialing + gossip-adoption from a detached task.
    ///
    /// Holds clones of the endpoint + gossip (both cheap `Clone`), so a spawned
    /// task that outlives the node's owner does NOT keep the node alive — unlike an
    /// `Arc<P2pNode>`, which would block `shutdown(self)`'s consume (an in-flight
    /// `Arc::clone` makes `Arc::into_inner` return `None` → shutdown silently
    /// skipped). The reconnect supervisor's bootstrap/sync-exchange tasks capture
    /// this; `sync_node` stays OWNED.
    pub fn dial_handle(&self) -> DialHandle {
        DialHandle {
            endpoint: self.endpoint.clone(),
            gossip: self.gossip.clone(),
        }
    }

    /// Dial `peer` on `alpn`, returning the raw QUIC [`Connection`] (Tier-2).
    ///
    /// The app opens its own streams on the returned connection (`open_bi()`) and
    /// frames bytes — p2p-core owns "dial this peer on this ALPN," the app owns the
    /// byte protocol. Absorbs the daemon's former raw `endpoint.connect(...)` sites
    /// (sync-exchange pump, supervisor bootstrap, pairing).
    ///
    /// FALLIBLE on a legacy/non-curve-point `PeerAddr` (returns the connect error);
    /// callers holding a transport-sourced id will never hit that arm.
    pub async fn connect(&self, peer: &PeerAddr, alpn: &[u8]) -> Result<Connection> {
        self.dial_handle().connect(peer, alpn).await
    }

    /// The peer's currently-active relay URL, if it is reachable via a relay right now.
    ///
    /// Snapshots iroh's `remote_info(peer)` and returns the first address that is
    /// both `Active` and a relay path — the URL learn-on-exchange should refresh
    /// the stored hint to. `None` for a LAN-direct connection (no active relay path)
    /// or an unknown/legacy peer. Mirrors `peer_conn_info`'s graceful degrade.
    pub async fn peer_relay(&self, peer: PeerId) -> Option<RelayAddr> {
        self.dial_handle().peer_relay(peer).await
    }

    /// Hand an externally-established [`Connection`] to the gossip subsystem, which
    /// adopts it exactly as it adopts an inbound accept (drains the queued Join →
    /// peer Active → NeighborUp). Used by the reconnect supervisor's bootstrap-connect
    /// to recover a peer whose bare-id gossip dial parked.
    pub async fn adopt_gossip_connection(&self, conn: Connection) -> Result<()> {
        self.dial_handle().adopt_gossip_connection(conn).await
    }

    /// A stream that yields `()` each time iroh observes a network address change
    /// (relay or direct addrs changed). Drives the daemon's reconnect-backoff reset.
    /// Hides `iroh::Watcher` + `watch_addr().stream_updates_only()`.
    ///
    /// `stream_updates_only` (not `stream`) skips the current address, so this
    /// reacts only to real changes — no spurious emission at startup.
    pub fn watch_net_changes(&self) -> impl n0_future::Stream<Item = ()> + Unpin + use<> {
        use futures::StreamExt;
        use iroh::Watcher;
        self.endpoint.watch_addr().stream_updates_only().map(|_| ())
    }

    /// Shut down the node, closing all connections.
    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        self.endpoint.close().await;
        Ok(())
    }
}

/// Test-only raw-handle accessors.
///
/// The `endpoint` / `peer_lookup` fields are `pub(crate)` so production can't reach
/// the raw iroh handles, but the integration tests need them for harness plumbing
/// (seed a `MemoryLookup`, await `endpoint.online()`, inspect relay status). The
/// `feature = "test-util"` half exposes the accessors to the cross-crate
/// integration tests (which build vault-sync / sync-daemon WITHOUT `#[cfg(test)]`);
/// the `test` half covers p2p-core's own in-crate tests.
#[cfg(any(test, feature = "test-util"))]
impl P2pNode {
    /// The raw iroh endpoint, for test harness plumbing only.
    pub fn endpoint_for_test(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The peer-relay hint lookup, for tests that inspect hint registration.
    pub fn peer_lookup_for_test(&self) -> &MemoryLookup {
        &self.peer_lookup
    }
}

/// A cheap-cloneable dial + gossip-adoption handle (see [`P2pNode::dial_handle`]).
///
/// Bundles clones of the endpoint + gossip behind one named handle plus the dial
/// logic, so detached supervisor tasks can `connect` / `adopt_gossip_connection` /
/// `peer_relay` without holding an `Arc<P2pNode>` that would block the node's
/// consuming `shutdown(self)`. The `P2pNode` methods of the same names delegate
/// here, keeping one source of truth for the dial logic.
#[derive(Clone)]
pub struct DialHandle {
    endpoint: Endpoint,
    gossip: Gossip,
}

impl DialHandle {
    /// This node's [`PeerId`] (matches our ed25519 public key) — same value as
    /// [`P2pNode::node_id`]. Lets a detached task that holds only a `DialHandle`
    /// (e.g. the parked pairing exchange) derive the local identity without the
    /// node.
    pub fn node_id(&self) -> PeerId {
        endpoint_to_peer(self.endpoint.id())
    }

    /// See [`P2pNode::connect`].
    pub async fn connect(&self, peer: &PeerAddr, alpn: &[u8]) -> Result<Connection> {
        let addr = peer
            .try_into_iroh()
            .map_err(|e| anyhow::anyhow!("connect target is not a valid key: {e}"))?;
        Ok(self.endpoint.connect(addr, alpn).await?)
    }

    /// See [`P2pNode::adopt_gossip_connection`].
    pub async fn adopt_gossip_connection(&self, conn: Connection) -> Result<()> {
        self.gossip
            .handle_connection(conn)
            .await
            .map_err(|e| anyhow::anyhow!("gossip rejected adopted connection: {e}"))
    }

    /// See [`P2pNode::peer_relay`].
    pub async fn peer_relay(&self, peer: PeerId) -> Option<RelayAddr> {
        use iroh::TransportAddr;
        use iroh::endpoint::TransportAddrUsage;

        let endpoint_id = try_peer_to_endpoint(peer).ok()?;
        let info = self.endpoint.remote_info(endpoint_id).await?;
        // VERBATIM the daemon's former `active_relay_url` selection: the first
        // address that is both currently `Active` and a relay path is the URL the
        // peer is reachable at right now. Round-trip the relay through its string
        // form (lossless for a valid relay URL) so the caller never names iroh's
        // URL type. A LAN-direct connection has no active relay path → `None`.
        info.addrs().find_map(|a| match (a.usage(), a.addr()) {
            (TransportAddrUsage::Active, TransportAddr::Relay(url)) => {
                RelayAddr::parse(&url.to_string()).ok()
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `topic_from_u64` and `u64_from_topic` must be exact inverses so a caller
    /// (vault-sync's `VaultGossipExt`) can recover the seed encoded in a topic.
    #[test]
    fn topic_from_u64_round_trips_through_u64_from_topic() {
        // Cover edge values: small seeds that zero-pad the high bytes, a full-width
        // seed, and the all-ones seed — the bit patterns most likely to expose a
        // byte-order or width mistake in the encode/decode pair.
        for raw in [1u64, 0xFF, 0x1234, 0xa1b2c3d4e5f67890, u64::MAX] {
            let topic = topic_from_u64(raw);
            let recovered = u64_from_topic(topic.as_bytes());
            assert_eq!(recovered, raw, "round-trip failed for {raw:#x}");
        }
    }
}
