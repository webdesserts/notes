//! The vault-sync wire protocol layered onto p2p-core's networking substrate.
//!
//! The generic node, gossip transport, mDNS discovery, relay, and pairing handler
//! all live in `p2p-core` (`P2pNode`). This module layers the *vault-sync
//! specifics* on top:
//!
//! ```text
//! p2p_core::P2pNode           — QUIC endpoint + Router + gossip transport (the substrate)
//! └── vault_sync::network     (this module, `network` feature)
//!     ├── SyncNodeSeam        — binds SYNC_ALPN + a caller-supplied sync stream handler
//!     ├── VaultGossipExt      — maps VaultId onto the generic gossip topic
//!     ├── gossip::VaultGossip — the GossipMessage codec over the byte-transport
//!     ├── peers::PeerRegistry — in-memory registry of known peers
//!     └── RejectSyncHandler   — SYNC_ALPN handler for nodes that serve no sync
//! ```
//!
//! Callers that need mDNS discovery or the pairing handler import them from
//! `p2p_core` directly. `p2p-core` is native-only today, so this feature is too.

pub mod gossip;
pub mod node_seam;
pub mod peers;

use p2p_core::{AcceptError, Connection, ProtocolHandler};

/// ALPN identifier for the vault-sync QUIC stream protocol.
///
/// iroh's Router dispatches incoming QUIC connections to the correct handler
/// based on ALPN. It is the vault-sync wire contract — do NOT change the literal.
pub const SYNC_ALPN: &[u8] = b"obsidian-memory/sync/1";

/// The node type the daemon names: p2p-core's `P2pNode`, with the vault-sync
/// constructors and `VaultId`-typed gossip helpers layered on through
/// [`SyncNodeSeam`] and [`VaultGossipExt`].
pub use p2p_core::P2pNode as SyncNode;

/// The gossip protocol ALPN, re-exported so downstream crates (e.g. the daemon's
/// reconnect supervisor) can open gossip-bound connections without taking a
/// direct dependency on `iroh-gossip`.
pub use p2p_core::GOSSIP_ALPN;

pub use gossip::{ChangeNotification, GossipEvent, GossipMessage, VaultGossip};
pub use node_seam::{SyncNodeSeam, VaultGossipExt};
pub use peers::PeerRegistry;

/// A `SYNC_ALPN` handler that serves no sync: it closes every inbound sync
/// connection immediately.
///
/// Used by nodes that must bind the sync ALPN (so the Router accepts the
/// endpoint layout) but never exchange vault data — e.g. the short-lived
/// pairing node in `sync-daemon pair`, and relay-routing tests that only assert
/// connectivity.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectSyncHandler;

impl ProtocolHandler for RejectSyncHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.close(0u32.into(), b"sync not served");
        Ok(())
    }
}
