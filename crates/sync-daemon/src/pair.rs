//! Interactive pairing CLI — `memory sync pair`.
//!
//! Discovers nearby meshes via mDNS, lets the user select one, then
//! runs the pairing exchange. On success, all mesh members are added
//! to the local allowlist so syncing can begin immediately.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use std::sync::Arc;
use tracing::debug;

use p2p_core::PeerId;
use p2p_core::allowlist::InMemoryAllowlist;
use p2p_core::discovery::{DiscoveredMesh, mesh_from_discovery_event};
use p2p_core::pairing::PairingHello;
use p2p_core::pairing_handler::pair_with_mesh_interactive;
use p2p_core::{FileAllowlistStorage, PeerAddr};
use vault_sync::network::{RejectSyncHandler, SyncNode, SyncNodeSeam};

use crate::persistence::DaemonConfig;

/// How long to listen for mDNS broadcasts before giving up.
const DISCOVERY_TIMEOUT_SECS: u64 = 10;

/// Run `memory sync pair` interactively.
///
/// Discovers meshes on the LAN, prompts the user to select one, prompts for
/// the pairing code shown on the mesh member's console, and on success writes
/// all mesh members to the local allowlist.
pub async fn run(vault_path: PathBuf, device_name: Option<String>) -> Result<()> {
    // Resolve device name: use provided name, or system hostname, or fallback.
    let device_name = device_name.unwrap_or_else(|| {
        let hostname = gethostname::gethostname();
        hostname.to_str().unwrap_or("Sync Daemon").to_string()
    });

    // Load or generate the identity key for this vault.
    let (_, identity_key) = DaemonConfig::load_or_generate(&vault_path, None).await?;
    let secret_key_bytes = identity_key.secret_key_bytes();

    // Pairing creates a new mesh connection using the PAIRING_ALPN (separate from gossip).
    // We pass an empty allowlist here — the gossip handler won't reject the pairing peer
    // because pairing uses its own ALPN, not the gossip ALPN.
    let pairing_allowlist = Arc::new(InMemoryAllowlist::new());

    // Create a minimal SyncNode (no relay needed for LAN pairing). Pairing only
    // drives the pairing ALPN, so inbound sync streams are rejected.
    let sync_node = SyncNode::new_with_sync_handler(
        secret_key_bytes,
        &[],
        pairing_allowlist,
        RejectSyncHandler,
    )
    .await
    .context("Failed to create iroh SyncNode")?;

    // Run the pairing logic, then unconditionally shut down the SyncNode to
    // release UDP sockets and tokio tasks regardless of the outcome.
    let result = pair_inner(&sync_node, &vault_path, &device_name).await;

    if let Err(e) = sync_node.shutdown().await {
        tracing::warn!("Error during SyncNode shutdown: {}", e);
    }

    result
}

/// Inner pairing logic — separated so `run()` can shut down `sync_node` on all paths.
async fn pair_inner(
    sync_node: &SyncNode,
    vault_path: &std::path::Path,
    device_name: &str,
) -> Result<()> {
    let self_peer_id = PeerId::from_bytes(*sync_node.node_id().as_bytes());

    // Subscribe to mDNS discovery.
    let Some(discovery_stream) = sync_node.subscribe_discovery().await else {
        eprintln!("mDNS discovery is not available on this platform.");
        return Ok(());
    };

    eprintln!("Discovering nearby meshes...");

    // Collect meshes for up to 10 seconds, deduplicating by vault_id.
    let mut meshes: HashMap<String, DiscoveredMesh> = HashMap::new();

    let deadline = tokio::time::sleep(p2p_core::time_scale::scaled(Duration::from_secs(
        DISCOVERY_TIMEOUT_SECS,
    )));
    futures::pin_mut!(discovery_stream);
    futures::pin_mut!(deadline);

    loop {
        tokio::select! {
            biased;
            Some(event) = discovery_stream.next() => {
                if let Some(mesh) = mesh_from_discovery_event(&event) {
                    // A single peer re-announces many times during the discovery
                    // window (fast-burst requery fires ~every 1.5s), so only add
                    // an endpoint_id that hasn't been seen yet.
                    let endpoint_id = mesh.peers[0];
                    let entry = meshes.entry(mesh.vault_id.clone()).or_insert_with(|| DiscoveredMesh {
                        peers: vec![],
                        online_count: 0,
                        ..mesh.clone()
                    });
                    if !entry.peers.contains(&endpoint_id) {
                        entry.peers.push(endpoint_id);
                    }
                    entry.online_count = entry.peers.len();
                    debug!(mesh = %mesh.mesh_name, vid = %mesh.vault_id, "Discovered mesh");
                }
            }
            _ = &mut deadline => {
                break;
            }
        }
    }

    if meshes.is_empty() {
        eprintln!(
            "No meshes found nearby. Make sure the other device has sync enabled \
             and is on the same network."
        );
        return Ok(());
    }

    // Display numbered list of discovered meshes, sorted by name for deterministic output.
    let mut mesh_list: Vec<&DiscoveredMesh> = meshes.values().collect();
    mesh_list.sort_by(|a, b| a.mesh_name.cmp(&b.mesh_name));
    eprintln!();
    for (i, mesh) in mesh_list.iter().enumerate() {
        eprintln!(
            "  {}. {} ({} online)",
            i + 1,
            mesh.mesh_name,
            mesh.online_count
        );
    }
    eprintln!();

    // Prompt for selection.
    let selection = {
        eprint!("Select mesh to join [1]: ");
        io::stderr().flush()?;

        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        let trimmed = line.trim();

        if trimmed.is_empty() {
            1usize
        } else {
            trimmed
                .parse::<usize>()
                .context("Invalid selection — enter a number")?
        }
    };

    if selection < 1 || selection > mesh_list.len() {
        eprintln!("Selection out of range.");
        return Ok(());
    }

    let selected_mesh = mesh_list[selection - 1];

    // Pick the first available peer in the mesh.
    let peer_endpoint_id = selected_mesh
        .peers
        .first()
        .copied()
        .context("Selected mesh has no reachable peers")?;

    let hello = PairingHello {
        node_id: self_peer_id,
        device_name: device_name.to_string(),
    };

    eprintln!("Connecting to {}...", selected_mesh.mesh_name);

    // The target came from mDNS discovery (a real key); `PeerAddr` defers the
    // curve-point check to `connect()`, which is infallible for it.
    let peer = PeerAddr::new(peer_endpoint_id);
    let dial = sync_node.dial_handle();

    // Connect and run the interactive pairing exchange.
    let result = pair_with_mesh_interactive(&dial, peer, &hello, |challenge| async move {
        eprintln!(
            "Connected to '{}'. Enter the 6-digit code shown on that device:",
            challenge.device_name
        );
        eprint!("> ");
        io::stderr().flush()?;

        let mut code = String::new();
        io::stdin().read_line(&mut code)?;
        Ok(code.trim().to_string())
    })
    .await
    .context("Pairing connection failed")?;

    if !result.success {
        eprintln!("Pairing failed. The code may be wrong or expired. Try again.");
        return Ok(());
    }

    // Write the mesh roster to the local allowlist (shared with the tray path).
    let allowlist = FileAllowlistStorage::new(vault_path);
    p2p_core::write_pair_allowlist(&allowlist, self_peer_id, device_name, &result.mesh_members)
        .await;

    // Adopt the mesh's VaultId so the next `memory sync up` joins the right
    // gossip topic. The CLI process exits after pairing, so this is an on-disk
    // metadata rewrite (no live re-join). Persist the mesh relay URL too, for
    // cross-network sync on next start.
    //
    // A successful pair without a vault topic is protocol-impossible — the
    // responder always sends one — but structurally possible. Treat it as a
    // failure rather than a silent success: without the adopted id the next
    // `memory sync up` joins the wrong gossip topic, so pairing "succeeds" but
    // sync never works.
    let new_id = crate::pair_shared::vault_id_from_pairing_topic(result.vault_topic)
        .context("Paired, but the mesh did not provide a vault topic. Try again.")?;
    if let Err(e) = crate::pair_shared::adopt_vault_id_on_disk(vault_path, new_id).await {
        eprintln!("Warning: failed to adopt the mesh VaultId: {}", e);
    }
    crate::pair_shared::persist_adopted_relay(
        vault_path,
        peer_endpoint_id,
        &result.relay_urls,
        // The CLI pairing flow runs no relay of its own (SyncNode created with
        // no relay above), so there is no advertised relay_url to preserve.
        None,
        sync_node,
    )
    .await;

    eprintln!();
    eprintln!("Pairing complete. Start the sync daemon to begin syncing.");
    eprintln!("Run: memory sync up --vault <path>");

    Ok(())
}
