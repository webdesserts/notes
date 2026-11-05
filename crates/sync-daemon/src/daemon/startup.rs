//! Startup seam: lock acquisition, vault init, node startup, and public entry points.
//!
//! This module owns `startup_inner`, `StartupBundle`, and the three public entry points
//! (`run_with_shutdown_controlled`, `run_with_shutdown`, `run`). It is the only place
//! that constructs a real `NativeFs` / `FileAllowlistStorage` `Daemon`.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use p2p_core::FileAllowlistStorage;
use p2p_core::PeerId;
use p2p_core::RelayAddr;
use p2p_core::allowlist::AllowlistStorage;
use p2p_core::discovery::MeshMetadata;
use vault_sync::Vault;
use vault_sync::fs::FileSystem;
use vault_sync::network::{SyncNode, SyncNodeSeam, VaultGossipExt};

use crate::daemon_lock::DaemonLock;
use crate::http;
use crate::native_fs::NativeFs;
use crate::pair_api::{DaemonControl, DaemonStatus, PAIRING_BROADCAST_CAPACITY};
use crate::persistence::{DaemonConfig, PeerRelay};
use crate::watcher::FileWatcher;
use p2p_core::EmbeddedRelay;

use super::Daemon;
use super::move_recovery;

/// Start the daemon and return a control handle before the event loop begins.
///
/// Unlike `run_with_shutdown`, this function splits startup from the event loop:
///
/// - The **outer `Result`** covers everything up to and including `Daemon::new()` —
///   lock acquire, vault load, identity load, relay start, SyncNode creation, mDNS
///   publish, gossip join, health endpoint, file watcher. Startup failures bubble up
///   as `Err` before any `DaemonControl` is materialized.
/// - **`DaemonControl`** is yielded after `Daemon::new()` returns successfully, giving
///   the caller a way to observe status and drive pairing.
/// - The **inner `JoinHandle<Result<()>>`** owns the event loop and graceful shutdown.
///   Its `Result` covers only runtime errors (not startup failures).
///
/// If `shutdown` fires during startup, the function returns `Ok(...)` with the join
/// handle already resolved — callers can safely await it.
pub async fn run_with_shutdown_controlled(
    config: super::DaemonRunConfig,
    shutdown: CancellationToken,
) -> Result<(DaemonControl, JoinHandle<Result<()>>)> {
    // Run startup. If `shutdown` fires before startup completes, return a no-op handle.
    let startup_result = tokio::select! {
        result = startup_inner(&config, shutdown.clone()) => result,
        _ = shutdown.cancelled() => {
            // Cancel during startup — DaemonLock cleanup is RAII (dropped with the future).
            info!("Daemon shutdown requested during startup — exiting cleanly");

            // Best-effort: if startup_inner wrote relay_url to daemon.toml before the
            // future was cancelled (it writes synchronously after the relay starts, then
            // awaits SyncNode::new), clear it now. Without this, the file is left with a
            // stale relay_url that peers could read between this cancelled run and the next
            // startup (which also clears it, but only after re-starting).
            let config_path = config.vault.join(".sync/daemon.toml");
            if config_path.exists() {
                match std::fs::read_to_string(&config_path)
                    .map_err(anyhow::Error::from)
                    .and_then(|s| toml::from_str::<DaemonConfig>(&s).map_err(Into::into))
                {
                    Ok(mut cfg) if cfg.relay_url.is_some() => {
                        if let Err(e) = cfg.set_relay_url(None, &config.vault) {
                            warn!("Failed to clear relay URL after mid-startup cancel: {}", e);
                        }
                    }
                    Ok(_) => {} // relay_url already absent — nothing to do
                    Err(e) => {
                        warn!("Could not parse daemon.toml to clear relay URL after cancel: {}", e);
                    }
                }
            }

            // Return a no-op handle that resolves immediately.
            let handle = tokio::spawn(async { Ok(()) });
            let (status_tx, status_rx) = watch::channel(DaemonStatus::initial());
            let (pairing_tx, pairing_rx) = broadcast::channel(PAIRING_BROADCAST_CAPACITY);
            let (command_tx, _command_rx) = mpsc::unbounded_channel();
            drop(status_tx);
            drop(pairing_tx);
            let control = DaemonControl { status_rx, pairing_rx, command_tx };
            return Ok((control, handle));
        }
    };

    let StartupBundle {
        mut daemon,
        embedded_relay,
        mut daemon_config,
        vault_path,
        mesh_name,
        _daemon_lock,
        _watcher,
    } = startup_result?;

    // Wire control channels into the daemon before the loop starts.
    let (status_tx, status_rx) = watch::channel(DaemonStatus::initial());
    let (pairing_tx, pairing_rx) = broadcast::channel(PAIRING_BROADCAST_CAPACITY);
    let (command_tx, command_rx) = mpsc::unbounded_channel();

    daemon.wire_control(status_tx, pairing_tx.clone(), command_rx, mesh_name);

    // Emit the initial status (Idle, 0 peers, relay URL set) so the tray has something
    // to display immediately without waiting for the first peer event.
    daemon.emit_status().await;

    let control = DaemonControl {
        status_rx,
        pairing_rx,
        command_tx,
    };

    let handle = tokio::spawn(async move {
        // DaemonLock and FileWatcher are moved here so they live for the duration
        // of run_loop. The underscore prefix silences the "unused" warning while
        // making the RAII intent explicit — both must not drop before run_loop exits.
        let _daemon_lock = _daemon_lock;
        let _watcher = _watcher;

        daemon.run_loop().await;
        // Graceful shutdown — sync_node.shutdown() consumes the node;
        // daemon is owned here (moved into this closure) so this is safe.
        if let Err(e) = daemon.sync_node.shutdown().await {
            warn!("Error during iroh node shutdown: {}", e);
        }
        if let Some(relay) = embedded_relay {
            relay.shutdown().await;
            if let Err(e) = daemon_config.set_relay_url(None, &vault_path) {
                warn!("Failed to clear relay URL from daemon.toml: {}", e);
            }
        }
        Ok(())
    });

    Ok((control, handle))
}

/// Run the sync daemon with the given configuration, honoring an externally-supplied
/// cancellation token.
///
/// Cancelling `shutdown` at any point — including during startup — causes the function
/// to return cleanly. Delegates to [`run_with_shutdown_controlled`]; the returned
/// [`DaemonControl`] handle is dropped immediately (no tray or UI consumer in the
/// headless path).
pub async fn run_with_shutdown(
    config: super::DaemonRunConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let (_control, handle) = run_with_shutdown_controlled(config, shutdown).await?;
    // Drop _control: its receivers/sender have no consumer in the headless path.
    // Sends in the run loop are fire-and-forget (watch::Sender and broadcast::Sender
    // both discard when there are no receivers), so dropping here is safe.
    info!("Daemon running. Press Ctrl+C to stop.");
    handle.await?
}

/// Run the sync daemon with the given configuration.
///
/// This is the main entry point for embedding the daemon in the `memory` binary.
/// Assumes logging is already configured by the caller.
pub async fn run(config: super::DaemonRunConfig) -> Result<()> {
    let token = CancellationToken::new();
    let signal_token = token.clone();
    tokio::spawn(async move {
        notes_core::runtime::shutdown_signal().await;
        signal_token.cancel();
    });
    run_with_shutdown(config, token).await
}

/// Everything `startup_inner` returns to the caller after a successful startup.
///
/// Keeps `DaemonLock` and `FileWatcher` alive until they are explicitly moved into
/// the `run_loop` task — if either were dropped at the call site, the OS-level lock
/// and file watcher would stop before the daemon processes any events.
struct StartupBundle {
    daemon: Daemon<NativeFs, FileAllowlistStorage>,
    embedded_relay: Option<EmbeddedRelay>,
    daemon_config: DaemonConfig,
    vault_path: PathBuf,
    mesh_name: String,
    /// Holds the exclusive flock on `.sync/daemon.lock` for the daemon's lifetime.
    _daemon_lock: DaemonLock,
    /// Keeps the OS-level file watcher alive; dropping it stops event delivery.
    _watcher: FileWatcher,
}

/// Startup phase: lock acquisition, vault init, node startup.
///
/// Returns a [`StartupBundle`] that includes `DaemonLock` and `FileWatcher` so
/// the caller can move them into the spawned `run_loop` task, keeping both alive
/// for the daemon's full lifetime. All startup failures surface as `Err` before
/// any `StartupBundle` is materialized.
///
/// Used by `run_with_shutdown_controlled`; the event loop is run by the caller.
async fn startup_inner(
    config: &super::DaemonRunConfig,
    shutdown: CancellationToken,
) -> Result<StartupBundle> {
    // Acquire exclusive daemon lock — must outlive this function. It is moved into
    // the StartupBundle and from there into the spawned run_loop task.
    let daemon_lock = DaemonLock::acquire(&config.vault).context(
        "Failed to acquire daemon lock — is another daemon already running on this vault?",
    )?;

    // Seed the process-global test-time-scale from the env var, once, before any
    // scaled duration is constructed below. Unset → never seeded → scale stays
    // 1.0 → production timing is unchanged. This is the ONLY place the env var is
    // read; the desktop app and WASM plugin never seed it.
    if let Ok(raw) = std::env::var("OBSIDIAN_MEMORY_TIME_SCALE") {
        match raw.parse::<f64>() {
            Ok(scale) if p2p_core::time_scale::set_time_scale(scale) => {
                info!(
                    scale,
                    "OBSIDIAN_MEMORY_TIME_SCALE active — durations scaled (test mode)"
                );
            }
            Ok(_) => {}
            Err(_) => warn!(raw = %raw, "Ignoring unparseable OBSIDIAN_MEMORY_TIME_SCALE"),
        }
    }

    info!("Starting sync daemon");
    info!("Vault path: {:?}", config.vault);

    let fs = NativeFs::new(config.vault.clone());

    // Load identity before the vault so we can author Loro ops under this
    // device's PeerId (config-load has no dependency on the vault).
    let (mut daemon_config, identity_key) =
        DaemonConfig::load_or_generate(&config.vault, config.identity_key.as_deref()).await?;

    info!("Daemon PeerId: {}", daemon_config.peer_id);

    // vault-sync authors Loro ops under a bare u64; derive it from this device's PeerId.
    let author = identity_key.peer_id().as_u64();

    // Move crash-recovery (P4f-2b-ii): a previous run may have crashed with a native
    // move buffered in the coalescer journal (`.sync/pending-moves.json`). Read it
    // BEFORE `fs` is moved into the load, map its DELETE records into re-stitch inputs,
    // and feed them to boot reconcile so a buffered move's lineage is re-attached at
    // the new path. The full `records` set (creates included) is retained for the
    // post-`Daemon::new` finalize. A fresh vault has no `.sync/`, so no journal.
    let recovery_records = if fs.exists(".sync").await? {
        move_recovery::read_pending_journal(&fs).await
    } else {
        Vec::new()
    };
    let restitch = move_recovery::restitch_inputs(&recovery_records);

    let vault = if fs.exists(".sync").await? {
        info!("Loading existing vault");
        Vault::load_with_journal(fs, author, Some(&restitch)).await?
    } else {
        info!("Initializing new vault");
        Vault::init(fs, author).await?
    };

    let vault_id = vault.vault_id();
    info!("Vault loaded, vault ID: {}", vault_id);

    // Wrap the vault in its shared handle NOW, before the SyncNode is built: the
    // pumped inbound handler (registered on `SYNC_ALPN` at SyncNode construction)
    // needs this exact Arc to process inbound syncs inline. The same Arc is later
    // handed to `Daemon::new`, so the daemon and the handler share one vault.
    let vault = Arc::new(Mutex::new(vault));

    if daemon_config.relay_url.is_some() {
        info!("Clearing stale relay URL from previous run");
        if let Err(e) = daemon_config.set_relay_url(None, &config.vault) {
            warn!("Failed to clear stale relay URL: {}", e);
        }
    }

    // Start the embedded relay before the SyncNode so we can pass its URL in.
    // When advertised_relay_url is set, bind on relay_listen but tell peers to dial
    // the advertised address (e.g. LAN IP instead of 0.0.0.0).
    // Failure is non-fatal: the daemon continues without relay support.
    let embedded_relay: Option<EmbeddedRelay> = if let Some(ref addr_str) = config.relay_listen {
        match addr_str.parse() {
            Ok(bind_addr) => {
                let relay_result = if let Some(ref adv_url) = config.advertised_relay_url {
                    EmbeddedRelay::start_with_advertised_url(bind_addr, adv_url).await
                } else {
                    EmbeddedRelay::start(bind_addr).await
                };
                match relay_result {
                    Ok(relay) => {
                        info!(url = %relay.relay_url().as_str(), "Embedded relay started");
                        Some(relay)
                    }
                    Err(e) => {
                        warn!("Failed to start embedded relay, continuing without: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Invalid relay-listen address '{}': {}, continuing without relay",
                    addr_str, e
                );
                None
            }
        }
    } else {
        None
    };

    let relay_url = embedded_relay.as_ref().map(|r| r.relay_url().clone());

    if let Some(ref url) = relay_url {
        if let Err(e) = daemon_config.set_relay_url(Some(url.as_str()), &config.vault) {
            warn!("Failed to persist relay URL to daemon.toml: {}", e);
        }

        // A SERVER's own relay belongs in `known_public_relays` too: that set is
        // "all public relays this node knows," and this node IS one of them. Without
        // this, a server only ever learns OTHERS' relays (via pairing-adopt), so its
        // own set stays empty → its cross-product `(allowlist × known_public_relays)`
        // is empty → its reconnect supervisor sits idle with no dial targets after a
        // restart. Seeding the own relay gives e.g. umbra a `(laptops) × {umbra}`
        // cross-product so it can re-dial laptops through its own relay (bidirectional
        // recovery). `add_known_public_relay`'s off-LAN-reachable guard keeps a
        // loopback-bound relay out (correctly — it's no use as a dial target), and it
        // dedups, so this is a no-op once the public domain is already present.
        daemon_config.add_known_public_relay(&url.as_str());
        if let Err(e) = daemon_config.save(&config.vault) {
            warn!(
                "Failed to persist own relay into known_public_relays: {}",
                e
            );
        }
    }

    let secret_key_bytes = identity_key.secret_key_bytes();
    let allowlist = Arc::new(FileAllowlistStorage::new(&config.vault));

    // Parse the persisted public-relay set once — it feeds BOTH wires below:
    // this node's RelayMap (a laptop's home/failover) and, crossed with the
    // allowlist, the live peer_lookup + supervisor snapshot. Malformed entries
    // are skipped with a warning rather than failing startup.
    let public_relays: Vec<RelayAddr> = daemon_config
        .known_public_relays
        .iter()
        .filter_map(|url| match RelayAddr::parse(url) {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                warn!(relay_url = %url, "Skipping known public relay — invalid URL: {e}");
                None
            }
        })
        .collect();

    // The set of relays this node homes on (its `RelayMap`).
    //
    // A SERVER (embedded relay running → `relay_url.is_some()`) homes on its OWN
    // public relay: it IS a relay, so it must be present in its own RelayMap to be
    // reachable. This is independent of `known_public_relays` — a server with an
    // empty public set still homes on its own relay (a server whose RelayMap ended
    // up `Disabled` would be unreachable, breaking the whole mesh).
    //
    // A LAPTOP (no embedded relay) homes on the learned `known_public_relays` set,
    // so it homes on a public relay it can reach off-LAN (and fails over across the
    // set). An empty set → `RelayMode::Disabled` (LAN-only — the never-met-a-server
    // edge).
    let home_relays: Vec<RelayAddr> = match relay_url.as_ref() {
        Some(own) => vec![own.clone()],
        None => public_relays.clone(),
    };
    // The daemon runs the variable-length vault-sync handshake, which needs a
    // PUMPED inbound path (multiple process_message turns over one bi-stream), not
    // the default one-shot SyncStreamHandler. Build our own handler holding the
    // shared vault + allowlist and register it on `SYNC_ALPN` via the additive
    // `new_with_sync_handler`. The handler stamps allowlist freshness inline and
    // fires the peer id on `inbound_seen_tx`; the run-loop drains the matching
    // receiver to stamp peer_registry liveness (S2). sync-wasm keeps the default
    // one-shot handler — this seam is opt-in.
    let (inbound_seen_tx, inbound_seen_rx) = mpsc::unbounded_channel();
    let sync_handler = crate::daemon::sync_stream::PumpedSyncHandler::new(
        vault.clone(),
        allowlist.clone(),
        inbound_seen_tx,
    );
    let sync_node = SyncNode::new_with_sync_handler(
        secret_key_bytes,
        &home_relays,
        allowlist.clone(),
        sync_handler,
    )
    .await
    .context("Failed to create iroh SyncNode")?;

    info!(node_id = %sync_node.node_id(), "Iroh node started");

    // Seed the live peer_lookup + the supervisor's working snapshot from the
    // CROSS-PRODUCT `(allowlist peers) × (known_public_relays)`. There is no
    // persisted per-peer relay hint anymore: to reach a paired peer off-LAN we try
    // its EndpointId through each known public relay (a peer registered on a public
    // relay is reached by dialing it through that relay). On-LAN peers are still
    // found live by mDNS, so they need no hint here.
    //
    // Two-wires separation: `known_public_relays` → this node's RelayMap (above);
    // `(allowlist × known_public_relays)` → peer_lookup + supervisor snapshot (here).
    //
    // SECURITY: the EndpointIds come from the ALLOWLIST (the trust anchor); the
    // relay URLs are untrusted transport hints. A relay never contributes or
    // changes an EndpointId — it is only ever paired with already-trusted IDs.
    // `connect(EndpointAddr::new(id).with_relay_url(url))` TLS-verifies `id`
    // regardless of `url`, so a hostile relay is DoS/metadata only, never
    // impersonation.
    //
    // Built HERE (before `bootstrap_ids` moves into `join_vault_gossip` and the
    // `allowlist` Arc moves into `Daemon::new`) so the same cross-product feeds
    // both wires, computed once. The resulting `Vec<PeerRelay>` is handed to
    // `seed_peer_relays_snapshot` after `Daemon::new`.
    let own_peer_id = sync_node.node_id();
    let supervisor_seed: Vec<PeerRelay> = {
        let allowlist_peers = match allowlist.list_peers().await {
            Ok(peers) => peers,
            Err(e) => {
                warn!("Failed to read allowlist for peer-relay seed: {}", e);
                vec![]
            }
        };
        let mut seed = Vec::new();
        for peer in &allowlist_peers {
            // Validate the peer key, skipping a legacy/non-curve-point id
            // gracefully (never panicking) — the deliberate degrade this seed has
            // always done. The seed itself speaks `PeerId`.
            if !peer.node_id.is_dialable() {
                warn!("Skipping invalid allowlist peer for peer-relay seed");
                continue;
            }
            // Skip self: the first-pair bootstrap adds this node to its OWN
            // allowlist, but seeding ourselves would make the supervisor (and
            // gossip bootstrap) try to dial this node through a relay — iroh
            // rejects self-directed relay paths. Mirrors `add_peer_relay`'s
            // self-skip on the live lookup.
            if peer.node_id == own_peer_id {
                continue;
            }
            let endpoint_hex = peer.node_id.to_string();
            for relay in &public_relays {
                sync_node.add_peer_relay(peer.node_id, relay);
                seed.push(PeerRelay::new(endpoint_hex.clone(), relay.as_str()));
            }
        }
        seed
    };
    if !supervisor_seed.is_empty() {
        info!(
            count = supervisor_seed.len(),
            "Seeded peer_lookup from (allowlist × known_public_relays)"
        );
    }

    let mesh_name = daemon_config.mesh_name.clone().unwrap_or_else(|| {
        config
            .vault
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("Obsidian Vault")
            .to_string()
    });
    let mesh_metadata = MeshMetadata {
        mesh: mesh_name.clone(),
        vid: vault_id.to_string(),
        ver: 1,
    };
    sync_node.publish_mesh_info(&mesh_metadata, relay_url.as_ref());

    let bootstrap_ids: Vec<PeerId> = match allowlist.list_peers().await {
        Ok(peers) => peers
            .iter()
            // Validate each key, skipping a non-curve-point id gracefully (it could
            // never be a reachable bootstrap target); keep the `PeerId`.
            .filter(|p| {
                let dialable = p.node_id.is_dialable();
                if !dialable {
                    warn!("Skipping invalid allowlist peer for gossip bootstrap");
                }
                dialable
            })
            .map(|p| p.node_id)
            .collect(),
        Err(e) => {
            warn!("Failed to read allowlist for gossip bootstrap: {}", e);
            vec![]
        }
    };

    let vault_gossip = sync_node
        .join_vault_gossip(&vault_id, bootstrap_ids)
        .await
        .context("Failed to join vault gossip topic")?;

    info!("Joined vault gossip topic");

    if let Some(ref health_addr) = config.health_listen {
        let health_addr = health_addr.clone();
        tokio::spawn(async move {
            http::serve_health(&health_addr).await;
        });
        info!(
            "Health endpoint started on {}",
            config.health_listen.as_ref().unwrap()
        );
    }

    let watcher = FileWatcher::new(config.vault.clone())?;
    info!("File watcher started");

    let discovery_rx: Option<tokio::sync::mpsc::Receiver<p2p_core::discovery::DiscoveredMesh>> = {
        use futures::StreamExt;
        use p2p_core::discovery::{DiscoveryEvent, mesh_from_discovery_event};

        if let Some(stream) = sync_node.subscribe_discovery().await {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            tokio::spawn(async move {
                futures::pin_mut!(stream);
                while let Some(event) = stream.next().await {
                    match event {
                        DiscoveryEvent::Discovered { .. } => {
                            if let Some(mesh) = mesh_from_discovery_event(&event) {
                                let _ = tx.try_send(mesh);
                            }
                        }
                        DiscoveryEvent::Expired { endpoint_id } => {
                            tracing::debug!(peer = %endpoint_id, "mDNS: peer expired");
                        }
                    }
                }
            });
            Some(rx)
        } else {
            None
        }
    };

    let device_name = {
        let hostname = gethostname::gethostname();
        hostname.to_str().unwrap_or("Sync Daemon").to_string()
    };
    info!(device_name = %device_name, "Device name resolved for pairing");

    // The watcher must outlive this function — it is moved into StartupBundle and
    // from there into the spawned run_loop task. Dropping it would stop OS events.
    let (file_event_rx, watcher) = watcher.into_event_rx();

    let mut daemon = Daemon::new(
        vault,
        sync_node,
        vault_gossip,
        file_event_rx,
        discovery_rx,
        allowlist,
        device_name,
        relay_url.as_ref().map(|u| u.as_str()),
        config.vault.clone(),
        shutdown,
    );

    // Drain the inbound-sync freshness channel (the other half of the one the
    // pumped handler sends on) so the run-loop stamps peer_registry liveness for
    // inbound-only peers (S2).
    daemon.set_inbound_seen_rx(inbound_seen_rx);

    // Wire the filesystem handle for the move-coalescer's crash-recovery journal
    // (`.sync/pending-moves.json`, P4f-2). The earlier `fs` was moved into
    // `Vault::load`/`init`, and `NativeFs` is a stateless path-wrapper, so a fresh
    // instance over the same vault path is equivalent — both resolve to the same
    // on-disk `.sync/` directory.
    let daemon_fs = Arc::new(NativeFs::new(config.vault.clone()));
    daemon.set_fs(daemon_fs.clone());

    // Finish move crash-recovery (P4f-2b-ii) BEFORE the run-loop spawns: re-stitch
    // already ran inside `load_with_journal`, so finalize the unmatched remainder (a
    // journaled delete whose old node is still live → a real tombstone via the
    // idempotent `on_file_deleted`), THEN clear the journal in one write so the next
    // boot recovers nothing. Order matters: a crash between finalize and clear leaves
    // the journal intact and the next boot re-runs the idempotent pass. At this point
    // no peer is alive (`alive_count() == 0`), so the finalize's broadcast is a no-op
    // and no half-recovered state reaches peers (§3.3).
    daemon.finalize_recovered_journal(&recovery_records).await;
    move_recovery::clear_pending_journal(daemon_fs.as_ref()).await;

    // Give the reconnect supervisor its starting address book — the same
    // cross-product `(allowlist × known_public_relays)` the live lookup was just
    // seeded with. After a partition, the supervisor re-bootstraps gossip from this
    // snapshot without a restart. (The freshness fields are runtime-only state, so
    // a restart resetting backoff is fine — nothing here is read from disk.)
    daemon.seed_peer_relays_snapshot(supervisor_seed);

    // Network-change detection: iroh's watch_addr fires when the endpoint's relay
    // or direct addresses change (i.e. the network changed). Forward each change
    // as a () so the run_loop can reset reconnect backoff and re-dial without a
    // restart. The endpoint is read here, after Daemon::new took ownership of
    // sync_node, but before daemon is moved into StartupBundle.
    //
    // We are only ONE consumer of this signal. iroh consumes the same change
    // internally to rebind sockets, re-home its relay, and re-publish our updated
    // EndpointAddr over gossip (the warm re-announce — see `join_vault_gossip`). So
    // do NOT add manual re-advertising of the iroh-level EndpointAddr on the GOSSIP
    // channel — iroh already does that, and only for peers we're already connected to.
    // That is a DIFFERENT channel from our mDNS service registration, which
    // `on_network_change` DOES kick (republish addresses + restart browse) to
    // re-establish LAN discovery with peers we got partitioned from on the move —
    // the one thing the gossip re-announce can't reach. Our other job here is to
    // re-inject gossip bootstrap (reset backoff → the supervisor re-dials), since a
    // partition empties gossip's peer views.
    {
        use futures::StreamExt;
        // Capacity 1: a single wifi switch emits several address changes; they
        // coalesce into one backoff reset (resetting twice is a no-op).
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        // `watch_net_changes` wraps iroh's `watch_addr().stream_updates_only()`
        // (skips the current address, so we react only to real changes — no
        // spurious reset at startup) and hides `iroh::Watcher`.
        let mut addr_stream = daemon.sync_node.watch_net_changes();
        tokio::spawn(async move {
            while addr_stream.next().await.is_some() {
                // try_send: if a reset is already queued, dropping the extra change
                // is correct — the handler re-dials ALL hints regardless of how
                // many changes fired. A closed channel (receiver gone, i.e. daemon
                // shut down) ends the task.
                if tx.try_send(()).is_err() && tx.is_closed() {
                    break;
                }
            }
        });
        daemon.set_net_change_rx(rx);
    }

    Ok(StartupBundle {
        daemon,
        embedded_relay,
        daemon_config,
        vault_path: config.vault.clone(),
        mesh_name,
        _daemon_lock: daemon_lock,
        _watcher: watcher,
    })
}
