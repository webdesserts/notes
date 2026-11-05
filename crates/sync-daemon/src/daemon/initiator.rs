//! Initiator pairing state machine for the two-step GUI pairing flow.
//!
//! This module owns the types and `impl Daemon` methods that drive the GUI-facing
//! pairing workflow: `StartDiscovery` → `RequestPairing` → `SubmitCode`. The
//! `run_initiator_pairing_parked` free function drives the QUIC exchange itself.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use p2p_core::allowlist::AllowlistStorage;
use p2p_core::discovery::MeshMetadata;
use p2p_core::pairing_handler::pair_with_mesh_interactive;
use p2p_core::{DialHandle, PeerAddr, RelayAddr};
use vault_sync::network::VaultGossipExt;
// `VaultId` here is the mesh's id recovered from the gossip topic (a
// `vault_sync::VaultId`), used by the iroh layer. It is bridged to a
// `vault_sync::VaultId` only at the `vault.adopt_vault_id` boundary below.
use p2p_core::PeerId;
use p2p_core::pairing::PairingHello;
use vault_sync::VaultId;
use vault_sync::fs::FileSystem;

use super::Daemon;

/// How long the initiator window scans for nearby meshes before stopping.
pub(super) const INITIATOR_DISCOVERY_TIMEOUT_SECS: u64 = 10;

/// A completed initiator pairing exchange, routed from the spawned pairing task
/// back to the event loop so the post-pair onboarding (allowlist write, VaultId
/// adoption + gossip re-join, relay persist) runs under `&mut self`.
///
/// Only a *completed* `PairingResult` travels this channel — whether it
/// succeeded or failed the HMAC check. Cancellation and connection errors reply
/// directly from the spawned task and never reach here, so the event loop only
/// ever sees outcomes that warrant adoption work.
pub(super) struct InitiatorPairOutcome {
    pub(super) result: p2p_core::pairing::PairingResult,
    pub(super) responder_device_name: String,
    /// The transport-verified `PeerId` of the responder — the QUIC connection
    /// target the initiator dialed. Used to key the persisted relay hint so we
    /// don't infer the responder's identity from `mesh_members` (which is not
    /// transport-verified).
    pub(super) responder_endpoint_id: PeerId,
}

/// State tracked between `StartDiscovery`, `RequestPairing`, and `SubmitCode`
/// for an in-flight initiator pairing session.
///
/// The discovery task writes to `discovered` as mDNS produces results.
/// `RequestPairing` resolves the peer, parks the QUIC connection, and stores
/// `code_tx`. `SubmitCode` stores `submit_reply` then fires `code_tx` to
/// unblock the parked task.
pub(super) struct InitiatorSession {
    /// Maps `vault_id` to the first observed peer's id, used by
    /// `RequestPairing` to dial the mesh.
    pub(super) discovered: Arc<Mutex<HashMap<String, PeerId>>>,
    /// Cancels the discovery scan and any in-progress pairing attempt.
    pub(super) cancel: CancellationToken,
    /// Filled by `RequestPairing` after the parked task spawns. `SubmitCode`
    /// takes it to unblock the `get_code` callback with the typed code.
    pub(super) code_tx: Option<oneshot::Sender<String>>,
    /// Stored by `SubmitCode` so the final `PairingResult` routes back to the
    /// Pair button's reply after post-pair onboarding. Taken by
    /// `on_initiator_pair_outcome`.
    pub(super) submit_reply: Option<oneshot::Sender<Result<String, String>>>,
}

impl<FS: FileSystem + 'static, AL: AllowlistStorage + 'static> Daemon<FS, AL> {
    /// Seed the active initiator's discovered map for testing.
    ///
    /// Integration tests have no mDNS, so there's no natural way for the
    /// discovered map to be populated. This helper pre-populates `vault_id →
    /// endpoint_id` so tests can drive `RequestPairing` without real mDNS.
    ///
    /// This method is `pub` solely for integration test access — use only in
    /// test code. Production callers should go through `StartDiscovery`.
    pub async fn test_seed_discovered(&mut self, vault_id: String, endpoint_id: PeerId) {
        if self.active_initiator.is_none() {
            let cancel = CancellationToken::new();
            self.active_initiator = Some(InitiatorSession {
                discovered: Arc::new(Mutex::new(HashMap::new())),
                cancel,
                code_tx: None,
                submit_reply: None,
            });
        }
        if let Some(ref session) = self.active_initiator {
            session
                .discovered
                .lock()
                .await
                .insert(vault_id, endpoint_id);
        }
    }

    /// Begin mDNS discovery for the initiator window.
    ///
    /// Cancels any prior initiator session, then spawns a task that subscribes
    /// to mDNS for `INITIATOR_DISCOVERY_TIMEOUT_SECS` and forwards each
    /// `DiscoveredMesh` to `reply`. The same task records `vault_id` →
    /// `EndpointId` in `active_initiator.discovered` so a subsequent
    /// `SubmitCode` can resolve the peer without re-scanning.
    ///
    /// Closing `reply` (by dropping the sender at the end of the task) signals
    /// "discovery finished" to the desktop side, which then surfaces the
    /// `pair://discovery-finished` event to the window.
    pub(super) async fn start_initiator_discovery(
        &mut self,
        reply: mpsc::UnboundedSender<p2p_core::discovery::DiscoveredMesh>,
    ) {
        // Replace any prior session, cancelling its in-flight tasks.
        if let Some(prev) = self.active_initiator.take() {
            prev.cancel.cancel();
        }

        let cancel = CancellationToken::new();
        let discovered = Arc::new(Mutex::new(HashMap::<String, PeerId>::new()));

        self.active_initiator = Some(InitiatorSession {
            discovered: discovered.clone(),
            cancel: cancel.clone(),
            code_tx: None,
            submit_reply: None,
        });

        let Some(stream) = self.sync_node.subscribe_discovery().await else {
            debug!("mDNS discovery not available on this platform; closing reply");
            drop(reply);
            return;
        };

        tokio::spawn(async move {
            use futures::StreamExt;
            use p2p_core::discovery::mesh_from_discovery_event;

            let deadline = tokio::time::sleep(p2p_core::time_scale::scaled(
                std::time::Duration::from_secs(INITIATOR_DISCOVERY_TIMEOUT_SECS),
            ));
            futures::pin_mut!(stream);
            futures::pin_mut!(deadline);

            loop {
                tokio::select! {
                    Some(event) = stream.next() => {
                        if let Some(mesh) = mesh_from_discovery_event(&event) {
                            // Dedupe by vault_id: only emit on first sighting.
                            // mDNS re-advertises every ~5s, so without this guard
                            // the UI would receive a flood of identical events.
                            // This is stricter than `pair.rs` — the CLI tracks
                            // additional peers in the same mesh and updates
                            // `online_count`. Phase 1.5's UI displays only the
                            // mesh name + a "1 online" hint, so the first sighting
                            // is enough; a richer peer count is Phase 6 work.
                            let endpoint_id = mesh.peers[0];
                            let mut map = discovered.lock().await;
                            let is_new = !map.contains_key(&mesh.vault_id);
                            map.entry(mesh.vault_id.clone()).or_insert(endpoint_id);
                            drop(map);

                            if !is_new {
                                continue;
                            }

                            // Send failures mean the desktop dropped the
                            // receiver (window closed) — stop the scan early.
                            if reply.send(mesh).is_err() {
                                debug!("Initiator discovery reply channel closed; ending scan");
                                return;
                            }
                        }
                    }
                    _ = &mut deadline => {
                        debug!("Initiator discovery scan window elapsed");
                        return;
                    }
                    _ = cancel.cancelled() => {
                        debug!("Initiator discovery cancelled");
                        return;
                    }
                }
            }
        });
    }

    /// Connect to the selected mesh's peer and park the QUIC connection open.
    ///
    /// Step 1 of the two-step GUI pairing flow. Resolves the peer endpoint from
    /// the active discovery session, spawns a task that opens the QUIC connection
    /// and sends `PairingHello` (triggering the responder to generate + display its
    /// code), then parks awaiting a code delivered later by `SubmitCode`. On
    /// connect, `reply` receives `Ok(responder_device_name)` — the UI's cue to
    /// reveal the code entry step. Connect errors reply `Err(...)` directly.
    pub(super) async fn request_initiator_pairing(
        &mut self,
        vault_id: String,
        reply: oneshot::Sender<Result<String, String>>,
    ) {
        let Some(session) = self.active_initiator.as_mut() else {
            let _ = reply.send(Err(
                "No active pairing session. Click 'Pair with nearby device…' first.".to_string(),
            ));
            return;
        };

        let peer_endpoint_id = {
            let map = session.discovered.lock().await;
            map.get(&vault_id).copied()
        };

        let Some(peer_endpoint_id) = peer_endpoint_id else {
            let _ = reply.send(Err(
                "Selected mesh has no discovered peers yet. Wait for discovery to find a peer."
                    .to_string(),
            ));
            return;
        };

        // Cancel any prior parked pairing attempt before spawning a fresh one.
        // (Covers re-request after a failed connect.)
        session.cancel.cancel();
        let cancel = CancellationToken::new();
        session.cancel = cancel.clone();
        session.code_tx = None;
        session.submit_reply = None;

        let (code_tx, code_rx) = oneshot::channel::<String>();
        session.code_tx = Some(code_tx);

        // A cheap-clone dial handle for the detached, parked pairing task — NOT a
        // borrow of `self.sync_node` (the task is `'static` and parks awaiting a
        // code) and NOT an `Arc<SyncNode>` (which would block `shutdown(self)`).
        let dial = self.sync_node.dial_handle();
        let self_node_id = *self.sync_node.node_id().as_bytes();
        let device_name = self.device_name.clone();
        let outcome_tx = self.initiator_outcome_tx.clone();

        tokio::spawn(async move {
            let exchange = tokio::select! {
                r = run_initiator_pairing_parked(
                    &dial,
                    peer_endpoint_id,
                    self_node_id,
                    &device_name,
                    reply,
                    code_rx,
                ) => r,
                _ = cancel.cancelled() => {
                    // Cancellation drops code_rx, which wakes any awaiting code_tx.send()
                    // in SubmitCode with a SendError (harmless — SubmitCode checks for that).
                    return;
                }
            };

            match exchange {
                Some((result, responder_device_name)) => {
                    // A completed exchange (success or bad HMAC) must be onboarded under
                    // `&mut self`. If the send fails the daemon is shutting down.
                    //
                    // `peer_endpoint_id` is the QUIC connection target the initiator dialed
                    // — transport-verified by the TLS handshake. We carry it through so
                    // `on_initiator_pair_outcome` can key the persisted relay hint without
                    // guessing from `mesh_members`.
                    let outcome = InitiatorPairOutcome {
                        result,
                        responder_device_name,
                        responder_endpoint_id: peer_endpoint_id,
                    };
                    let _ = outcome_tx.send(outcome);
                }
                None => {
                    // Connect error — already replied via connect_reply inside the function.
                }
            }
        });
    }

    /// Deliver the typed 6-digit code into the parked pairing request.
    ///
    /// Step 2 of the two-step GUI pairing flow. Takes `code_tx` from the session
    /// (set by `RequestPairing`) to unblock the parked `get_code` callback.
    /// Stores `reply` on the session so `on_initiator_pair_outcome` can route the
    /// final `PairingResult` back to the Pair button after post-pair onboarding.
    pub(super) async fn submit_initiator_code(
        &mut self,
        vault_id: String,
        code: String,
        reply: oneshot::Sender<Result<String, String>>,
    ) {
        let Some(session) = self.active_initiator.as_mut() else {
            let _ = reply.send(Err(
                "No active pairing session. Click 'Pair with nearby device…' first.".to_string(),
            ));
            return;
        };

        // Verify the code is for the mesh that was selected at request time.
        // This fires when the vault_id doesn't match the discovered map — e.g.
        // a stale window submitting a code for a mesh that was never discovered
        // in this session.
        {
            let map = session.discovered.lock().await;
            if !map.contains_key(&vault_id) {
                let _ = reply.send(Err(
                    "That mesh is no longer the active pairing target.".to_string()
                ));
                return;
            }
        }

        let Some(code_tx) = session.code_tx.take() else {
            let _ = reply.send(Err(
                "No pairing request in progress. Click 'Request pairing' first.".to_string(),
            ));
            return;
        };

        // Store the Pair button's reply before unblocking the parked task. The
        // outcome channel routes the PairingResult back here after onboarding.
        session.submit_reply = Some(reply);

        if code_tx.send(code).is_err() {
            // The parked task has already exited (connect error or cancellation).
            let reply = session.submit_reply.take();
            if let Some(r) = reply {
                let _ = r.send(Err(
                    "Pairing request is no longer active. Try again.".to_string()
                ));
            }
        }
    }

    /// Run the shared post-pair onboarding for a completed initiator exchange,
    /// then reply to the desktop Pair button's oneshot.
    ///
    /// Runs on the event loop (`&mut self`) so it can adopt the mesh VaultId and
    /// re-join gossip in place. The shared helper writes the allowlist; on a
    /// successful pair we then adopt + re-join + persist the relay. A failed HMAC
    /// check replies with the standard "wrong/expired code" error. Every path
    /// sends exactly one reply.
    pub(super) async fn on_initiator_pair_outcome(&mut self, outcome: InitiatorPairOutcome) {
        let InitiatorPairOutcome {
            result,
            responder_device_name,
            responder_endpoint_id,
        } = outcome;

        // Recover the Pair button's reply oneshot from the session. If absent
        // (race: session was cancelled between SubmitCode and here), log + drop.
        let reply = self
            .active_initiator
            .as_mut()
            .and_then(|s| s.submit_reply.take());

        let Some(reply) = reply else {
            warn!(
                "on_initiator_pair_outcome: no submit_reply to route result (session cancelled?)"
            );
            return;
        };

        if !result.success {
            let _ = reply.send(Err(
                "Pairing failed. The code may be wrong or expired. Try again.".to_string(),
            ));
            return;
        }

        let self_peer_id = PeerId::from_bytes(*self.sync_node.node_id().as_bytes());
        p2p_core::write_pair_allowlist(
            self.allowlist.as_ref(),
            self_peer_id,
            &self.device_name,
            &result.mesh_members,
        )
        .await;

        // Recover the mesh VaultId from the topic and adopt it: rewrite
        // metadata.toml, re-join the mesh's gossip topic, re-publish mDNS.
        //
        // A successful pair without a vault topic is protocol-impossible — the
        // responder always sends one — but structurally possible. Treat it as a
        // failure rather than a silent success: skipping adoption would land the
        // device on the wrong gossip topic, so pairing "succeeds" but sync never
        // works. Surfacing the error lets the user retry instead.
        let Some(new_vault_id) =
            crate::pair_shared::vault_id_from_pairing_topic(result.vault_topic)
        else {
            error!("Pairing succeeded but the mesh did not provide a vault topic");
            let _ = reply.send(Err(
                "Paired, but the mesh did not provide a vault topic. Try again.".to_string(),
            ));
            return;
        };

        if let Err(e) = self
            .adopt_and_rejoin(new_vault_id, result.mesh_members.clone())
            .await
        {
            error!("Failed to adopt mesh VaultId after pairing: {:#}", e);
            let _ = reply.send(Err(format!("Paired, but failed to join the mesh: {e:#}")));
            return;
        }

        // Adopt the responder's PUBLIC relay into the persisted public-relay set
        // (the cold off-LAN store) and seed the live lookup so the current session
        // can reach them through their relay without a restart.
        //
        // `responder_endpoint_id` is the QUIC connection target the initiator
        // dialed — not inferred from `mesh_members` — so the binding is correct
        // even if `mesh_members` ordering or contents differ.
        crate::pair_shared::persist_adopted_relay(
            &self.vault_path,
            responder_endpoint_id,
            &result.relay_urls,
            self.relay_url.clone(),
            &self.sync_node,
        )
        .await;

        // Give the reconnect supervisor an in-memory per-peer hint for this
        // freshly-paired peer so a post-pair partition can re-dial it without a
        // restart. This is distinct from what `persist_adopted_relay` wrote: that
        // adopted the responder's relay into the persisted PUBLIC-relay set (a
        // node-level home/failover input), whereas the supervisor's working set is
        // per-peer. The peer was added to the allowlist AFTER boot, so it is NOT in
        // the startup `(allowlist × known_public_relays)` cross-product seed —
        // hence this in-memory-only twin. Mirrors persist's URL selection (first
        // non-empty advertised relay).
        if let Some(url_str) = result.relay_urls.iter().find(|u| !u.is_empty()).cloned() {
            self.upsert_peer_relay_snapshot(responder_endpoint_id.to_string(), url_str);
        }

        let _ = reply.send(Ok(responder_device_name));
    }

    /// Adopt a new VaultId and re-subscribe to its gossip topic at runtime.
    ///
    /// Used after a pairing initiator joins an existing mesh: the device must
    /// abandon its own VaultId and land on the mesh's gossip topic so a
    /// `NeighborUp` fires and the full-sync pull begins. Steps:
    /// 1. Rewrite `.sync/metadata.toml` + in-memory id (`Vault::adopt_vault_id`).
    /// 2. Join gossip on the new topic, bootstrapping off the mesh members.
    /// 3. Swap `self.vault_gossip` — dropping the old `VaultGossip` auto-leaves
    ///    the old topic (iroh-gossip leaves once both sender + receiver drop).
    /// 4. Re-publish mDNS so LAN discovery groups us under the new VaultId.
    ///
    /// Runs to completion within a single event-loop turn, so the next
    /// `run_loop` `select!` re-borrows the freshly-swapped `self.vault_gossip`.
    pub async fn adopt_and_rejoin(
        &mut self,
        new_vault_id: VaultId,
        mesh_members: Vec<PeerId>,
    ) -> Result<()> {
        // 1. Adopt the VaultId (metadata.toml + in-memory), recovered from the
        //    gossip topic.
        self.vault.lock().await.adopt_vault_id(new_vault_id).await?;

        // 2. Bootstrap gossip off the mesh members (same validate-and-keep as
        //    startup): skip a non-curve-point id gracefully, keep the `PeerId`.
        let bootstrap_ids: Vec<PeerId> = mesh_members
            .iter()
            .filter(|p| {
                let dialable = p.is_dialable();
                if !dialable {
                    warn!("Skipping invalid mesh member for gossip bootstrap");
                }
                dialable
            })
            .copied()
            .collect();

        // 3. Join the new topic and swap — the old VaultGossip drops here,
        //    auto-leaving the old topic.
        let new_gossip = self
            .sync_node
            .join_vault_gossip(&new_vault_id, bootstrap_ids)
            .await
            .context("Failed to re-join vault gossip on adopted VaultId")?;
        self.vault_gossip = new_gossip;
        info!(vault_id = %new_vault_id, "Re-joined gossip on adopted VaultId");

        // 4. Re-publish mDNS under the new VaultId so LAN peers regroup us.
        let mesh = self
            .mesh_name
            .clone()
            .unwrap_or_else(|| self.device_name.clone());
        let mesh_metadata = MeshMetadata {
            mesh,
            vid: new_vault_id.to_string(),
            ver: 1,
        };
        let relay_url = self
            .relay_url
            .as_ref()
            .and_then(|u| RelayAddr::parse(u).ok());
        self.sync_node
            .publish_mesh_info(&mesh_metadata, relay_url.as_ref());

        self.emit_status().await;
        Ok(())
    }
}

/// Drive the initiator pairing protocol against a discovered peer, parking
/// between connect and code submission.
///
/// This is the pure pairing-exchange driver for the two-step GUI flow. It has
/// **no** allowlist or adoption side effects. The function:
/// 1. Opens the QUIC connection and sends `PairingHello` (triggering the
///    responder to generate + display its 6-digit code).
/// 2. Fires `connect_reply` `Ok(responder_device_name)` to signal the GUI to
///    reveal the code entry step.
/// 3. Parks awaiting a code delivered via `code_rx` (filled by `SubmitCode`).
/// 4. Sends `PairingResponse { hmac(code) }` and awaits `PairingResult`.
///
/// Returns `Some((result, responder_device_name))` on a completed exchange
/// (success or failed HMAC check). Returns `None` when a connection error
/// occurs — `connect_reply` carries the `Err` in that case.
async fn run_initiator_pairing_parked(
    dial: &DialHandle,
    peer_endpoint_id: PeerId,
    self_node_id_bytes: [u8; 32],
    device_name: &str,
    connect_reply: oneshot::Sender<Result<String, String>>,
    code_rx: oneshot::Receiver<String>,
) -> Option<(p2p_core::pairing::PairingResult, String)> {
    let self_peer_id = PeerId::from_bytes(self_node_id_bytes);
    let hello = PairingHello {
        node_id: self_peer_id,
        device_name: device_name.to_string(),
    };
    // The target is a discovered mesh peer whose id came from a real mDNS-advertised
    // key; `PeerAddr` defers the curve-point check to `connect()`, which cannot hit
    // the legacy arm for a transport-sourced id.
    let peer = PeerAddr::new(peer_endpoint_id);

    // The PairingChallenge carries the responder's device_name; capture it
    // here so we can return it to the UI's success message.
    let captured_device_name = Arc::new(Mutex::new(String::new()));
    let captured_device_name_setter = captured_device_name.clone();

    // `connect_reply` must be fired exactly once. Move it into an Option so
    // the error path can take it without risk of a second fire.
    let connect_reply_cell = Arc::new(Mutex::new(Some(connect_reply)));
    let connect_reply_for_closure = connect_reply_cell.clone();

    let result = pair_with_mesh_interactive(dial, peer, &hello, move |challenge| {
        let setter = captured_device_name_setter.clone();
        let reply_cell = connect_reply_for_closure.clone();
        let code_rx = code_rx; // move into closure — runs exactly once
        async move {
            let responder_name = challenge.device_name.clone();
            *setter.lock().await = responder_name.clone();

            // Signal the GUI: connection established, responder is showing its code.
            if let Some(reply) = reply_cell.lock().await.take() {
                let _ = reply.send(Ok(responder_name));
            }

            // Park here until SubmitCode delivers the typed code.
            code_rx
                .await
                .map_err(|_| anyhow::anyhow!("pairing cancelled before code was entered"))
        }
    })
    .await;

    match result {
        Ok(pairing_result) => {
            let responder_device_name = captured_device_name.lock().await.clone();
            Some((pairing_result, responder_device_name))
        }
        Err(e) => {
            // Connection error — fire connect_reply with the error if not already sent.
            if let Some(reply) = connect_reply_cell.lock().await.take() {
                let _ = reply.send(Err(format!("Pairing connection failed: {e:#}")));
            }
            None
        }
    }
}
