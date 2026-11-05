//! Peer events: liveness and connect logging, neighbor up/down, allowlist
//! roster exchange and inbound pairing.

use super::*;

/// The transport label for a peer's connection, used both in the connect line
/// ("via LAN" / "via relay" / "via unknown path") and as the persisted
/// `last_conn_type` descriptor a later disconnect echoes.
pub(super) fn conn_type_label(conn: &PeerConnType) -> &'static str {
    match conn {
        PeerConnType::Lan { .. } => "LAN",
        PeerConnType::Relay { .. } => "relay",
        PeerConnType::Unknown => "unknown",
    }
}

/// Format the human-readable INFO connect line for a peer.
///
/// Shapes:
/// - `connected to <name> (192.168.x.y) via LAN`
/// - `connected to <name> (umbra.computer) via relay`
/// - `connected to <name> via unknown path` (no address when the path is
///   unsettled — graceful degradation, never an error)
///
/// `name` is the allowlist `device_name` (or a short peer-id fallback, resolved
/// by the caller). The relay display uses the relay host only, not the full URL.
pub(super) fn fmt_peer_connect_line(name: &str, conn: &PeerConnType) -> String {
    match conn {
        PeerConnType::Lan { addr } => {
            format!("connected to {name} ({}) via LAN", addr.ip())
        }
        PeerConnType::Relay { url } => {
            // `RelayAddr`'s `Display` is host-only with a full-URL fallback, the
            // same shape the former `relay_display` helper produced.
            format!("connected to {name} ({url}) via relay")
        }
        PeerConnType::Unknown => format!("connected to {name} via unknown path"),
    }
}

/// Format the INFO disconnect line for a peer.
///
/// Echoes the last-known connection path when one was captured at connect time
/// (`disconnected from rhea (was via LAN)`); falls back to a bare
/// `disconnected from rhea` when the path is unknown.
pub(super) fn fmt_peer_disconnect_line(name: &str, last_conn_type: Option<&str>) -> String {
    match last_conn_type {
        Some(label) => format!("disconnected from {name} (was via {label})"),
        None => format!("disconnected from {name}"),
    }
}

/// The short peer-id used as a friendly-name fallback: the first 8 hex chars of
/// the peer's public key, enough to disambiguate at a glance.
pub(super) fn short_peer_id(peer_id: &PeerId) -> String {
    peer_id.to_string().chars().take(8).collect()
}

/// Pick the friendly name for `peer_id` from an allowlist roster.
///
/// Returns the peer's stored `device_name` only when it is meaningful; an empty
/// name or the [`PLACEHOLDER_DEVICE_NAME`] (left by a gossip-membership add until
/// the real name self-heals) falls back to the short peer-id, so the connect line
/// never prints a placeholder like "unknown" as if it were a name.
pub(super) fn resolve_peer_name_from(peers: &[AllowedPeer], peer_id: &PeerId) -> String {
    peers
        .iter()
        .find(|p| &p.node_id == peer_id)
        .map(|p| p.device_name.clone())
        .filter(|name| !name.is_empty() && name != PLACEHOLDER_DEVICE_NAME)
        .unwrap_or_else(|| short_peer_id(peer_id))
}

impl<FS: FileSystem + 'static, AL: AllowlistStorage + 'static> Daemon<FS, AL> {
    /// Check whether a peer is allowed to sync.
    ///
    /// Returns `true` only if the peer's `node_id` is in the allowlist.
    ///
    /// Returns `false` if:
    /// - The allowlist is empty (no peers paired yet — deny all until paired), OR
    /// - The peer is not in the allowlist, OR
    /// - The allowlist cannot be read (fail-closed for safety).
    ///
    /// The "open until first pair" behavior was removed because it creates a
    /// window where any device on the network can sync before pairing completes.
    /// Pair first with `notes sync pair`, then start the daemon.
    pub(super) async fn is_peer_allowed(&self, peer_id: &PeerId) -> bool {
        // Delegate to the trait `is_allowed` so the tombstone filter is the single
        // source of truth — an inline `list_peers().any(...)` scan here would treat
        // a revoked (tombstoned) peer as still trusted, bypassing revocation.
        // Fail closed: any read error denies sync (matches the deny-on-error policy).
        match self.allowlist.is_allowed(peer_id).await {
            Ok(allowed) => allowed,
            Err(e) => {
                error!("Failed to read allowlist, denying sync: {}", e);
                false
            }
        }
    }

    /// Resolve a peer's friendly name: its allowlist `device_name`, or a short
    /// peer-id fallback.
    ///
    /// Degrades gracefully — an allowlist read error, a peer not yet in the
    /// allowlist, or a peer whose stored `device_name` is not yet meaningful
    /// (empty, or the [`PLACEHOLDER_DEVICE_NAME`] a gossip-membership add leaves
    /// behind until the real name self-heals) all fall back to the short peer-id
    /// rather than printing a placeholder or suppressing the whole log line. The
    /// short id is the first 8 hex chars of the peer's public key, enough to
    /// disambiguate at a glance.
    pub(super) async fn resolve_peer_name(&self, peer_id: &PeerId) -> String {
        match self.allowlist.list_peers().await {
            Ok(peers) => resolve_peer_name_from(&peers, peer_id),
            Err(e) => {
                debug!("Allowlist read failed during connect log, using short peer-id: {e}");
                short_peer_id(peer_id)
            }
        }
    }

    /// Mark a peer alive from any liveness signal and log the connect line once.
    ///
    /// The single entry point for the non-NeighborUp liveness signals (inbound
    /// sync, gossip roster, allowlist update) that prove a peer is reachable even
    /// when iroh-gossip's edge-triggered `NeighborUp` never fires (the observed
    /// bug). `mark_alive` collapses duplicate signals to `AlreadyAlive`, so the
    /// enriched connect line is emitted exactly once per Unknown/Dead → Alive
    /// transition no matter how many signals arrive.
    ///
    /// These signals carry a `PeerId`. A dialable (real-key) id takes the enriched
    /// connect path with a connection-path snapshot; a legacy/non-curve-point id
    /// degrades to the `Unknown`-path log rather than dropping it (SF-1 — never
    /// panic on a non-dialable id).
    pub(super) async fn on_peer_liveness(&self, peer_id: PeerId) {
        let transition = self.peer_registry.lock().await.mark_alive(peer_id);
        if transition != ConnectTransition::NewlyAlive {
            return;
        }
        if peer_id.is_dialable() {
            self.log_peer_connected(peer_id).await;
        } else {
            debug!(%peer_id, "Invalid peer key for connect-path lookup, logging unknown path");
            self.log_peer_connected_unknown(peer_id).await;
        }
        // The transition changed `alive_count`, so refresh the tray-UI status to
        // match — mirrors the emit on the NeighborUp/Down paths.
        self.emit_status().await;
    }

    /// Emit the enriched INFO connect line for a freshly-alive peer.
    ///
    /// Composes the connection path (from the iroh node), the friendly device
    /// name (from the allowlist), and the formatted line — and captures the
    /// connection-path label on the registry entry so a later disconnect can
    /// echo how we had been reaching the peer. Called only on a true
    /// Unknown/Dead → Alive transition so duplicate liveness signals don't spam.
    pub(super) async fn log_peer_connected(&self, peer_id: PeerId) {
        // `peer_conn_info` takes the `PeerId` (it degrades to an Unknown path on a
        // non-curve-point key). The `%peer_id` log field renders the same 64-hex as
        // the former `%node_id` (pinned by `peer_id_display_matches_iroh_endpoint_id`).
        let conn_type = self.sync_node.peer_conn_info(peer_id).await.conn_type;
        self.log_connect_with_conn_type(peer_id, conn_type).await;
    }

    /// Connect-log fallback when the iroh `EndpointId` couldn't be recovered.
    ///
    /// Without a valid `EndpointId` there is no connection-path snapshot to take,
    /// so the path degrades to `Unknown`. The full peer-id is still logged so the
    /// canonical identity is present. Reuses the node's own `PeerId` Display for
    /// the structured `peer` field.
    pub(super) async fn log_peer_connected_unknown(&self, peer_id: PeerId) {
        let name = self.resolve_peer_name(&peer_id).await;
        info!(
            peer = %peer_id,
            "{}",
            fmt_peer_connect_line(&name, &PeerConnType::Unknown)
        );
        // Store `None`, not the "unknown" label: a later disconnect should fall
        // through to the bare `disconnected from X` form rather than echoing a
        // useless `(was via unknown)`.
        self.peer_registry
            .lock()
            .await
            .set_last_conn_type(&peer_id, None);
    }

    /// Shared connect-log body: resolve the name, emit the INFO line, and capture
    /// the connection-path label for a later disconnect echo.
    pub(super) async fn log_connect_with_conn_type(
        &self,
        peer_id: PeerId,
        conn_type: PeerConnType,
    ) {
        let name = self.resolve_peer_name(&peer_id).await;
        info!(peer = %peer_id, "{}", fmt_peer_connect_line(&name, &conn_type));

        // Record how we reached the peer so disconnect can say "was via LAN".
        let label = conn_type_label(&conn_type).to_string();
        self.peer_registry
            .lock()
            .await
            .set_last_conn_type(&peer_id, Some(label));
    }

    /// Emit the enriched INFO disconnect line for a peer that left.
    ///
    /// Echoes the last-known connection path captured at connect time when
    /// available.
    pub(super) async fn log_peer_disconnected(&self, peer_id: PeerId) {
        let last_conn_type = self
            .peer_registry
            .lock()
            .await
            .get_all_peers()
            .iter()
            .find(|e| e.node_id == peer_id)
            .and_then(|e| e.last_conn_type.clone());
        let name = self.resolve_peer_name(&peer_id).await;
        info!(
            peer = %peer_id,
            "{}",
            fmt_peer_disconnect_line(&name, last_conn_type.as_deref())
        );
    }

    /// A peer joined the gossip swarm — initiate a full sync via QUIC.
    ///
    /// The allowlist check and sync-request preparation happen synchronously so
    /// the peer registry is updated before the event loop moves on. The QUIC
    /// exchange and response processing are spawned as a background task so the
    /// event loop remains free to handle inbound sync requests from the same peer
    /// — both sides fire NeighborUp simultaneously and each tries to connect to
    /// the other, which would deadlock if the QUIC call blocked the select loop.
    pub(super) async fn on_neighbor_up(&mut self, peer_id: PeerId) {
        let transition = self.peer_registry.lock().await.mark_alive(peer_id);
        if transition == ConnectTransition::NewlyAlive {
            self.log_peer_connected(peer_id).await;
        }
        // `%peer_id` renders the same 64-hex as the former `%node_id` EndpointId.
        debug!(peer = %peer_id, "Gossip NeighborUp — initiating full sync");
        self.emit_status().await;

        if !self.is_peer_allowed(&peer_id).await {
            warn!(peer = %peer_id, "Peer not in allowlist, skipping sync");
            return;
        }

        let vault = self.vault.lock().await;
        let request_bytes = match vault.prepare_request().await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to prepare sync request for {}: {}", peer_id, e);
                return;
            }
        };
        drop(vault);

        // Spawn the QUIC exchange as a background task so the event loop can
        // continue processing inbound sync requests. When both peers fire
        // NeighborUp simultaneously, each needs to respond to the other's
        // inbound sync — which can only happen if the event loop isn't blocked.
        spawn_sync_exchange(
            peer_id,
            peer_id,
            request_bytes,
            self.vault.clone(),
            self.allowlist.clone(),
            self.peer_registry.clone(),
            self.sync_node.dial_handle(),
            self.exchange_learn_tx.clone(),
            SyncExchangeKind::NeighborUp,
        );

        // Push our full membership roster to the swarm so the peer that just came
        // up converges to it — the eventually-consistent fix for the one-shot
        // lossy `AllowlistUpdate` delta (a peer offline at a pairing's broadcast
        // instant would otherwise never learn the rest of the mesh). The newly
        // joined peer is a current neighbor and will receive it; re-sending to
        // peers that already have the entries is safe (merge_roster is idempotent).
        self.push_allowlist_roster().await;
    }

    /// A peer left the gossip swarm.
    pub(super) async fn on_neighbor_down(&mut self, peer_id: PeerId) {
        self.log_peer_disconnected(peer_id).await;
        self.peer_registry.lock().await.on_neighbor_down(&peer_id);
        self.emit_status().await;
    }

    /// A change notification arrived via gossip — pull the changed file via QUIC.
    ///
    /// The allowlist check happens synchronously. The QUIC exchange is spawned as
    /// a background task so the event loop stays free to process other events while
    /// the pull is in flight.
    pub(super) async fn on_change_received(&mut self, from: PeerId, path: String) {
        debug!(peer = %from, path = %path, "Change notification received — pulling update");

        if !self.is_peer_allowed(&from).await {
            warn!(peer = %from, "Change notification from non-allowlisted peer, ignoring");
            return;
        }

        let vault = self.vault.lock().await;
        let request_bytes = match vault.prepare_request().await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to prepare sync request for change pull: {}", e);
                return;
            }
        };
        drop(vault);

        spawn_sync_exchange(
            from,
            from,
            request_bytes,
            self.vault.clone(),
            self.allowlist.clone(),
            self.peer_registry.clone(),
            self.sync_node.dial_handle(),
            self.exchange_learn_tx.clone(),
            SyncExchangeKind::ChangePull { path },
        );
    }

    /// Broadcast our full membership roster to the gossip swarm.
    ///
    /// This is the eventually-consistent convergence path that replaces the
    /// one-shot, lossy `AllowlistUpdate` delta: any peer that missed a pairing's
    /// single broadcast learns the whole roster on the next NeighborUp/reconcile.
    /// The receiver's `merge_roster` is a union with tombstone-precedence, so
    /// re-sending to peers that already have the entries is safe and idempotent.
    ///
    /// Bounded fan-out (spec rule 4): a `GossipMessage::AllowlistRoster` rides the
    /// same 1KB-capped `broadcast_message` path as every other gossip envelope.
    /// We PRE-CHECK the encoded size against the cap rather than relying on the
    /// `Err` from `broadcast_message` (a returned `Err` there is a BUG, not the
    /// expected over-cap path). When the roster would exceed the cap we fall back
    /// to chunked per-peer `AllowlistUpdate` deltas — the common 3-machine path
    /// stays a single message and a large mesh degrades safely instead of erroring.
    pub(super) async fn push_allowlist_roster(&mut self) {
        // Include tombstones — filtering to live-only breaks revocation
        // propagation (a removed peer would never converge to "removed" elsewhere).
        let roster = match self.allowlist.list_peers().await {
            Ok(peers) => peers,
            Err(e) => {
                warn!("Failed to read allowlist for roster push: {}", e);
                return;
            }
        };
        if roster.is_empty() {
            return;
        }

        // Pre-check the encoded envelope size against the gossip cap. Build the
        // same `GossipMessage::AllowlistRoster` the broadcast will, so the size we
        // measure matches what `broadcast_message` would frame.
        let encoded_len = bincode::serialize(&GossipMessage::AllowlistRoster(roster.clone()))
            .map(|b| b.len())
            .unwrap_or(usize::MAX);

        if encoded_len > MAX_GOSSIP_MESSAGE_SIZE {
            warn!(
                roster_len = roster.len(),
                encoded_len,
                cap = MAX_GOSSIP_MESSAGE_SIZE,
                "Allowlist roster exceeds gossip size cap — falling back to per-peer deltas"
            );
            for peer in &roster {
                if let Err(e) = self.vault_gossip.broadcast_allowlist_update(peer).await {
                    warn!(peer_id = %peer.node_id, "Failed to broadcast allowlist delta: {}", e);
                }
            }
            return;
        }

        if let Err(e) = self.vault_gossip.broadcast_allowlist_roster(&roster).await {
            warn!("Failed to broadcast allowlist roster: {}", e);
        }
    }

    /// Handle a full membership roster received via gossip from a mesh member.
    ///
    /// Same sender-trust gate as `on_allowlist_update_received` — we only merge a
    /// roster from a peer already in our allowlist. `merge_roster` unions the
    /// incoming entries with tombstone-precedence, so this both adds peers we were
    /// missing and honors revocations a trusted member propagated.
    pub(super) async fn on_allowlist_roster_received(&self, from: PeerId, peers: Vec<AllowedPeer>) {
        if !self.is_peer_allowed(&from).await {
            warn!(peer = %from, "Allowlist roster from non-allowlisted peer, ignoring");
            return;
        }

        // A roster broadcast arriving from this peer proves it is connected even
        // when our HyParView never fired NeighborUp — log the connect on a true
        // transition (the reliability fix).
        self.on_peer_liveness(from).await;

        match self.allowlist.merge_roster(&peers).await {
            Ok(()) => {
                info!(from = %from, count = peers.len(), "Merged allowlist roster via gossip");
            }
            Err(e) => {
                error!("Failed to merge allowlist roster from {}: {}", from, e);
            }
        }
    }

    /// Handle an allowlist update received via gossip from a mesh member.
    ///
    /// Only processes the update if the sender is already in our allowlist —
    /// we don't trust gossip from peers we haven't explicitly paired with.
    pub(super) async fn on_allowlist_update_received(&self, from: PeerId, peer: AllowedPeer) {
        if !self.is_peer_allowed(&from).await {
            warn!(peer = %from, "Allowlist update from non-allowlisted peer, ignoring");
            return;
        }

        // The update broadcast proves the sender is connected — same reliability
        // fix as the roster path.
        self.on_peer_liveness(from).await;

        match self
            .allowlist
            .add_peer(peer.node_id, &peer.device_name)
            .await
        {
            Ok(()) => {
                info!(peer_id = %peer.node_id, device = %peer.device_name, "Added peer via gossip allowlist update");
            }
            Err(e) => {
                error!(
                    "Failed to add peer {} from allowlist update: {}",
                    peer.node_id, e
                );
            }
        }
    }

    /// Handle an inbound pairing request from a new device.
    ///
    /// Generates a 6-digit code and logs it so the user can relay it to the new
    /// device. Only one pairing session at a time — concurrent requests are dropped.
    pub(super) async fn on_inbound_pairing_request(&mut self, exchange: InboundPairingExchange) {
        // Reject concurrent pairing attempts — only one session at a time.
        if let Some(ref existing) = self.active_pairing
            && !existing.is_expired()
        {
            warn!(
                device = %exchange.hello.device_name,
                "Pairing request rejected: session already active"
            );
            // Dropping reply_tx signals rejection to the handler.
            drop(exchange.reply_tx);
            return;
        }

        let session = PairingSession::new(exchange.remote_id, &exchange.hello.device_name);

        info!(
            "Device '{}' wants to join. Pairing code: {} (expires in 5:00)",
            exchange.hello.device_name, session.code
        );

        let challenge = PairingChallenge {
            node_id: PeerId::from_bytes(*self.sync_node.node_id().as_bytes()),
            device_name: self.device_name.clone(),
        };

        // Collect vault gossip topic bytes for the new device.
        let vault_topic = *self.vault_gossip.topic().as_bytes();

        // Include our relay URL if the embedded relay is running.
        let relay_urls: Vec<String> = self.relay_url.iter().cloned().collect();

        // Include all alive peers plus self as mesh members.
        let mut mesh_members: Vec<PeerId> = self
            .peer_registry
            .lock()
            .await
            .get_alive_peers()
            .iter()
            .map(|e| e.node_id)
            .collect();
        mesh_members.push(PeerId::from_bytes(*self.sync_node.node_id().as_bytes()));

        let approval = PairingApproval {
            code: session.code.clone(),
            challenge,
            vault_topic,
            relay_urls,
            mesh_members,
        };

        // Pairing sessions expire after 5 minutes, matching PAIRING_TIMEOUT_SECS in p2p-core's pairing module.
        let expires_at_ms = now_ms() + (5 * 60 * 1000);

        self.active_pairing = Some(session);

        // Ignore send error — the handler may have timed out or been dropped.
        let _ = exchange.reply_tx.send(approval);

        // Notify the desktop tray so it can open the responder window.
        self.emit_pairing_event(PairingUiEvent::InboundRequest {
            device_name: exchange.hello.device_name.clone(),
            code: self
                .active_pairing
                .as_ref()
                .map(|s| s.code.clone())
                .unwrap_or_default(),
            expires_at_ms,
        });
    }

    /// Handle successful pairing — add the new peer to the allowlist and propagate.
    pub(super) async fn on_pairing_completed(&mut self, peer_id: PeerId, device_name: String) {
        // Verify the completing peer matches the active session. A mismatch would indicate
        // a race between two concurrent pairing attempts (which we reject at the request
        // stage) or a stale event from a previous session — both should be dropped.
        if self.active_pairing.as_ref().map(|s| &s.remote_id) != Some(&peer_id) {
            warn!(
                peer_id = %peer_id,
                "Pairing completed event does not match active session, ignoring"
            );
            return;
        }

        self.active_pairing = None;

        let allowed_peer = AllowedPeer::new(peer_id, device_name.clone());

        // Bootstrap: if this is the first pairing, also add ourselves to the allowlist.
        let is_first_pair =
            matches!(self.allowlist.list_peers().await, Ok(peers) if peers.is_empty());

        if let Err(e) = self.allowlist.add_peer(peer_id, &device_name).await {
            error!("Failed to add paired peer to allowlist: {}", e);
            return;
        }

        if is_first_pair {
            let self_id = PeerId::from_bytes(*self.sync_node.node_id().as_bytes());
            if let Err(e) = self.allowlist.add_peer(self_id, &self.device_name).await {
                warn!("Failed to add self to allowlist on first pair: {}", e);
            }
        }

        // Propagate the new peer to existing mesh members via gossip.
        if let Err(e) = self
            .vault_gossip
            .broadcast_allowlist_update(&allowed_peer)
            .await
        {
            warn!(
                "Failed to broadcast allowlist update for {}: {}",
                device_name, e
            );
        }

        info!("Device '{}' joined the mesh", device_name);

        self.emit_pairing_event(PairingUiEvent::InboundCompleted {
            device_name: device_name.clone(),
        });
        self.emit_status().await;
    }

    /// Handle a failed pairing attempt.
    pub(super) fn on_pairing_failed(&mut self, peer_id: PeerId, reason: String) {
        // Notify the tray before clearing the session so the responder window can
        // auto-close. This covers timeout, bad-code, and gossip-layer errors — the
        // only path not covered here is an explicit user reject (handled in
        // on_daemon_command via DaemonCommand::RejectInbound, which also calls
        // emit_pairing_event before taking active_pairing).
        self.emit_pairing_event(PairingUiEvent::InboundFailed {
            reason: reason.clone(),
        });
        // Clear the active session so a new pairing attempt can start fresh. This means
        // the user must re-initiate pairing (triggering a new code) rather than retrying
        // with the same code. Keeping the session alive for retry would require matching
        // new QUIC connections to existing sessions, which is more complex. For v1 this
        // tradeoff is acceptable.
        self.active_pairing = None;
        warn!("Pairing failed for {}: {}", peer_id, reason);
    }
}

#[cfg(test)]
mod connect_log_tests {
    use super::{
        conn_type_label, fmt_peer_connect_line, fmt_peer_disconnect_line, resolve_peer_name_from,
    };
    use p2p_core::{PLACEHOLDER_DEVICE_NAME, PeerConnType};
    use p2p_core::{PeerId, allowlist::AllowedPeer};

    fn lan(ip_port: &str) -> PeerConnType {
        PeerConnType::Lan {
            addr: ip_port.parse().unwrap(),
        }
    }

    fn relay(url: &str) -> PeerConnType {
        PeerConnType::Relay {
            url: url.parse().unwrap(),
        }
    }

    #[test]
    fn connect_line_lan_shows_ip_and_via_lan() {
        let line = fmt_peer_connect_line("rhea.local", &lan("192.168.10.226:11204"));
        assert_eq!(line, "connected to rhea.local (192.168.10.226) via LAN");
    }

    #[test]
    fn connect_line_relay_shows_host_and_via_relay() {
        let line = fmt_peer_connect_line("umbra", &relay("https://umbra.computer/"));
        assert_eq!(line, "connected to umbra (umbra.computer) via relay");
    }

    #[test]
    fn connect_line_unknown_omits_address() {
        let line = fmt_peer_connect_line("rhea.local", &PeerConnType::Unknown);
        assert_eq!(line, "connected to rhea.local via unknown path");
    }

    #[test]
    fn connect_line_uses_provided_name_verbatim() {
        // The caller resolves the short-peer-id fallback; the formatter just uses
        // whatever name string it's given.
        let line = fmt_peer_connect_line("a1b2c3d4", &lan("10.0.0.5:9000"));
        assert_eq!(line, "connected to a1b2c3d4 (10.0.0.5) via LAN");
    }

    #[test]
    fn disconnect_line_echoes_last_conn_type() {
        let line = fmt_peer_disconnect_line("rhea.local", Some("LAN"));
        assert_eq!(line, "disconnected from rhea.local (was via LAN)");
    }

    #[test]
    fn disconnect_line_bare_when_path_unknown() {
        let line = fmt_peer_disconnect_line("rhea.local", None);
        assert_eq!(line, "disconnected from rhea.local");
    }

    #[test]
    fn conn_type_label_maps_each_variant() {
        assert_eq!(conn_type_label(&lan("192.168.1.1:1")), "LAN");
        assert_eq!(conn_type_label(&relay("https://umbra.computer/")), "relay");
        assert_eq!(conn_type_label(&PeerConnType::Unknown), "unknown");
    }

    fn peer_named(name: &str) -> (PeerId, AllowedPeer) {
        let id = PeerId::generate();
        (id, AllowedPeer::new(id, name))
    }

    fn short_id(peer_id: &PeerId) -> String {
        peer_id.to_string().chars().take(8).collect()
    }

    #[test]
    fn resolve_name_uses_real_device_name() {
        let (id, peer) = peer_named("rhea.local");
        assert_eq!(resolve_peer_name_from(&[peer], &id), "rhea.local");
    }

    #[test]
    fn resolve_name_falls_back_for_placeholder_device_name() {
        // umbra's stored value is the literal placeholder until its real name
        // self-heals; we must not print "unknown" as if it were a name.
        let (id, peer) = peer_named(PLACEHOLDER_DEVICE_NAME);
        assert_eq!(resolve_peer_name_from(&[peer], &id), short_id(&id));
    }

    #[test]
    fn resolve_name_falls_back_for_empty_device_name() {
        let (id, peer) = peer_named("");
        assert_eq!(resolve_peer_name_from(&[peer], &id), short_id(&id));
    }

    #[test]
    fn resolve_name_falls_back_for_absent_peer() {
        let (id, _peer) = peer_named("rhea.local");
        // The roster does not contain `id`, so there is no name to resolve.
        assert_eq!(resolve_peer_name_from(&[], &id), short_id(&id));
    }
}
