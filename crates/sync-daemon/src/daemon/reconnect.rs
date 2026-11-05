//! Reconnect supervisor: per-hint backoff, the reconnect tick, network-change
//! re-dials and relay-hint learning.

use super::*;

/// First reconnect attempt fires ~5s after a daemon drops to zero neighbors —
/// fast enough that a brief flap heals quickly, slow enough to let a transient
/// blip self-resolve before the supervisor acts.
pub(super) const RECONNECT_BASE_MS: u64 = 5_000;

/// Base interval for a fresh/briefly-unreachable hint's reconnect attempts.
///
/// `60s` matches the old global reconnect ceiling, so a healthy hint retries on
/// the same cadence the supervisor used before per-hint backoff existed — no
/// behavior change for the common transient-drop case.
pub(super) const HINT_BACKOFF_BASE_MS: u64 = 60_000;

/// Shift ceiling for the per-hint backoff. `60s << 5 ≈ 32 min`, which bounds
/// growth and keeps the shift well clear of overflow.
pub(super) const HINT_BACKOFF_CEIL: u32 = 5;

/// Maximum per-hint backoff. A dead hint is re-added + dialed roughly twice an
/// hour — frequent enough to catch a returning peer, rare enough that the
/// failed-dial burst is negligible noise (vs the continuous loop we're fixing).
pub(super) const MAX_HINT_BACKOFF_MS: u64 = 1_800_000;

/// Grace window after a network change during which per-hint backoff is floored
/// at base, regardless of accumulated failures.
///
/// Off-LAN after a network change, the first reconnect dials fire while iroh is
/// still re-homing its OWN relay, so they fail and ramp the backoff (60s → 2m →
/// 4m → … → 30m) with no second reset once iroh's relay is ready — leaving the
/// daemon parked at a multi-minute backoff even though the public relay is now
/// reachable (the ~29-min off-LAN rehome). For this window we keep re-dialing on
/// the base cadence so a peer is re-reached within ~1 min of the network
/// settling instead of after the full ramp. 3 min comfortably covers iroh's
/// relay re-home while staying short enough that a genuinely-dead hint resumes
/// its real backoff promptly. Scaled for test-time-scale consistency with the
/// other backoff windows.
pub(super) const NET_CHANGE_GRACE_MS: u64 = 180_000;

/// Consecutive failed attempts after which a hint is considered "stale": it
/// gets the max-backoff cadence and the one-shot eviction log. Low enough to
/// quiet noise within ~an hour of a peer going dark, high enough not to throttle
/// a few-minute flaky outage.
pub(super) const STALE_FAILURE_THRESHOLD: u32 = 5;

/// Per-hint exponential backoff window from a hint's consecutive failure count.
///
/// `HINT_BACKOFF_BASE_MS << min(failure_count, HINT_BACKOFF_CEIL)`, saturating
/// at [`MAX_HINT_BACKOFF_MS`]. Throttles a dead hint's attempt cadence WITHOUT
/// abandoning it — even the stalest hint is re-dialed once per returned window.
pub(super) fn per_hint_backoff(failure_count: u32) -> u64 {
    let shift = failure_count.min(HINT_BACKOFF_CEIL);
    // Scale both the per-failure window and the ceiling so the test-time-scale
    // shrinks the backoff cadence consistently (D4/D5). At scale 1.0 (prod) both
    // calls return their input unchanged.
    scaled_ms(HINT_BACKOFF_BASE_MS << shift).min(scaled_ms(MAX_HINT_BACKOFF_MS))
}

/// Whether a hint is due for another reconnect attempt at `now_ms`, honoring an
/// optional post-network-change grace window.
///
/// A never-attempted hint (`last_attempt_ms == None`) is due immediately — we
/// have never tried it, so there is nothing to back off from. For an
/// already-attempted hint the elapsed math uses `saturating_sub` so a backward
/// clock jump (a `last_attempt_ms` in the future) yields `0` elapsed — the hint
/// reads not-yet-due for one window, then becomes due again, never permanently
/// wedged.
///
/// While `now_ms` is inside the grace window (`grace_until_ms`, set by
/// [`Daemon::on_network_change`]), the effective failure count is floored to `0`,
/// so the due-gate uses the base backoff window regardless of how many early
/// dials have failed. This is the [`NET_CHANGE_GRACE_MS`] rehome fix: it keeps a
/// post-net-change hint re-dialing on the base cadence while iroh re-homes its
/// own relay, instead of ramping to a multi-minute backoff that strands the peer.
/// Only the GATE is floored — the stored `failure_count` keeps climbing (see
/// [`Daemon::record_due_hint_failures`]) so real backoff resumes once the window
/// expires.
pub(super) fn effective_backoff_due(
    failure_count: u32,
    last_attempt_ms: Option<u64>,
    now_ms: u64,
    grace_until_ms: Option<u64>,
) -> bool {
    let in_grace = grace_until_ms.is_some_and(|g| now_ms < g);
    let effective = if in_grace { 0 } else { failure_count };
    last_attempt_ms.is_none_or(|last| now_ms.saturating_sub(last) >= per_hint_backoff(effective))
}

/// Decide whether a relay learned from a peer should be adopted into this node's
/// public-relay set (C6 gossip expansion).
///
/// Once connected, gossip carries each peer's home relay; a server's home relay
/// IS its public relay, so a laptop can discover a second server's relay it was
/// never paired with and add it for failover redundancy. We adopt only when the
/// learned relay is:
/// - present (a LAN-direct exchange yields `None` — nothing to learn), AND
/// - off-LAN-reachable (a private LAN-IP relay is useless once we leave that LAN,
///   so it must never become a home candidate — mirrors the `add_known_public_relay`
///   classifier guard), AND
/// - not already in `known_public_relays` (the idempotency guard: iroh exposes no
///   public `Endpoint::contains_relay()`, so the persisted set stands in for
///   RelayMap membership — every relay we insert we also persist and vice-versa —
///   preventing per-exchange RelayMap churn and duplicate cross-product entries).
pub(super) fn learned_public_relay_to_adopt(
    learned: Option<&RelayAddr>,
    known_public_relays: &[String],
) -> Option<RelayAddr> {
    let url = learned?;
    let url_str = url.as_str();
    if !relay_is_offlan_reachable(&url_str) {
        return None;
    }
    if known_public_relays.iter().any(|u| u == &url_str) {
        return None;
    }
    Some(url.clone())
}

impl<FS: FileSystem + 'static, AL: AllowlistStorage + 'static> Daemon<FS, AL> {
    /// Reconnect supervisor: heal a partition by re-dialing peer relay hints,
    /// without a process restart — now with per-hint backoff and eviction.
    ///
    /// Why this exists (it is NOT reinventing iroh): iroh-gossip does not re-dial
    /// bootstrap peers once a node's active AND passive views both empty — a total
    /// partition leaves no internal mechanism that dials anything; only an
    /// app-issued `Join` re-injects bootstrap peers. iroh also keepalives live
    /// connections but never re-dials a DROPPED peer, so re-dial cadence is entirely
    /// app policy. This supervisor is that policy. See [[iroh vs Hand-Rolled
    /// Connectivity]] (KEEP verdict) before assuming it's redundant.
    ///
    /// Runs on a steady tick. There is no single global backoff gate: each hint
    /// carries its own freshness (`failure_count` / `last_attempt_ms`) and is
    /// throttled independently. The state machine:
    ///
    /// 1. A live neighbor → connected. Return (no log spam, no churn).
    /// 2. No known peer relays → nothing to chase (unpaired or hint-less). Return.
    /// 3. For each hint, `effective_backoff_due` decides:
    ///    - **Due:** re-seed its relay (`set_peer_relay`) and re-bootstrap gossip
    ///      toward its EndpointId. An unparseable URL skips the seed but STILL
    ///      bootstraps by EndpointId — mDNS or a prior direct address may reach
    ///      it on-LAN. `last_attempt_ms` is stamped now.
    ///    - **Not due (throttled):** EVICT the hint from the address-lookup
    ///      (`remove_peer_relay`) so iroh-gossip's HyParView stops re-resolving
    ///      and re-feeding the dead relay to iroh's relay actor — UNLESS it is
    ///      the only hint we have OR the only off-LAN-reachable hint, in which
    ///      case it is RETAINED (see the sole-hint / off-LAN-lifeline rule below).
    /// 4. `rejoin_peers` with just the due hints. A successful re-dial surfaces
    ///    as a normal `NeighborUp` → `on_neighbor_up` → full sync.
    /// 5. Since step 1 already returned when connected, reaching here means zero
    ///    neighbors — so every hint dialed this tick failed. Bump `failure_count`
    ///    (in memory + persisted, so the throttle survives a restart) and emit
    ///    the one-shot eviction log when a hint first crosses the stale threshold.
    ///
    /// **Last-hint guarantee:** a due hint is ALWAYS re-added regardless of how
    /// high its `failure_count` is (the per-hint backoff caps, so even the
    /// stalest hint comes due within [`MAX_HINT_BACKOFF_MS`]). Eviction only ever
    /// removes the in-memory lookup entry, never the durable `PeerRelay` row — a
    /// genuinely off-LAN peer that returns is still reached on the slow cadence,
    /// and learn-on-exchange resets it the instant it reconnects.
    ///
    /// **Sole-hint / off-LAN-lifeline rule (relay-reap fix):** while partitioned,
    /// a throttled hint is never evicted if it is the ONLY hint we have OR the
    /// ONLY off-LAN-reachable one (a public/domain relay, classified by
    /// [`relay_is_offlan_reachable`]). Throttling caps how often we DIAL it, but
    /// its address must stay resident so a reaped non-home relay actor can be
    /// respawned and the partition can heal without a process restart. Evicting
    /// the only address is what caused the 12+ minute non-recovery this rule
    /// exists to prevent; the off-LAN generalization additionally keeps a laptop's
    /// public-relay lifeline resident when its other hints are dead LAN-IP relays
    /// (the n0-DNS-removal regression). A throttled hint is still evicted when a
    /// real alternative remains — another hint when LAN-only protection applies,
    /// or another off-LAN route.
    ///
    /// **Net-change interaction:** [`on_network_change`] un-throttles every hint
    /// (flipping it back to *due*) so the next tick re-seeds rather than evicts.
    /// Together with this off-LAN-lifeline retention, the public-relay lifeline
    /// survives a network switch from both sides: it is never evicted while
    /// throttled, and a net change re-dials it immediately on the new network.
    pub(super) async fn on_reconnect_tick(&mut self) {
        // Step 1: connected — stay quiet and don't churn the swarm.
        //
        // The reconnect/dial logic below is the shipped, validated supervisor and
        // is untouched. The ONLY addition on the connected path is a throttled
        // allowlist-roster reconcile: re-converge any membership drift (a missed
        // NeighborUp, a restart) on a slow cadence. It is gated behind its own
        // timer so it fires ~once/ROSTER_RECONCILE_MS, not on every 5s wake, and
        // it does not touch the dial/hint machinery — when connected we still
        // return before reaching it.
        if self.peer_registry.lock().await.alive_count() > 0 {
            self.maybe_reconcile_allowlist_roster().await;
            return;
        }

        // Step 2: no hints to chase.
        if self.peer_relays.is_empty() {
            return;
        }

        let now = now_ms();
        let mut bootstrap_ids: Vec<PeerId> = Vec::new();
        // Peer ids dialed this tick, so we can attribute the (coarse) failure
        // after the attempt if no neighbor materializes.
        let mut due_ids: Vec<PeerId> = Vec::new();
        // Due hints whose relay URL parses, paired with that URL. Drives the
        // supervisor-issued relay-carrying connect below — the recovery for peers
        // whose bare-id gossip dial parked and can never be re-dialed by gossip.
        let mut due_relay_targets: Vec<(PeerId, RelayAddr)> = Vec::new();

        // When this is the only hint we have, it is the lookup's sole route to
        // the peer — we never evict it while partitioned (see the throttled
        // branch below). Step 1 already returned if connected, so reaching here
        // means `alive_count == 0`.
        let is_sole_hint = self.peer_relays.len() == 1;

        // How many hints are reachable from OFF the local network (a public/domain
        // relay or a globally-routable IP). When exactly one is, that hint is the
        // last off-LAN lifeline and must not be evicted while throttled — LAN-only
        // hints are no alternative once the laptop leaves the LAN. See the
        // throttled branch below.
        let offlan_reachable_count = self
            .peer_relays
            .iter()
            .filter(|h| relay_is_offlan_reachable(&h.relay_url))
            .count();

        // Decide per hint, applying node calls (set/remove) and stamping the
        // in-memory `last_attempt_ms` for due hints as we go.
        for hint in self.peer_relays.iter_mut() {
            let Ok(peer_id) = hint.endpoint_id.parse::<PeerId>() else {
                // Malformed snapshot entry — skip silently; startup already warned
                // about this entry at load time.
                continue;
            };

            if effective_backoff_due(
                hint.failure_count,
                hint.last_attempt_ms,
                now,
                self.net_change_grace_until_ms,
            ) {
                // Re-add the hint and dial it. Re-add is UNCONDITIONAL of
                // failure_count — the last-hint guarantee that a returning
                // off-LAN peer is never stranded.
                if let Ok(relay_addr) = RelayAddr::parse(&hint.relay_url) {
                    self.sync_node.set_peer_relay(peer_id, &relay_addr);
                    // Also drive a supervisor-issued connect for this hint (below):
                    // gossip's bare-id Dialer can park forever and is then never
                    // re-dialed, so we establish the relay-carrying connection
                    // ourselves and hand it to gossip.
                    due_relay_targets.push((peer_id, relay_addr));
                }
                // Even with an unparseable URL, still bootstrap by peer id —
                // mDNS or a prior direct address may reach the peer on-LAN.
                hint.last_attempt_ms = Some(now);
                bootstrap_ids.push(peer_id);
                due_ids.push(peer_id);
            } else {
                // Throttled. Evict from the lookup UNLESS this hint is a lifeline
                // we must never drop while partitioned:
                //   - the only hint we have at all (sole-hint rule), OR
                //   - the only OFF-LAN-reachable hint — its loss strands us
                //     off-LAN even though LAN-only alternatives remain, because
                //     those LAN-only hints cannot be dialed off-LAN.
                // The off-LAN rule is a strict SUPERSET of the sole-hint rule: a
                // sole LAN-only hint is still retained by `is_sole_hint` (it is
                // the only address we have, useful or not), so this never regresses
                // the original protection.
                let is_sole_offlan_lifeline =
                    relay_is_offlan_reachable(&hint.relay_url) && offlan_reachable_count == 1;
                if !is_sole_hint && !is_sole_offlan_lifeline {
                    // A live alternative remains (another hint, or another off-LAN
                    // route) — safe to evict this one so nothing re-resolves the
                    // dead relay until it comes due again.
                    self.sync_node.remove_peer_relay(peer_id);
                }
                // Otherwise RETAINED in the lookup — the relay-reap reconnect fix,
                // generalized to the off-LAN lifeline.
                //
                // We throttle the dial FREQUENCY (the `effective_backoff_due` gate
                // above already does this), not the address PRESENCE — never
                // evicting a partitioned peer's only lifeline. If we removed it,
                // the next due tick would re-seed an address that had been gone,
                // but more importantly a sole reaped relay actor would never be
                // driven back to life between due windows. The off-LAN case adds:
                // a laptop that paired with both a public-relay peer and a LAN-IP
                // peer must keep the public hint resident, because off-LAN the
                // LAN-IP hint is a dead end and the public one is the only route
                // home (this is the n0-DNS-removal regression — n0's parallel DNS
                // resolver used to mask this eviction window).
                //
                // Tradeoff (intentional, and it diverges from `remove_peer_relay`'s
                // own docstring warning): retaining such a hint means a
                // permanently-dead peer's address lingers in `MemoryLookup`, where
                // iroh-gossip's HyParView maintenance re-resolves it on its own ~60s
                // cadence — low-grade relay-actor activity that the general eviction
                // path otherwise quiets between due windows. We accept that churn
                // because the alternative (evicting the only off-LAN address while
                // partitioned) is what caused the 12+ minute non-recovery this fix
                // exists to kill. `MAX_HINT_BACKOFF_MS` still bounds how often WE
                // actively dial it, so retention does not reintroduce a hot dial loop.
            }
        }

        if bootstrap_ids.is_empty() {
            return;
        }

        let attempt_count = bootstrap_ids.len();
        if let Err(e) = self.vault_gossip.rejoin_peers(bootstrap_ids).await {
            warn!("Reconnect attempt failed to re-bootstrap gossip: {e}");
        } else {
            info!(
                hints = attempt_count,
                "Reconnect attempt: re-seeded due hints, re-bootstrapping gossip"
            );
        }

        // Supervisor-issued relay-carrying connect for each due hint.
        //
        // The `rejoin_peers` above re-queues a Join, but gossip dials a bootstrap
        // peer by BARE EndpointId, and that dial can park forever in iroh's
        // address resolution (relay-only, off-LAN — no path ever resolves, no
        // timeout). Once parked, the peer sits `Pending` with a non-empty queue
        // and gossip's `queue.is_empty()` guard suppresses every later re-dial, so
        // the peer is unreachable until restart. We break that by establishing the
        // connection ourselves with the relay attached (an `EndpointAddr` carrying
        // the relay resolves immediately — no park) and handing it to gossip, which
        // adopts it exactly as it adopts an inbound accept: `accept_conn` drains the
        // queued Join, flips the peer Active, and the handshake yields NeighborUp.
        // This is independent of gossip's parked Dialer, so it also recovers a peer
        // that is ALREADY stuck.
        for (peer, relay_url) in due_relay_targets {
            self.spawn_bootstrap_connect(peer, relay_url);
        }

        // Step 5: every due hint failed to yield a neighbor this tick (step 1
        // returned when connected). Bump the in-memory throttle so the per-hint
        // backoff ramps for the rest of this session.
        self.record_due_hint_failures(&due_ids);
    }

    /// Establish a relay-carrying connection to a due peer and hand it to gossip,
    /// recovering peers whose bare-id gossip bootstrap dial parked (see
    /// `on_reconnect_tick`).
    ///
    /// Spawned, not awaited inline: `endpoint.connect` can take up to the QUIC
    /// handshake timeout to fail when the relay is unreachable, and blocking the
    /// daemon's single event loop that long would stall sync handling and the
    /// net-change channel. The in-flight `HashSet` guard is load-bearing: the
    /// supervisor ticks every ~200ms after a network change, so without it the
    /// same peer would accumulate dozens of concurrent connects. We skip a peer
    /// whose connect is already in flight, insert before spawning, and remove when
    /// the task finishes (both success and failure).
    pub(super) fn spawn_bootstrap_connect(&self, peer: PeerId, relay_url: RelayAddr) {
        // A cheap-clone dial handle (endpoint + gossip clones) for the detached
        // task — NOT an `Arc<SyncNode>`, which would block the node's consuming
        // `shutdown(self)` if this task is still in flight at shutdown.
        let dial = self.sync_node.dial_handle();
        let rejoin = self.vault_gossip.rejoin_handle();
        let inflight = self.bootstrap_connect_inflight.clone();

        tokio::spawn(async move {
            // Skip if a connect to this peer is already in flight; otherwise claim
            // the slot. Holding the lock across the insert keeps the check-and-set
            // atomic against concurrent ticks.
            {
                let mut set = inflight.lock().await;
                if !set.insert(peer) {
                    return;
                }
            }

            // Re-queue a Join so a fresh Join is present in gossip's proto queue
            // for `accept_conn` to drain on adoption (deterministic ordering — the
            // queued Join is what initiates the handshake from our side once the
            // connection is adopted).
            if let Err(e) = rejoin.rejoin_peers(vec![peer]).await {
                debug!(peer = %peer, "bootstrap re-join before connect failed: {e}");
            }

            // Dial through p2p-core with the relay-carrying addr (inserted as an
            // `App` path before address lookup runs, so the connect resolves
            // immediately and cannot park). `connect` returns `Err` for a
            // legacy/non-curve-point hint id, so the former explicit pre-check
            // collapses into the `Err` arm below (slot released either way).
            let addr = PeerAddr::new(peer).with_relay(relay_url);
            match dial.connect(&addr, GOSSIP_ALPN).await {
                Ok(conn) => {
                    // Adopt the connection into gossip: drains the queued Join →
                    // peer Active → handshake → NeighborUp.
                    if let Err(e) = dial.adopt_gossip_connection(conn).await {
                        debug!(peer = %peer, "gossip rejected supervisor bootstrap connection: {e}");
                    }
                }
                Err(e) => {
                    // A real, fast failure now (vs the parked bare-id dial) — the
                    // relay is genuinely unreachable this attempt (or, for a legacy
                    // id, un-dialable). The next due tick retries.
                    debug!(peer = %peer, "supervisor bootstrap connect failed: {e}");
                }
            }

            inflight.lock().await.remove(&peer);
        });
    }

    /// Connected-path allowlist reconcile, throttled to `roster_reconcile_interval_ms`.
    ///
    /// Called from `on_reconnect_tick`'s connected branch. Re-pushes the
    /// membership roster to live neighbors on a slow cadence so members that
    /// drifted across a restart or a missed `NeighborUp` re-converge even with no
    /// new pairings. Cheap: one small gossip broadcast at most once per window.
    pub(super) async fn maybe_reconcile_allowlist_roster(&mut self) {
        let now = now_ms();
        let due = match self.last_roster_reconcile_ms {
            None => true,
            Some(last) => now.saturating_sub(last) >= self.roster_reconcile_interval_ms,
        };
        if !due {
            return;
        }
        self.last_roster_reconcile_ms = Some(now);
        self.push_allowlist_roster().await;
    }

    /// Shrink the connected-path roster-reconcile throttle so integration tests
    /// can observe a reconcile within `wait_until`'s budget.
    ///
    /// `pub` solely for integration-test access — same rationale as
    /// [`Daemon::set_reconnect_interval`] (the test binary compiles this crate
    /// without the `test` cfg, so a `#[cfg(test)]` gate would break it). This is a
    /// SEAM on the daemon's own reconcile timer, not a widening of the allowlist
    /// API. Do not call from production code paths.
    pub fn set_roster_reconcile_interval(&mut self, period: std::time::Duration) {
        self.roster_reconcile_interval_ms = period.as_millis() as u64;
    }

    /// Bump `failure_count` for each hint dialed-but-unconnected this tick.
    ///
    /// Updates the in-memory snapshot (drives the next tick's throttle) and
    /// bumps the hint's in-memory `failure_count` and emits the one-shot eviction
    /// log when a hint first crosses [`STALE_FAILURE_THRESHOLD`].
    ///
    /// The throttle is runtime-only: the supervisor's working set is seeded from
    /// the `allowlist × known_public_relays` cross-product on each boot, so a
    /// restart resetting backoff is fine and there is nothing to persist.
    ///
    /// Grace-window interaction: this keeps bumping `failure_count` even during a
    /// post-net-change grace window. The grace window floors only the DUE-GATE
    /// (see [`effective_backoff_due`]), not the counter — so the accumulated
    /// failure history stays intact and real backoff resumes from it the moment
    /// the window expires. (A successful dial still zeroes the count via
    /// [`on_exchange_learned`], grace or not.)
    pub(super) fn record_due_hint_failures(&mut self, due_ids: &[PeerId]) {
        if due_ids.is_empty() {
            return;
        }

        let due_hex: Vec<String> = due_ids.iter().map(|id| id.to_string()).collect();

        // In-memory bump + one-shot stale-transition log.
        for hint in self.peer_relays.iter_mut() {
            if due_hex.contains(&hint.endpoint_id) {
                hint.failure_count = hint.failure_count.saturating_add(1);
                if hint.failure_count == STALE_FAILURE_THRESHOLD {
                    info!(
                        endpoint_id = %hint.endpoint_id,
                        failure_count = hint.failure_count,
                        backoff_ms = MAX_HINT_BACKOFF_MS,
                        "Peer relay hint stale — evicting from lookup, retrying slowly"
                    );
                }
            }
        }
    }

    /// A network change occurred (wifi switch, hotspot, new LAN). Reset every
    /// peer-relay hint's backoff so the next supervisor tick re-dials immediately
    /// on the new network — the fix for "every net change needs a restart."
    ///
    /// Purely additive: the supervisor's dial/evict/sole-hint logic
    /// (`on_reconnect_tick`) is untouched; we only un-throttle the hints by
    /// zeroing `failure_count`/`last_attempt_ms`, which flips `effective_backoff_due`
    /// to `true` for every hint so they all re-dial on the next wake. We ALSO open
    /// a [`NET_CHANGE_GRACE_MS`] grace window (`net_change_grace_until_ms`) so the
    /// early post-change dials that fail while iroh re-homes its own relay can't
    /// ramp the backoff back up before the relay is reachable (the off-LAN rehome
    /// fix; see [`effective_backoff_due`]).
    ///
    /// This pairs with `on_reconnect_tick`'s off-LAN-lifeline retention to protect
    /// the public-relay lifeline across a network switch: retention keeps it
    /// resident in the lookup even if a reconnect tick races ahead of this reset,
    /// and this reset re-dials it immediately on the new network.
    ///
    /// It ALSO kicks the mDNS discovery channel (republish addresses + restart
    /// the browse) so same-LAN re-discovery after a migration is prompt. Note this
    /// is a DIFFERENT channel from iroh's gossip re-announce: iroh re-publishes our
    /// `EndpointAddr` to peers we're ALREADY connected to (see `startup.rs`), while
    /// mDNS republish+re-browse is what re-establishes LAN discovery with peers we
    /// got partitioned from on the move. They don't overlap.
    pub(super) async fn on_network_change(&mut self) {
        if self.peer_relays.is_empty() {
            return; // unpaired / hint-less — nothing to re-dial.
        }
        info!(
            hints = self.peer_relays.len(),
            "Network change detected — resetting reconnect backoff"
        );

        // (1) In-memory snapshot (the supervisor's source of truth). This working
        // set is runtime-only — re-seeded from the cross-product on each boot — so
        // there is nothing to persist; a restart mid-flap re-seeds fresh anyway.
        for hint in self.peer_relays.iter_mut() {
            hint.failure_count = 0;
            hint.last_attempt_ms = None;
        }

        // (1b) Open the post-net-change grace window. Zeroing failure_count above
        // resets backoff to base, but the early reconnect dials fail while iroh
        // re-homes its own relay, ramping the backoff right back up with no second
        // reset once the relay is ready. The grace window floors the due-gate at
        // base for NET_CHANGE_GRACE_MS so those early failures don't strand a
        // now-reachable peer behind a multi-minute backoff (the off-LAN rehome fix).
        // Scaled so the window shrinks under the test-time-scale, consistent with
        // per_hint_backoff's own scaling.
        self.net_change_grace_until_ms = Some(now_ms() + scaled_ms(NET_CHANGE_GRACE_MS));

        // (2) Kick mDNS: re-advertise addresses + restart the browse for prompt
        // same-LAN re-discovery. Strictly additive — synchronous, best-effort,
        // and a no-op when mDNS is unavailable.
        self.sync_node.republish_mdns_on_net_change();
    }

    /// Refresh a peer's hint after a successful background sync exchange.
    ///
    /// This is what makes stale-hint eviction safe: a reachable peer's
    /// in-memory hint is continuously reset (success stamped, `failure_count`
    /// zeroed) so it never goes stale, and a fresh relay URL learned mid-session
    /// replaces a moved peer's stale one. Touches three runtime places, kept in
    /// step (the per-peer hint is NOT persisted — `known_public_relays` is the
    /// sole durable networking store; this only refreshes session state):
    ///
    /// 1. **In-memory snapshot** (supervisor's truth): stamp + reset the matching
    ///    hint. If we learned a relay for a peer we had NO hint for, INSERT one —
    ///    this fixes the responder-side asymmetry where only the pairing initiator
    ///    ever seeded the other side's relay.
    /// 2. **Public-relay set** (`learn_public_relay`): a learned relay that is a
    ///    NEW public relay (a second server's home) is adopted into the RelayMap +
    ///    persisted `known_public_relays` for failover. No-op otherwise.
    /// 3. **Live lookup** (`set_peer_relay`): only when a URL is known, so the
    ///    next dial uses the fresh hint.
    pub(super) async fn on_exchange_learned(&mut self, learned: ExchangeLearned) {
        let endpoint_hex = learned.endpoint_id.to_string();
        let now = now_ms();

        // (1) In-memory snapshot (session-only; re-seeded from the cross-product
        // on the next boot).
        if let Some(hint) = self
            .peer_relays
            .iter_mut()
            .find(|r| r.endpoint_id == endpoint_hex)
        {
            hint.last_success_ms = Some(now);
            hint.failure_count = 0;
            if let Some(ref url) = learned.relay_url {
                hint.relay_url = url.as_str();
            }
        } else if let Some(ref url) = learned.relay_url {
            // No hint yet, but we learned this peer's relay — record it so the
            // supervisor can re-dial them this session (responder-asymmetry fix).
            let mut entry = crate::persistence::PeerRelay::new(endpoint_hex.clone(), url.as_str());
            entry.last_success_ms = Some(now);
            self.peer_relays.push(entry);
        }

        // (2 — C6 gossip expansion) The learned relay may also be a NEW public
        // relay (a second server's home relay), in which case we adopt it into our
        // own RelayMap + persisted public-set for failover redundancy. Borrows the
        // relay URL so block (3) below can still move it. No-op for LAN-direct
        // (`None`), private, or already-known relays.
        self.learn_public_relay(learned.relay_url.as_ref()).await;

        // (3) Live re-seed so the next dial uses the fresh hint.
        if let Some(url) = learned.relay_url {
            self.sync_node.set_peer_relay(learned.endpoint_id, &url);
        }

        debug!(endpoint_id = %endpoint_hex, "Refreshed peer relay hint on successful exchange");
    }

    /// Adopt a relay learned from a peer into this node's public-relay set, when it
    /// is a new public relay (C6 gossip expansion).
    ///
    /// A server's home relay IS its public relay, so once a laptop joins the mesh
    /// it can discover public relays it was never paired with (e.g. a second
    /// server's) and adopt them for failover redundancy — live (no restart) and
    /// persisted (so the next cold start homes on them too).
    ///
    /// Three effects, gated together on the adoption decision (so a private,
    /// already-known, or absent relay does nothing):
    /// 1. **Live RelayMap** (`add_home_relay` → `insert_relay`) so net-report can
    ///    probe + fail over to it THIS session.
    /// 2. **Persisted public set** (`add_known_public_relay`) so it survives restart.
    /// 3. **Supervisor cross-product** — a new public relay means new
    ///    `(allowlist peers) × {new relay}` reconnect targets, so each trusted peer
    ///    gains a `(peer, new_relay)` hint (in-memory snapshot + live `peer_lookup`),
    ///    letting the supervisor dial already-trusted peers through it.
    ///
    /// SECURITY: this only ever adds a TRANSPORT hint. The trusted EndpointIds come
    /// from the ALLOWLIST; the relay never contributes or changes an identity. A
    /// learned relay paired with an already-trusted EndpointId is TLS-verified on
    /// dial regardless of the relay, so a hostile relay is DoS/metadata only.
    ///
    /// The idempotency guard reads the persisted `known_public_relays` (iroh exposes
    /// no `Endpoint::contains_relay()`): since every relay we insert we also persist
    /// and vice-versa, set-membership stands in for RelayMap membership and prevents
    /// per-exchange churn. The config read is skipped for the common LAN-direct /
    /// private cases via the cheap classifier pre-check below.
    pub(super) async fn learn_public_relay(&mut self, learned: Option<&RelayAddr>) {
        // Cheap pre-filter: skip the disk read for the overwhelmingly common cases
        // (LAN-direct exchange → None; a loopback/LAN relay → private). Only a
        // genuinely public learned relay is worth a config round-trip.
        let candidate = match learned {
            Some(url) if relay_is_offlan_reachable(&url.as_str()) => url,
            _ => return,
        };

        // Load the persisted set to apply the idempotency guard. A read failure is
        // non-fatal: skip this adoption rather than risk churn on an unknown state.
        let known = match crate::persistence::DaemonConfig::load_or_generate(&self.vault_path, None)
            .await
        {
            Ok((config, _)) => config.known_public_relays,
            Err(e) => {
                warn!("Failed to read config for learned-relay adoption: {e}");
                return;
            }
        };
        let Some(url) = learned_public_relay_to_adopt(Some(candidate), &known) else {
            return;
        };
        let url_str = url.as_str();

        // (1) Live RelayMap.
        self.sync_node.add_home_relay(&url).await;

        // (2) Persist into the cold store.
        let persist = crate::persistence::persist_config_change(
            &self.vault_path,
            self.relay_url.clone(),
            |config| config.add_known_public_relay(&url_str),
        )
        .await;
        if let Err(e) = persist {
            warn!("Failed to persist learned public relay: {e}");
        }

        // (3) Refresh the supervisor's cross-product with (allowlist peers) × {new
        // relay}. Mirrors the startup seed in `startup.rs`: skip self (iroh rejects
        // self-directed relay paths) and invalid EndpointIds; dedup against the
        // existing snapshot so re-learning is a no-op.
        let own_peer_id = self.sync_node.node_id();
        let peers = match self.allowlist.list_peers().await {
            Ok(peers) => peers,
            Err(e) => {
                warn!("Failed to read allowlist for learned-relay cross-product: {e}");
                return;
            }
        };
        for peer in &peers {
            // Validate the peer key up front, skipping a legacy/non-curve-point id
            // gracefully (never panicking) — the deliberate degrade this loop has
            // always done. The seed itself now speaks `PeerId`.
            if !peer.node_id.is_dialable() {
                warn!("Skipping invalid allowlist peer for learned-relay seed");
                continue;
            }
            if peer.node_id == own_peer_id {
                continue;
            }
            let endpoint_hex = peer.node_id.to_string();
            let already = self
                .peer_relays
                .iter()
                .any(|h| h.endpoint_id == endpoint_hex && h.relay_url == url_str);
            if already {
                continue;
            }
            self.sync_node.add_peer_relay(peer.node_id, &url);
            self.peer_relays.push(crate::persistence::PeerRelay::new(
                endpoint_hex,
                url_str.clone(),
            ));
        }
    }

    /// Shrink the reconnect-supervisor tick interval so integration tests can
    /// observe recovery within `wait_until`'s 10s budget.
    ///
    /// This method is `pub` solely for integration test access — call it before
    /// `run_loop` starts. Production uses the [`RECONNECT_BASE_MS`] cadence built
    /// in `Daemon::new`. It cannot be `#[cfg(test)]`: integration-test binaries
    /// compile this crate without the `test` cfg, so the gate would break them.
    /// Do not call from production code paths.
    pub fn set_reconnect_interval(&mut self, period: std::time::Duration) {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        self.reconnect_tick = tick;
    }

    /// Populate the in-memory peer-relay snapshot the supervisor re-dials from.
    ///
    /// Called once at startup with the `allowlist × known_public_relays`
    /// cross-product (built in `startup.rs` — the same set that seeds the live
    /// lookup), and refreshed at runtime by pairing / learn-on-exchange. Held in
    /// memory rather than reloaded from disk per tick: the per-peer hint is no
    /// longer persisted (`known_public_relays` is the sole durable store), and the
    /// integration harness — which runs against a non-existent vault path — can
    /// seed it directly.
    ///
    /// Asymmetry (inherited from pairing, see `pair_shared.rs`): only the
    /// INITIATOR learns the responder's relay at pair time, so after a fresh pair
    /// only the initiator's snapshot gains a direct entry — the responder reaches
    /// the initiator via the cross-product (the initiator's public relay is in
    /// `known_public_relays`) or once a learn-on-exchange follow-up fires.
    ///
    /// In `MemoryLookup` test wiring the actual dial resolves via direct
    /// addresses, so a seeded relay URL only needs to parse, not point anywhere
    /// real.
    pub fn seed_peer_relays_snapshot(&mut self, peer_relays: Vec<crate::persistence::PeerRelay>) {
        self.peer_relays = peer_relays;
    }

    /// Drive the C6 learned-public-relay adoption path with a fabricated learned
    /// relay, for integration-test access.
    ///
    /// A genuine relay-routed exchange between in-process nodes learns a loopback
    /// URL, which the off-LAN-reachable classifier (correctly) rejects — so the
    /// positive adoption path (a public relay a real second server advertises) is
    /// only reachable in tests by injecting the URL. Exercises the real
    /// `learn_public_relay` logic; the production loop reaches it via
    /// `on_exchange_learned`. `pub` for the same seam rationale as
    /// [`Daemon::seed_peer_relays_snapshot`] — integration-test binaries compile
    /// this crate without the `test` cfg, so a `#[cfg(test)]` gate would break them.
    /// Not a public production API.
    pub async fn learn_public_relay_for_test(&mut self, relay_url: &RelayAddr) {
        self.learn_public_relay(Some(relay_url)).await;
    }

    /// Read-only view of the supervisor's in-memory peer-relay snapshot, for
    /// integration-test assertions (e.g. that a learned public relay seeded the
    /// cross-product). `pub` for the same seam rationale as the methods above.
    pub fn peer_relays_snapshot_for_test(&self) -> &[crate::persistence::PeerRelay] {
        &self.peer_relays
    }

    /// Drive the real learn-on-exchange refresh with a fabricated exchange
    /// outcome, for integration-test access.
    ///
    /// Exercises the production [`Daemon::on_exchange_learned`] handler — the
    /// path a genuine NeighborUp sync reaches via the run loop's
    /// `exchange_learn_rx` arm. A test owns the daemon and drives this directly so
    /// it can then assert the in-memory reset via [`Daemon::peer_relays_snapshot_for_test`]
    /// (the supervisor's working set lives inside the daemon, so it can't be read
    /// from a daemon that has been moved into a spawned `run_loop` task). `pub` for
    /// the same seam rationale as [`Daemon::seed_peer_relays_snapshot`] —
    /// integration-test binaries compile this crate without the `test` cfg, so a
    /// `#[cfg(test)]` gate would break them. Not a public production API.
    pub async fn apply_exchange_success_for_test(
        &mut self,
        endpoint_id: PeerId,
        relay_url: Option<RelayAddr>,
    ) {
        self.on_exchange_learned(ExchangeLearned {
            endpoint_id,
            relay_url,
        })
        .await;
    }

    /// Record a single learned/refreshed peer relay into the in-memory snapshot
    /// (last-write-wins per endpoint_id), keeping the supervisor's view current
    /// after a runtime pairing without a restart.
    pub fn upsert_peer_relay_snapshot(&mut self, endpoint_id: String, relay_url: String) {
        if let Some(existing) = self
            .peer_relays
            .iter_mut()
            .find(|r| r.endpoint_id == endpoint_id)
        {
            existing.relay_url = relay_url;
        } else {
            self.peer_relays
                .push(crate::persistence::PeerRelay::new(endpoint_id, relay_url));
        }
    }
}

#[cfg(test)]
mod backoff_tests {
    use super::{
        HINT_BACKOFF_BASE_MS, MAX_HINT_BACKOFF_MS, NET_CHANGE_GRACE_MS, effective_backoff_due,
        per_hint_backoff,
    };
    use crate::persistence::PeerRelay;

    // A failure count comfortably past the shift ceiling, to prove the clamp.
    const STALE_OR_MORE: u32 = 10;

    /// Build a synthetic hint carrying just the freshness state the pure
    /// functions read (the endpoint_id/relay_url are irrelevant here).
    fn hint(failure_count: u32, last_attempt_ms: Option<u64>) -> PeerRelay {
        let mut h = PeerRelay::new("x".repeat(64), "http://relay:3340/".to_string());
        h.failure_count = failure_count;
        h.last_attempt_ms = last_attempt_ms;
        h
    }

    /// Whether a hint is due with NO grace window active (the steady-state path).
    /// Keeps the backoff-cadence tests reading as "is this hint due now" while
    /// routing through the real `effective_backoff_due` gate.
    fn due(h: &PeerRelay, now_ms: u64) -> bool {
        effective_backoff_due(h.failure_count, h.last_attempt_ms, now_ms, None)
    }

    /// Per-hint backoff doubles per failure, saturates at the cap, never overflows.
    #[test]
    fn per_hint_backoff_grows_and_caps() {
        // Zero failures → the base interval.
        assert_eq!(per_hint_backoff(0), HINT_BACKOFF_BASE_MS);
        // Each failure doubles the window.
        assert_eq!(per_hint_backoff(1), HINT_BACKOFF_BASE_MS * 2);
        assert_eq!(per_hint_backoff(2), HINT_BACKOFF_BASE_MS * 4);

        // Climbs to and saturates at the cap; a huge failure count cannot
        // overflow the shift (it's clamped to the ceiling) nor exceed the cap.
        assert_eq!(per_hint_backoff(STALE_OR_MORE), MAX_HINT_BACKOFF_MS);
        assert_eq!(per_hint_backoff(u32::MAX), MAX_HINT_BACKOFF_MS);
        for fc in 0..50 {
            assert!(
                per_hint_backoff(fc) <= MAX_HINT_BACKOFF_MS,
                "backoff must never exceed the cap"
            );
        }
    }

    /// A never-attempted hint is due immediately.
    #[test]
    fn never_attempted_hint_is_due() {
        assert!(due(&hint(0, None), 0));
        assert!(due(&hint(9, None), 5_000_000));
    }

    /// A fresh hint (no failures) is due on the base cadence: not due before the
    /// base window elapses, due once it does.
    #[test]
    fn fresh_hint_due_on_base_cadence() {
        let h = hint(0, Some(1_000));
        // Just before the base window: not due.
        assert!(!due(&h, 1_000 + HINT_BACKOFF_BASE_MS - 1));
        // Exactly at the window: due.
        assert!(due(&h, 1_000 + HINT_BACKOFF_BASE_MS));
    }

    /// A throttled hint stays not-due inside its (longer) window and becomes due
    /// after it — the failure_count widens the window.
    #[test]
    fn throttled_hint_respects_wider_window() {
        let h = hint(2, Some(10_000));
        let window = per_hint_backoff(2);
        assert!(!due(&h, 10_000 + window - 1));
        assert!(due(&h, 10_000 + window));
    }

    /// Last-hint guarantee: even the stalest hint comes due within the capped
    /// window, so a returning off-LAN peer is never permanently abandoned.
    #[test]
    fn stalest_hint_eventually_due() {
        let h = hint(u32::MAX, Some(0));
        // One nanosecond before the cap elapses: still throttled.
        assert!(!due(&h, MAX_HINT_BACKOFF_MS - 1));
        // At the cap: due again — re-added and dialed.
        assert!(due(&h, MAX_HINT_BACKOFF_MS));
    }

    /// Clock-backward tolerance: a `last_attempt_ms` in the future (clock jumped
    /// back) yields `0` elapsed via saturating_sub, so the hint simply reads
    /// not-yet-due rather than panicking or wedging. It comes due again once the
    /// clock catches up — self-recovering within the re-add window.
    #[test]
    fn clock_backward_does_not_panic_or_wedge() {
        let h = hint(0, Some(1_000_000));
        // "Now" is before the recorded attempt — not due, no panic.
        assert!(!due(&h, 0));
        // Once the clock advances past attempt + window, it's due again.
        assert!(due(&h, 1_000_000 + HINT_BACKOFF_BASE_MS));
    }

    /// The post-net-change grace window floors backoff at base (the off-LAN
    /// rehome fix). A hint that has failed enough to back off well past the base
    /// window (here `failure_count = 3` → `60s << 3 = 8min`) is nonetheless due
    /// again on the BASE cadence (60s) while inside the grace window, so the early
    /// post-change dials that fail while iroh re-homes its relay keep retrying
    /// promptly instead of being stranded behind the ramped backoff. Without a
    /// grace window the same hint must wait the full 8-minute window.
    #[test]
    fn grace_window_floors_backoff_to_base() {
        let t0 = 1_000;
        let grace_until = Some(t0 + NET_CHANGE_GRACE_MS);
        // Sanity: failure_count 3 normally backs off far past the base window.
        assert!(per_hint_backoff(3) > HINT_BACKOFF_BASE_MS);

        // Just past the base window, still inside the grace window → due (the
        // grace floor uses per_hint_backoff(0) = base regardless of failures).
        assert!(effective_backoff_due(
            3,
            Some(t0),
            t0 + HINT_BACKOFF_BASE_MS + 1_000,
            grace_until,
        ));
        // Same instant with NO grace window → NOT due (the real 8-minute window
        // governs), proving the grace floor is what makes the difference.
        assert!(!effective_backoff_due(
            3,
            Some(t0),
            t0 + HINT_BACKOFF_BASE_MS + 1_000,
            None,
        ));
    }

    /// Once the grace window expires, normal backoff resumes from the still-intact
    /// failure_count (the counter keeps climbing during grace; only the due-gate
    /// is floored). A hint that was due-on-base mid-window is no longer due just
    /// past base once `now` passes the grace deadline — it must wait its full
    /// failure-scaled window again.
    #[test]
    fn backoff_resumes_after_grace_window_expires() {
        let t0 = 1_000;
        let grace_deadline = t0 + NET_CHANGE_GRACE_MS;
        let grace_until = Some(grace_deadline);
        let last_attempt = grace_deadline; // attempted right as grace ends
        let just_past_base = last_attempt + HINT_BACKOFF_BASE_MS + 1_000;

        // `now` is past the grace deadline, and only the base window has elapsed
        // since the last attempt — the full failure-scaled window has not, so the
        // hint is throttled again (grace no longer applies).
        assert!(just_past_base >= grace_deadline);
        assert!(!effective_backoff_due(
            3,
            Some(last_attempt),
            just_past_base,
            grace_until
        ));

        // It becomes due once the real failure-scaled window elapses.
        assert!(effective_backoff_due(
            3,
            Some(last_attempt),
            last_attempt + per_hint_backoff(3),
            grace_until,
        ));
    }
}

/// Tests for the C6 gossip-expansion decision: which learned relays a node
/// should adopt into its public-relay set (RelayMap + `known_public_relays`).
#[cfg(test)]
mod learn_public_relay_tests {
    use super::learned_public_relay_to_adopt;
    use p2p_core::RelayAddr;

    fn url(s: &str) -> RelayAddr {
        RelayAddr::parse(s).expect("test relay URL should parse")
    }

    /// A peer's home relay that is off-LAN-reachable (a public server relay) and
    /// not yet in our set is adopted — this is how a laptop discovers a second
    /// server's relay from the mesh for failover redundancy.
    #[test]
    fn adopts_new_public_relay() {
        let learned = url("https://server2.example.com/");
        let known: Vec<String> = vec!["https://umbra.computer/".to_string()];
        assert_eq!(
            learned_public_relay_to_adopt(Some(&learned), &known),
            Some(learned.clone())
        );
    }

    /// A LAN-direct exchange carries no relay path (`relay_url = None`) — there is
    /// nothing to adopt.
    #[test]
    fn lan_direct_exchange_adopts_nothing() {
        let known: Vec<String> = vec![];
        assert_eq!(learned_public_relay_to_adopt(None, &known), None);
    }

    /// A learned private LAN-IP relay is never adopted into the public set: it is
    /// useless to any peer that isn't on that LAN, so it must not become a home
    /// candidate. Mirrors the `add_known_public_relay` classifier guard.
    #[test]
    fn private_lan_relay_is_not_adopted() {
        let learned = url("http://192.168.68.52:3340/");
        let known: Vec<String> = vec![];
        assert_eq!(learned_public_relay_to_adopt(Some(&learned), &known), None);
    }

    /// A relay we already track is not re-adopted — this is the idempotency guard
    /// that prevents per-exchange RelayMap churn and duplicate cross-product
    /// reconnect entries.
    #[test]
    fn already_known_relay_is_not_re_adopted() {
        let learned = url("https://umbra.computer/");
        let known: Vec<String> = vec!["https://umbra.computer/".to_string()];
        assert_eq!(learned_public_relay_to_adopt(Some(&learned), &known), None);
    }
}
