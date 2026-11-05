//! Background QUIC sync-exchange helper shared by `on_neighbor_up` and
//! `on_change_received`.
//!
//! Both handlers follow the same tail: open a bi-stream to a peer, drive the
//! variable-length vault-sync handshake to termination (via the outbound pump in
//! `sync_stream`), and update last-seen timestamps. Only the log messages and a
//! `path` argument (present only for change-pull) differ.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use p2p_core::PeerId;
use p2p_core::allowlist::AllowlistStorage;
use p2p_core::{DialHandle, PeerAddr};
use vault_sync::Vault;
use vault_sync::fs::FileSystem;
use vault_sync::network::PeerRegistry;

use super::{ExchangeLearned, now_ms};

/// Identifies which event triggered the sync exchange, driving log output.
pub(super) enum SyncExchangeKind {
    /// Triggered by a gossip `NeighborUp` event — full bidirectional sync.
    ///
    /// Logs "Synced N file(s) from X on NeighborUp" on success, and a `debug!`
    /// when no changes were applied.
    NeighborUp,
    /// Triggered by a gossip change notification — pull the changed file.
    ///
    /// Includes the path that triggered the notification for log context.
    /// Does NOT log when there are no changes (the `else` branch is omitted —
    /// this matches the original `on_change_received` behavior).
    ChangePull {
        /// The file path from the gossip change notification.
        path: String,
    },
}

/// Spawn a background task that connects to `target`, sends `request_bytes`,
/// processes the response, and updates last-seen timestamps.
///
/// The `kind` parameter controls which log messages are emitted. The caller
/// is responsible for the allowlist check and sync-request preparation before
/// calling this — the spawned task inherits pre-cloned Arc handles and does not
/// need to re-acquire `&mut self`.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_sync_exchange<FS, AL>(
    target: PeerId,
    peer_id: PeerId,
    request_bytes: Vec<u8>,
    vault: Arc<Mutex<Vault<FS>>>,
    allowlist: Arc<AL>,
    peer_registry: Arc<Mutex<PeerRegistry>>,
    dial: DialHandle,
    exchange_learn_tx: mpsc::UnboundedSender<ExchangeLearned>,
    kind: SyncExchangeKind,
) where
    FS: FileSystem + 'static,
    AL: AllowlistStorage + 'static,
{
    tokio::spawn(async move {
        // Dial the peer on SYNC_ALPN through p2p-core. `target` is transport-sourced
        // (this exchange is driven by a live gossip neighbour / received change), so
        // the connect cannot hit the legacy-id arm.
        let connection = match dial
            .connect(&PeerAddr::new(target), vault_sync::network::SYNC_ALPN)
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                match &kind {
                    SyncExchangeKind::NeighborUp => {
                        warn!("Failed to sync with {}: {}", target, e);
                    }
                    SyncExchangeKind::ChangePull { .. } => {
                        warn!("Failed to pull change from {}: {}", target, e);
                    }
                }
                return;
            }
        };

        // Drive the variable-length vault-sync handshake to termination over one
        // bi-stream (digest opener → … → terminal `reply: None`), processing each
        // leg on the local vault. The pump scopes the vault lock to each
        // `process_message` (never across a wire await) so a concurrent
        // reverse-initiated sync can't deadlock against it.
        let modified =
            match super::sync_stream::pump_outbound(connection, &request_bytes, &vault).await {
                Ok(modified) => modified,
                Err(e) => {
                    match &kind {
                        SyncExchangeKind::NeighborUp => {
                            // NeighborUp fires before the peer's QUIC listener is ready on some
                            // OS configurations. Log a warning and move on — the peer will
                            // initiate a sync in the other direction.
                            warn!("Failed to sync with {}: {}", target, e);
                        }
                        SyncExchangeKind::ChangePull { .. } => {
                            warn!("Failed to pull change from {}: {}", target, e);
                        }
                    }
                    return;
                }
            };

        peer_registry.lock().await.update_last_seen(&peer_id);
        if let Err(e) = allowlist.update_last_seen(&peer_id, now_ms()).await {
            warn!("Failed to update allowlist last_seen for {}: {}", target, e);
        }

        match &kind {
            SyncExchangeKind::NeighborUp => {
                if !modified.is_empty() {
                    info!(
                        "Synced {} file(s) from {} on NeighborUp",
                        modified.len(),
                        target
                    );
                } else {
                    debug!("Full sync with {} complete — no changes", target);
                }
            }
            SyncExchangeKind::ChangePull { path } => {
                if !modified.is_empty() {
                    info!(
                        "Pulled {} file(s) from {} after change notification on {}",
                        modified.len(),
                        target,
                        path
                    );
                }
                // No `else` branch: `on_change_received` did not log when
                // no changes were applied. Preserved intentionally.
            }
        }

        // Learn-on-exchange: the peer is reachable, so refresh its hint. The
        // vault lock is already dropped (above), so this actor round-trip never
        // extends the lock-hold. A LAN-direct connection yields no active relay
        // path → `None`, which still resets freshness without inventing a URL.
        // `peer_relay` absorbs the former `remote_info` → `active_relay_url`
        // selection (same first-active-relay semantics).
        let relay_url = dial.peer_relay(target).await;
        // send() only fails if the run-loop receiver is gone (shutdown) — benign.
        let _ = exchange_learn_tx.send(ExchangeLearned {
            endpoint_id: target,
            relay_url,
        });
    });
}
