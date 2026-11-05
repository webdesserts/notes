//! Daemon-side pumped sync transport.
//!
//! vault-sync's handshake is variable-length: a converged pair settles in one
//! round-trip (`SyncRequest → InSync`), a diverged pair runs the full
//! `SyncRequest → DigestMismatch → SyncExchange → SyncResponse`. Either way the
//! exchange terminates when a side's `process_message` yields `reply: None`. The
//! daemon must therefore PUMP a single QUIC bi-stream — feeding each reply back
//! and reading the next — rather than the one-request / one-reply / half-close
//! that the removed v1 `connect_and_sync_raw` / `SyncStreamHandler` path did.
//!
//! This module is the daemon's own pumped path for BOTH directions:
//! - [`pump_outbound`] drives the exchange as the initiator (used by
//!   `spawn_sync_exchange`).
//! - [`PumpedSyncHandler`] is the inbound `ProtocolHandler`, registered on
//!   `SYNC_ALPN` via [`vault_sync::network::SyncNode::new_with_sync_handler`].
//!
//! The framing here ([`write_frame`]/[`read_frame`]) uses the same
//! `[u32 LE len][bytes]` envelope as p2p-core's `streams` helpers, re-derived
//! locally so the pump owns the stream lifetime (it keeps the stream open for the
//! continuation). The cap below keeps the 64 MiB limit of the v1 transport this
//! path replaced, so the wire format is unchanged.
//!
//! # Lock discipline (the load-bearing correctness rule)
//!
//! Both pump directions NEVER hold `vault.lock()` across a network `.await`. The
//! lock is scoped to each `process_message` call only — acquire, process, drop,
//! THEN read/write the wire. Holding it across a network await would deadlock
//! against a concurrent reverse-initiated sync (the "both sides initiate" design)
//! that also wants the vault.

use std::sync::Arc;

use p2p_core::{AcceptError, Connection, ProtocolHandler, ReadExactError, RecvStream, SendStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use p2p_core::PeerId;
use p2p_core::allowlist::AllowlistStorage;
use p2p_core::time_scale::scaled;
use vault_sync::Vault;
use vault_sync::fs::FileSystem;

/// Maximum byte length for a framed sync message.
///
/// 64 MiB, the cap of the v1 one-shot transport this path replaced — the daemon
/// frames on its own pumped path (so it can keep the stream open for the
/// continuation), and keeps the wire format and cap unchanged.
const MAX_MESSAGE_BYTES: u32 = 64 * 1024 * 1024;

/// Hard cap on the number of legs a single pumped exchange may run.
///
/// The vault-sync handshake is at most four legs (`SyncRequest`, `DigestMismatch`,
/// `SyncExchange`, `SyncResponse`), so 8 is generous headroom that still bounds a
/// protocol bug into a logged abort instead of an unbounded loop.
const MAX_PUMP_MESSAGES: usize = 8;

/// Per-leg idle read timeout. A peer that stalls mid-pump must not hang the
/// stream forever. Mirrors the 30s idle close the old one-shot inbound handler
/// applied (`streams.rs`), scaled by the test time-scale so integration tests
/// don't wait wall-clock seconds.
fn idle_read_timeout() -> std::time::Duration {
    scaled(std::time::Duration::from_secs(30))
}

/// Drive the variable-length vault-sync handshake to termination as the
/// initiator, over a single QUIC bi-stream.
///
/// Over the established `SYNC_ALPN` `connection`, sends `request_bytes` (the
/// digest opener), then ping-pongs: read the peer's reply, `process_message` it on
/// the LOCAL vault, and send the next reply back — until the local side returns
/// `reply: None` (terminal), at which point the send half is finished and the
/// loop ends. The returned `Vec<DocId>` accumulates every document the initiator
/// applied across the exchange (what the caller's success log + last-seen
/// bookkeeping read).
///
/// Replaces the removed v1 `connect_and_sync_raw`, which finished the send half
/// after one write and read exactly one reply — correct for the v1
/// single-round-trip protocol, but it foreclosed vault-sync's continuation.
///
/// The caller (`spawn_sync_exchange`) dials the peer on `SYNC_ALPN` via
/// `P2pNode::connect` / `DialHandle::connect` and hands the established
/// [`Connection`] here; this function opens the bi-stream and pumps the handshake.
pub(super) async fn pump_outbound<FS: FileSystem>(
    connection: Connection,
    request_bytes: &[u8],
    vault: &Arc<Mutex<Vault<FS>>>,
) -> anyhow::Result<Vec<vault_sync::DocId>> {
    // The peer's transport id, for the leg-cap warning's log field (byte-identical
    // to the `%target` the caller used to pass).
    let target = connection.remote_id();
    let (mut send, mut recv) = connection.open_bi().await?;

    // Send the opener. Do NOT finish the send half here — that would foreclose
    // the continuation legs.
    write_frame(&mut send, request_bytes).await?;

    let mut modified: Vec<vault_sync::DocId> = Vec::new();

    let mut hit_cap = true;
    for _ in 0..MAX_PUMP_MESSAGES {
        let reply_bytes = match read_frame(&mut recv).await? {
            Some(bytes) => bytes,
            // Clean EOF before a terminal local reply: the peer finished without
            // continuing. For a well-behaved peer the loop ends via `reply: None`
            // below; reaching here means the responder closed early, which is a
            // benign end (we simply have nothing more to process).
            None => {
                hit_cap = false;
                break;
            }
        };

        // Scope the lock to `process_message` ONLY — drop the guard before the
        // next wire `.await` so a concurrent reverse-initiated sync can take it.
        let outcome = {
            let v = vault.lock().await;
            v.process_message(&reply_bytes).await?
        };
        modified.extend(outcome.modified);

        match outcome.reply {
            Some(next) => {
                write_frame(&mut send, &next).await?;
            }
            None => {
                // Terminal leg: we have nothing more to send. Finish the send
                // half so the peer's next read sees a clean EOF and ends its loop.
                send.finish()?;
                hit_cap = false;
                break;
            }
        }
    }

    if hit_cap {
        // Ran the full leg budget without a terminal reply or EOF: the handshake
        // did not converge. Stopping here is the safety bound (never an unbounded
        // loop), but a healthy exchange settles in ≤4 legs, so reaching the cap
        // points at a protocol bug or a misbehaving peer worth surfacing.
        warn!(
            %target,
            max = MAX_PUMP_MESSAGES,
            "Outbound sync pump hit the leg cap without terminating — handshake did not converge"
        );
    }

    Ok(modified)
}

/// Inbound pumped `SYNC_ALPN` handler.
///
/// Registered on the daemon's `SyncNode` via `new_with_sync_handler`, this
/// REPLACES the default one-shot `SyncStreamHandler` for the daemon (sync-wasm
/// keeps the default). It holds the vault and allowlist directly so it can pump
/// multiple `process_message` turns inline on one stream, with no per-message
/// event-loop round-trip.
///
/// Freshness coupling (S2): a peer that ONLY ever connects inbound (never
/// initiates) must still count as alive, or its changes never broadcast (the
/// `alive_count` gate reads inbound-connection freshness too). The handler can't
/// reach `peer_registry` — that lives on `Daemon`, built after this handler is
/// registered at `SyncNode` construction — so it stamps the allowlist inline (it
/// holds that Arc) and fires the peer id on `inbound_seen_tx` for the run-loop to
/// stamp `peer_registry`. This is the same fire-and-forget shape as the outbound
/// learn-on-exchange channel — it carries no reply and never gates the stream.
///
/// `pub` so the integration-test harness can build a node with the same pumped
/// inbound path production uses (`build_node` → `new_with_sync_handler`).
#[derive(Clone)]
pub struct PumpedSyncHandler<FS: FileSystem, AL> {
    vault: Arc<Mutex<Vault<FS>>>,
    allowlist: Arc<AL>,
    /// Fire-and-forget signal: a remote peer completed an inbound sync. Drained
    /// by the run-loop, which stamps `peer_registry.update_last_seen`.
    inbound_seen_tx: mpsc::UnboundedSender<PeerId>,
}

// `ProtocolHandler` requires `Debug`, but the held vault/allowlist need not be
// printable — a name-only impl satisfies the bound without constraining `FS`/`AL`.
impl<FS: FileSystem, AL> std::fmt::Debug for PumpedSyncHandler<FS, AL> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PumpedSyncHandler").finish_non_exhaustive()
    }
}

impl<FS: FileSystem, AL> PumpedSyncHandler<FS, AL> {
    pub fn new(
        vault: Arc<Mutex<Vault<FS>>>,
        allowlist: Arc<AL>,
        inbound_seen_tx: mpsc::UnboundedSender<PeerId>,
    ) -> Self {
        Self {
            vault,
            allowlist,
            inbound_seen_tx,
        }
    }
}

impl<FS, AL> ProtocolHandler for PumpedSyncHandler<FS, AL>
where
    FS: FileSystem + 'static,
    AL: AllowlistStorage + 'static,
{
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_endpoint_id = connection.remote_id();
        let remote_id = PeerId::from_bytes(*remote_endpoint_id.as_bytes());

        // Allowlist check once per connection — the remote id is fixed for the
        // life of the stream. Fail closed on a read error (deny-on-error policy,
        // matching `is_peer_allowed`).
        let allowed = match self.allowlist.is_allowed(&remote_id).await {
            Ok(allowed) => allowed,
            Err(e) => {
                warn!(peer = %remote_endpoint_id, "Failed to read allowlist for inbound sync, denying: {e}");
                false
            }
        };
        if !allowed {
            warn!(peer = %remote_endpoint_id, "Inbound sync from non-allowlisted peer, dropping");
            return Ok(());
        }

        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .map_err(AcceptError::from_err)?;

        let mut total_modified = 0usize;

        let mut hit_cap = true;
        for _ in 0..MAX_PUMP_MESSAGES {
            // Bound each read so a peer stalling mid-pump can't hang the handler.
            let frame = match tokio::time::timeout(idle_read_timeout(), read_frame(&mut recv)).await
            {
                Ok(Ok(Some(bytes))) => bytes,
                // Clean EOF: the initiator finished its send half after the
                // terminal leg. Normal completion of a pumped exchange.
                Ok(Ok(None)) => {
                    hit_cap = false;
                    break;
                }
                Ok(Err(e)) => return Err(AcceptError::from_boxed(e.into())),
                Err(_) => {
                    debug!(peer = %remote_endpoint_id, "Inbound sync idle past timeout — closing");
                    hit_cap = false;
                    break;
                }
            };

            // Scope the lock to `process_message` ONLY — never hold it across the
            // wire `.await` below (deadlock guard, same as the outbound pump).
            let outcome = {
                let vault = self.vault.lock().await;
                match vault.process_message(&frame).await {
                    Ok(o) => o,
                    Err(e) => {
                        return Err(AcceptError::from_boxed(
                            anyhow::anyhow!("Failed to process inbound sync message: {e}").into(),
                        ));
                    }
                }
            };
            total_modified += outcome.modified.len();

            match outcome.reply {
                Some(reply) => {
                    write_frame(&mut send, &reply)
                        .await
                        .map_err(|e| AcceptError::from_boxed(e.into()))?;
                }
                None => {
                    // Terminal leg processed (e.g. a final `SyncResponse`): nothing
                    // more to send. Finish so the peer's read ends cleanly.
                    send.finish().map_err(AcceptError::from_err)?;
                    hit_cap = false;
                    break;
                }
            }
        }

        if hit_cap {
            // Ran the full leg budget without a terminal reply, EOF, or idle close:
            // the handshake did not converge. The cap is the safety bound against an
            // unbounded loop, but a healthy exchange settles in ≤4 legs, so reaching
            // it points at a protocol bug or a misbehaving peer worth surfacing.
            warn!(
                peer = %remote_endpoint_id,
                max = MAX_PUMP_MESSAGES,
                "Inbound sync pump hit the leg cap without terminating — handshake did not converge"
            );
        }

        if total_modified > 0 {
            info!("Applied {} doc(s) from inbound sync", total_modified);
        }

        // Stamp freshness for the broadcast `alive_count` gate (S2). Allowlist is
        // stamped inline (we hold the Arc); peer_registry is stamped by the
        // run-loop via the fire-and-forget channel.
        if let Err(e) = self
            .allowlist
            .update_last_seen(&remote_id, super::now_ms())
            .await
        {
            warn!("Failed to update allowlist last_seen for {remote_endpoint_id}: {e}");
        }
        // send() only fails if the run-loop receiver is gone (shutdown) — benign.
        let _ = self.inbound_seen_tx.send(remote_id);

        Ok(())
    }
}

/// Write a length-prefixed byte slice to the stream.
///
/// Format: `[u32 little-endian length][bytes]`, the same envelope as p2p-core's
/// `streams` helpers — see the module-level note on why this is re-derived here.
async fn write_frame(send: &mut SendStream, bytes: &[u8]) -> anyhow::Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| anyhow::anyhow!("Message too large to frame: {} bytes", bytes.len()))?;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(bytes).await?;
    Ok(())
}

/// Read a length-prefixed frame from the stream.
///
/// Returns `Ok(Some(bytes))` for a full frame, `Ok(None)` for a CLEAN end of
/// stream at a frame boundary (the peer finished its send half after a terminal
/// leg — the normal way a pumped exchange ends), and `Err` for any genuine read
/// error or a truncated frame.
///
/// Distinguishing clean-EOF from an error is what lets the pump loop end
/// gracefully: the side that sends the terminal `reply: None` finishes its send
/// half, and the other side's next `read_frame` returns `Ok(None)` rather than
/// surfacing a spurious error.
async fn read_frame(recv: &mut RecvStream) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        // Clean stream finish exactly at a frame boundary (no length prefix
        // started) — the peer is done, not an error.
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf);

    if len > MAX_MESSAGE_BYTES {
        return Err(anyhow::anyhow!(
            "Sync message too large: {} bytes (max {})",
            len,
            MAX_MESSAGE_BYTES
        ));
    }

    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf).await?;
    Ok(Some(buf))
}
