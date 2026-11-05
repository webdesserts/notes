//! Integration test for the Daemon event loop — inbound-sync freshness signal (S2).
//!
//! Peeled out of `daemon_move_recovery.rs` to keep that file under the line-size
//! target. A single test that drives a real `SYNC_ALPN` handshake into a
//! `PumpedSyncHandler` and asserts the paired `inbound_seen_rx` (the receiver
//! `set_inbound_seen_rx` consumes) yields the initiator's `PeerId` — proving an
//! inbound-only peer is stamped alive. Harness lives in `common`.

mod common;

mod daemon_inbound_freshness {
    use std::sync::Arc;
    use std::time::Duration;

    use p2p_core::MemoryLookup;
    use tokio::sync::{Mutex, mpsc};

    use p2p_core::PeerId;
    use p2p_core::allowlist::{AllowlistStorage, InMemoryAllowlist};
    use vault_sync::Vault;
    use vault_sync::fs::InMemoryFs;
    use vault_sync::network::{SyncNode, SyncNodeSeam};

    use super::common::build_node;

    // ── inbound-sync freshness signal (S2) ────────────────────────────────────

    /// The receiver `Daemon::set_inbound_seen_rx` wires is the one the pumped inbound
    /// handler fires on: a peer that completes an inbound sync is reported on that
    /// channel so the run loop can stamp its freshness, which is what lets an
    /// inbound-ONLY peer (one that never initiates) still count as alive (S2).
    ///
    /// Proven end-to-end without a full daemon: drive a real `SYNC_ALPN` handshake
    /// opener into a `PumpedSyncHandler` and assert the paired `inbound_seen_rx` —
    /// the exact receiver `set_inbound_seen_rx` consumes — yields the initiator's
    /// `PeerId`. A regression that drops the handler's `inbound_seen_tx.send` (or
    /// hands the setter a different channel) would leave the receiver empty here.
    #[tokio::test]
    async fn inbound_sync_fires_the_freshness_signal_for_the_wired_receiver() -> anyhow::Result<()>
    {
        // Responder: a node holding the pumped handler, plus the receiver the daemon
        // would wire via `set_inbound_seen_rx`.
        let responder_fs = Arc::new(InMemoryFs::new());
        let responder_author = PeerId::from_secret_bytes(super::common::seed(80));
        let responder_vault = Arc::new(Mutex::new(
            Vault::init(responder_fs.clone(), responder_author.as_u64()).await?,
        ));
        let responder_allowlist = Arc::new(InMemoryAllowlist::new());
        let (inbound_seen_tx, mut inbound_seen_rx) = mpsc::unbounded_channel();
        let handler = sync_daemon::daemon::PumpedSyncHandler::new(
            responder_vault.clone(),
            responder_allowlist.clone(),
            inbound_seen_tx,
        );
        let responder = SyncNode::new_with_sync_handler(
            super::common::seed(80),
            &[],
            responder_allowlist.clone(),
            handler,
        )
        .await?;

        // Initiator: a node that opens the handshake. Its vault builds a real
        // `SyncRequest` opener so the responder's handler runs its true path.
        let initiator = build_node(81).await?;
        let initiator_id = initiator.node_id;

        // The handler allowlists once per connection (deny-on-error) — the initiator
        // must be allowed, or the inbound sync is dropped before it fires the signal.
        responder_allowlist
            .add_peer(initiator_id, "initiator")
            .await?;

        // Teach the initiator how to reach the responder's endpoint.
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(responder.endpoint_for_test().addr());
        initiator
            .sync_node
            .endpoint_for_test()
            .address_lookup()?
            .add(lookup);

        // Open the handshake: connect on `SYNC_ALPN` and write the framed opener. The
        // wire format is the `[u32 LE len][bytes]` envelope (re-derived here, as the
        // daemon's own `write_frame` does — see `sync_stream.rs`).
        let opener = initiator.vault.lock().await.prepare_request().await?;
        // Dial through `P2pNode::connect` (the public Tier-1 method); the bare
        // `PeerAddr` resolves via the lookup we just seeded.
        let connection = initiator
            .sync_node
            .connect(
                &p2p_core::PeerAddr::new(responder.node_id()),
                vault_sync::network::SYNC_ALPN,
            )
            .await?;
        let (mut send, _recv) = connection.open_bi().await?;
        let len = u32::try_from(opener.len()).expect("opener fits in u32");
        send.write_all(&len.to_le_bytes()).await?;
        send.write_all(&opener).await?;
        send.finish()?;

        // The handler processes the opener and fires the initiator's id on the
        // freshness channel. Bound the wait so a regression fails fast.
        let received = tokio::time::timeout(Duration::from_secs(5), inbound_seen_rx.recv())
            .await
            .expect("inbound-seen signal should arrive before the timeout");

        assert_eq!(
            received,
            Some(initiator_id),
            "the wired receiver must yield the initiator's PeerId after an inbound sync"
        );

        // Hold the responder + connection until the assertion completes (dropping the
        // node early would tear the endpoint down before the handler runs).
        drop(connection);
        drop(responder);
        Ok(())
    }
}
