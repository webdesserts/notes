//! The application protocol seam: p2p-core's OWN handler trait + error type,
//! plus the internal adapter that bridges to iroh's `ProtocolHandler`.
//!
//! An app registers a [`ProtocolHandler`] under an ALPN via
//! `P2pNode::with_sync_alpn`. p2p-core wraps it in [`IrohHandlerAdapter`] before
//! handing it to iroh's Router, so the app never names `iroh::protocol::*`. The
//! [`Connection`] the trait hands the app is iroh's (Tier-2 re-export) — p2p-core
//! wraps the EXTENSION POINT, not the raw byte-I/O.

// Tier-2 re-exports: the raw QUIC byte-I/O handles. These stay iroh's types,
// surfaced through p2p-core so consumers (the daemon's `sync_stream.rs`,
// vault-sync's `RejectSyncHandler`) name them via `p2p_core` and keep `iroh` out of their
// manifests. `ReadExactError` is included because `read_frame` matches it by
// variant, so the daemon must name the type. p2p-core wraps the EXTENSION POINT
// (the handler trait, `connect`), not the byte-I/O — swappable for a real wrapper
// later behind the same path.
pub use iroh::endpoint::{Connection, ReadExactError, RecvStream, SendStream};

/// Error returned from a [`ProtocolHandler::accept`].
///
/// Transparent wrapper over iroh's accept error — same value, no p2p-core name in
/// the app's handler signature. Construct via [`AcceptError::from_err`] /
/// [`AcceptError::from_boxed`] exactly as the iroh API was used (the bounds mirror
/// `iroh::protocol::AcceptError`'s, so existing call sites are unchanged).
#[derive(Debug)]
pub struct AcceptError(iroh::protocol::AcceptError);

impl AcceptError {
    /// Create from an arbitrary error type (mirrors `iroh::protocol::AcceptError::from_err`).
    #[track_caller]
    pub fn from_err<T: std::error::Error + Send + Sync + 'static>(value: T) -> Self {
        Self(iroh::protocol::AcceptError::from_err(value))
    }

    /// Create from a boxed error (mirrors `iroh::protocol::AcceptError::from_boxed`).
    #[track_caller]
    pub fn from_boxed(value: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self(iroh::protocol::AcceptError::from_boxed(value))
    }

    /// p2p-core-internal: unwrap to iroh's error at the adapter boundary.
    pub(crate) fn into_iroh(self) -> iroh::protocol::AcceptError {
        self.0
    }
}

/// An application protocol registered on the node under an ALPN.
///
/// p2p-core's analogue of `iroh::protocol::ProtocolHandler`. The app implements
/// this; p2p-core adapts it to iroh internally via [`IrohHandlerAdapter`]. The
/// [`Connection`] is iroh's (re-exported), so handler bodies (`accept_bi`, framing)
/// are unchanged.
///
/// The returned future is `+ Send` because iroh's `ProtocolHandler` requires it
/// (the Router drives handlers across threads); an `async fn` here would desugar
/// to a future not provably `Send`, which the adapter could not bridge.
pub trait ProtocolHandler: Send + Sync + std::fmt::Debug + 'static {
    fn accept(
        &self,
        connection: Connection,
    ) -> impl std::future::Future<Output = Result<(), AcceptError>> + Send;
}

/// p2p-core-internal bridge: any [`ProtocolHandler`] is usable where iroh wants
/// one. Transparent pass-through — NO behavior, NO logging, NO reordering. Only
/// unwraps the [`AcceptError`] to iroh's error at the boundary.
#[derive(Debug)]
pub(crate) struct IrohHandlerAdapter<H>(pub(crate) H);

impl<H: ProtocolHandler> iroh::protocol::ProtocolHandler for IrohHandlerAdapter<H> {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        self.0
            .accept(connection)
            .await
            .map_err(AcceptError::into_iroh)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The adapter forwards a handler's error to iroh transparently. `into_iroh`
    /// is the only logic in `IrohHandlerAdapter::accept`, so pinning that the
    /// original error message survives the `AcceptError` → `iroh::AcceptError`
    /// unwrap is the hermetic guard that the adapter does not swallow errors —
    /// the live integration suites cover the full over-the-wire accept-error path.
    #[test]
    fn accept_error_carries_message_through_into_iroh() {
        let original = "handler refused the connection";
        let wrapped = AcceptError::from_err(std::io::Error::other(original));
        let iroh_err = wrapped.into_iroh();
        // iroh's `User` variant is `#[error(transparent)]`, so its Display is the
        // wrapped error's — the message must survive the unwrap verbatim.
        assert!(
            format!("{iroh_err}").contains(original),
            "into_iroh dropped the error message: {iroh_err}"
        );
    }
}
