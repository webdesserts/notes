//! Shutdown controller for the Quit handler.
//!
//! Owns the cancellation token and `done_rx` signal so the Quit menu handler can
//! request daemon shutdown and wait — with a timeout — for the watchdog to
//! confirm the daemon task has exited. Separated from `daemon_task.rs` so the
//! timeout/cancel semantics are unit-testable without standing up a full
//! `DaemonHandle`.

use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Owns the shutdown signal for the running daemon task.
///
/// `shutdown()` is `self`-consuming so the Quit handler enforces single-call
/// semantics; managed-state callers take this controller via `Option::take` on
/// the first Quit event.
pub struct ShutdownController {
    pub token: CancellationToken,
    pub done_rx: oneshot::Receiver<()>,
}

impl ShutdownController {
    /// Cancel the daemon and wait up to `timeout` for the watchdog to confirm exit.
    ///
    /// On timeout, logs a warning and returns; the caller should still proceed to
    /// `app.exit()` so the process exits even if the daemon hung.
    /// Returns `true` once the watchdog confirmed the exit, `false` on timeout.
    pub async fn shutdown(self, timeout: Duration) -> bool {
        self.token.cancel();
        match tokio::time::timeout(timeout, self.done_rx).await {
            Ok(Ok(())) => {
                info!("Daemon shut down cleanly");
                true
            }
            Ok(Err(_)) => {
                info!("Daemon task completed (watchdog exited)");
                true
            }
            Err(_) => {
                warn!("Daemon shutdown exceeded {}s", timeout.as_secs());
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// shutdown() waits for done_rx and returns cleanly when the watchdog fires.
    #[tokio::test]
    async fn test_shutdown_waits_for_watchdog() {
        let token = CancellationToken::new();
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let controller = ShutdownController {
            token: token.clone(),
            done_rx,
        };

        // Fire the "watchdog done" signal after a short delay.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = done_tx.send(());
        });

        let start = std::time::Instant::now();
        controller.shutdown(Duration::from_secs(5)).await;

        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(token.is_cancelled());
    }

    /// shutdown() returns after the timeout if the watchdog never fires.
    #[tokio::test]
    async fn test_shutdown_times_out_if_daemon_hangs() {
        let token = CancellationToken::new();
        let (_done_tx, done_rx) = oneshot::channel::<()>(); // never sent
        let controller = ShutdownController { token, done_rx };

        let start = std::time::Instant::now();
        controller.shutdown(Duration::from_millis(100)).await;

        assert!(start.elapsed() >= Duration::from_millis(90));
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
