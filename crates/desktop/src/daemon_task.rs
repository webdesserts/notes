//! Cancellation-token-aware daemon spawn helper.
//!
//! Runs `sync_daemon::daemon::run_with_shutdown_controlled` inside Tauri's
//! runtime and returns a `DaemonHandle` carrying the shutdown token, the
//! `DaemonControl` for the tray status driver and pairing commands, and a
//! `done_rx` the watchdog fires once the daemon task has exited.
//!
//! The watchdog exits the application if the running daemon fails or panics,
//! surfacing daemon errors as a visible app exit rather than a silent zombie
//! tray icon. A clean exit (cancellation on restart or Quit) keeps the app up.

use anyhow::Result;
use sync_daemon::daemon::{DaemonRunConfig, run_with_shutdown_controlled};
use sync_daemon::pair_api::DaemonControl;
use tauri::AppHandle;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Handle to one running daemon task.
pub struct DaemonHandle {
    pub token: CancellationToken,
    /// Live daemon control handle — subscribe to status, send pairing commands.
    pub control: DaemonControl,
    /// Receives a signal when the watchdog has observed the daemon task exit.
    done_rx: oneshot::Receiver<()>,
}

impl DaemonHandle {
    /// Run the daemon's startup phase, then spawn the watchdog over its event loop.
    ///
    /// Startup failures (e.g. another instance already holds the vault lock) are
    /// returned to the caller: fatal at app launch, an HTTP error on a restart.
    pub async fn spawn(config: DaemonRunConfig, app: AppHandle) -> Result<Self> {
        let token = CancellationToken::new();
        let (control, join_handle) = run_with_shutdown_controlled(config, token.clone()).await?;
        let (done_tx, done_rx) = oneshot::channel::<()>();
        tauri::async_runtime::spawn(async move {
            match join_handle.await {
                Ok(Ok(())) => info!("Daemon task exited cleanly"),
                Ok(Err(e)) => {
                    error!("Daemon exited with error: {e:#}");
                    app.exit(1);
                }
                Err(e) => {
                    error!("Daemon task panicked: {e}");
                    app.exit(1);
                }
            }
            let _ = done_tx.send(());
        });
        Ok(DaemonHandle {
            token,
            control,
            done_rx,
        })
    }

    /// Decompose this handle into its constituent parts.
    pub fn into_parts(self) -> (CancellationToken, DaemonControl, oneshot::Receiver<()>) {
        (self.token, self.control, self.done_rx)
    }
}
