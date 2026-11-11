//! Bridges the daemon's `pairing_rx` broadcast into the Tauri UI layer.
//!
//! Subscribes to `DaemonControl.pairing_rx` and translates each `PairingUiEvent`
//! into the appropriate UI action:
//!
//! - `InboundRequest`: post a macOS notification (best-effort) and open the
//!   responder window via `pair_window::open_responder`.
//! - `InboundCompleted` / `InboundFailed`: emit a Tauri event to the responder
//!   window so its JS-side handler can show a brief status message and close;
//!   Rust also defensively closes the window after a short delay in case the
//!   listeners weren't yet registered.
//!
//! Lifecycle: the consumer task runs for the life of the app. If the daemon
//! shuts down, the broadcast is dropped — `recv()` returns `Closed` and the
//! task exits cleanly.

use std::time::Duration;

use serde::Serialize;
use sync_daemon::pair_api::PairingUiEvent;
use tauri::{AppHandle, Emitter};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::notification;
use crate::pair_window;

/// Tauri event payload describing a failed inbound pairing exchange.
#[derive(Serialize, Clone)]
struct ResponderFailedPayload {
    reason: String,
}

/// Spawn the pairing-events consumer task.
///
/// Owns the broadcast `Receiver` and the `AppHandle` clone for the lifetime of
/// the app. `start` returns immediately after spawning.
pub fn start(
    app: AppHandle,
    mut pairing_rx: broadcast::Receiver<PairingUiEvent>,
) -> tauri::async_runtime::JoinHandle<()> {
    tauri::async_runtime::spawn(async move {
        loop {
            match pairing_rx.recv().await {
                Ok(event) => handle_event(&app, event).await,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    // Pairing events are rare, so lagging means the consumer is
                    // genuinely stuck — log loudly so the situation surfaces.
                    warn!(
                        "Pairing events consumer lagged behind broadcast by {} events",
                        skipped
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("Pairing event broadcast closed — consumer exiting");
                    return;
                }
            }
        }
    })
}

async fn handle_event(app: &AppHandle, event: PairingUiEvent) {
    match event {
        PairingUiEvent::InboundRequest {
            device_name,
            code,
            expires_at_ms,
        } => {
            notification::post_pair_request(app, &device_name, &code);
            pair_window::open_responder(app, &device_name, &code, expires_at_ms);
        }
        PairingUiEvent::InboundCompleted { device_name } => {
            // Tell the responder window to show a brief confirmation and close.
            // The event-only path lets the JS provide a smoother transition
            // than an abrupt window.close().
            if let Err(e) = app.emit_to(
                tauri::EventTarget::labeled(pair_window::RESPONDER_LABEL),
                "pair://responder-completed",
                device_name,
            ) {
                warn!("Failed to emit pair://responder-completed: {}", e);
            }
            // Defensive close: if the window's JS never registered its listener
            // (e.g. the page hadn't finished loading when InboundCompleted
            // fired), the Rust side closes it after enough time for the JS
            // status flash. No-op if the window already closed itself.
            schedule_close(app);
        }
        PairingUiEvent::InboundFailed { reason } => {
            if let Err(e) = app.emit_to(
                tauri::EventTarget::labeled(pair_window::RESPONDER_LABEL),
                "pair://responder-failed",
                ResponderFailedPayload { reason },
            ) {
                warn!("Failed to emit pair://responder-failed: {}", e);
            }
            schedule_close(app);
        }
    }
}

/// Schedule a delayed close of the responder window. Gives the JS-side handler
/// time to show its status message and call `currentWindow.close()` itself
/// before Rust forcibly closes the window as a backstop.
fn schedule_close(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(Duration::from_millis(2000)).await;
        pair_window::close_responder(&app);
    });
}
