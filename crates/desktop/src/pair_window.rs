//! Pairing window lifecycle helpers.
//!
//! Creates and manages the initiator and responder pairing windows via
//! `WebviewWindowBuilder`. All window construction runs on the macOS main thread
//! via `AppHandle::run_on_main_thread` to avoid the macOS UI panic that occurs
//! when `WebviewWindowBuilder::build()` is called from a worker thread.

use sync_daemon::pair_api::DaemonCommand;
use tauri::{AppHandle, Manager, TitleBarStyle, WebviewUrl, WebviewWindowBuilder, WindowEvent};
use tokio::sync::oneshot;
use tracing::warn;

use crate::daemon_host::HostState;

/// Label assigned to the initiator window. Used by capabilities and event
/// targeting (`emit_to(labeled("pair-initiator"), ...)`).
pub const INITIATOR_LABEL: &str = "pair-initiator";

/// Label assigned to the responder window.
pub const RESPONDER_LABEL: &str = "pair-responder";

/// Open the pair-initiator window.
///
/// If the window already exists, focuses it instead of creating a duplicate.
/// Window construction is dispatched to the macOS main thread to avoid the
/// platform's "WebView build off main thread" panic.
///
/// Registers a close handler that issues `DaemonCommand::CancelInitiate` so the
/// daemon drops its in-flight initiator session when the user closes the window
/// via the X / Cmd+W rather than the in-window Cancel button. The daemon-side
/// command is idempotent, so it is safe whether or not an initiator session is
/// actually active.
pub fn open_initiator(app: &AppHandle) -> tauri::Result<()> {
    let app = app.clone();
    app.clone().run_on_main_thread(move || {
        if let Some(existing) = app.get_webview_window(INITIATOR_LABEL) {
            if let Err(e) = existing.set_focus() {
                warn!("Failed to focus existing pair-initiator window: {}", e);
            }
            return;
        }

        let url = WebviewUrl::App("pair-initiator.html".into());
        let build_result = WebviewWindowBuilder::new(&app, INITIATOR_LABEL, url)
            .title("Pair with nearby device")
            .inner_size(420.0, 360.0)
            .resizable(false)
            .title_bar_style(TitleBarStyle::Overlay)
            .build();

        let window = match build_result {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to create pair-initiator window: {}", e);
                return;
            }
        };

        let app_for_close = app.clone();
        window.on_window_event(move |event| {
            if matches!(event, WindowEvent::CloseRequested { .. }) {
                send_cancel_initiate(&app_for_close);
            }
        });
    })
}

/// Fire `DaemonCommand::CancelInitiate` so the daemon drops its in-flight
/// initiator session. The reply oneshot is awaited on a background task and
/// discarded — the close handler returns immediately so window teardown isn't
/// blocked on the daemon's reply.
fn send_cancel_initiate(app: &AppHandle) {
    let Some(control) = app.state::<HostState>().control() else {
        return;
    };
    let command_tx = control.command_tx.clone();
    let (reply_tx, reply_rx) = oneshot::channel::<()>();

    if let Err(e) = command_tx.send(DaemonCommand::CancelInitiate { reply: reply_tx }) {
        warn!(
            "Failed to send CancelInitiate on initiator window close: {}",
            e
        );
        return;
    }

    tauri::async_runtime::spawn(async move {
        let _ = reply_rx.await;
    });
}

/// Open the pair-responder window in response to an inbound pairing request.
///
/// The init data (responder's displayed code, the requesting device name, and
/// expiry timestamp) is delivered via the URL query string so the page reads it
/// synchronously on load — avoiding the listen-vs-emit race that would arise
/// from posting an event after `build()`.
///
/// `always_on_top: true` makes the window break above other apps so a LAN
/// pairing request is hard to miss. If a responder window already exists when
/// a new request arrives, the existing window is focused rather than replaced;
/// the daemon's `active_pairing` invariant ensures only one inbound request can
/// be live at a time, so the focus path covers the "redundant request" case.
///
/// Registers a close handler that fires `DaemonCommand::RejectInbound` for the
/// X / Cmd+W path, mirroring the Reject button. The daemon-side reject handler
/// is idempotent, so this is safe even after pairing has completed.
pub fn open_responder(app: &AppHandle, device_name: &str, code: &str, expires_at_ms: u64) {
    let app_for_main = app.clone();
    let device_name = device_name.to_string();
    let code = code.to_string();

    let dispatch_result = app_for_main.clone().run_on_main_thread(move || {
        if let Some(existing) = app_for_main.get_webview_window(RESPONDER_LABEL) {
            if let Err(e) = existing.set_focus() {
                warn!("Failed to focus existing pair-responder window: {}", e);
            }
            return;
        }

        let query = format!(
            "pair-responder.html?device={}&code={}&expires={}",
            percent_encode(&device_name),
            percent_encode(&code),
            expires_at_ms,
        );
        let url = WebviewUrl::App(query.into());

        let build_result = WebviewWindowBuilder::new(&app_for_main, RESPONDER_LABEL, url)
            .title("Pair request")
            .inner_size(420.0, 280.0)
            .resizable(false)
            .always_on_top(true)
            .title_bar_style(TitleBarStyle::Overlay)
            .build();

        let window = match build_result {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to create pair-responder window: {}", e);
                return;
            }
        };

        let app_for_close = app_for_main.clone();
        window.on_window_event(move |event| {
            if matches!(event, WindowEvent::CloseRequested { .. }) {
                send_reject_inbound(&app_for_close);
            }
        });
    });

    if let Err(e) = dispatch_result {
        warn!("Failed to dispatch open_responder to main thread: {}", e);
    }
}

/// Close the responder window if it's open. No-op if it isn't.
///
/// Used by the pairing-events consumer when the daemon reports `InboundCompleted`
/// or `InboundFailed` — the JS-side listeners also self-close, but emitting the
/// event AND calling this from Rust gives the window a Rust-side fallback in
/// case the page hasn't registered its listeners yet (race window between
/// `build()` and `await listen(...)`).
pub fn close_responder(app: &AppHandle) {
    let app = app.clone();
    let _ = app.clone().run_on_main_thread(move || {
        if let Some(window) = app.get_webview_window(RESPONDER_LABEL)
            && let Err(e) = window.close()
        {
            warn!("Failed to close pair-responder window: {}", e);
        }
    });
}

/// Fire `DaemonCommand::RejectInbound` so the daemon drops its in-flight
/// responder session.
fn send_reject_inbound(app: &AppHandle) {
    let Some(control) = app.state::<HostState>().control() else {
        return;
    };
    let command_tx = control.command_tx.clone();
    let (reply_tx, reply_rx) = oneshot::channel::<()>();

    if let Err(e) = command_tx.send(DaemonCommand::RejectInbound { reply: reply_tx }) {
        warn!(
            "Failed to send RejectInbound on responder window close: {}",
            e
        );
        return;
    }

    tauri::async_runtime::spawn(async move {
        let _ = reply_rx.await;
    });
}

/// Percent-encode a value for safe inclusion in a URL query string.
///
/// Encodes everything outside the unreserved set (`A-Za-z0-9-._~`) as UTF-8
/// percent bytes. Non-ASCII characters become multi-byte `%XX%YY` sequences
/// that `URLSearchParams` decodes back to the original string on the JS side.
fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        let unreserved = byte.is_ascii_alphanumeric()
            || *byte == b'-'
            || *byte == b'_'
            || *byte == b'.'
            || *byte == b'~';
        if unreserved {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encode_handles_reserved_characters() {
        assert_eq!(percent_encode("Michael's Laptop"), "Michael%27s%20Laptop");
        assert_eq!(percent_encode("a=b&c#d"), "a%3Db%26c%23d");
        assert_eq!(percent_encode("100%"), "100%25");
    }

    #[test]
    fn percent_encode_passes_through_unreserved_ascii() {
        assert_eq!(percent_encode("MacBook-Pro_1.0~rev"), "MacBook-Pro_1.0~rev");
    }

    #[test]
    fn percent_encode_serializes_non_ascii_as_utf8_bytes() {
        // "Café" in UTF-8 is 43 61 66 C3 A9 — URLSearchParams decodes these
        // back to the original string.
        assert_eq!(percent_encode("Caf\u{00e9}"), "Caf%C3%A9");
    }
}
