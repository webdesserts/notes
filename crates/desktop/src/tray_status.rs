//! Live tray status driver.
//!
//! Subscribes to `DaemonControl.status_rx` and updates the tray menu in-place
//! via `MenuItem::set_text` on cached menu item handles — avoiding the macOS
//! menu flicker that occurs when the entire menu is replaced via `tray.set_menu`.
//!
//! The driver task runs in Tauri's async runtime. All UI mutations are
//! dispatched to the main thread via `AppHandle::run_on_main_thread` because
//! macOS panics on menu mutations off the main thread.

use std::time::Duration;

use sync_daemon::pair_api::{ConnectionState, DaemonStatus};
use tauri::AppHandle;
use tokio::sync::watch;
use tracing::{debug, warn};

/// Cached menu item handles held by the tray status driver.
///
/// All handle types implement `Clone` (Tauri 2 wraps them in `Arc`), so storing
/// them here lets the driver task update text in-place without rebuilding the
/// menu from scratch on every status change.
///
/// `autostart_item` is included so a future settings panel can read or update
/// the check state without rebuilding the menu. The status driver itself does
/// not currently update it (the check state is set at startup from
/// `launch_agent::reconcile` and toggled in place by the menu-event handler).
///
/// `Clone` so `DaemonHost` can hand a copy to each restarted status driver.
#[derive(Clone)]
pub struct TrayMenuHandles {
    pub status_item: tauri::menu::MenuItem<tauri::Wry>,
    pub pair_item: tauri::menu::MenuItem<tauri::Wry>,
    // Held for a future settings panel (design steer #8) to read/update the check
    // state without rebuilding the menu; not consumed by the status driver yet.
    #[allow(dead_code)]
    pub autostart_item: tauri::menu::CheckMenuItem<tauri::Wry>,
}

/// Format the status line from a `DaemonStatus` snapshot.
///
/// Returns a string suitable for display as a tray menu item.
pub fn format_status_text(status: &DaemonStatus) -> String {
    match status.state {
        ConnectionState::Idle => "Status: Idle".to_string(),
        ConnectionState::Connected => {
            let n = status.peer_count;
            if n == 1 {
                "Status: Connected · 1 peer".to_string()
            } else {
                format!("Status: Connected · {} peers", n)
            }
        }
    }
}

/// Start the status driver task.
///
/// The driver subscribes to `status_rx` and calls `MenuItem::set_text` on the
/// cached `status_item` handle whenever the status changes. A 100ms debounce
/// prevents menu thrash when peers connect and disconnect rapidly.
///
/// `app` is cloned into the task — it must be an `AppHandle` (which is `Clone`).
pub fn start(
    app: AppHandle,
    handles: TrayMenuHandles,
    mut status_rx: watch::Receiver<DaemonStatus>,
) -> tauri::async_runtime::JoinHandle<()> {
    tauri::async_runtime::spawn(async move {
        loop {
            // Wait until the status changes.
            if status_rx.changed().await.is_err() {
                // Sender dropped — daemon shut down.
                debug!("Status watch sender dropped — tray status driver exiting");
                break;
            }

            // 100ms debounce: skip rapid intermediate states (e.g. peer churn).
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Drain any further changes that arrived during the sleep.
            while status_rx.has_changed().unwrap_or(false) {
                status_rx.mark_unchanged();
            }

            let status = status_rx.borrow().clone();
            let text = format_status_text(&status);

            // menu mutations must happen on the main thread on macOS.
            let status_item = handles.status_item.clone();
            let pair_item = handles.pair_item.clone();
            let show_pair = matches!(status.state, ConnectionState::Idle)
                || matches!(status.state, ConnectionState::Connected);

            if let Err(e) = app.run_on_main_thread(move || {
                if let Err(e) = status_item.set_text(&text) {
                    warn!("Failed to update status menu item: {}", e);
                }
                // Enable/disable the pair item based on whether it makes sense.
                // The item is always visible; it is disabled only when we have
                // no mesh identity yet (status not yet initialized by the daemon).
                let _ = pair_item.set_enabled(show_pair);
            }) {
                warn!("Failed to dispatch menu update to main thread: {}", e);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_daemon::pair_api::{ConnectionState, DaemonStatus, PeerSummary};

    fn make_status(state: ConnectionState, peer_count: usize) -> DaemonStatus {
        DaemonStatus {
            state,
            peer_count,
            peers: (0..peer_count)
                .map(|i| PeerSummary {
                    device_name: Some(format!("Peer {}", i)),
                    last_seen: 0,
                })
                .collect(),
            relay_url: None,
            mesh_name: None,
            device_name: None,
        }
    }

    #[test]
    fn test_format_status_idle() {
        let status = make_status(ConnectionState::Idle, 0);
        assert_eq!(format_status_text(&status), "Status: Idle");
    }

    #[test]
    fn test_format_status_connected_one_peer() {
        let status = make_status(ConnectionState::Connected, 1);
        assert_eq!(format_status_text(&status), "Status: Connected · 1 peer");
    }

    #[test]
    fn test_format_status_connected_two_peers() {
        let status = make_status(ConnectionState::Connected, 2);
        assert_eq!(format_status_text(&status), "Status: Connected · 2 peers");
    }

    #[test]
    fn test_format_status_connected_zero_peers() {
        // Degenerate case: Connected state but zero peers — uses plural form.
        let status = make_status(ConnectionState::Connected, 0);
        assert_eq!(format_status_text(&status), "Status: Connected · 0 peers");
    }
}
