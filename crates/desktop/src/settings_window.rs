//! Settings window lifecycle helper.
//!
//! Creates the settings window via `WebviewWindowBuilder`. Construction runs on the
//! macOS main thread via `AppHandle::run_on_main_thread` to avoid the macOS UI panic
//! that occurs when `WebviewWindowBuilder::build()` is called from a worker thread
//! (same constraint as the pairing windows).

use tauri::{AppHandle, Manager, TitleBarStyle, WebviewUrl, WebviewWindowBuilder};
use tracing::warn;

/// Label assigned to the settings window. Used by the `settings-window` capability.
pub const SETTINGS_LABEL: &str = "settings";

/// Open the settings window.
///
/// If the window already exists, focuses it instead of creating a duplicate. Window
/// construction is dispatched to the macOS main thread to avoid the platform's
/// "WebView build off main thread" panic.
///
/// Unlike the pairing windows, there is no close handler: the settings window holds
/// no daemon-side session, so closing it (X / Cmd+W / Cancel) needs no cleanup.
pub fn open(app: &AppHandle) {
    let app = app.clone();
    let dispatch_result = app.clone().run_on_main_thread(move || {
        if let Some(existing) = app.get_webview_window(SETTINGS_LABEL) {
            if let Err(e) = existing.set_focus() {
                warn!("Failed to focus existing settings window: {}", e);
            }
            return;
        }

        let url = WebviewUrl::App("settings.html".into());
        let build_result = WebviewWindowBuilder::new(&app, SETTINGS_LABEL, url)
            .title("NotesSync Settings")
            .inner_size(480.0, 360.0)
            .resizable(false)
            .title_bar_style(TitleBarStyle::Overlay)
            .build();

        if let Err(e) = build_result {
            warn!("Failed to create settings window: {}", e);
        }
    });

    if let Err(e) = dispatch_result {
        warn!(
            "Failed to dispatch open settings window to main thread: {}",
            e
        );
    }
}
