// Prevents a console window from popping up on Windows.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app_settings;
mod commands;
mod control;
mod daemon_host;
mod daemon_task;
mod launch_agent;
mod notification;
mod pair_events;
mod pair_window;
mod settings_window;
mod shutdown;
mod tray_status;

use std::sync::Arc;

use anyhow::{Context, Result};
use app_settings::{NotesDesktopPaths, ensure_admin_token, migrate_legacy_settings};
use daemon_host::{DaemonHost, HostState};
use sync_daemon::pair_api::DaemonControl;
use tauri::{
    AppHandle, Manager,
    menu::{CheckMenuItemBuilder, MenuBuilder, MenuItemBuilder, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_updater::UpdaterExt;
use tracing::{info, warn};

/// Live daemon control handle, cloned out of [`DaemonHost`] by command handlers
/// (never held across a restart).
pub type ControlState = Arc<DaemonControl>;

/// The port the embedded iroh relay listens on.
const RELAY_PORT: u16 = 3340;

/// Default `:8082` control API address (`NOTES_CONTROL_LISTEN` overrides). 8081 is
/// commonly taken (e.g. llama-swap), so the suite standardizes on 8082.
const DEFAULT_CONTROL_LISTEN: &str = "127.0.0.1:8082";
/// Default listen address for the shared notes MCP HTTP server (`NOTES_MCP_LISTEN` overrides).
const DEFAULT_MCP_LISTEN: &str = "127.0.0.1:8083";

/// Pick the explicitly-configured relay URL, preferring the env var over the
/// stored value.
///
/// A non-empty result means "this box is a relay server": it gates whether the
/// embedded relay starts and supplies the URL it advertises. The env var
/// (`NOTES_RELAY_URL`) is an override that always wins; the stored value
/// (set via the settings panel) is the fallback used by autostarted instances that
/// launch without shell env vars. Both inputs are expected to be already
/// empty-filtered by the caller. A `None` result means this box is a client/laptop
/// — no embedded relay, no advertised URL.
///
/// Extracted as a pure function so the precedence is testable without launching Tauri.
fn resolve_configured_relay_url(
    env_relay_url: Option<String>,
    stored_relay_url: Option<String>,
) -> Option<String> {
    env_relay_url.or(stored_relay_url)
}

/// Decide the embedded-relay role from the configured relay URL.
///
/// Returns `(relay_listen, advertised_relay_url)`:
/// - `Some(url)` (a relay-URL setting is filled) → SERVER: bind the embedded relay
///   on `0.0.0.0:RELAY_PORT` and advertise the operator-declared `url`. We never
///   fall back to a detected LAN IP — a LAN-IP relay URL is unroutable off-LAN and
///   is exactly the stale-URL bug this gating removes.
/// - `None` (blank setting) → LAPTOP/client: no embedded relay (`relay_listen` is
///   `None`, so startup never starts one) and no advertised URL. A laptop reaches
///   off-LAN peers via the public relays it homes on (its RelayMap), not its own.
///
/// Extracted as a pure function so the gating is testable without launching Tauri.
fn relay_server_config(
    configured_relay_url: Option<String>,
    listen_enabled: bool,
) -> (Option<String>, Option<String>) {
    match configured_relay_url {
        Some(url) => {
            let listen = if listen_enabled {
                Some(format!("0.0.0.0:{}", RELAY_PORT))
            } else {
                None
            };
            (listen, Some(url))
        }
        None => (None, None),
    }
}

fn main() -> Result<()> {
    // `notes` CLI through the cask's argv[0] symlink: dispatch before logs or Tauri.
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if notes::cli::should_dispatch_cli(&args) {
        return notes::cli::run_from_os_args(args);
    }

    // Resolve `~/.config/notes` + `~/.local/state/notes` (app-config) before
    // tracing so the log lands in `<state>/desktop.log`. Soft-fail on mkdir error so
    // a permissions problem doesn't prevent the app from starting — logs fall
    // through to stderr only in that case.
    let paths = NotesDesktopPaths::resolve()?;
    if let Err(e) = paths.ensure_dirs() {
        eprintln!("warning: could not create notes config/state dirs: {e:#}");
    }
    let log_path = paths.log_file.clone();

    // The WorkerGuard flushes the background log-writer thread when dropped. It must
    // remain in scope for the full duration of main() so buffered lines aren't dropped
    // early. tauri.run() is blocking, so _guard held here outlives the entire app run.
    let _guard = notes_core::runtime::init_tracing_with_file(false, "desktop", &log_path);

    info!("Starting NotesSync");

    // One-time import of the v0.5/v0.6 `tauri-plugin-store` settings; a malformed
    // legacy store fails loudly rather than starting unconfigured.
    if let Some(home) = &paths.legacy_home
        && let Some(source) = migrate_legacy_settings(home, &paths)?
    {
        info!(
            "Migrated legacy settings {} -> {}",
            source.display(),
            paths.config_file.display()
        );
    }

    // Single instance: own `:8082` for the whole `.app` lifetime, bound before the
    // daemon or Tauri start. A second launch (Finder double-open, login item) that
    // finds a live instance answering there exits 0, so neither a second tray icon
    // appears nor does `KeepAlive.SuccessfulExit = false` respawn it in a loop.
    let control_listen = std::env::var("NOTES_CONTROL_LISTEN")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_CONTROL_LISTEN.to_string());
    let control_listener = match control::bind(&control_listen) {
        Ok(listener) => listener,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && control::probe(&control_listen) => {
            info!("NotesSync.app already running; exiting cleanly");
            return Ok(());
        }
        Err(e) => return Err(e).with_context(|| format!("bind control API on {control_listen}")),
    };
    let admin_token = ensure_admin_token(&paths)?;

    tauri::Builder::default()
        .setup(move |app| {
            app.manage(paths.clone());

            // Tray-only mode: suppress the dock icon so the app lives entirely
            // in the menu bar. `"windows": []` in tauri.conf.json removes the
            // main window; this call removes the Dock icon.
            #[cfg(target_os = "macos")]
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);

            let handle = app.handle().clone();

            // Build the tray menu with cached item handles so the status driver
            // can update text in-place via MenuItem::set_text.
            let status_item = MenuItemBuilder::new("Status: Connecting…")
                .id("status")
                .enabled(false) // display-only label, not interactive
                .build(app)?;

            // Reconcile ~/Library/LaunchAgents with the persisted preference
            // (default on); the plist on disk is what the checkmark shows.
            let is_autostart_enabled = launch_agent::reconcile(&paths);

            let autostart_item = CheckMenuItemBuilder::new("Launch at Login")
                .id("toggle-autostart")
                .checked(is_autostart_enabled)
                .build(app)?;

            let pair_item = MenuItemBuilder::new("Pair with nearby device…")
                .id("pair")
                .build(app)?;
            let restart_item = MenuItemBuilder::new("Restart Daemon")
                .id("restart-daemon")
                .build(app)?;
            let update_item = MenuItemBuilder::new("Check for Updates…")
                .id("check-updates")
                .build(app)?;
            let settings_item = MenuItemBuilder::new("Settings…")
                .id("settings")
                .build(app)?;

            // Menu order: [status, sep, autostart, sep, pair, sep, restart daemon,
            // check for updates, sep, settings…, sep, quit]
            let menu = MenuBuilder::new(app)
                .items(&[
                    &status_item,
                    &PredefinedMenuItem::separator(app)?,
                    &autostart_item,
                    &PredefinedMenuItem::separator(app)?,
                    &pair_item,
                    &PredefinedMenuItem::separator(app)?,
                    &restart_item,
                    &update_item,
                    &PredefinedMenuItem::separator(app)?,
                    &settings_item,
                    &PredefinedMenuItem::separator(app)?,
                    &MenuItemBuilder::new("Quit").id("quit").build(app)?,
                ])
                .build()?;

            // The suite's notes glyph as a monochrome template: macOS tints it for the
            // menu bar's appearance (light, dark, highlighted).
            let icon = tauri::include_image!("icons/tray-template.png");

            // Clone handles for the event closure before ownership moves into TrayIconBuilder.
            let autostart_item_for_events = autostart_item.clone();

            let app_for_events = handle.clone();
            TrayIconBuilder::new()
                .icon(icon)
                .icon_as_template(true)
                .menu(&menu)
                .on_menu_event(move |_tray, event| match event.id().as_ref() {
                    "quit" => {
                        let app = app_for_events.clone();
                        let host = app.state::<HostState>().inner().clone();
                        tauri::async_runtime::spawn(async move {
                            host.shutdown_app().await;
                            app.exit(0);
                        });
                    }
                    "restart-daemon" => {
                        let host = app_for_events.state::<HostState>().inner().clone();
                        tauri::async_runtime::spawn(async move {
                            match host.restart().await {
                                Ok(()) => info!("Daemon restarted from the tray"),
                                Err(e) => warn!("Daemon restart failed: {e:#}"),
                            }
                        });
                    }
                    "check-updates" => {
                        let app = app_for_events.clone();
                        tauri::async_runtime::spawn(async move {
                            match check_updates(&app).await {
                                Ok(Some(v)) => info!("Update available: {v}"),
                                Ok(None) => info!("NotesSync is up to date"),
                                Err(e) => warn!("Update check failed: {e:#}"),
                            }
                        });
                    }
                    "pair" => {
                        if let Err(e) = pair_window::open_initiator(&app_for_events) {
                            warn!("Failed to open pair initiator window: {}", e);
                        }
                    }
                    "settings" => {
                        settings_window::open(&app_for_events);
                    }
                    "toggle-autostart" => {
                        // Menu events run on the main thread; the toggle is two small
                        // file writes. On failure, show the state actually registered.
                        let checked = match launch_agent::toggle(&app_for_events) {
                            Ok(new_state) => new_state,
                            Err(e) => {
                                warn!("Failed to toggle Launch at Login: {e:#}");
                                launch_agent::is_enabled()
                            }
                        };
                        let _ = autostart_item_for_events.set_checked(checked);
                    }
                    _ => {}
                })
                .on_tray_icon_event(|_tray, event| {
                    if let TrayIconEvent::Click {
                        button: MouseButton::Left,
                        button_state: MouseButtonState::Up,
                        ..
                    } = event
                    {
                        // Left-click on the tray icon — no action in Phase 1.5.
                        // Phase 6 will open the status popup here.
                    }
                })
                .build(app)?;

            // DaemonHost owns the sync daemon, the `:8083` MCP server and the
            // per-daemon tray-status/pairing drivers; restarts serialize on its lock.
            let handles = tray_status::TrayMenuHandles {
                status_item,
                pair_item,
                autostart_item,
            };
            let host: HostState = Arc::new(DaemonHost::new(handle.clone(), paths.clone(), handles));
            app.manage(host.clone());
            tauri::async_runtime::block_on(host.start())?;

            control::spawn(
                control_listener,
                Arc::new(control::TauriOps::new(handle.clone(), host.clone())),
                admin_token,
                app.package_info().version.to_string(),
                host.control_shutdown(),
            );

            Ok(())
        })
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .invoke_handler(tauri::generate_handler![
            commands::start_pair_discovery,
            commands::request_pairing,
            commands::submit_pair_code,
            commands::cancel_pair_discovery,
            commands::reject_inbound_pair,
            commands::get_settings,
            commands::save_settings,
        ])
        .build(tauri::generate_context!())
        .map_err(|e| anyhow::anyhow!("Tauri application error: {e}"))?
        .run(|_app, event| {
            // Tray-only app: closing a transient window (e.g. the pair dialog)
            // must NOT terminate the process. Tauri fires ExitRequested with
            // code=None whenever the last window closes; only honor it when
            // code is Some (the Quit menu calls `app.exit(0)`).
            if let tauri::RunEvent::ExitRequested {
                code: None, api, ..
            } = &event
            {
                api.prevent_exit();
            }
        });

    Ok(())
}

/// Tray "Check for Updates…": the latest version when a newer release exists.
async fn check_updates(app: &AppHandle) -> Result<Option<String>> {
    let update = app
        .updater()
        .context("updater unavailable")?
        .check()
        .await
        .context("check for updates")?;
    Ok(update.map(|u| u.version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_relay_env_wins_over_stored() {
        assert_eq!(
            resolve_configured_relay_url(
                Some("http://umbra.computer:3340/".to_string()),
                Some("http://stored.example/".to_string()),
            ),
            Some("http://umbra.computer:3340/".to_string()),
        );
    }

    #[test]
    fn configured_relay_stored_used_when_no_env() {
        assert_eq!(
            resolve_configured_relay_url(None, Some("http://stored.example/".to_string())),
            Some("http://stored.example/".to_string()),
        );
    }

    #[test]
    fn configured_relay_none_when_neither_set() {
        assert_eq!(resolve_configured_relay_url(None, None), None);
    }

    /// A box with the relay-URL setting filled is a SERVER: it starts the embedded
    /// relay (`relay_listen` bound) and advertises exactly that configured URL —
    /// never a detected LAN IP.
    #[test]
    fn configured_url_runs_relay_server() {
        let configured = Some("http://umbra.computer:3340/".to_string());
        let (relay_listen, advertised) = relay_server_config(configured, true);
        assert_eq!(relay_listen, Some(format!("0.0.0.0:{}", RELAY_PORT)));
        assert_eq!(advertised, Some("http://umbra.computer:3340/".to_string()));
    }

    /// `"relay_listen": false` keeps the advertised URL but releases `0.0.0.0:3340`
    /// so `webdesserts-mesh` can bind it (Phase 3 cutover).
    #[test]
    fn relay_listen_disabled_keeps_advertised_url() {
        let configured = Some("https://umbra.computer/relay".to_string());
        let (relay_listen, advertised) = relay_server_config(configured, false);
        assert_eq!(relay_listen, None);
        assert_eq!(advertised, Some("https://umbra.computer/relay".to_string()));
    }

    /// A laptop (blank relay-URL setting) runs NO embedded relay and advertises NO
    /// relay URL — the core "laptops don't host a private LAN-IP relay" guarantee.
    #[test]
    fn blank_config_runs_no_relay() {
        let (relay_listen, advertised) = relay_server_config(None, true);
        assert_eq!(relay_listen, None);
        assert_eq!(advertised, None);
    }
}
