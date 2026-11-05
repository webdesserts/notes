// Prevents a console window from popping up on Windows.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app_settings;
mod commands;
mod daemon_task;
mod notification;
mod pair_events;
mod pair_window;
mod settings_window;
mod shutdown;
mod tray_status;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use app_settings::{AppSettings, resolve_vault_path};
use daemon_task::DaemonHandle;
use shutdown::ShutdownController;
use sync_daemon::daemon::DaemonRunConfig;
use sync_daemon::pair_api::DaemonControl;
use tauri::{
    Manager,
    menu::{CheckMenuItemBuilder, MenuBuilder, MenuItemBuilder, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_autostart::ManagerExt;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Managed state wrapper so the Quit handler can take ownership of ShutdownController.
type ShutdownState = Arc<Mutex<Option<ShutdownController>>>;

/// Managed state for the daemon control handle, used by Tauri commands to send
/// commands to the running daemon (pairing, discovery, etc.).
///
/// `Arc` because Tauri managed state requires `Send + Sync + 'static`, and
/// `DaemonControl` holds non-`Clone` channels that can't be wrapped in plain `Mutex`
/// without losing the ability to clone receivers. The inner fields use tokio's own
/// thread-safe channel primitives.
pub type ControlState = Arc<DaemonControl>;

/// The port the embedded iroh relay listens on.
const RELAY_PORT: u16 = 3340;

/// The port the local health endpoint binds on. 8081 is the conventional default
/// but is commonly taken (e.g. llama-swap), so we standardize on 8082 across all
/// machines. Make this env-configurable later if a machine ever needs to differ.
const HEALTH_PORT: u16 = 8082;
/// Default listen address for the shared notes MCP HTTP server (`NOTES_MCP_LISTEN` overrides).
const DEFAULT_MCP_LISTEN: &str = "127.0.0.1:8083";

/// Pick the explicitly-configured relay URL, preferring the env var over the
/// stored value.
///
/// A non-empty result means "this box is a relay server": it gates whether the
/// embedded relay starts and supplies the URL it advertises. The env var
/// (`OBSIDIAN_MEMORY_RELAY_URL`) is an override that always wins; the stored value
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
fn relay_server_config(configured_relay_url: Option<String>) -> (Option<String>, Option<String>) {
    match configured_relay_url {
        Some(url) => (Some(format!("0.0.0.0:{}", RELAY_PORT)), Some(url)),
        None => (None, None),
    }
}

fn main() -> Result<()> {
    // Resolve the log path and create its directory before initializing tracing.
    // Using a stable directory name so the path survives the pending
    // `desktop` → `memory-desktop` bundle rename (app_log_dir() would move at rename).
    // Soft-fail on mkdir error so a permissions problem doesn't prevent the app from
    // starting — logs fall through to stderr only in that case.
    let log_path =
        notes_core::runtime::expand_tilde("~/Library/Logs/webdesserts-memory/desktop.log");
    if let Some(log_dir) = log_path.parent()
        && let Err(e) = std::fs::create_dir_all(log_dir)
    {
        eprintln!("warning: could not create log directory {log_dir:?}: {e}");
    }

    // The WorkerGuard flushes the background log-writer thread when dropped. It must
    // remain in scope for the full duration of main() so buffered lines aren't dropped
    // early. tauri.run() is blocking, so _guard held here outlives the entire app run.
    let _guard = notes_core::runtime::init_tracing_with_file(false, "desktop", &log_path);

    info!("Starting NotesSync");

    tauri::Builder::default()
        // Plugins must be registered before setup() so that their APIs are
        // available inside the setup hook via app.store() and app.autolaunch().
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            None, // no extra args — vault path is read from the store, not CLI
        ))
        .setup(move |app| {
            // --- Vault path resolution (amendment S2: daemon_config built here) ---
            //
            // Priority: OBSIDIAN_MEMORY_VAULT env var > stored path from
            // app-settings.json. The env var is always written back to the store so
            // that autostarted instances (which launch without shell env vars) find
            // the vault at the same location (amendment B2).
            //
            // A CLI --vault flag is deliberately avoided: `tauri-cli` does not
            // forward trailing arguments to the bundled binary in `tauri dev` mode,
            // which would silently ignore the flag in development.
            let mut settings = AppSettings::load(app.handle())?;
            let env_vault = std::env::var("OBSIDIAN_MEMORY_VAULT").ok();
            let vault_path = resolve_vault_path(
                env_vault.as_deref(),
                settings.vault_path().and_then(|p| p.to_str()),
            )
            .ok_or_else(|| {
                anyhow!(
                    "OBSIDIAN_MEMORY_VAULT is not set and no vault path is stored.\n\
                     Set it to the path of your Obsidian vault, e.g.:\n\
                     \x20 OBSIDIAN_MEMORY_VAULT=~/notes cargo tauri dev"
                )
            })?;

            info!("Vault: {:?}", vault_path);

            // Unconditionally refresh the stored vault path whenever the env var is set
            // (amendment B2). This keeps the stored path current if the user moves their
            // vault and reruns with the updated env var, so a future autostarted launch
            // (which has no env var) still finds the right location.
            if env_vault.as_deref().is_some_and(|v| !v.is_empty()) {
                settings.set_vault_path(vault_path.clone());
                if let Err(e) = settings.save(app.handle()) {
                    warn!("Failed to persist vault path to app settings: {e}");
                }
            }

            // Decide whether this box runs an embedded relay SERVER, gated solely
            // on the relay-URL setting being filled.
            //
            // Priority for the setting: OBSIDIAN_MEMORY_RELAY_URL env var > relay
            // URL stored in app-settings.json (set via the settings panel). The env
            // var takes precedence so stable-hostname machines (e.g. umbra) can
            // declare their public URL; the stored value lets autostarted instances —
            // which launch without shell env vars — still pick it up.
            //
            // A filled setting => this box is a SERVER: start the embedded relay and
            // advertise the operator-declared public URL. A blank setting => this box
            // is a LAPTOP: run NO embedded relay and advertise NO relay URL. A laptop
            // reaches off-LAN peers by homing on the PUBLIC relays it has learned
            // (its RelayMap, wired in a later commit), never on a private LAN-IP relay
            // of its own — which is exactly the stale-launch-time-URL bug this kills.
            //
            // A server must declare its own public URL; we deliberately do NOT fall
            // back to a detected LAN IP here, because a LAN-IP relay URL is the same
            // unroutable-once-you-leave-the-LAN class of bug.
            let env_relay_url = std::env::var("OBSIDIAN_MEMORY_RELAY_URL")
                .ok()
                .filter(|v| !v.is_empty());
            let stored_relay_url = settings.relay_url().map(String::from);
            let configured_relay_url =
                resolve_configured_relay_url(env_relay_url.clone(), stored_relay_url.clone());

            let (relay_listen, advertised_relay_url) =
                relay_server_config(configured_relay_url.clone());

            match &advertised_relay_url {
                Some(url) if env_relay_url.is_some() => {
                    info!("Relay server: advertising URL from OBSIDIAN_MEMORY_RELAY_URL: {}", url);
                }
                Some(url) => {
                    info!("Relay server: advertising URL from stored settings: {}", url);
                }
                None => {
                    info!(
                        "No relay URL configured — running as a client (no embedded relay). \
                         Off-LAN reach uses learned public relays; LAN uses mDNS + direct."
                    );
                }
            }

            let mcp_vault = vault_path.to_string_lossy().into_owned();

            let daemon_config = DaemonRunConfig {
                vault: vault_path,
                identity_key: None,
                health_listen: Some(format!("127.0.0.1:{}", HEALTH_PORT)),
                relay_listen,
                advertised_relay_url,
            };

            // Tray-only mode: suppress the dock icon so the app lives entirely
            // in the menu bar. `"windows": []` in tauri.conf.json removes the
            // main window; this call removes the Dock icon.
            #[cfg(target_os = "macos")]
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);

            let handle = app.handle().clone();

            // Spawn the sync daemon and block until startup completes. Returns
            // a DaemonHandle carrying the control, cancellation token, and done_rx.
            let daemon = DaemonHandle::spawn(daemon_config, handle.clone());

            // Decompose the handle so we can move each piece to its owner:
            // - `control` → Tauri managed state (for command handlers)
            // - `token` + `done_rx` → ShutdownController (for Quit handler)
            let (token, control, done_rx) = daemon.into_parts();

            // Shared notes MCP HTTP server: one GraphIndex + EmbeddingManager +
            // VaultWatcher for every local agent instead of one stdio child per
            // agent. It stops with the sync daemon on Quit via a child token. A
            // bind failure (port already taken) only logs a warning and leaves
            // sync running.
            let mcp_listen = std::env::var("NOTES_MCP_LISTEN")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_MCP_LISTEN.to_string());
            let mcp_token = token.child_token();
            tauri::async_runtime::spawn(async move {
                info!("Notes MCP server: starting on http://{}/mcp", mcp_listen);
                let shutdown = async move { mcp_token.cancelled().await };
                if let Err(e) =
                    notes::serve_http(notes::Config::new(&mcp_vault), &mcp_listen, shutdown).await
                {
                    warn!("Notes MCP server on {} stopped: {}", mcp_listen, e);
                }
            });

            // Receivers cloned/extracted before control moves into Arc.
            //
            // `status_rx` is a watch::Receiver (Clone), but `pairing_rx` is a
            // broadcast::Receiver (not Clone). We resubscribe from the
            // broadcast sender via `Receiver::resubscribe` to obtain an
            // independent stream of pairing events for the consumer task.
            let status_rx = control.status_rx.clone();
            let pairing_rx = control.pairing_rx.resubscribe();

            // Store the DaemonControl in managed state so Tauri command handlers
            // can access it via `State<ControlState>`.
            let control: ControlState = Arc::new(control);
            app.manage(control);

            // Shutdown controller — taken once by the Quit handler.
            let shutdown = ShutdownController { token, done_rx };
            let shutdown_state: ShutdownState = Arc::new(Mutex::new(Some(shutdown)));
            app.manage(shutdown_state);

            // Build the tray menu with cached item handles so the status driver
            // can update text in-place via MenuItem::set_text.
            let status_item = MenuItemBuilder::new("Status: Connecting…")
                .id("status")
                .enabled(false) // display-only label, not interactive
                .build(app)?;

            // Read the autostart state from the OS (the LaunchAgent plist) to
            // reflect what was previously registered, not what the store says.
            // The store is written only on change; the plist is ground truth.
            let is_autostart_enabled = app.autolaunch().is_enabled().unwrap_or(false);

            let autostart_item = CheckMenuItemBuilder::new("Launch at Login")
                .id("toggle-autostart")
                .checked(is_autostart_enabled)
                .build(app)?;

            let pair_item = MenuItemBuilder::new("Pair with nearby device…")
                .id("pair")
                .build(app)?;

            let settings_item = MenuItemBuilder::new("Settings…").id("settings").build(app)?;

            // Menu order (D1): [status, sep, autostart, sep, pair, sep, settings…, sep, quit]
            let menu = MenuBuilder::new(app)
                .items(&[
                    &status_item,
                    &PredefinedMenuItem::separator(app)?,
                    &autostart_item,
                    &PredefinedMenuItem::separator(app)?,
                    &pair_item,
                    &PredefinedMenuItem::separator(app)?,
                    &settings_item,
                    &PredefinedMenuItem::separator(app)?,
                    &MenuItemBuilder::new("Quit").id("quit").build(app)?,
                ])
                .build()?;

            let icon = tauri::include_image!("icons/32x32.png");

            // Clone handles for the event closure before ownership moves into TrayIconBuilder.
            let autostart_item_for_events = autostart_item.clone();

            let app_for_events = handle.clone();
            TrayIconBuilder::new()
                .icon(icon)
                .menu(&menu)
                .on_menu_event(move |_tray, event| match event.id().as_ref() {
                    "quit" => {
                        let app = app_for_events.clone();
                        tauri::async_runtime::spawn(async move {
                            let state = app.state::<ShutdownState>();
                            let shutdown = state.lock().await.take();
                            if let Some(s) = shutdown {
                                s.shutdown(Duration::from_secs(5)).await;
                            }
                            app.exit(0);
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
                        let app = app_for_events.clone();
                        let item = autostart_item_for_events.clone();
                        tauri::async_runtime::spawn(async move {
                            let autolaunch = app.autolaunch();

                            // Read current state from the OS plist (ground truth).
                            let currently_enabled = autolaunch.is_enabled().unwrap_or(false);

                            // Toggle the LaunchAgent registration. Only update the
                            // checkmark and persist the new state when the OS call
                            // succeeds (amendment B1). On failure, warn and leave the
                            // menu reflecting the actual registered state.
                            let result = if currently_enabled {
                                autolaunch.disable()
                            } else {
                                autolaunch.enable()
                            };

                            match result {
                                Ok(()) => {
                                    let new_state = !currently_enabled;
                                    // Update the checkmark on the main thread
                                    // (macOS panics on menu mutations off main thread).
                                    let _ = app.run_on_main_thread(move || {
                                        let _ = item.set_checked(new_state);
                                    });
                                    // Persist the new autostart state to the store.
                                    let mut settings = match AppSettings::load(&app) {
                                        Ok(s) => s,
                                        Err(e) => {
                                            warn!(
                                                "Failed to load app settings for autostart save: {e}"
                                            );
                                            return;
                                        }
                                    };
                                    settings.set_autostart_enabled(new_state);
                                    if let Err(e) = settings.save(&app) {
                                        warn!("Failed to save autostart state: {e}");
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to toggle autostart: {e}");
                                }
                            }
                        });
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

            // Start the tray status driver. It subscribes to status_rx and calls
            // MenuItem::set_text on the status_item handle whenever the daemon
            // status changes, without rebuilding the entire menu.
            let handles = tray_status::TrayMenuHandles {
                status_item,
                pair_item,
                autostart_item,
            };
            tray_status::start(handle.clone(), handles, status_rx);

            // Start the pairing-events consumer. Translates daemon-side
            // PairingUiEvents into the responder window + macOS notification.
            pair_events::start(handle.clone(), pairing_rx);

            Ok(())
        })
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_dialog::init())
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
            if let tauri::RunEvent::ExitRequested { code: None, api, .. } = &event {
                api.prevent_exit();
            }
        });

    Ok(())
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
        let (relay_listen, advertised) = relay_server_config(configured);
        assert_eq!(relay_listen, Some(format!("0.0.0.0:{}", RELAY_PORT)));
        assert_eq!(advertised, Some("http://umbra.computer:3340/".to_string()));
    }

    /// A laptop (blank relay-URL setting) runs NO embedded relay and advertises NO
    /// relay URL — the core "laptops don't host a private LAN-IP relay" guarantee.
    #[test]
    fn blank_config_runs_no_relay() {
        let (relay_listen, advertised) = relay_server_config(None);
        assert_eq!(relay_listen, None);
        assert_eq!(advertised, None);
    }
}
