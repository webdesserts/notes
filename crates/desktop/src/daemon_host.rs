//! Supervised, serialized sync-daemon host for `NotesSync.app`.
//!
//! `DaemonHost` owns the running sync daemon together with the shared `:8083`
//! MCP server and the per-daemon tray-status and pairing tasks. Every start,
//! restart and shutdown runs under `restart_lock`, so `POST /daemon/restart`,
//! the tray "Restart Daemon" item and `save_settings` serialize one at a time,
//! and a restart awaits the old `:3340`/`:8083` listeners' drop before rebinding.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use sync_daemon::daemon::DaemonRunConfig;
use sync_daemon::pair_api::DaemonControl;
use tauri::AppHandle;
use tauri::async_runtime::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::app_settings::{AppSettings, NotesDesktopPaths, resolve_vault_path};
use crate::daemon_task::DaemonHandle;
use crate::shutdown::ShutdownController;
use crate::tray_status::{self, TrayMenuHandles};
use crate::{DEFAULT_MCP_LISTEN, pair_events, relay_server_config, resolve_configured_relay_url};

/// Graceful stop budget before the MCP, tray and pairing tasks are aborted.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Tauri managed state.
pub type HostState = Arc<DaemonHost>;

/// What one daemon (re)start resolved from settings and env.
#[derive(Debug, Clone, PartialEq)]
pub struct LaunchConfig {
    pub vault_path: PathBuf,
    pub mcp_listen: String,
    pub relay_listen: Option<String>,
    pub advertised_relay_url: Option<String>,
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Reload `AppSettings` plus `NOTES_VAULT` / `NOTES_RELAY_URL` / `NOTES_MCP_LISTEN`.
///
/// Priority is env over stored settings. `NOTES_VAULT` is written back so
/// autostarted launches (no shell env) find the same vault, and
/// `relay_server_config(.., settings.relay_listen())` keeps `relay_listen = false`
/// (Mesh.app owns `:3340`) enforced on every start and hot restart. A filled relay
/// URL makes this box a relay SERVER; a blank one a client that never advertises
/// a LAN-IP relay.
pub fn launch_config(paths: &NotesDesktopPaths) -> Result<LaunchConfig> {
    let mut settings = AppSettings::load(paths)?;
    let env_vault = env_nonempty("NOTES_VAULT");
    let vault_path = resolve_vault_path(
        env_vault.as_deref(),
        settings.vault_path().and_then(|p| p.to_str()),
    )
    .ok_or_else(|| {
        anyhow!(
            "NOTES_VAULT is not set and no vault path is stored.\n\
             Set it to the path of your Obsidian vault, e.g.:\n\
             \x20 NOTES_VAULT=~/notes cargo tauri dev"
        )
    })?;
    if env_vault.is_some() {
        settings.set_vault_path(vault_path.clone());
        if let Err(e) = settings.save(paths) {
            warn!("Failed to persist vault path to app settings: {e}");
        }
    }

    let env_relay_url = env_nonempty("NOTES_RELAY_URL");
    let configured = resolve_configured_relay_url(
        env_relay_url.clone(),
        settings.relay_url().map(String::from),
    );
    let (relay_listen, advertised_relay_url) =
        relay_server_config(configured, settings.relay_listen());
    match &advertised_relay_url {
        Some(url) if env_relay_url.is_some() => {
            info!(
                "Relay server: advertising URL from NOTES_RELAY_URL: {}",
                url
            )
        }
        Some(url) => info!(
            "Relay server: advertising URL from stored settings: {}",
            url
        ),
        None => info!(
            "No relay URL configured — running as a client (no embedded relay). \
             Off-LAN reach uses learned public relays; LAN uses mDNS + direct."
        ),
    }

    Ok(LaunchConfig {
        vault_path,
        mcp_listen: env_nonempty("NOTES_MCP_LISTEN")
            .unwrap_or_else(|| DEFAULT_MCP_LISTEN.to_string()),
        relay_listen,
        advertised_relay_url,
    })
}

/// One running daemon and the tasks tied to its lifetime.
pub struct RunningDaemon {
    pub control: Arc<DaemonControl>,
    shutdown: ShutdownController,
    mcp_task: JoinHandle<()>,
    tray_task: JoinHandle<()>,
    pair_task: JoinHandle<()>,
    pub config: LaunchConfig,
}

pub struct DaemonHost {
    app: AppHandle,
    paths: NotesDesktopPaths,
    tray: TrayMenuHandles,
    running: Mutex<Option<RunningDaemon>>,
    restart_lock: tokio::sync::Mutex<()>,
    control_shutdown: CancellationToken,
}

impl DaemonHost {
    pub fn new(app: AppHandle, paths: NotesDesktopPaths, tray: TrayMenuHandles) -> Self {
        Self {
            app,
            paths,
            tray,
            running: Mutex::new(None),
            restart_lock: tokio::sync::Mutex::new(()),
            control_shutdown: CancellationToken::new(),
        }
    }

    fn running(&self) -> MutexGuard<'_, Option<RunningDaemon>> {
        self.running.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Clone the live control handle; never hold `running` across an `.await`.
    pub fn control(&self) -> Option<Arc<DaemonControl>> {
        self.running().as_ref().map(|r| r.control.clone())
    }

    pub fn is_running(&self) -> bool {
        self.running().is_some()
    }

    /// Cancelled once by [`Self::shutdown_app`] to stop the `:8082` control server.
    pub fn control_shutdown(&self) -> CancellationToken {
        self.control_shutdown.clone()
    }

    /// Daemon fields of `GET /status`.
    pub fn status_json(&self) -> Value {
        match self.running().as_ref() {
            Some(r) => {
                let status = r.control.status_rx.borrow();
                json!({
                    "daemon_running": true,
                    "connection_state": format!("{:?}", status.state),
                    "peer_count": status.peer_count,
                    "vault_path": r.config.vault_path,
                    "mcp_listen": r.config.mcp_listen,
                    "relay_listen": r.config.relay_listen.is_some(),
                    "relay_url": r.config.advertised_relay_url,
                })
            }
            None => json!({
                "daemon_running": false,
                "connection_state": null,
                "peer_count": 0,
                "vault_path": null,
                "mcp_listen": null,
                "relay_listen": null,
                "relay_url": null,
            }),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let _guard = self.restart_lock.lock().await;
        self.start_locked().await
    }

    pub async fn restart(&self) -> Result<()> {
        self.restart_after(|_| Ok(())).await
    }

    /// Under `restart_lock`: run `before` (e.g. persist settings), stop, start.
    pub async fn restart_after(
        &self,
        before: impl FnOnce(&NotesDesktopPaths) -> Result<()> + Send,
    ) -> Result<()> {
        let _guard = self.restart_lock.lock().await;
        before(&self.paths)?;
        self.stop_locked().await;
        self.start_locked().await
    }

    pub async fn shutdown(&self) {
        let _guard = self.restart_lock.lock().await;
        self.stop_locked().await;
    }

    /// Stop the daemon and the `:8082` control server (Quit, app restart, update).
    pub async fn shutdown_app(&self) {
        self.shutdown().await;
        self.control_shutdown.cancel();
    }

    async fn start_locked(&self) -> Result<()> {
        let config = launch_config(&self.paths)?;
        info!("Vault: {:?}", config.vault_path);
        let daemon = DaemonHandle::spawn(
            DaemonRunConfig {
                vault: config.vault_path.clone(),
                identity_key: None,
                // `:8082` belongs to the `.app`-lifetime control server (control.rs).
                health_listen: None,
                relay_listen: config.relay_listen.clone(),
                advertised_relay_url: config.advertised_relay_url.clone(),
            },
            self.app.clone(),
        )
        .await?;
        let (token, control, done_rx) = daemon.into_parts();

        // Shared notes MCP HTTP server, stopped with the daemon via a child token.
        // A bind failure only logs a warning and leaves sync running.
        let mcp_token = token.child_token();
        let mcp_vault = config.vault_path.to_string_lossy().into_owned();
        let mcp_listen = config.mcp_listen.clone();
        let mcp_task = tauri::async_runtime::spawn(async move {
            info!("Notes MCP server: starting on http://{}/mcp", mcp_listen);
            let shutdown = async move { mcp_token.cancelled().await };
            if let Err(e) =
                notes::serve_http(notes::Config::new(&mcp_vault), &mcp_listen, shutdown).await
            {
                warn!("Notes MCP server on {} stopped: {}", mcp_listen, e);
            }
        });

        let tray_task = tray_status::start(
            self.app.clone(),
            self.tray.clone(),
            control.status_rx.clone(),
        );
        let pair_task = pair_events::start(self.app.clone(), control.pairing_rx.resubscribe());

        *self.running() = Some(RunningDaemon {
            control: Arc::new(control),
            shutdown: ShutdownController { token, done_rx },
            mcp_task,
            tray_task,
            pair_task,
            config,
        });
        Ok(())
    }

    /// Cancel the daemon and await `:3340` + `:8083` listener drop (abort after 5s).
    async fn stop_locked(&self) {
        let taken = self.running().take();
        let Some(running) = taken else {
            return;
        };
        let RunningDaemon {
            shutdown,
            mut mcp_task,
            tray_task,
            pair_task,
            ..
        } = running;
        let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
        let daemon_done = shutdown.shutdown(STOP_TIMEOUT).await;
        let mcp_done = tokio::time::timeout_at(deadline, &mut mcp_task)
            .await
            .is_ok();
        if !(daemon_done && mcp_done) {
            warn!(
                "Daemon stop exceeded {}s — aborting MCP, tray and pairing tasks",
                STOP_TIMEOUT.as_secs()
            );
            mcp_task.abort();
            let _ = mcp_task.await;
        }
        // Both drivers end once the daemon drops its senders; abort covers a stuck one.
        for task in [tray_task, pair_task] {
            task.abort();
            let _ = task.await;
        }
    }
}
