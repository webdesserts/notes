//! Tauri command handlers bridging the pairing UI to the daemon event loop.
//!
//! Each command sends a `DaemonCommand` to the running daemon via
//! `DaemonControl.command_tx` and awaits the reply. Commands are registered in
//! `main.rs` via `.invoke_handler(tauri::generate_handler![...])`.
//!
//! The daemon-side implementations of `StartDiscovery` and `SubmitCode` live in
//! `crates/sync-daemon/src/daemon.rs` (commit 4).

use std::path::Path;

use serde::Serialize;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use sync_daemon::pair_api::DaemonCommand;

use crate::ControlState;
use crate::app_settings::{AppSettings, NotesDesktopPaths};

/// Send a oneshot `DaemonCommand` and await its reply.
///
/// `make_cmd` receives the reply sender and wraps it into the appropriate
/// `DaemonCommand` variant. Returns an error string if the daemon's command
/// channel is closed (not running) or if the reply channel is dropped before
/// the daemon responds (disconnected mid-flight).
async fn dispatch<T>(
    control: &ControlState,
    make_cmd: impl FnOnce(oneshot::Sender<T>) -> DaemonCommand,
) -> Result<T, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    control
        .command_tx
        .send(make_cmd(reply_tx))
        .map_err(|_| "Daemon is not running".to_string())?;
    reply_rx
        .await
        .map_err(|_| "Daemon disconnected before replying".to_string())
}

/// Payload emitted to the pair-initiator window for each discovered mesh.
#[derive(Serialize, Clone)]
pub struct MeshDiscoveredPayload {
    pub vault_id: String,
    pub mesh_name: String,
    pub online_count: usize,
}

/// Result returned by `submit_pair_code` on success.
#[derive(Serialize)]
pub struct PairSuccessResult {
    pub device_name: String,
}

/// Start mDNS discovery and stream discovered meshes to the pair-initiator window.
///
/// Sends `DaemonCommand::StartDiscovery` to the daemon. The daemon runs mDNS
/// for up to 10 seconds, forwarding each `DiscoveredMesh` back through the
/// reply channel. For each mesh, this command emits a `pair://mesh-discovered`
/// event to the `pair-initiator` window. After the scan completes (channel
/// closed), it emits `pair://discovery-finished`.
#[tauri::command]
pub async fn start_pair_discovery(
    app: AppHandle,
    control: State<'_, ControlState>,
) -> Result<(), String> {
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();

    control
        .command_tx
        .send(DaemonCommand::StartDiscovery { reply: reply_tx })
        .map_err(|_| "Daemon is not running".to_string())?;

    // Spawn a task to forward mesh events to the pair-initiator window as
    // Tauri events, then emit discovery-finished when the daemon closes the channel.
    tauri::async_runtime::spawn(async move {
        while let Some(mesh) = reply_rx.recv().await {
            let payload = MeshDiscoveredPayload {
                vault_id: mesh.vault_id,
                mesh_name: mesh.mesh_name,
                online_count: mesh.online_count,
            };
            if let Err(e) = app.emit_to(
                tauri::EventTarget::labeled("pair-initiator"),
                "pair://mesh-discovered",
                payload,
            ) {
                warn!("Failed to emit mesh-discovered event: {}", e);
            }
        }
        // Channel closed — scan finished. Notify the window.
        if let Err(e) = app.emit_to(
            tauri::EventTarget::labeled("pair-initiator"),
            "pair://discovery-finished",
            (),
        ) {
            warn!("Failed to emit discovery-finished event: {}", e);
        }
    });

    Ok(())
}

/// Connect to the selected mesh and park the QUIC connection, triggering the
/// responder to generate + display its 6-digit code.
///
/// Step 1 of the two-step GUI pairing flow. Sends `DaemonCommand::RequestPairing`
/// and waits until the connection is established and the responder is showing its
/// code. On success, returns the responder's device name so the UI can show
/// "Enter the code shown on <device>". On failure, returns an error string so the
/// UI can re-enable the Request button and let the user retry.
#[tauri::command]
pub async fn request_pairing(
    vault_id: String,
    control: State<'_, ControlState>,
) -> Result<PairSuccessResult, String> {
    let device_name = dispatch(&control, |reply| DaemonCommand::RequestPairing {
        vault_id,
        reply,
    })
    .await??;
    Ok(PairSuccessResult { device_name })
}

/// Submit the 6-digit pairing code for the currently-selected mesh.
///
/// Step 2 of the two-step GUI pairing flow. Requires a prior `request_pairing`
/// call to have succeeded. Sends `DaemonCommand::SubmitCode` and waits for the
/// daemon to complete the HMAC exchange. Returns the paired device's name on
/// success, or an error string on failure.
#[tauri::command]
pub async fn submit_pair_code(
    vault_id: String,
    code: String,
    control: State<'_, ControlState>,
) -> Result<PairSuccessResult, String> {
    let device_name = dispatch(&control, |reply| DaemonCommand::SubmitCode {
        code,
        vault_id,
        reply,
    })
    .await??;
    Ok(PairSuccessResult { device_name })
}

/// Cancel the active initiator pairing session. Idempotent.
///
/// Sends `DaemonCommand::CancelInitiate`. Returns `Ok(())` whether or not a
/// session is currently active — handles the race where the user clicks Cancel
/// after pairing has already completed but the window is still closing.
#[tauri::command]
pub async fn cancel_pair_discovery(control: State<'_, ControlState>) -> Result<(), String> {
    dispatch(&control, |reply| DaemonCommand::CancelInitiate { reply }).await
}

/// Reject the currently-active inbound pairing request. Idempotent.
///
/// Sends `DaemonCommand::RejectInbound`. The daemon drops `active_pairing`,
/// which closes the pairing handler's reply channel and surfaces to the
/// requesting peer as a failed pairing. Returns `Ok(())` whether or not an
/// inbound session is currently active — the responder window can call this
/// safely after the daemon has already failed or completed the exchange.
#[tauri::command]
pub async fn reject_inbound_pair(control: State<'_, ControlState>) -> Result<(), String> {
    dispatch(&control, |reply| DaemonCommand::RejectInbound { reply }).await
}

/// Current persisted app settings, sent to the settings panel on mount.
///
/// Both fields use the panel's own naming: an empty string means "not set" on the
/// JS side, which is how the panel renders blank inputs. Absent stored values are
/// serialized as empty strings rather than `null` so the panel can bind them
/// directly to text inputs without null-coalescing.
#[derive(Serialize)]
pub struct SettingsPayload {
    pub relay_url: String,
    pub vault_path: String,
}

/// Validate a relay URL per decision D3: empty (cleared) is allowed, otherwise it
/// must start with `http://` or `https://`. No network call is made — this is a
/// cheap shape check to catch typos like a bare hostname or `ftp://`.
///
/// Returns `Ok(())` if valid, or an error message suitable for display in the panel.
fn validate_relay_url(relay_url: &str) -> Result<(), String> {
    if relay_url.is_empty() || relay_url.starts_with("http://") || relay_url.starts_with("https://")
    {
        Ok(())
    } else {
        Err("Relay URL must start with http:// or https://".to_string())
    }
}

/// Validate a vault path per decision D4: it must be non-empty and point at an
/// existing directory. The `is_dir` check is a cheap typo guard so a mistyped path
/// is caught at save time rather than silently failing on the next launch.
///
/// Returns `Ok(())` if valid, or an error message suitable for display in the panel.
fn validate_vault_path(vault_path: &str) -> Result<(), String> {
    if vault_path.is_empty() {
        return Err("Vault folder is required".to_string());
    }
    if !Path::new(vault_path).is_dir() {
        return Err(format!("Vault folder does not exist: {vault_path}"));
    }
    Ok(())
}

/// Return the current persisted app settings for the settings panel.
///
/// Absent values come back as empty strings (see [`SettingsPayload`]).
#[tauri::command]
pub fn get_settings(
    paths: tauri::State<'_, NotesDesktopPaths>,
) -> Result<SettingsPayload, String> {
    let settings = AppSettings::load(&paths).map_err(|e| e.to_string())?;
    Ok(SettingsPayload {
        relay_url: settings.relay_url().unwrap_or_default().to_string(),
        vault_path: settings
            .vault_path()
            .and_then(|p| p.to_str())
            .unwrap_or_default()
            .to_string(),
    })
}

/// Validate and persist the settings entered in the panel.
///
/// Validates both fields up front (D3 relay-URL shape, D4 vault-folder existence)
/// and returns the first error without writing anything, so a rejected save leaves
/// the stored settings untouched. On success, loads the current settings, sets both
/// fields, and saves once — a single atomic store flush rather than two partial
/// writes. Both fields take effect on the next app launch (see the restart notice
/// in the panel); the running daemon is not reconfigured mid-session.
#[tauri::command]
pub fn save_settings(
    paths: tauri::State<'_, NotesDesktopPaths>,
    relay_url: String,
    vault_path: String,
) -> Result<(), String> {
    validate_relay_url(&relay_url)?;
    validate_vault_path(&vault_path)?;

    let mut settings = AppSettings::load(&paths).map_err(|e| e.to_string())?;
    settings.set_relay_url(Some(relay_url));
    settings.set_vault_path(vault_path.into());
    settings.save(&paths).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_url_empty_is_allowed() {
        // Clearing the relay field is valid — it falls back to the detected LAN IP.
        assert!(validate_relay_url("").is_ok());
    }

    #[test]
    fn relay_url_http_and_https_allowed() {
        assert!(validate_relay_url("http://192.168.1.10:3340/").is_ok());
        assert!(validate_relay_url("https://umbra.computer/").is_ok());
    }

    #[test]
    fn relay_url_rejects_bare_host_and_other_schemes() {
        assert!(validate_relay_url("umbra.computer").is_err());
        assert!(validate_relay_url("ftp://umbra.computer/").is_err());
    }

    #[test]
    fn vault_path_empty_is_rejected() {
        assert!(validate_vault_path("").is_err());
    }

    #[test]
    fn vault_path_nonexistent_is_rejected() {
        assert!(validate_vault_path("/no/such/vault/path/exists").is_err());
    }

    #[test]
    fn vault_path_existing_directory_is_accepted() {
        // A directory that reliably exists across machines; proves the is_dir guard
        // accepts a real folder rather than rejecting everything.
        let tmp = std::env::temp_dir();
        assert!(
            validate_vault_path(tmp.to_str().unwrap()).is_ok(),
            "temp dir {tmp:?} should validate as an existing directory"
        );
    }

    #[test]
    fn vault_path_existing_file_is_rejected() {
        // A file is not a directory — is_dir must reject it (the vault must be a folder).
        let mut file = std::env::temp_dir();
        file.push("obsidian-memory-validate-vault-test-file");
        std::fs::write(&file, b"x").unwrap();
        let result = validate_vault_path(file.to_str().unwrap());
        let _ = std::fs::remove_file(&file);
        assert!(result.is_err());
    }
}
