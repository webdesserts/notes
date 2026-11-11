//! Persistent app-level settings in `~/.config/notes/config.toml` (via `app-config`).
//!
//! App settings (vault path, relay URL, autostart, relay listen) persist across runs
//! in `config.toml` (`0600`, config dir `0700`). This is distinct from per-vault
//! config (`<vault>/.sync/daemon.toml`), which covers vault-specific daemon options.
//! `v0.6.2` and earlier kept them in a `tauri-plugin-store` JSON file under
//! `~/Library/Application Support/`; [`migrate_legacy_settings`] imports it once and
//! archives (never deletes) the legacy directories.

use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use app_config::{AppPaths, ResolveOptions};
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Overrides the config dir (tests, dev runs); state goes to `<dir>/state` and the
/// legacy-store migration is skipped so a dev run never archives real settings.
pub const NOTES_CONFIG_DIR_ENV: &str = "NOTES_CONFIG_DIR";

/// `NOTES_XDG_{CONFIG,STATE,..}_DIR` override the XDG bases (`app-config`).
const NOTES_XDG_ENV_PREFIX: &str = "NOTES_XDG";

/// Legacy `tauri-plugin-store` dirs under `~/Library/Application Support`, in
/// migration priority order: `v0.6.x` first, then `v0.5.x`.
const LEGACY_STORE_DIRS: [&str; 2] = [
    "com.webdesserts.notes-sync",
    "com.webdesserts.obsidian-memory",
];

/// Legacy store filename inside each [`LEGACY_STORE_DIRS`] entry.
const LEGACY_STORE_FILE: &str = "app-settings.json";

/// On-disk layout of the desktop app: `~/.config/notes` + `~/.local/state/notes`.
#[derive(Debug, Clone)]
pub struct NotesDesktopPaths {
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
    /// `<config>/config.toml` (`0600`).
    pub config_file: PathBuf,
    /// `<config>/admin.token` (`0600`, 64 hex chars) for the `:8082` control API.
    pub admin_token_file: PathBuf,
    /// `<state>/desktop.log`.
    pub log_file: PathBuf,
    /// Home dir whose legacy stores get migrated; `None` under [`NOTES_CONFIG_DIR_ENV`].
    pub legacy_home: Option<PathBuf>,
}

impl NotesDesktopPaths {
    /// Layout rooted at explicit dirs (no legacy migration).
    pub fn at(config_dir: impl Into<PathBuf>, state_dir: impl Into<PathBuf>) -> Self {
        let config_dir = config_dir.into();
        let state_dir = state_dir.into();
        Self {
            config_file: config_dir.join("config.toml"),
            admin_token_file: config_dir.join("admin.token"),
            log_file: state_dir.join("desktop.log"),
            config_dir,
            state_dir,
            legacy_home: None,
        }
    }

    /// `NOTES_CONFIG_DIR` override, else the XDG layout from `app-config`.
    pub fn resolve() -> Result<Self> {
        if let Some(dir) = std::env::var_os(NOTES_CONFIG_DIR_ENV).filter(|d| !d.is_empty()) {
            let dir = PathBuf::from(dir);
            return Ok(Self::at(&dir, dir.join("state")));
        }
        let app = AppPaths::resolve_with(
            "notes",
            &ResolveOptions::new().env_prefix(NOTES_XDG_ENV_PREFIX),
        )
        .context("resolve notes app paths")?;
        let home = std::env::var_os("HOME")
            .filter(|h| !h.is_empty())
            .map(PathBuf::from)
            .context("HOME is not set")?;
        let mut paths = Self::at(app.config_dir(), app.state_dir());
        paths.legacy_home = Some(home);
        Ok(paths)
    }

    /// Create the config and state dirs (`0700`).
    pub fn ensure_dirs(&self) -> Result<()> {
        create_private_dir(&self.config_dir)?;
        create_private_dir(&self.state_dir)
    }

    fn backups_dir(&self) -> PathBuf {
        self.state_dir.join("backups")
    }
}

fn create_private_dir(dir: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("create {}", dir.display()))?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 0700 {}", dir.display()))
}

/// Persistent app-level settings (`config.toml`).
///
/// All fields have safe defaults (no vault path, no relay URL, Launch at Login
/// unset, embedded relay listening).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AppSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    vault_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_url: Option<String>,
    /// `None` until first reconciled; Launch at Login then defaults on.
    #[serde(skip_serializing_if = "Option::is_none")]
    autostart_enabled: Option<bool>,
    /// Whether this install hosts the embedded relay on `0.0.0.0:3340`.
    relay_listen: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            vault_path: None,
            relay_url: None,
            autostart_enabled: None,
            relay_listen: true,
        }
    }
}

impl AppSettings {
    /// Load `config.toml`; a missing file (first run) yields defaults, a malformed
    /// one is reported with line/column and never overwritten.
    pub fn load(paths: &NotesDesktopPaths) -> Result<Self> {
        let settings: Option<Self> =
            app_config::load_toml_optional(&paths.config_file).context("load notes config.toml")?;
        Ok(settings.unwrap_or_default().normalized())
    }

    /// Atomically persist to `config.toml` (`0600`).
    pub fn save(&self, paths: &NotesDesktopPaths) -> Result<()> {
        create_private_dir(&paths.config_dir)?;
        app_config::save_secret_toml_atomic(&paths.config_file, self)
            .context("save notes config.toml")
    }

    /// Blank strings are treated as absent so a hand-edited `""` never surfaces as
    /// `Some("")` to the vault or relay-URL resolution paths.
    fn normalized(mut self) -> Self {
        self.vault_path = self.vault_path.filter(|p| !p.as_os_str().is_empty());
        self.relay_url = self.relay_url.filter(|s| !s.is_empty());
        self
    }

    pub fn vault_path(&self) -> Option<&Path> {
        self.vault_path.as_deref()
    }

    pub fn relay_url(&self) -> Option<&str> {
        self.relay_url.as_deref()
    }

    /// Whether this install hosts the embedded relay (`0.0.0.0:3340`).
    ///
    /// Defaults to `true`; the Phase 3 cutover sets `relay_listen = false` so
    /// `webdesserts-mesh` can bind `:3340` instead.
    pub fn relay_listen(&self) -> bool {
        self.relay_listen
    }

    pub fn autostart_enabled(&self) -> Option<bool> {
        self.autostart_enabled
    }

    pub fn set_vault_path(&mut self, path: PathBuf) {
        self.vault_path = Some(path);
    }

    /// Set the relay URL, normalizing an empty string to `None` so clearing the
    /// field in the settings panel removes the key rather than persisting a blank.
    pub fn set_relay_url(&mut self, url: Option<String>) {
        self.relay_url = url.filter(|s| !s.is_empty());
    }

    pub fn set_autostart_enabled(&mut self, enabled: bool) {
        self.autostart_enabled = Some(enabled);
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn set_relay_listen(&mut self, enabled: bool) {
        self.relay_listen = enabled;
    }
}

/// Shape of the legacy `tauri-plugin-store` `app-settings.json` (flat object).
#[derive(Debug, Deserialize)]
struct LegacyStore {
    vault_path: Option<String>,
    relay_url: Option<String>,
    autostart_enabled: Option<bool>,
    relay_listen: Option<bool>,
}

impl From<LegacyStore> for AppSettings {
    fn from(legacy: LegacyStore) -> Self {
        AppSettings {
            vault_path: legacy.vault_path.map(PathBuf::from),
            relay_url: legacy.relay_url,
            autostart_enabled: legacy.autostart_enabled,
            // Only an absent key defaults to `true`; an explicit `false` (set
            // on paired hosts after the relay cutover) is preserved.
            relay_listen: legacy.relay_listen.unwrap_or(true),
        }
        .normalized()
    }
}

/// One-time import of the legacy `app-settings.json` into `config.toml`.
///
/// No-op once `config.toml` exists. Otherwise reads the `v0.6.x` store first and
/// only falls back to the `v0.5.x` store when the former does not exist; a store
/// that exists but cannot be read or parsed fails loudly (never falls through).
/// After `config.toml` is written, every legacy dir is renamed (never removed) into
/// `<state>/backups/<dir_name>-<unix_ts>`. Returns the imported file, if any.
pub fn migrate_legacy_settings(home: &Path, paths: &NotesDesktopPaths) -> Result<Option<PathBuf>> {
    if paths.config_file.symlink_metadata().is_ok() {
        return Ok(None);
    }
    let support = home.join("Library/Application Support");
    let mut source = None;
    for dir in LEGACY_STORE_DIRS {
        let json = support.join(dir).join(LEGACY_STORE_FILE);
        let text = match fs::read_to_string(&json) {
            Ok(text) => text,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| format!("read legacy settings {}", json.display()));
            }
        };
        let legacy: LegacyStore = serde_json::from_str(&text)
            .with_context(|| format!("parse legacy settings {}", json.display()))?;
        source = Some((json, AppSettings::from(legacy)));
        break;
    }
    let Some((json, settings)) = source else {
        return Ok(None);
    };
    // Write first: a crash before the renames just leaves the legacy dirs in place.
    settings.save(paths)?;
    let backups = paths.backups_dir();
    let ts = unix_now();
    for dir in LEGACY_STORE_DIRS {
        let legacy_dir = support.join(dir);
        if legacy_dir.symlink_metadata().is_err() {
            continue;
        }
        create_private_dir(&backups)?;
        let target = backup_target(&backups, dir, ts);
        fs::rename(&legacy_dir, &target)
            .with_context(|| format!("archive {} to {}", legacy_dir.display(), target.display()))?;
    }
    Ok(Some(json))
}

fn backup_target(backups: &Path, name: &str, ts: u64) -> PathBuf {
    let mut target = backups.join(format!("{name}-{ts}"));
    let mut n = 1;
    while target.symlink_metadata().is_ok() {
        target = backups.join(format!("{name}-{ts}-{n}"));
        n += 1;
    }
    target
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Read `admin.token`, generating it (32 random bytes, hex, `0600`) on first boot.
/// A present but malformed token fails loudly instead of being overwritten.
pub fn ensure_admin_token(paths: &NotesDesktopPaths) -> Result<String> {
    let file = &paths.admin_token_file;
    match fs::read_to_string(file) {
        Ok(text) => {
            let token = text.trim();
            if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
                bail!(
                    "{} is not a 64-char hex token; fix or delete it",
                    file.display()
                );
            }
            Ok(token.to_owned())
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let mut bytes = [0u8; 32];
            rand::rng().fill(&mut bytes);
            let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            create_private_dir(&paths.config_dir)?;
            app_config::write_secret_atomic(file, &token)
                .with_context(|| format!("write {}", file.display()))?;
            Ok(token)
        }
        Err(e) => Err(e).with_context(|| format!("read {}", file.display())),
    }
}

/// Resolve the vault path from the environment variable or a stored fallback.
///
/// Priority (highest to lowest):
/// 1. `env_vault` — the value of `NOTES_VAULT` if set and non-empty.
///    Takes precedence so the env var always works as an override on dev machines.
/// 2. `stored_vault` — the path previously persisted in `config.toml`.
///    Used by autostarted instances that launch without the env var.
/// 3. `None` — neither source has a vault path; the caller should fail with a
///    useful error message.
///
/// Extracted as a pure function so it can be unit-tested without launching Tauri.
pub fn resolve_vault_path(env_vault: Option<&str>, stored_vault: Option<&str>) -> Option<PathBuf> {
    if let Some(v) = env_vault.filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(v));
    }
    stored_vault.filter(|v| !v.is_empty()).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_vault_present_returns_env_path() {
        assert_eq!(
            resolve_vault_path(Some("/tmp/legacy-vault"), None),
            Some(PathBuf::from("/tmp/legacy-vault")),
        );
    }

    #[test]
    fn env_vault_absent_stored_present_returns_stored_path() {
        assert_eq!(
            resolve_vault_path(None, Some("/tmp/legacy-vault")),
            Some(PathBuf::from("/tmp/legacy-vault")),
        );
    }

    #[test]
    fn env_vault_takes_priority_over_stored() {
        assert_eq!(
            resolve_vault_path(
                Some("/tmp/legacy-vault-env"),
                Some("/tmp/legacy-vault-stored")
            ),
            Some(PathBuf::from("/tmp/legacy-vault-env")),
        );
    }

    #[test]
    fn neither_source_returns_none() {
        assert_eq!(resolve_vault_path(None, None), None);
    }

    #[test]
    fn empty_env_string_falls_through_to_stored() {
        // An empty env var (e.g. `NOTES_VAULT=`) is treated as unset,
        // mirroring how the relay-URL env var is empty-filtered before use.
        assert_eq!(
            resolve_vault_path(Some(""), Some("/tmp/legacy-vault")),
            Some(PathBuf::from("/tmp/legacy-vault")),
        );
    }

    #[test]
    fn empty_stored_path_treated_as_absent() {
        // A stored empty string (e.g. corrupted write) must not produce a blank path.
        assert_eq!(resolve_vault_path(None, Some("")), None);
    }

    #[test]
    fn relay_listen_defaults_true_and_round_trips() {
        assert!(AppSettings::default().relay_listen());
        let mut settings = AppSettings::default();
        settings.set_relay_listen(false);
        assert!(!settings.relay_listen());
    }

    #[test]
    fn relay_url_absent_by_default() {
        assert_eq!(AppSettings::default().relay_url(), None);
    }

    #[test]
    fn relay_url_round_trips_through_setter() {
        let mut settings = AppSettings::default();
        settings.set_relay_url(Some("https://relay.example.com/".to_string()));
        assert_eq!(settings.relay_url(), Some("https://relay.example.com/"));
    }

    #[test]
    fn set_relay_url_none_clears_value() {
        let mut settings = AppSettings::default();
        settings.set_relay_url(Some("https://relay.example.com/".to_string()));
        settings.set_relay_url(None);
        assert_eq!(settings.relay_url(), None);
    }

    #[test]
    fn set_relay_url_empty_string_normalizes_to_none() {
        let mut settings = AppSettings::default();
        settings.set_relay_url(Some(String::new()));
        assert_eq!(settings.relay_url(), None);
    }

    // --- config.toml, legacy migration, admin.token ---

    fn layout(root: &Path) -> (PathBuf, NotesDesktopPaths) {
        let home = root.join("home");
        fs::create_dir_all(home.join("Library/Application Support")).unwrap();
        let paths =
            NotesDesktopPaths::at(home.join(".config/notes"), home.join(".local/state/notes"));
        (home, paths)
    }

    fn write_legacy(home: &Path, dir: &str, body: &str) -> PathBuf {
        let d = home.join("Library/Application Support").join(dir);
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join(LEGACY_STORE_FILE), body).unwrap();
        d
    }

    fn mode(p: &Path) -> u32 {
        fs::metadata(p).unwrap().permissions().mode() & 0o777
    }

    fn backups(paths: &NotesDesktopPaths, prefix: &str) -> Vec<PathBuf> {
        match fs::read_dir(paths.backups_dir()) {
            Ok(rd) => rd
                .map(|e| e.unwrap().path())
                .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with(prefix))
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    #[test]
    fn load_missing_config_yields_defaults_and_save_round_trips_private() {
        let tmp = tempfile::tempdir().unwrap();
        let (_home, paths) = layout(tmp.path());
        assert_eq!(AppSettings::load(&paths).unwrap(), AppSettings::default());

        let mut s = AppSettings::default();
        s.set_vault_path("/tmp/notes-vault".into());
        s.set_relay_url(Some("https://relay.example.com/".into()));
        s.set_relay_listen(false);
        s.set_autostart_enabled(true);
        s.save(&paths).unwrap();

        assert_eq!(AppSettings::load(&paths).unwrap(), s);
        assert_eq!(mode(&paths.config_file), 0o600);
        assert_eq!(mode(&paths.config_dir), 0o700);
    }

    #[test]
    fn blank_strings_in_config_toml_load_as_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let (_home, paths) = layout(tmp.path());
        paths.ensure_dirs().unwrap();
        fs::write(&paths.config_file, "vault_path = \"\"\nrelay_url = \"\"\n").unwrap();
        let s = AppSettings::load(&paths).unwrap();
        assert_eq!(s.vault_path(), None);
        assert_eq!(s.relay_url(), None);
        assert!(s.relay_listen());
    }

    #[test]
    fn migrate_prefers_notes_sync_store_and_preserves_relay_listen_false() {
        let tmp = tempfile::tempdir().unwrap();
        let (home, paths) = layout(tmp.path());
        let ns = write_legacy(
            &home,
            "com.webdesserts.notes-sync",
            r#"{"vault_path":"/tmp/notes-vault","relay_url":"https://relay.example.com/","autostart_enabled":true,"relay_listen":false}"#,
        );
        let om = write_legacy(
            &home,
            "com.webdesserts.obsidian-memory",
            r#"{"vault_path":"/old","relay_url":"https://old/","autostart_enabled":false}"#,
        );

        let src = migrate_legacy_settings(&home, &paths).unwrap();
        assert_eq!(src, Some(ns.join(LEGACY_STORE_FILE)));

        let s = AppSettings::load(&paths).unwrap();
        assert_eq!(s.vault_path(), Some(Path::new("/tmp/notes-vault")));
        assert_eq!(s.relay_url(), Some("https://relay.example.com/"));
        assert_eq!(s.autostart_enabled(), Some(true));
        assert!(!s.relay_listen());
        assert_eq!(mode(&paths.config_file), 0o600);

        // Both legacy dirs archived, never deleted; contents intact.
        assert!(!ns.exists() && !om.exists());
        let ns_bk = backups(&paths, "com.webdesserts.notes-sync-");
        let om_bk = backups(&paths, "com.webdesserts.obsidian-memory-");
        assert_eq!((ns_bk.len(), om_bk.len()), (1, 1));
        assert!(ns_bk[0].join(LEGACY_STORE_FILE).is_file());
        assert!(om_bk[0].join(LEGACY_STORE_FILE).is_file());

        // Idempotent: config.toml exists → no-op.
        assert_eq!(migrate_legacy_settings(&home, &paths).unwrap(), None);
    }

    #[test]
    fn migrate_falls_back_to_obsidian_memory_and_defaults_relay_listen_true() {
        let tmp = tempfile::tempdir().unwrap();
        let (home, paths) = layout(tmp.path());
        let om = write_legacy(
            &home,
            "com.webdesserts.obsidian-memory",
            r#"{"vault_path":"/tmp/legacy-vault","relay_url":"","autostart_enabled":true}"#,
        );
        let src = migrate_legacy_settings(&home, &paths).unwrap();
        assert_eq!(src, Some(om.join(LEGACY_STORE_FILE)));
        let s = AppSettings::load(&paths).unwrap();
        assert_eq!(s.vault_path(), Some(Path::new("/tmp/legacy-vault")));
        assert_eq!(s.relay_url(), None);
        assert!(s.relay_listen());
        assert!(!om.exists());
        assert_eq!(backups(&paths, "com.webdesserts.obsidian-memory-").len(), 1);
    }

    #[test]
    fn migrate_malformed_notes_sync_store_fails_loudly_without_fallthrough() {
        let tmp = tempfile::tempdir().unwrap();
        let (home, paths) = layout(tmp.path());
        let ns = write_legacy(&home, "com.webdesserts.notes-sync", "{not json");
        let om = write_legacy(
            &home,
            "com.webdesserts.obsidian-memory",
            r#"{"vault_path":"/old"}"#,
        );
        let err = migrate_legacy_settings(&home, &paths).unwrap_err();
        assert!(format!("{err:#}").contains("com.webdesserts.notes-sync"));
        assert!(!paths.config_file.exists());
        assert!(ns.exists() && om.exists());
    }

    #[test]
    fn migrate_is_noop_when_config_exists_or_no_legacy_store() {
        let tmp = tempfile::tempdir().unwrap();
        let (home, paths) = layout(tmp.path());
        assert_eq!(migrate_legacy_settings(&home, &paths).unwrap(), None);
        assert!(!paths.config_file.exists());

        AppSettings::default().save(&paths).unwrap();
        let ns = write_legacy(
            &home,
            "com.webdesserts.notes-sync",
            r#"{"vault_path":"/x"}"#,
        );
        assert_eq!(migrate_legacy_settings(&home, &paths).unwrap(), None);
        assert!(ns.exists());
        assert_eq!(AppSettings::load(&paths).unwrap().vault_path(), None);
    }

    #[test]
    fn admin_token_generated_once_private_and_validated() {
        let tmp = tempfile::tempdir().unwrap();
        let (_home, paths) = layout(tmp.path());
        let t1 = ensure_admin_token(&paths).unwrap();
        assert_eq!(t1.len(), 64);
        assert!(t1.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(mode(&paths.admin_token_file), 0o600);
        assert_eq!(ensure_admin_token(&paths).unwrap(), t1);

        fs::write(&paths.admin_token_file, "short").unwrap();
        assert!(ensure_admin_token(&paths).is_err());
        assert_eq!(
            fs::read_to_string(&paths.admin_token_file).unwrap(),
            "short"
        );
    }
}
