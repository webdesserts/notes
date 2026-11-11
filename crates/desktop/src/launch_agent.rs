//! Native macOS `LaunchAgent` for Launch at Login (replaces `tauri-plugin-autostart`).
//!
//! Writes `~/Library/LaunchAgents/com.webdesserts.notes-sync.plist` (`RunAtLoad`,
//! `KeepAlive.SuccessfulExit = false`) — the same path the 0.6.2 plugin used, so
//! an existing login item is rewritten in place. It never calls
//! `launchctl bootout`: a login-launched app runs *inside* that job, so a
//! bootout would SIGTERM the app itself.

use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use tauri::{AppHandle, Manager};

use crate::app_settings::{AppSettings, NotesDesktopPaths};

pub const LABEL: &str = "com.webdesserts.notes-sync";
pub const EXPECTED_BUNDLE_SUFFIX: &str = ".app/Contents/MacOS/desktop";

pub struct LaunchAgent {
    dir: PathBuf,
}

impl LaunchAgent {
    /// The current user's `~/Library/LaunchAgents`.
    pub fn for_user() -> Result<Self> {
        let home = std::env::var_os("HOME")
            .filter(|h| !h.is_empty())
            .context("HOME is not set")?;
        Ok(Self::at(PathBuf::from(home).join("Library/LaunchAgents")))
    }

    pub fn at(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn plist_path(&self) -> PathBuf {
        self.dir.join(format!("{LABEL}.plist"))
    }

    pub fn is_enabled(&self) -> bool {
        self.plist_path().is_file()
    }

    /// Write the plist for the running executable. Returns `Ok(false)` (and
    /// writes nothing) when not running from an installed `NotesSync.app` bundle.
    pub fn enable(&self) -> Result<bool> {
        let exe = std::env::current_exe().context("current_exe")?;
        self.enable_with_exe(&exe)
    }

    pub fn enable_with_exe(&self, exe: &Path) -> Result<bool> {
        let Some(exe) = validate_bundle_exe(exe) else {
            tracing::warn!(exe = %exe.display(), "not an installed NotesSync.app bundle; skipping LaunchAgent plist");
            return Ok(false);
        };
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("create {}", self.dir.display()))?;
        let path = self.plist_path();
        let tmp = self.dir.join(format!(".{LABEL}.plist.tmp"));
        std::fs::write(&tmp, render_plist(LABEL, &exe))
            .with_context(|| format!("write {}", tmp.display()))?;
        std::fs::rename(&tmp, &path).with_context(|| format!("rename to {}", path.display()))?;
        Ok(true)
    }

    /// Remove the plist (takes effect at next login; never bootouts this job).
    pub fn disable(&self) -> Result<()> {
        let path = self.plist_path();
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("remove {}", path.display())),
        }
    }
}

/// Whether the login item is currently registered (plist on disk).
pub fn is_enabled() -> bool {
    LaunchAgent::for_user()
        .map(|a| a.is_enabled())
        .unwrap_or(false)
}

/// Canonicalize `exe` and accept it only inside an installed `.app` bundle
/// (not `target/debug`, not an AppTranslocation mount).
pub fn validate_bundle_exe(exe: &Path) -> Option<PathBuf> {
    let canonical = std::fs::canonicalize(exe).ok()?;
    is_valid_bundle_exe(&canonical).then_some(canonical)
}

fn is_valid_bundle_exe(canonical: &Path) -> bool {
    let s = canonical.to_string_lossy();
    s.ends_with(EXPECTED_BUNDLE_SUFFIX) && !s.contains("/AppTranslocation/")
}

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

pub fn render_plist(label: &str, exe: &Path) -> String {
    let label = xml_escape(label);
    let exe = xml_escape(&exe.to_string_lossy());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
</dict>
</plist>
"#
    )
}

/// Startup reconciliation: make `~/Library/LaunchAgents` match the persisted
/// preference and return the live state for the tray checkmark.
///
/// An unset preference (first run, or a 0.6.2 store that never toggled) and an
/// already-registered 0.6.2 plist both mean "on"; the plist is rewritten in
/// place so it gains `KeepAlive.SuccessfulExit = false`.
pub fn reconcile(paths: &NotesDesktopPaths) -> bool {
    let agent = match LaunchAgent::for_user() {
        Ok(agent) => agent,
        Err(e) => {
            tracing::warn!(error = %e, "LaunchAgent unavailable");
            return false;
        }
    };
    let mut settings = match AppSettings::load(paths) {
        Ok(settings) => settings,
        Err(e) => {
            tracing::warn!(error = %e, "load settings for Launch at Login failed; leaving plist untouched");
            return agent.is_enabled();
        }
    };
    let exe = std::env::current_exe().ok();
    reconcile_with(&agent, paths, &mut settings, exe.as_deref())
}

fn reconcile_with(
    agent: &LaunchAgent,
    paths: &NotesDesktopPaths,
    settings: &mut AppSettings,
    exe: Option<&Path>,
) -> bool {
    let desired = settings.autostart_enabled().unwrap_or(true) || agent.is_enabled();
    if desired {
        // A dev build (not an installed bundle) writes nothing and leaves any
        // existing login item alone.
        if let Some(Err(e)) = exe.map(|exe| agent.enable_with_exe(exe)) {
            tracing::warn!(error = %e, "enable LaunchAgent failed");
        }
    } else if let Err(e) = agent.disable() {
        tracing::warn!(error = %e, "disable LaunchAgent failed");
    }
    if settings.autostart_enabled() != Some(desired) {
        settings.set_autostart_enabled(desired);
        if let Err(e) = settings.save(paths) {
            tracing::warn!(error = %e, "persist autostart_enabled failed");
        }
    }
    agent.is_enabled()
}

/// Tray `Launch at Login` toggle: flip the plist, persist the choice, return it.
pub fn toggle(app: &AppHandle) -> Result<bool> {
    let agent = LaunchAgent::for_user()?;
    let new_state = !agent.is_enabled();
    if new_state {
        if !agent.enable()? {
            bail!("not running from an installed NotesSync.app; Launch at Login unavailable");
        }
    } else {
        agent.disable()?;
    }
    let paths = app.state::<NotesDesktopPaths>();
    let mut settings = AppSettings::load(&paths)?;
    settings.set_autostart_enabled(new_state);
    settings.save(&paths)?;
    Ok(new_state)
}

/// Kickstart through launchd only when launchd started this process.
fn should_kickstart(xpc_service_name: Option<&str>) -> bool {
    xpc_service_name == Some(LABEL)
}

/// Relaunch the app: `launchctl kickstart -k` our own job when launchd owns this
/// process (never `bootout`, which would kill us mid-call), else Tauri's restart.
pub fn relaunch_app(app: &AppHandle) {
    if should_kickstart(std::env::var("XPC_SERVICE_NAME").ok().as_deref()) {
        // SAFETY: getuid has no preconditions and cannot fail.
        let uid = unsafe { libc::getuid() };
        let target = format!("gui/{uid}/{LABEL}");
        match std::process::Command::new("/bin/launchctl")
            .args(["kickstart", "-k", &target])
            .status()
        {
            Ok(status) if status.success() => {
                tracing::info!(%target, "launchd kickstart requested");
                return;
            }
            Ok(status) => {
                tracing::warn!(%status, "launchctl kickstart failed; falling back to restart")
            }
            Err(e) => {
                tracing::warn!(error = %e, "launchctl kickstart failed; falling back to restart")
            }
        }
    }
    app.restart();
}

#[cfg(test)]
mod kickstart_tests {
    use super::*;

    #[test]
    fn kickstarts_only_under_our_own_launchd_job() {
        assert!(should_kickstart(Some(LABEL)));
        assert!(!should_kickstart(Some("com.webdesserts.mesh")));
        assert!(!should_kickstart(None));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_bundle(root: &Path) -> PathBuf {
        let exe = root.join("Applications/NotesSync.app/Contents/MacOS/desktop");
        std::fs::create_dir_all(exe.parent().unwrap()).unwrap();
        std::fs::write(&exe, b"").unwrap();
        exe
    }

    fn fixture(root: &Path) -> (LaunchAgent, NotesDesktopPaths) {
        (
            LaunchAgent::at(root.join("LaunchAgents")),
            NotesDesktopPaths::at(root.join("config"), root.join("state")),
        )
    }

    #[test]
    fn render_plist_escapes_and_keeps_alive_on_failure() {
        let text = render_plist(LABEL, Path::new("/Apps/A&B <x>.app/Contents/MacOS/desktop"));
        assert!(text.contains("<string>com.webdesserts.notes-sync</string>"));
        assert!(
            text.contains("<string>/Apps/A&amp;B &lt;x&gt;.app/Contents/MacOS/desktop</string>")
        );
        assert!(text.contains("<key>RunAtLoad</key>\n  <true/>"));
        assert!(text.contains("<key>SuccessfulExit</key>\n    <false/>"));
    }

    #[test]
    fn bundle_exe_validation() {
        assert!(is_valid_bundle_exe(Path::new(
            "/Applications/NotesSync.app/Contents/MacOS/desktop"
        )));
        assert!(!is_valid_bundle_exe(Path::new("/x/target/debug/desktop")));
        assert!(!is_valid_bundle_exe(Path::new(
            "/private/var/folders/x/AppTranslocation/ABC/d/NotesSync.app/Contents/MacOS/desktop"
        )));
        assert!(!is_valid_bundle_exe(Path::new(
            "/Applications/NotesSync.app/Contents/MacOS/notes"
        )));
    }

    #[test]
    fn enable_disable_and_invalid_exe() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent, _) = fixture(tmp.path());
        assert!(
            !agent
                .enable_with_exe(&tmp.path().join("target/debug/desktop"))
                .unwrap()
        );
        assert!(!agent.is_enabled());
        // A symlink into the bundle resolves to the real binary.
        let exe = fake_bundle(tmp.path());
        let link = tmp.path().join("desktop-link");
        std::os::unix::fs::symlink(&exe, &link).unwrap();
        assert!(agent.enable_with_exe(&link).unwrap());
        let text = std::fs::read_to_string(agent.plist_path()).unwrap();
        assert!(
            text.contains(
                &std::fs::canonicalize(&exe)
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            )
        );
        agent.disable().unwrap();
        assert!(!agent.is_enabled());
        agent.disable().unwrap();
    }

    #[test]
    fn reconcile_unset_defaults_on_and_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent, paths) = fixture(tmp.path());
        let exe = fake_bundle(tmp.path());
        let mut s = AppSettings::default();
        assert_eq!(s.autostart_enabled(), None);
        assert!(reconcile_with(&agent, &paths, &mut s, Some(&exe)));
        assert_eq!(
            AppSettings::load(&paths).unwrap().autostart_enabled(),
            Some(true)
        );
    }

    #[test]
    fn reconcile_rewrites_legacy_plist_in_place_even_if_store_says_off() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent, paths) = fixture(tmp.path());
        let exe = fake_bundle(tmp.path());
        std::fs::create_dir_all(agent.plist_path().parent().unwrap()).unwrap();
        std::fs::write(
            agent.plist_path(),
            "<plist>0.6.2 plugin, no KeepAlive</plist>",
        )
        .unwrap();
        let mut s = AppSettings::default();
        s.set_autostart_enabled(false);
        assert!(reconcile_with(&agent, &paths, &mut s, Some(&exe)));
        let text = std::fs::read_to_string(agent.plist_path()).unwrap();
        assert!(text.contains("<key>SuccessfulExit</key>"));
        assert_eq!(
            AppSettings::load(&paths).unwrap().autostart_enabled(),
            Some(true)
        );
    }

    #[test]
    fn reconcile_dev_build_keeps_existing_plist() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent, paths) = fixture(tmp.path());
        std::fs::create_dir_all(agent.plist_path().parent().unwrap()).unwrap();
        std::fs::write(agent.plist_path(), "legacy").unwrap();
        let mut s = AppSettings::default();
        let dev = tmp.path().join("target/debug/desktop");
        assert!(reconcile_with(&agent, &paths, &mut s, Some(&dev)));
        assert_eq!(
            std::fs::read_to_string(agent.plist_path()).unwrap(),
            "legacy"
        );
    }

    #[test]
    fn reconcile_off_stays_off() {
        let tmp = tempfile::tempdir().unwrap();
        let (agent, paths) = fixture(tmp.path());
        let exe = fake_bundle(tmp.path());
        let mut s = AppSettings::default();
        s.set_autostart_enabled(false);
        assert!(!reconcile_with(&agent, &paths, &mut s, Some(&exe)));
        assert!(!agent.is_enabled());
    }
}
