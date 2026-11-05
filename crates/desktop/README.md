# NotesSync — Desktop App

A native macOS menu-bar app that runs the notes sync daemon and
exposes its pairing flow through a tray UI. The app embeds `sync-daemon` as
a Tokio task — there is no separate daemon process to start, no CLI to keep
running.

This is a Tauri v2 app. The Rust crate is `crates/desktop`; the frontend lives
under `crates/desktop/frontend` (Vite + plain HTML; the pairing windows are
static files served from `public/windows/`, no React).

## Status

This is the authoritative desktop install, approval, update and recovery guide.
The private `webdesserts/tap/notes-sync` cask is not published yet. v0.5.8
shipped as the `webdesserts/tap/webdesserts-memory` cask (Memory.app); its exact
public DMG, pinned self-signed identity and final cask passed the full verify-only
gate. A quarantined, same-token 0.5.7 → 0.5.8 smoke on arm64 macOS 26.6.2
confirmed healthy target launch, quit and relaunch through LaunchServices without
a second blocking approval. The owner separately attested the baseline approval
and Spotlight relaunch. Exact-0.5.8 owner launches from Applications/Finder and
Spotlight remain a publication blocker; use the install command below only once
the cask is visible in the tap.

**What works:**

- macOS menu bar tray with live status (Idle / Connected · N peers).
- "Pair with nearby device…" menu item opens an initiator window that scans
  the LAN via mDNS and submits a 6-digit code to the responder.
- Inbound pairing requests pop a responder window showing the code + a
  5-minute countdown. Reject button + macOS notification included.
- Embedded iroh relay listens on `0.0.0.0:3340` and advertises the machine's
  detected LAN IP so peers on the same network can reach it.
- Quit handler cleanly shuts the daemon down and clears `relay_url` in
  `daemon.toml`.

**Out of scope for v0.5.x:** cross-network pairing (invite codes / URLs are
Phase 6), auto-port-forwarding, the React popup UI on left-click and in-app
auto-update. Private distribution uses a checksum-pinned, self-signed cask;
Developer ID signing and notarization are out of scope.

## Running

Terminal development runs can use `OBSIDIAN_MEMORY_VAULT` (no `--vault` flag).
Normal GUI launch requires a persisted vault path in `app-settings.json`;
Finder/Spotlight do not inherit a terminal's environment. There is no first-run
vault onboarding UI. The mandatory smoke preseeds settings for a scratch vault
as **test preparation**, not as proof of clean-install onboarding.

> **Local development:** the frontend is embedded at **compile time**, and
> `dist/windows/` (the pairing UIs) is **not committed**. Build the frontend
> before compiling Rust or the pairing windows open blank. The binary-only
> helper below is not the packaged-release path; use the gated publisher tooling
> described under [Releasing the desktop app](#releasing-the-desktop-app), not
> ad-hoc npm Tauri commands.

```bash
# 1. Build the frontend once (emits frontend/dist incl. the pairing windows)
cd crates/desktop/frontend && npm install && npm run build

# 2. Build + sign the app — from the repo root. (Do NOT use `cargo run`: it
#    launches the binary before it can be code-signed.)
./scripts/build-desktop.sh

# 3. Run the signed binary (starts the daemon + tray)
OBSIDIAN_MEMORY_VAULT=~/notes ./target/debug/desktop
```

With a configured vault, the tray icon appears in the menu bar. The app is dockless by
design — the `Accessory` activation policy is set in `main.rs`. The health
endpoint binds `127.0.0.1:8082` (the `HEALTH_PORT` const in `main.rs`; 8081 is
avoided because it's commonly taken, e.g. by llama-swap). Change the const if needed.

**Blank pairing window?** The `windows/*.html` weren't embedded — you compiled
before building the frontend (or changed `dist/` afterward). Run `npm run build`
in `frontend/`, then `cargo clean -p desktop` to force a re-embed, rebuild with
`build-desktop.sh`, and launch the signed binary.

### Code signing (firewall persistence)

`build-desktop.sh` signs the binary with the **`ObsidianMemory Dev Signing`**
self-signed certificate. This matters on machines running the macOS Application
Firewall (especially MDM/stealth setups): ad-hoc signing (`codesign -s -`)
re-anchors the firewall's allow-rule on the binary's cdhash, which changes every
build — so you get a fresh "allow incoming connections" prompt on every rebuild.
A stable certificate supported firewall-rule persistence in the recorded local
MDM test (2026-06-13); that is not evidence of Gatekeeper approval persistence
for packaged-app upgrades.

Policy: the stable identity is owner-managed in **Apple Keychain on one
designated publisher Mac**. Local development signing requires owner-approved
Keychain provisioning and a runbook. This guide does not establish provisioning
on any host or authorize key distribution or trust-policy changes.

## Private cask installation and updates

Once the cask is visible in the tap, install it with:

```text
brew install --cask webdesserts/tap/notes-sync
```

The cask targets **arm64** and declares macOS **Tahoe** as its conservative
support floor. The recorded smoke ran on macOS 26.6.2; that single-host result is
not proof of compatibility with every newer release. Normal GUI launch also
requires a persisted vault path in `app-settings.json`; the app does not yet
provide first-run vault selection.

### Expected first-launch approval

With the vault path already persisted, launch NotesSync from Applications in
Finder or through Spotlight. A visible unverified-developer Gatekeeper approval
is expected on first install. The recorded smoke used **Open Anyway** after the
artifact digest and signer were independently verified; the owner recognized a
standard non-malware warning, but its exact wording was not captured and must not
be reconstructed as a quotation:

1. Verify that the cask resolves to the intended release and checksum.
2. **Only for the expected standard unverified-developer warning for NotesSync**,
   use **System Settings > Privacy & Security > Open Anyway** and the ordinary
   confirmation dialog.
3. Relaunch normally from Applications/Finder/Spotlight; use the menu-bar icon.

An unexpected warning means **stop and escalate to the owner**; do not approve
it. Preserve quarantine. Do not use `--no-quarantine`, remove quarantine with
`xattr`, install custom trust policies, globally disable Gatekeeper/SIP, or use
right-click as a bypass. The explicit System Settings flow is the sole trust
exception, not a reason to weaken system security.

Updates are user-triggered:

```text
brew update
brew upgrade --cask webdesserts/tap/notes-sync
```

On the recorded macOS 26.6.2 smoke host, an ordinary same-token upgrade from the
approved, same-signed 0.5.7 baseline to exact 0.5.8 retained quarantine and
launched/relaunched without another blocking approval. This is one observation,
not a promise that approval persists on every host or future release. The app
remains dockless/menu-bar based and does not automatically restart after an
upgrade. Quit before upgrading and relaunch normally afterward.

### Recovery

A failed or absent desktop artifact fails the publication gate and preserves
the prior cask. If a bad candidate was already published:

1. Disable cask publication automation so it cannot undo recovery.
2. Have the owner revert the **exact tap cask commit**, then verify the retained
   release asset and its digest against the restored cask.
3. Quit NotesSync, then use the restored definition:

```text
brew update
brew uninstall --cask webdesserts/tap/notes-sync
brew install --cask webdesserts/tap/notes-sync
```

This is an owner-coordinated tap rollback, not an `@version` install. Relaunch
normally; stop on unexpected warnings. Replacing the certificate is an identity
change requiring a new probe and validation, not a routine upgrade with promised
approval persistence.

## Releasing the desktop app

[`scripts/t299/publish_desktop.py`](../../scripts/t299/publish_desktop.py) defaults
to **build-and-validate only**, with no release upload. Signing is restricted to
the designated publisher Mac using the owner-managed Keychain identity. The
single Tauri packaging path uses pinned **tauri-cli 2.11.4**; its compatibility
probe must pass before real use. Probe failure means stop and replan: there is
no fallback signing/build path. The embedded `NotesSync.app` in the DMG must pass
validation; authored tooling alone is not proof of a valid package.

Explicit `--publish` additionally requires an **existing owner-authorized tag
and release** and upload approval. It does not create tags or replace assets.
There is no Developer ID/notarization route and no Apple credentials are needed
or handled. CI never holds the signing key or builds/signs desktop release bytes.

The separate [`desktop-cask` workflow](../../.github/workflows/desktop-cask.yml)
defaults to **verify**, using the exact tag and expected digest. Cask publication
requires explicit publish intent, successful verification and owner enablement/
environment gates. [`cask_gate.py`](../../scripts/t299/cask_gate.py) defaults to
validate-only; pre-publication validation cannot commit or push. The owner must
prevent competing releases and desktop publications until publication completes;
the queue check is defensive, not an atomic lock. Broader automatic publication
remains disabled.

Production `signer_config.json` now contains the owner-approved public identity
and exact pre-approval observations from the local compatibility probe; it
contains no key material. It remains the sole source of spctl/ticket
preapproval classification: the official Homebrew audit does not inspect
Gatekeeper.

The real v0.5.8 candidate audit first failed when the source used a concrete
release URL and suggested `sha256 :no_check`, which is forbidden policy. The
generator now emits Homebrew version interpolation (`v#{version}` /
`NotesSync_#{version}`) while retaining the exact checksum. Full verify-only runs
on the `macos-14` runner subsequently passed signer validation, isolated source
resolution, collision handling, exact fetch, Ruby syntax, `brew style` and
`brew audit --cask --online` with the Tahoe floor. Audit output is never treated
as a Gatekeeper classifier.

The v0.5.8 build, immutable tag, release upload, quarantined same-token upgrade
and post-smoke full candidate verification are complete. The target's healthy
launch/relaunch evidence is agent-driven through LaunchServices; the owner has
not yet performed the required exact-0.5.8 Applications/Finder and Spotlight
follow-up, so first tap publication remains blocked. Broader automatic
publication is separately owner-gated and stays disabled. Runner recognition of
the Tahoe symbol is a packaging gate, not evidence that every Tahoe or newer
host is compatible. This guide remains the authoritative approval and recovery
guide; the tap README provides the short install/update entry.

## Frontend / ui dependency

The settings panel uses [`@webdesserts/ui`](https://github.com/webdesserts/ui)
for its components and design tokens. It's consumed as a **SHA-pinned git
dependency on the built package**, not a source alias:

```json
"@webdesserts/ui": "github:webdesserts/ui#<full-sha>"
```

`npm install` clones that ref, runs ui's `prepare` script to build `dist`, and
installs the package into `node_modules/@webdesserts/ui`. The frontend resolves
ui through the package's `exports` map — JS from `dist`, CSS tokens from the
`@webdesserts/ui/tokens` and `@webdesserts/ui/presets/*` subpaths, and the
Tailwind `@source` scans `node_modules/@webdesserts/ui/dist` for the component
classes. No sibling `~/code/webdesserts/ui` checkout is required to build.

**Bumping the ui version:** edit the pinned `<full-sha>` in
`frontend/package.json`, then `npm install` to refresh the lockfile. Pin the
full SHA (not a branch name) so builds are reproducible.

### Local ui co-development (HMR)

The pinned package is rebuilt only on `npm install`, so editing ui source has no
effect on the frontend until you re-install. For active co-development, `npm
link` a local ui checkout and run a `tsc --watch` so `dist` rebuilds on every ui
source edit:

```bash
# In the ui checkout — register the package and keep dist fresh:
cd ~/code/webdesserts/ui
npm link
npx tsc -p tsconfig.build.json --watch   # ui's `build` is plain tsc (no watch)

# In the frontend — link the local checkout, then run the dev server:
cd crates/desktop/frontend
npm link @webdesserts/ui
npm run dev
```

The frontend's `@source "../../node_modules/@webdesserts/ui/dist"` follows the
symlink to the live-rebuilt `dist`, so token and class changes flow through once
`tsc --watch` re-emits.

**Tradeoff vs. the old source alias:** the source alias gave zero-build instant
HMR (edit ui source → frontend hot-reloads). The package model trades that for
reproducibility: `npm link` + `tsc --watch` restores HMR, but a ui source edit
reflects only after tsc re-emits `dist` (sub-second, not literally instant), and
you have to keep the watch process running. For frontend work that doesn't touch
ui, no link is needed — the pinned package just works.

**To return to the pinned package:** `npm unlink @webdesserts/ui` then `npm
install` (restores the git-dep from `package.json`).

> **Footgun — gitignored `dist`:** ui's `dist` is gitignored, so the git ref
> commit contains no built output; the install rebuilds it via `prepare`. This
> is also why a stale `npm link` or a sibling checkout on the wrong branch can
> mask the real package — if a build behaves unexpectedly, confirm
> `node_modules/@webdesserts/ui` is a real directory (the git install), not a
> leftover symlink.

## Architecture

The crate is laid out so each module owns one concern:

- `daemon_task.rs` — spawns `sync_daemon::daemon::run_with_shutdown_controlled`
  as a Tauri async task, returns a `DaemonHandle` carrying the cancellation
  token, `DaemonControl`, and watchdog `done_rx`. The watchdog calls
  `app.exit(1)` if the daemon errors or panics.
- `shutdown.rs` — `ShutdownController` is taken once by the Quit handler and
  awaits the watchdog for up to 5s before force-exiting.
- `tray_status.rs` — driver task subscribing to `DaemonControl.status_rx`,
  updates the cached `MenuItem` handles via `MenuItem::set_text` (no full
  menu rebuild — avoids the macOS flicker).
- `pair_window.rs` — `open_initiator(app)` and `open_responder(app, ...)`.
  Both wrap their `WebviewWindowBuilder` calls in `app.run_on_main_thread`;
  off-main-thread WebView creation panics on macOS. Close handlers fire
  `DaemonCommand::CancelInitiate` / `RejectInbound` so the daemon drops its
  in-flight session when the user closes via X / Cmd+W.
- `pair_events.rs` — consumer task subscribing to
  `DaemonControl.pairing_rx`. Translates each `PairingUiEvent` into the right
  UI action: `InboundRequest` → notification + responder window;
  `InboundCompleted` / `InboundFailed` → status event to the responder window
  + a delayed-close backstop.
- `notification.rs` — thin wrapper around `tauri-plugin-notification`. The
  notification is best-effort — failure does not block the pairing flow.
- `commands.rs` — Tauri `invoke` handlers: `start_pair_discovery`,
  `submit_pair_code`, `cancel_pair_discovery`, `reject_inbound_pair`.
- `frontend/public/windows/` — vanilla static HTML/JS. Vite copies these
  files into `dist/` at build verbatim. The pairing UI does not go through
  the React build pipeline.

## Cross-network pairing

v0.5.x scopes pairing to the LAN. Discovery uses mDNS; the embedded relay
binds to `0.0.0.0:3340` so it accepts peers from any interface. If a user
wants to expose the relay across the internet, that's their responsibility:

- Port-forward `:3340` on the router and tell peers your public address.
- Or run a TLS-terminating reverse proxy (Caddy, nginx) in front of the
  relay; daemon.toml's `relay_url` accepts an arbitrary string.
- Or wait for Phase 6's invite-code flow, which will bundle the relay URL
  into a shareable invite payload.

The app itself doesn't try to template Caddy configs, hole-punch through
NATs, or expose UPnP. Those decisions are best made by the user with
visibility into their network.

## Known follow-ups

These are non-blocking but worth tracking:

- `crates/sync-daemon/src/daemon.rs` is ~1600 lines and growing. Worth
  splitting into something like `daemon/mod.rs` + `daemon/initiator.rs` +
  `daemon/responder.rs` before Phase 6 adds more handlers.
- `run_initiator_pairing_parked` (daemon.rs) drives the two-step GUI flow;
  `pair.rs::pair_inner` (CLI) drives the equivalent single-step flow. Both
  share post-pair logic via `pair_shared::write_pair_allowlist`.
- `SyncNode::subscribe_discovery()` is called twice in production (once by
  the daemon's run_loop, once per `DaemonCommand::StartDiscovery`). Verified
  in passing that iroh's `MdnsAddressLookup` supports multiple subscribers;
  if a regression ever appears, fall back to a fanout `broadcast` in
  `SyncNode` (~50 lines).
- macOS notification permission flow ([Phase 1.5 plan][plan] S7) requires
  manual smoke on a fresh bundle. The notification fallback path (denied
  permission → responder window still opens) is wired but not yet validated
  end-to-end.

## See also

- [`specs/sync/desktop-pairing.feature`](../../specs/sync/desktop-pairing.feature)
  — BDD spec for the pairing UX.
- [Memory Desktop App project note] — long-form design rationale (private).
- Phase 1.5 plan — full architecture + verification checklist (private).

[plan]: ../../../.claude/plans/desktop-phase-1-5-tray-status-lan-pairing.md
[Memory Desktop App project note]: obsidian://memory-desktop-app
