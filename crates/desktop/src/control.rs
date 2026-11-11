//! `:8082` loopback control API for `NotesSync.app`, bound once for the `.app` lifetime.
//!
//! Unauthenticated GETs: `/health` (wire-compatible with the 0.6.2 daemon's
//! `:8082/health` probe), `/status` and `/update/check`. The mutating POSTs
//! (`/daemon/restart`, `/app/restart`, `/app/update`) require
//! `Authorization: Bearer <admin.token>`, compared in constant time.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tauri::AppHandle;
use tauri_plugin_updater::UpdaterExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::daemon_host::HostState;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// HTTP status + message for a failed updater call.
pub type OpError = (StatusCode, String);

/// What the routes need from the running app; faked in tests.
pub trait ControlOps: Send + Sync + 'static {
    fn daemon_running(&self) -> bool;
    fn autostart_enabled(&self) -> bool;
    /// Daemon fields merged into `GET /status`.
    fn status(&self) -> Value;
    /// Serialized daemon restart (shares `DaemonHost::restart_lock`).
    fn restart_daemon(&self) -> BoxFuture<'_, Result<()>>;
    /// Spawn shutdown + relaunch; the caller responds first.
    fn restart_app(&self);
    /// `Ok(Some(version))` when a newer release is published.
    fn check_update(&self) -> BoxFuture<'_, Result<Option<String>, OpError>>;
    /// Spawn download → install → relaunch; `Ok(false)` when already up to date.
    fn install_update(&self) -> BoxFuture<'_, Result<bool, OpError>>;
}

/// Production [`ControlOps`] over the Tauri app and its [`DaemonHost`](crate::daemon_host::DaemonHost).
pub struct TauriOps {
    app: AppHandle,
    host: HostState,
}

impl TauriOps {
    pub fn new(app: AppHandle, host: HostState) -> Self {
        Self { app, host }
    }
}

impl ControlOps for TauriOps {
    fn daemon_running(&self) -> bool {
        self.host.is_running()
    }

    fn autostart_enabled(&self) -> bool {
        crate::launch_agent::is_enabled()
    }

    fn status(&self) -> Value {
        self.host.status_json()
    }

    fn restart_daemon(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.host.restart())
    }

    fn restart_app(&self) {
        let app = self.app.clone();
        let host = self.host.clone();
        tauri::async_runtime::spawn(async move {
            // Let the HTTP response flush before the process restarts.
            tokio::time::sleep(Duration::from_millis(100)).await;
            host.shutdown_app().await;
            crate::launch_agent::relaunch_app(&app);
        });
    }

    fn check_update(&self) -> BoxFuture<'_, Result<Option<String>, OpError>> {
        Box::pin(async move {
            let updater = self
                .app
                .updater()
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
            let update = updater
                .check()
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
            Ok(update.map(|u| u.version))
        })
    }

    fn install_update(&self) -> BoxFuture<'_, Result<bool, OpError>> {
        Box::pin(async move {
            let updater = self
                .app
                .updater()
                .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
            let Some(update) = updater
                .check()
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?
            else {
                return Ok(false);
            };
            let app = self.app.clone();
            let host = self.host.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(e) = update
                    .download_and_install(|_chunk, _total| {}, || {})
                    .await
                {
                    warn!("Update download failed: {e}");
                    return;
                }
                host.shutdown_app().await;
                crate::launch_agent::relaunch_app(&app);
            });
            Ok(true)
        })
    }
}

#[derive(Clone)]
struct Ctx {
    ops: Arc<dyn ControlOps>,
    token: Arc<str>,
    version: Arc<str>,
}

/// Build the control router; `token` is the `admin.token` contents.
pub fn router(ops: Arc<dyn ControlOps>, token: String, version: String) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/update/check", get(update_check))
        .route("/daemon/restart", post(daemon_restart))
        .route("/app/restart", post(app_restart))
        .route("/app/update", post(app_update))
        .with_state(Ctx {
            ops,
            token: token.into(),
            version: version.into(),
        })
}

/// Bind synchronously, before Tauri or the daemon start (single-instance gate).
pub fn bind(addr: &str) -> std::io::Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// `true` when something on `addr` answers `GET /health` with an HTTP response.
pub fn probe(addr: &str) -> bool {
    use std::io::{Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    let timeout = Duration::from_millis(500);
    let Some(sock) = addr.to_socket_addrs().ok().and_then(|mut a| a.next()) else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&sock, timeout) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    if stream
        .write_all(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 12];
    matches!(stream.read(&mut buf), Ok(n) if n >= 5 && buf.starts_with(b"HTTP/"))
}

/// Serve `router` on the pre-bound `listener` until `shutdown` fires.
pub async fn serve(
    listener: std::net::TcpListener,
    router: Router,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener).context("adopt control listener")?;
    info!(
        "Control API: listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .context("serve control API")
}

/// Spawn [`serve`] on Tauri's runtime.
pub fn spawn(
    listener: std::net::TcpListener,
    ops: Arc<dyn ControlOps>,
    token: String,
    version: String,
    shutdown: CancellationToken,
) {
    tauri::async_runtime::spawn(async move {
        if let Err(e) = serve(listener, router(ops, token, version), shutdown).await {
            warn!("Control API stopped: {e:#}");
        }
    });
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "ok": false, "error": message.into() })),
    )
        .into_response()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|presented| constant_time_eq(presented.as_bytes(), token.as_bytes()))
}

async fn health(State(ctx): State<Ctx>) -> Response {
    if ctx.ops.daemon_running() {
        (StatusCode::OK, "OK").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "stopped").into_response()
    }
}

async fn status(State(ctx): State<Ctx>) -> Json<Value> {
    let mut body = json!({
        "ok": true,
        "version": &*ctx.version,
        "pid": std::process::id(),
        "autostart_enabled": ctx.ops.autostart_enabled(),
    });
    if let (Some(out), Value::Object(fields)) = (body.as_object_mut(), ctx.ops.status()) {
        out.extend(fields);
    }
    Json(body)
}

async fn update_check(State(ctx): State<Ctx>) -> Response {
    match ctx.ops.check_update().await {
        Ok(latest) => Json(json!({
            "current_version": &*ctx.version,
            "update_available": latest.is_some(),
            "latest_version": latest.as_deref().unwrap_or(&ctx.version),
        }))
        .into_response(),
        Err((status, message)) => {
            warn!("Update check failed: {message}");
            error(status, message)
        }
    }
}

async fn daemon_restart(State(ctx): State<Ctx>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &ctx.token) {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    match ctx.ops.restart_daemon().await {
        Ok(()) => Json(json!({ "ok": true, "restarting": true })).into_response(),
        Err(e) => {
            warn!("Daemon restart failed: {e:#}");
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
        }
    }
}

async fn app_restart(State(ctx): State<Ctx>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &ctx.token) {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    ctx.ops.restart_app();
    Json(json!({ "ok": true, "restarting": true })).into_response()
}

async fn app_update(State(ctx): State<Ctx>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &ctx.token) {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    match ctx.ops.install_update().await {
        Ok(true) => Json(json!({ "ok": true, "updating": true })).into_response(),
        Ok(false) => {
            Json(json!({ "ok": true, "updating": false, "reason": "up to date" })).into_response()
        }
        Err((status, message)) => {
            warn!("Update failed: {message}");
            error(status, message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[derive(Default)]
    struct Fake {
        running: AtomicBool,
        restarts: AtomicUsize,
        app_restarts: AtomicUsize,
    }

    impl ControlOps for Fake {
        fn daemon_running(&self) -> bool {
            self.running.load(SeqCst)
        }
        fn autostart_enabled(&self) -> bool {
            true
        }
        fn status(&self) -> Value {
            json!({ "daemon_running": self.daemon_running(), "peer_count": 2 })
        }
        fn restart_daemon(&self) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.restarts.fetch_add(1, SeqCst);
                Ok(())
            })
        }
        fn restart_app(&self) {
            self.app_restarts.fetch_add(1, SeqCst);
        }
        fn check_update(&self) -> BoxFuture<'_, Result<Option<String>, OpError>> {
            Box::pin(async { Ok(Some("0.6.4".to_string())) })
        }
        fn install_update(&self) -> BoxFuture<'_, Result<bool, OpError>> {
            Box::pin(async { Ok(false) })
        }
    }

    async fn start(fake: Arc<Fake>) -> (SocketAddr, CancellationToken) {
        let listener = bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let token = CancellationToken::new();
        tokio::spawn(serve(
            listener,
            router(fake, TOKEN.to_string(), "0.6.3".to_string()),
            token.clone(),
        ));
        (addr, token)
    }

    async fn request(
        addr: SocketAddr,
        method: &str,
        path: &str,
        auth: Option<&str>,
    ) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let auth = auth
            .map(|t| format!("Authorization: Bearer {t}\r\n"))
            .unwrap_or_default();
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Length: 0\r\nConnection: close\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf).into_owned();
        let code = text[9..12].parse().unwrap();
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (code, body)
    }

    fn parse(body: &str) -> Value {
        serde_json::from_str(body).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn health_tracks_daemon_and_probe_detects_live_instance() {
        let fake = Arc::new(Fake::default());
        let (addr, token) = start(fake.clone()).await;
        assert_eq!(
            request(addr, "GET", "/health", None).await,
            (503, "stopped".into())
        );
        fake.running.store(true, SeqCst);
        assert_eq!(
            request(addr, "GET", "/health", None).await,
            (200, "OK".into())
        );

        // Single-instance gate: the port is taken and answers HTTP.
        assert_eq!(
            bind(&addr.to_string()).unwrap_err().kind(),
            std::io::ErrorKind::AddrInUse
        );
        let a = addr.to_string();
        assert!(
            tokio::task::spawn_blocking(move || probe(&a))
                .await
                .unwrap()
        );
        token.cancel();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn probe_is_false_when_nothing_listens() {
        let addr = bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        assert!(
            !tokio::task::spawn_blocking(move || probe(&addr))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn status_and_update_check_are_open() {
        let fake = Arc::new(Fake::default());
        fake.running.store(true, SeqCst);
        let (addr, _token) = start(fake).await;
        let (code, body) = request(addr, "GET", "/status", None).await;
        assert_eq!(code, 200);
        let v = parse(&body);
        assert_eq!(v["ok"], true);
        assert_eq!(v["version"], "0.6.3");
        assert_eq!(v["pid"], std::process::id());
        assert_eq!(v["autostart_enabled"], true);
        assert_eq!(v["daemon_running"], true);
        assert_eq!(v["peer_count"], 2);

        let (code, body) = request(addr, "GET", "/update/check", None).await;
        assert_eq!(code, 200);
        assert_eq!(
            parse(&body),
            json!({ "current_version": "0.6.3", "update_available": true, "latest_version": "0.6.4" })
        );
    }

    #[tokio::test]
    async fn mutating_routes_require_the_admin_token() {
        let fake = Arc::new(Fake::default());
        let (addr, _token) = start(fake.clone()).await;
        let wrong = "b".repeat(64);
        for path in ["/daemon/restart", "/app/restart", "/app/update"] {
            for auth in [None, Some(wrong.as_str()), Some("short")] {
                let (code, body) = request(addr, "POST", path, auth).await;
                assert_eq!(code, 401, "{path} {auth:?}");
                assert_eq!(
                    parse(&body),
                    json!({ "ok": false, "error": "unauthorized" })
                );
            }
        }
        assert_eq!(fake.restarts.load(SeqCst), 0);
        assert_eq!(fake.app_restarts.load(SeqCst), 0);

        let (code, body) = request(addr, "POST", "/daemon/restart", Some(TOKEN)).await;
        assert_eq!(
            (code, parse(&body)),
            (200, json!({ "ok": true, "restarting": true }))
        );
        assert_eq!(fake.restarts.load(SeqCst), 1);
        let (code, _) = request(addr, "POST", "/app/restart", Some(TOKEN)).await;
        assert_eq!(code, 200);
        assert_eq!(fake.app_restarts.load(SeqCst), 1);
        let (code, body) = request(addr, "POST", "/app/update", Some(TOKEN)).await;
        assert_eq!(
            (code, parse(&body)),
            (
                200,
                json!({ "ok": true, "updating": false, "reason": "up to date" })
            )
        );
    }

    #[test]
    fn constant_time_eq_checks_length_and_bytes() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
