//! relay_probe — diagnostic: can two relay-only iroh nodes connect *through a given relay*?
//!
//! Spins up two fresh iroh endpoints, both with **no IP transports**
//! (`clear_ip_transports`) and both using ONLY the given relay URL as their
//! `RelayMap`. The "listener" accepts a one-shot echo ALPN; the "dialer" connects
//! to it purely over the relay and round-trips a few bytes. Because neither endpoint
//! has any direct/loopback path, a successful round-trip proves the relay actually
//! *relayed* application traffic between two registered clients.
//!
//! This isolates the relay's relaying ability from all gossip/allowlist/discovery
//! machinery. Point it at the embedded relay's local bind to test the server alone,
//! or at the public URL to test the full Caddy + NAT-hairpin path:
//!
//!     cargo run -p p2p-core --example relay_probe -- http://localhost:3340/
//!     cargo run -p p2p-core --example relay_probe -- https://umbra.computer/
//!
//! Set `RUST_LOG=iroh=debug,iroh_relay=debug` to watch the relay client handshake.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMap, RelayMode, RelayUrl};

const ALPN: &[u8] = b"relay-probe/1";
const STEP_TIMEOUT: Duration = Duration::from_secs(25);

/// Build a relay-only endpoint: no IP transports, a single custom relay. With every
/// direct path stripped, the relay is the *only* way these nodes can reach each other,
/// so a successful connection is unambiguous proof the relay carried the traffic.
async fn build_relay_only(relay: &RelayUrl, alpns: Vec<Vec<u8>>) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(RelayMap::from_iter([relay.clone()])))
        .clear_ip_transports()
        // Trust the OS/system CA store, matching the node's production endpoint —
        // keeps this diagnostic tool working through the same corporate TLS-
        // intercepting proxies it's meant to diagnose against (e.g. umbra.computer).
        .ca_tls_config(iroh::tls::CaTlsConfig::system());
    if !alpns.is_empty() {
        builder = builder.alpns(alpns);
    }
    builder
        .bind()
        .await
        .map_err(|err| anyhow!("{err}"))
        .context("bind relay-only endpoint")
}

/// Like [`build_relay_only`] but KEEPS IP transports — to test whether a node WITH direct
/// paths still registers with its home relay (umbra's daemon topology).
async fn build_with_ip(relay: &RelayUrl, alpns: Vec<Vec<u8>>) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(RelayMap::from_iter([relay.clone()])))
        // See build_relay_only: trust the OS/system CA store, matching production.
        .ca_tls_config(iroh::tls::CaTlsConfig::system());
    if !alpns.is_empty() {
        builder = builder.alpns(alpns);
    }
    builder
        .bind()
        .await
        .map_err(|err| anyhow!("{err}"))
        .context("bind endpoint (ip transports kept)")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let relay_url: RelayUrl = std::env::args()
        .nth(1)
        .context("usage: relay_probe <relay-url>  (e.g. https://umbra.computer/ or http://localhost:3340/)")?
        .parse()
        .map_err(|err| anyhow!("{err}"))
        .context("parse relay url")?;

    println!("== relay_probe ==");
    println!("relay         = {relay_url}");

    // ---- reach mode: connect to an EXISTING node by id, via the relay ----
    // Usage: relay_probe <relay-url> reach <endpoint-id-hex> [<alpn>]
    // Proves whether a specific node (e.g. umbra's live daemon) is reachable through the
    // relay *right now* — i.e. whether it currently holds a registered client connection to
    // it. A relay-only prober has no direct path, so a successful connect can only mean the
    // relay forwarded to a registered target.
    let argv: Vec<String> = std::env::args().collect();
    if let Some(pos) = argv.iter().position(|a| a == "reach") {
        let target_hex = argv
            .get(pos + 1)
            .context("reach mode needs <endpoint-id-hex>")?;
        let alpn = argv
            .get(pos + 2)
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_else(|| b"obsidian-memory/sync/1".to_vec());
        let target: EndpointId = target_hex
            .parse()
            .map_err(|err| anyhow!("{err}"))
            .context("parse target endpoint id")?;
        println!("mode          = REACH");
        println!("target        = {target}");
        println!("alpn          = {}", String::from_utf8_lossy(&alpn));

        let prober = build_relay_only(&relay_url, vec![]).await?;
        println!("prober id     = {}", prober.id());
        let _ = tokio::time::timeout(STEP_TIMEOUT, prober.online()).await;

        let addr = EndpointAddr::new(target).with_relay_url(relay_url.clone());
        let t = Instant::now();
        match tokio::time::timeout(STEP_TIMEOUT, prober.connect(addr, alpn.as_slice())).await {
            Ok(Ok(conn)) => {
                println!(
                    "\nRESULT: REACHABLE — QUIC connection to {} ESTABLISHED via the relay in {:.1?}.\n  \
                     The target node IS a registered client of this relay (off-LAN peers CAN reach it).",
                    conn.remote_id(),
                    t.elapsed()
                );
                conn.close(0u32.into(), b"probe done");
            }
            Ok(Err(err)) => println!(
                "\nRESULT: REACHED-BUT-ERRORED after {:.1?}: {err:#}\n  \
                 The target RESPONDED through the relay (so it IS reachable/registered), but the\n  \
                 connection then errored (likely ALPN/app-level). Reachability is PROVEN.",
                t.elapsed()
            ),
            Err(_) => println!(
                "\nRESULT: UNREACHABLE — connect TIMED OUT after {STEP_TIMEOUT:?}.\n  \
                 The target did not respond through the relay → it is NOT a registered client of\n  \
                 this relay → off-LAN peers cannot reach it. (This is the suspected bug.)"
            ),
        }
        prober.close().await;
        return Ok(());
    }

    // ---- listener (accept side) ----
    // Pass `keep-ip` to keep the listener's IP transports — tests whether a node WITH direct
    // paths still registers with its home relay (umbra's daemon topology) vs a relay-only node.
    let keep_ip = std::env::args().any(|a| a == "keep-ip");
    let listener = if keep_ip {
        build_with_ip(&relay_url, vec![ALPN.to_vec()]).await?
    } else {
        build_relay_only(&relay_url, vec![ALPN.to_vec()]).await?
    };
    println!(
        "listener id   = {}  (ip transports: {})",
        listener.id(),
        if keep_ip { "KEPT" } else { "relay-only" }
    );

    let t_online = Instant::now();
    if tokio::time::timeout(STEP_TIMEOUT, listener.online())
        .await
        .is_err()
    {
        println!(
            "\nRESULT: FAIL (listener) — never came ONLINE within {STEP_TIMEOUT:?}.\n  \
             The relay did not register the listener as a client (relay handshake never\n  \
             completed over this URL). The relay is unusable as a home relay here."
        );
        return Ok(());
    }
    println!(
        "listener      = ONLINE (registered w/ relay) in {:.1?}",
        t_online.elapsed()
    );

    let listener_addr = listener.addr();
    println!("listener addr = {listener_addr:?}");

    // Accept loop: read the request bytes, echo them straight back. Only inherent
    // iroh stream methods are used (no tokio io-util needed).
    let _accept = tokio::spawn({
        let listener = listener.clone();
        async move {
            while let Some(incoming) = listener.accept().await {
                tokio::spawn(async move {
                    let conn = match incoming.await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("listener: incoming failed: {e}");
                            return;
                        }
                    };
                    if let Ok((mut send, mut recv)) = conn.accept_bi().await
                        && let Ok(data) = recv.read_to_end(1024).await
                    {
                        let _ = send.write_all(&data).await;
                        let _ = send.finish();
                    }
                    conn.closed().await;
                });
            }
        }
    });

    // ---- dialer (connect side) ----
    let dialer = build_relay_only(&relay_url, vec![]).await?;
    println!("dialer id     = {}", dialer.id());
    let _ = tokio::time::timeout(STEP_TIMEOUT, dialer.online()).await;

    let t_dial = Instant::now();
    let dial = async {
        let conn = dialer
            .connect(listener_addr.clone(), ALPN)
            .await
            .map_err(|err| anyhow!("{err}"))
            .context("connect via relay")?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|err| anyhow!("{err}"))
            .context("open_bi")?;
        send.write_all(b"relay-probe-ping")
            .await
            .map_err(|err| anyhow!("{err}"))
            .context("write")?;
        send.finish()
            .map_err(|err| anyhow!("{err}"))
            .context("finish")?;
        let echoed = recv
            .read_to_end(1024)
            .await
            .map_err(|err| anyhow!("{err}"))
            .context("read echo")?;
        Ok::<_, anyhow::Error>(echoed)
    };

    match tokio::time::timeout(STEP_TIMEOUT, dial).await {
        Ok(Ok(bytes)) => println!(
            "\nRESULT: SUCCESS — relayed an echo round-trip ({} bytes) in {:.1?}.\n  \
             The relay CAN relay application traffic between two registered clients over this URL.",
            bytes.len(),
            t_dial.elapsed()
        ),
        Ok(Err(err)) => println!(
            "\nRESULT: FAIL — connect/echo errored after {:.1?}:\n  {err:#}",
            t_dial.elapsed()
        ),
        Err(_) => println!(
            "\nRESULT: FAIL — connect/echo TIMED OUT after {STEP_TIMEOUT:?}.\n  \
             Both endpoints came ONLINE (registered) but the relay could not carry traffic\n  \
             between them — forwarding or post-handshake streaming is broken over this URL."
        ),
    }

    listener.close().await;
    dialer.close().await;
    Ok(())
}
