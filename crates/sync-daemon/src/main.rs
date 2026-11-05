//! sync-daemon: Headless P2P sync daemon for home server.
//!
//! Thin wrapper around `sync_daemon::daemon::run()` — all logic lives in the
//! library so the `notes` binary can embed it behind a feature flag.

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use sync_daemon::daemon::DaemonRunConfig;

#[derive(Parser, Debug)]
#[command(name = "sync-daemon")]
#[command(about = "P2P vault sync daemon")]
struct Args {
    /// Path to the vault directory
    #[arg(short, long)]
    vault: PathBuf,

    /// Address to serve the /health endpoint on (e.g. 127.0.0.1:8081)
    #[arg(long)]
    health_listen: Option<String>,

    /// Start an embedded iroh relay on this address (e.g. 0.0.0.0:3340)
    #[arg(long)]
    relay_listen: Option<String>,

    /// Path to an alternate ed25519 identity key file (default: .sync/daemon.key)
    #[arg(long)]
    identity_key: Option<PathBuf>,

    /// URL to advertise to peers for this node's embedded relay.
    ///
    /// Use this on machines that bind the relay on 0.0.0.0 but need to advertise
    /// a stable public hostname (e.g. `http://umbra.computer:3340/`). Peers dial
    /// this URL to reach the relay; without it they can only reach the relay via
    /// LAN mDNS discovery.
    #[arg(long)]
    advertised_relay_url: Option<String>,

    /// Enable verbose logging
    #[arg(long)]
    verbose: bool,
}

impl From<Args> for DaemonRunConfig {
    fn from(args: Args) -> Self {
        Self {
            vault: args.vault,
            identity_key: args.identity_key,
            health_listen: args.health_listen,
            relay_listen: args.relay_listen,
            advertised_relay_url: args.advertised_relay_url,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    memory_common::init_tracing(args.verbose, "sync_daemon");

    sync_daemon::daemon::run(args.into()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// `--advertised-relay-url` maps through to `DaemonRunConfig.advertised_relay_url`.
    #[test]
    fn advertised_relay_url_arg_wires_into_run_config() {
        let args = Args::parse_from([
            "sync-daemon",
            "--vault",
            "/tmp/vault",
            "--advertised-relay-url",
            "http://umbra.computer:3340/",
        ]);

        let config: DaemonRunConfig = args.into();
        assert_eq!(
            config.advertised_relay_url,
            Some("http://umbra.computer:3340/".to_string()),
        );
    }

    /// Without `--advertised-relay-url`, `DaemonRunConfig.advertised_relay_url` is `None`.
    #[test]
    fn advertised_relay_url_defaults_to_none() {
        let args = Args::parse_from(["sync-daemon", "--vault", "/tmp/vault"]);
        let config: DaemonRunConfig = args.into();
        assert_eq!(config.advertised_relay_url, None);
    }
}
