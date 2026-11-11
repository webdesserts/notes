//! `notes` CLI: MCP server (stdio / HTTP) and P2P vault sync.
//!
//! Shared by the standalone `notes` binary and `NotesSync.app`, which dispatches
//! here when launched through a path whose file stem is `notes` (the cask
//! `binary` symlink). Only `argv[0]` decides; `args[1..]` are never inspected.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::{Config, run_http_server, run_stdio_server};
use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "notes", version, about = "Notes MCP server and P2P vault sync CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(clap::Subcommand)]
pub enum Command {
    /// MCP server
    Mcp {
        #[command(subcommand)]
        action: McpAction,
    },

    /// Sync daemon
    Sync {
        #[command(subcommand)]
        action: SyncAction,
    },

    /// Start all services (sync + MCP HTTP)
    #[cfg(feature = "sync")]
    Up {
        /// Path to the vault directory
        #[arg(long)]
        vault: PathBuf,

        /// Address for MCP HTTP to listen on
        #[arg(long, default_value = "0.0.0.0:3000")]
        listen: String,

        /// Address for the sync daemon health endpoint (optional)
        #[arg(long)]
        health_listen: Option<String>,

        /// Enable verbose logging
        #[arg(long)]
        verbose: bool,
    },
}

#[derive(clap::Subcommand)]
pub enum McpAction {
    /// Start MCP stdio transport
    Io {
        /// Path to the vault directory
        #[arg(long)]
        vault: PathBuf,
    },

    /// Start MCP HTTP server
    Up {
        /// Path to the vault directory
        #[arg(long)]
        vault: PathBuf,

        /// Address to listen on
        #[arg(long, default_value = "127.0.0.1:3000")]
        listen: String,
    },
}

#[derive(clap::Subcommand)]
pub enum SyncAction {
    /// Start sync daemon
    #[cfg(feature = "sync")]
    Up {
        /// Path to the vault directory
        #[arg(long)]
        vault: PathBuf,

        /// Address for the health endpoint (optional, e.g. 127.0.0.1:8081)
        #[arg(long)]
        health_listen: Option<String>,

        /// Path to an alternate ed25519 identity key file (default: .sync/daemon.key)
        #[arg(long)]
        identity_key: Option<std::path::PathBuf>,

        /// Start an embedded iroh relay on this address (e.g. 0.0.0.0:3340)
        #[arg(long)]
        relay_listen: Option<String>,

        /// Enable verbose logging
        #[arg(long)]
        verbose: bool,
    },

    /// Pair this device with a sync mesh on the local network
    #[cfg(feature = "sync")]
    Pair {
        /// Path to the vault directory
        #[arg(long)]
        vault: PathBuf,

        /// Device name to advertise during pairing (default: system hostname)
        #[arg(long)]
        device_name: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Entrypoints
// ---------------------------------------------------------------------------

/// `true` solely when `argv[0]`'s file stem is `notes`.
pub fn should_dispatch_cli(args: &[OsString]) -> bool {
    args.first()
        .and_then(|a| Path::new(a).file_stem())
        .is_some_and(|stem| stem == "notes")
}

/// Parse `args` (including `argv[0]`) and run the CLI on a fresh tokio runtime.
pub fn run_from_os_args(args: Vec<OsString>) -> anyhow::Result<()> {
    let cli = Cli::parse_from(args);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run(cli)).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Run a parsed CLI invocation.
pub async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {

    match cli.command {
        // `notes` with no args → help
        None => {
            use clap::CommandFactory;
            Cli::command().print_help()?;
            println!();
            Ok(())
        }

        Some(Command::Mcp { action }) => match action {
            McpAction::Io { vault } => {
                let vault = vault.to_string_lossy().to_string();
                init_mcp_tracing();
                run_stdio_server(Config::new(&vault)).await
            }

            McpAction::Up { vault, listen } => {
                let vault = vault.to_string_lossy().to_string();
                init_mcp_tracing();
                run_http_server(Config::new(&vault), &listen).await
            }
        },

        #[cfg(feature = "sync")]
        Some(Command::Sync { action }) => match action {
            SyncAction::Up {
                vault,
                health_listen,
                identity_key,
                relay_listen,
                verbose,
            } => {
                notes_core::runtime::init_tracing(verbose, "sync_daemon");
                sync_daemon::daemon::run(sync_daemon::daemon::DaemonRunConfig {
                    vault,
                    identity_key,
                    health_listen,
                    relay_listen,
                    advertised_relay_url: None,
                })
                .await?;
                Ok(())
            }

            SyncAction::Pair { vault, device_name } => {
                // Initialize tracing so allowlist/adoption warnings surface. The
                // pairing helpers log via `tracing`, and the CLI runs without the
                // daemon's subscriber, so without this those warnings vanish.
                notes_core::runtime::init_tracing(false, "sync_daemon");
                sync_daemon::pair::run(vault, device_name).await?;
                Ok(())
            }
        },

        #[cfg(not(feature = "sync"))]
        Some(Command::Sync { .. }) => {
            eprintln!("Sync daemon is not available in this build.");
            eprintln!("Rebuild with: cargo build --features daemon");
            std::process::exit(1);
        }

        #[cfg(feature = "sync")]
        Some(Command::Up {
            vault,
            listen,
            health_listen,
            verbose,
        }) => {
            notes_core::runtime::init_tracing(verbose, "notes");

            let vault_str = vault.to_string_lossy().to_string();

            // Spawn sync daemon as background task
            let sync_config = sync_daemon::daemon::DaemonRunConfig {
                vault,
                identity_key: None,
                health_listen,
                relay_listen: None,
                advertised_relay_url: None,
            };
            let sync_handle = tokio::spawn(async move {
                if let Err(e) = sync_daemon::daemon::run(sync_config).await {
                    tracing::error!("Sync daemon error: {}", e);
                }
            });

            // Run MCP HTTP server on main task
            let mcp_result = run_http_server(Config::new(&vault_str), &listen).await;

            // On shutdown, abort the sync daemon
            sync_handle.abort();
            mcp_result
        }
    }
}

/// Initialize tracing for MCP server (logs to stderr, respects RUST_LOG).
fn init_mcp_tracing() {
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<OsString> {
        v.iter().map(OsString::from).collect()
    }

    #[test]
    fn dispatches_only_on_argv0_stem() {
        assert!(should_dispatch_cli(&args(&["notes"])));
        assert!(should_dispatch_cli(&args(&["/opt/homebrew/bin/notes", "mcp"])));
        assert!(!should_dispatch_cli(&args(&[
            "/Applications/NotesSync.app/Contents/MacOS/desktop",
            "notes"
        ])));
        assert!(!should_dispatch_cli(&args(&["notes-sync"])));
        assert!(!should_dispatch_cli(&[]));
    }

    #[test]
    fn cli_parses_version_and_subcommands() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
        assert!(Cli::try_parse_from(["notes", "--version"]).is_err());
        assert!(Cli::try_parse_from(["notes", "mcp", "io", "--vault", "/tmp/v"]).is_ok());
    }
}
