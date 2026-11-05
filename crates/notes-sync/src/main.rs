//! `notes` CLI: MCP server (stdio / HTTP) and P2P vault sync.

use clap::Parser;
use notes::{Config, run_http_server, run_stdio_server};
use std::path::PathBuf;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "notes", about = "Notes MCP server and P2P vault sync CLI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand)]
enum Command {
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
enum McpAction {
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
enum SyncAction {
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
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

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
                memory_common::init_tracing(verbose, "sync_daemon");
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
                memory_common::init_tracing(false, "sync_daemon");
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
            memory_common::init_tracing(verbose, "notes");

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
