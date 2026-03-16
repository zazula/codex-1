//! Binary entry point for the app-server bridge.
//!
//! This is a stdio MCP server that bridges to a remote codex app-server via WebSocket.

use anyhow::Result;
use clap::Parser;
use codex_app_server_bridge::BridgeConfig;

/// Stdio MCP server that bridges to a remote codex app-server.
#[derive(Parser)]
#[command(author = "Codex", version, about = "Bridge MCP to remote app-server", long_about = None)]
struct Cli {
    /// WebSocket URL of the remote app-server.
    #[arg(long, default_value = "ws://127.0.0.1:4222")]
    url: String,

    /// Client name to report to the app-server.
    #[arg(long, default_value = "codex-app-server-bridge")]
    client_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging
    init_tracing();

    let cli = Cli::parse();

    let config = BridgeConfig {
        app_server_url: cli.url,
        client_name: cli.client_name,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    // Run the bridge
    codex_app_server_bridge::run_bridge(config).await?;

    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
