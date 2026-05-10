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

    /// Environment variable name containing websocket URL for the remote app-server.
    ///
    /// If both this and --url are provided, --url takes precedence.
    #[arg(long)]
    url_env: Option<String>,

    /// Client name to report to the app-server.
    #[arg(long, default_value = "codex-app-server-bridge")]
    client_name: String,

    /// Bearer token value for websocket Authorization header.
    ///
    /// Sends `Authorization: Bearer <token>` during websocket handshake.
    #[arg(long)]
    auth_token: Option<String>,

    /// Environment variable name containing bearer token for websocket auth.
    ///
    /// If both this and --auth-token are provided, --auth-token takes precedence.
    #[arg(long)]
    auth_token_env: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging
    init_tracing();

    let cli = Cli::parse();
    let app_server_url = resolve_url(&cli);
    let auth_bearer_token = resolve_auth_token(&cli);

    let config = BridgeConfig {
        app_server_url,
        client_name: cli.client_name,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        auth_bearer_token,
    };

    // Run the bridge
    codex_app_server_bridge::run_bridge(config).await?;

    Ok(())
}

fn resolve_url(cli: &Cli) -> String {
    // clap always provides a value for --url because of default_value.
    // Treat the default as lower-priority than --url-env, while preserving
    // explicit --url precedence.
    let default_url = "ws://127.0.0.1:4222";
    let cli_url = cli.url.as_str();

    if cli_url != default_url {
        return cli.url.clone();
    }

    if let Some(env_name) = cli.url_env.as_deref() {
        if let Ok(url) = std::env::var(env_name)
            && !url.is_empty()
        {
            return url;
        }
    }

    cli.url.clone()
}

fn resolve_auth_token(cli: &Cli) -> Option<String> {
    if let Some(token) = cli.auth_token.clone() {
        return Some(token);
    }
    let env_name = cli.auth_token_env.as_deref()?;
    std::env::var(env_name).ok().filter(|value| !value.is_empty())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
