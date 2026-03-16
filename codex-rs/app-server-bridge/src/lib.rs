//! MCP server bridge to remote app-server.
//!
//! This crate provides a stdio MCP server that connects to a remote `codex app-server`
//! instance via WebSocket and translates between MCP protocol (for clients) and
//! app-server JSON-RPC protocol (for the remote server).

#![deny(clippy::print_stdout, clippy::print_stderr)]

mod bridge;
mod websocket_client;

pub use bridge::BridgeConfig;
pub use bridge::McpServer;
pub use bridge::run_bridge;
pub use websocket_client::WebSocketClient;

/// Size of the bounded channels used for async communication.
const CHANNEL_CAPACITY: usize = 128;
