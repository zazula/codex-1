//! Bridge between MCP protocol (stdio) and app-server protocol (WebSocket).
//!
//! This module implements an MCP server that forwards tool calls to a remote
//! app-server via WebSocket and streams responses back.
//!
//! Exposed tools provide comprehensive coverage of the app-server v2 API:
//! - Thread management: thread_start, thread_read, thread_list, thread_resume
//! - Turn execution: turn_start
//! - Configuration: config_list, config_read, config_write
//! - Models: model_list
//! - Review: review_start
//! - Skills: skills_list

use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::v2::*;
use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::Content;
use rmcp::model::ErrorCode;
use rmcp::model::ErrorData;
use rmcp::model::Implementation;
use rmcp::model::InitializeResult;
use rmcp::model::JsonRpcError;
use rmcp::model::JsonRpcNotification;
use rmcp::model::JsonRpcRequest;
use rmcp::model::JsonRpcResponse;
use rmcp::model::JsonRpcVersion2_0;
use rmcp::model::ListToolsResult;
use rmcp::model::RequestId as McpRequestId;
use rmcp::model::ServerCapabilities;
use rmcp::model::Tool;
use rmcp::model::ToolsCapability;
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::{self};
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::websocket_client::WebSocketClient;

/// Helper to create a JsonObject from json! macro output.
fn make_input_schema(value: serde_json::Value) -> Arc<serde_json::Map<String, serde_json::Value>> {
    match value {
        serde_json::Value::Object(map) => Arc::new(map),
        _ => Arc::new(serde_json::Map::new()),
    }
}

/// Configuration for the app-server bridge.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// WebSocket URL of the remote app-server.
    pub app_server_url: String,
    /// Client name to report to the app-server.
    pub client_name: String,
    /// Client version to report to the app-server.
    pub client_version: String,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            app_server_url: "ws://127.0.0.1:4222".to_string(),
            client_name: "codex-app-server-bridge".to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Run the MCP server bridge.
///
/// This function:
/// 1. Connects to the remote app-server via WebSocket
/// 2. Performs the app-server handshake
/// 3. Listens for MCP messages on stdin
/// 4. Forwards MCP tool calls to the app-server
/// 5. Writes MCP responses to stdout
pub async fn run_bridge(config: BridgeConfig) -> Result<()> {
    // Connect to app-server
    let mut client = WebSocketClient::connect(&config.app_server_url).await?;
    info!("Connected to app-server at {}", config.app_server_url);

    // Perform app-server handshake
    initialize_app_server(&mut client, &config).await?;

    // Set up channels
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<String>();
    let (incoming_tx, mut incoming_rx) = mpsc::channel::<String>(super::CHANNEL_CAPACITY);

    // Clone sender for use in the processor
    let outgoing_tx_clone = outgoing_tx.clone();

    // Spawn stdin reader task
    let stdin_reader = tokio::spawn(async move {
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await.unwrap_or_default() {
            if incoming_tx.send(line).await.is_err() {
                break;
            }
        }
        debug!("stdin reader finished");
    });

    // Spawn stdout writer task
    let stdout_writer = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(msg) = outgoing_rx.recv().await {
            if let Err(e) = stdout.write_all(msg.as_bytes()).await {
                error!("Failed to write to stdout: {e}");
                break;
            }
            if let Err(e) = stdout.write_all(b"\n").await {
                error!("Failed to write newline to stdout: {e}");
                break;
            }
        }
        info!("stdout writer finished");
    });

    // Process incoming MCP messages
    let processor = tokio::spawn(async move {
        let mut server = McpServer::new(outgoing_tx_clone, client);

        while let Some(line) = incoming_rx.recv().await {
            if line.trim().is_empty() {
                continue;
            }

            // Parse the incoming MCP message
            let message: serde_json::Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(e) => {
                    error!("Failed to parse MCP message: {e}");
                    continue;
                }
            };

            // Determine if it's a request or notification
            if message.get("id").is_some() && message.get("method").is_some() {
                // It's a request
                match serde_json::from_str::<JsonRpcRequest<rmcp::model::ClientRequest>>(&line) {
                    Ok(request) => {
                        if let Err(e) = server.handle_mcp_request(request).await {
                            error!("Error handling MCP request: {e}");
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize MCP request: {e}");
                    }
                }
            } else if message.get("method").is_some() {
                // It's a notification
                match serde_json::from_str::<JsonRpcNotification<rmcp::model::ClientNotification>>(
                    &line,
                ) {
                    Ok(notification) => {
                        server.handle_mcp_notification(notification);
                    }
                    Err(e) => {
                        error!("Failed to deserialize MCP notification: {e}");
                    }
                }
            } else if message.get("id").is_some() && message.get("result").is_some() {
                // It's a response (shouldn't happen for server)
                warn!("Received unexpected MCP response");
            } else {
                warn!("Received unknown MCP message format");
            }
        }

        info!("MCP processor finished");
    });

    // Wait for stdin to close
    stdin_reader.await?;
    processor.abort();
    stdout_writer.abort();

    Ok(())
}

/// Initialize app-server connection.
async fn initialize_app_server(client: &mut WebSocketClient, config: &BridgeConfig) -> Result<()> {
    let request_id = client.next_request_id();
    let request = JSONRPCRequest {
        id: request_id.clone(),
        method: "initialize".to_string(),
        params: Some(serde_json::to_value(InitializeParams {
            client_info: ClientInfo {
                name: config.client_name.clone(),
                title: Some("Codex App Server Bridge".to_string()),
                version: config.client_version.clone(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                opt_out_notification_methods: None,
            }),
        })?),
        trace: None,
    };

    let _: serde_json::Value = client
        .send_request(&request)
        .await
        .context("app-server initialize failed")?;

    // Send initialized notification
    let notification = JSONRPCNotification {
        method: "initialized".to_string(),
        params: None,
    };
    client.send_notification(&notification).await?;

    info!("App-server handshake complete");
    Ok(())
}

/// MCP server implementation that bridges to app-server.
pub struct McpServer {
    /// Channel to send outgoing MCP messages.
    outgoing_tx: mpsc::UnboundedSender<String>,
    /// WebSocket client.
    client: WebSocketClient,
    /// Whether MCP handshake is complete.
    mcp_initialized: bool,
}

impl McpServer {
    /// Create a new MCP server.
    pub fn new(outgoing_tx: mpsc::UnboundedSender<String>, client: WebSocketClient) -> Self {
        Self {
            outgoing_tx,
            client,
            mcp_initialized: false,
        }
    }

    /// Handle an incoming MCP request.
    pub async fn handle_mcp_request(
        &mut self,
        request: JsonRpcRequest<rmcp::model::ClientRequest>,
    ) -> anyhow::Result<()> {
        let request_id = request.id.clone();

        match request.request {
            rmcp::model::ClientRequest::InitializeRequest(params) => {
                self.handle_initialize(request_id, params.params).await?;
            }
            rmcp::model::ClientRequest::ListToolsRequest(params) => {
                self.handle_list_tools(request_id, params.params).await?;
            }
            rmcp::model::ClientRequest::CallToolRequest(params) => {
                self.handle_call_tool(request_id, params.params).await?;
            }
            rmcp::model::ClientRequest::PingRequest(_) => {
                self.send_mcp_response(request_id, json!({})).await?;
            }
            other => {
                self.send_mcp_error(
                    request_id,
                    ErrorCode::METHOD_NOT_FOUND,
                    format!("method not found: {other:?}"),
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Handle MCP initialize request.
    async fn handle_initialize(
        &mut self,
        request_id: McpRequestId,
        params: rmcp::model::InitializeRequestParams,
    ) -> anyhow::Result<()> {
        info!("MCP initialize: {:?}", params);

        if self.mcp_initialized {
            self.send_mcp_error(
                request_id,
                ErrorCode::INVALID_REQUEST,
                "already initialized".to_string(),
            )
            .await?;
            return Ok(());
        }

        let server_info = Implementation {
            name: "codex-app-server-bridge".to_string(),
            title: Some("Codex App Server Bridge".to_string()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: Some("Bridge MCP protocol to remote codex app-server".to_string()),
            icons: None,
            website_url: None,
        };

        let result = InitializeResult {
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability {
                    list_changed: Some(false),
                }),
                ..Default::default()
            },
            instructions: Some("Use this MCP server to interact with a remote Codex instance via the app-server protocol.".to_string()),
            protocol_version: params.protocol_version,
            server_info,
        };

        self.send_mcp_response(request_id, serde_json::to_value(result)?)
            .await?;
        self.mcp_initialized = true;

        Ok(())
    }

    /// Handle MCP tools/list request.
    async fn handle_list_tools(
        &mut self,
        request_id: McpRequestId,
        _params: Option<rmcp::model::PaginatedRequestParams>,
    ) -> anyhow::Result<()> {
        let tools = vec![
            // === Thread Management ===
            make_thread_start_tool(),
            make_thread_read_tool(),
            make_thread_list_tool(),
            make_thread_resume_tool(),
            make_thread_fork_tool(),
            make_thread_archive_tool(),
            make_thread_unarchive_tool(),
            make_thread_unsubscribe_tool(),
            make_thread_set_name_tool(),
            make_thread_compact_start_tool(),
            make_thread_background_terminals_clean_tool(),
            make_thread_rollback_tool(),
            make_thread_loaded_list_tool(),
            // === Turn Execution ===
            make_turn_start_tool(),
            make_turn_steer_tool(),
            make_turn_interrupt_tool(),
            // === Configuration ===
            make_config_list_tool(),
            make_config_read_tool(),
            make_config_write_tool(),
            // === Models ===
            make_model_list_tool(),
            // === Review ===
            make_review_start_tool(),
            // === Skills ===
            make_skills_list_tool(),
            make_skills_remote_read_tool(),
            make_skills_remote_write_tool(),
            make_skills_config_write_tool(),
            // === Apps ===
            make_apps_list_tool(),
            // === Account ===
            make_account_get_tool(),
            make_account_rate_limits_tool(),
            // === Experimental ===
            make_experimental_feature_list_tool(),
            // === Collaboration ===
            make_collaboration_mode_list_tool(),
            // === MCP Server ===
            make_mcp_server_status_tool(),
            // === Feedback ===
            make_feedback_upload_tool(),
            // === Command ===
            make_command_exec_tool(),
            // === Simple/Legacy tools for backward compatibility ===
            make_thread_start_simple_tool(),
            make_turn_start_simple_tool(),
        ];

        self.send_mcp_response(
            request_id,
            serde_json::to_value(ListToolsResult {
                tools,
                next_cursor: None,
                meta: None,
            })?,
        )
        .await?;

        Ok(())
    }

    /// Handle MCP tools/call request.
    async fn handle_call_tool(
        &mut self,
        request_id: McpRequestId,
        params: CallToolRequestParams,
    ) -> anyhow::Result<()> {
        let tool_name = params.name.clone();
        let arguments = params.arguments.unwrap_or_default();

        info!("Calling tool: {} with args: {:?}", tool_name, arguments);

        let result: CallToolResult = match tool_name.as_ref() {
            // === Thread Management ===
            "thread_start" => self.call_thread_start(arguments).await?,
            "thread_start_simple" => self.call_thread_start_simple(arguments).await?,
            "thread_read" => self.call_thread_read(arguments).await?,
            "thread_list" => self.call_thread_list(arguments).await?,
            "thread_resume" => self.call_thread_resume(arguments).await?,
            "thread_fork" => self.call_thread_fork(arguments).await?,
            "thread_archive" => self.call_thread_archive(arguments).await?,
            "thread_unarchive" => self.call_thread_unarchive(arguments).await?,
            "thread_unsubscribe" => self.call_thread_unsubscribe(arguments).await?,
            "thread_set_name" => self.call_thread_set_name(arguments).await?,
            "thread_compact_start" => self.call_thread_compact_start(arguments).await?,
            "thread_background_terminals_clean" => {
                self.call_thread_background_terminals_clean(arguments)
                    .await?
            }
            "thread_rollback" => self.call_thread_rollback(arguments).await?,
            "thread_loaded_list" => self.call_thread_loaded_list(arguments).await?,
            // === Turn Execution ===
            "turn_start" => self.call_turn_start(arguments).await?,
            "turn_start_simple" => self.call_turn_start_simple(arguments).await?,
            "turn_steer" => self.call_turn_steer(arguments).await?,
            "turn_interrupt" => self.call_turn_interrupt(arguments).await?,
            // === Configuration ===
            "config_list" => self.call_config_list(arguments).await?,
            "config_read" => self.call_config_read(arguments).await?,
            "config_write" => self.call_config_write(arguments).await?,
            // === Models ===
            "model_list" => self.call_model_list(arguments).await?,
            // === Review ===
            "review_start" => self.call_review_start(arguments).await?,
            // === Skills ===
            "skills_list" => self.call_skills_list(arguments).await?,
            "skills_remote_read" => self.call_skills_remote_read(arguments).await?,
            "skills_remote_write" => self.call_skills_remote_write(arguments).await?,
            "skills_config_write" => self.call_skills_config_write(arguments).await?,
            // === Apps ===
            "apps_list" => self.call_apps_list(arguments).await?,
            // === Account ===
            "account_get" => self.call_account_get(arguments).await?,
            "account_rate_limits" => self.call_account_rate_limits(arguments).await?,
            // === Experimental ===
            "experimental_feature_list" => self.call_experimental_feature_list(arguments).await?,
            // === Collaboration ===
            "collaboration_mode_list" => self.call_collaboration_mode_list(arguments).await?,
            // === MCP Server ===
            "mcp_server_status" => self.call_mcp_server_status(arguments).await?,
            // === Feedback ===
            "feedback_upload" => self.call_feedback_upload(arguments).await?,
            // === Command ===
            "command_exec" => self.call_command_exec(arguments).await?,
            _ => {
                self.send_mcp_error(
                    request_id.clone(),
                    ErrorCode::METHOD_NOT_FOUND,
                    format!("unknown tool: {tool_name}"),
                )
                .await?;
                return Ok(());
            }
        };

        self.send_mcp_response(request_id, serde_json::to_value(result)?)
            .await?;
        Ok(())
    }

    // ============================================================
    // Thread Tools
    // ============================================================

    /// Call thread/start with full parameters.
    async fn call_thread_start(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_start_params(args);
        self.send_thread_start(params).await
    }

    /// Simple thread start (backward compatible).
    async fn call_thread_start_simple(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = ThreadStartParams {
            cwd: args.get("cwd").and_then(|v| v.as_str()).map(String::from),
            model: args.get("model").and_then(|v| v.as_str()).map(String::from),
            ..Default::default()
        };
        self.send_thread_start(params).await
    }

    /// Send thread/start request.
    async fn send_thread_start(
        &mut self,
        params: ThreadStartParams,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/start".to_string(),
            params: Some(serde_json::to_value(params)?),
            trace: None,
        };

        let response: ThreadStartResponse = self
            .client
            .send_request(&request)
            .await
            .context("thread/start request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/read.
    async fn call_thread_read(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let thread_id = args
            .get("threadId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing threadId"))?
            .to_string();
        let include_turns = args
            .get("includeTurns")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/read".to_string(),
            params: Some(json!({
                "threadId": thread_id,
                "includeTurns": include_turns
            })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/read request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/list with full parameters.
    async fn call_thread_list(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/list".to_string(),
            params: Some(parse_thread_list_params(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/resume with full parameters.
    async fn call_thread_resume(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/resume".to_string(),
            params: Some(parse_thread_resume_params(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/resume request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/fork.
    async fn call_thread_fork(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_fork_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/fork".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/fork request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/archive.
    async fn call_thread_archive(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_id_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/archive".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/archive request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/unarchive.
    async fn call_thread_unarchive(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_id_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/unarchive".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/unarchive request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/unsubscribe.
    async fn call_thread_unsubscribe(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_id_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/unsubscribe".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/unsubscribe request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/setName.
    async fn call_thread_set_name(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_set_name_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/setName".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/setName request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/compactStart.
    async fn call_thread_compact_start(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_id_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/compactStart".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/compactStart request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/backgroundTerminalsClean.
    async fn call_thread_background_terminals_clean(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_id_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/backgroundTerminalsClean".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/backgroundTerminalsClean request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/rollback.
    async fn call_thread_rollback(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_thread_rollback_params(args);
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/rollback".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/rollback request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call thread/loadedList.
    async fn call_thread_loaded_list(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "thread/loadedList".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("thread/loadedList request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Turn Tools
    // ============================================================

    /// Call turn/start with full parameters.
    async fn call_turn_start(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_turn_start_params(args)?;
        self.send_turn_start(params).await
    }

    /// Simple turn start (backward compatible).
    async fn call_turn_start_simple(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let thread_id = args
            .get("thread_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing thread_id"))?
            .to_string();
        let message = args
            .get("message")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing message"))?
            .to_string();

        let params = TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: message,
                text_elements: vec![],
            }],
            ..Default::default()
        };
        self.send_turn_start(params).await
    }

    /// Send turn/start request and stream response.
    async fn send_turn_start(&mut self, params: TurnStartParams) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id.clone(),
            method: "turn/start".to_string(),
            params: Some(serde_json::to_value(params)?),
            trace: None,
        };

        let response: TurnStartResponse = self
            .client
            .send_request(&request)
            .await
            .context("turn/start request failed")?;

        // Stream notifications until turn completes
        let turn_id = response.turn.id.clone();
        let mut output = format!("Turn started: {turn_id}\n");

        loop {
            let notification = self.client.recv_notification().await?;

            if let Ok(server_notif) = ServerNotification::try_from(notification) {
                match server_notif {
                    ServerNotification::TurnCompleted(payload) => {
                        if payload.turn.id == turn_id {
                            output.push_str(&format!(
                                "\nTurn completed with status: {:?}",
                                payload.turn.status
                            ));
                            break;
                        }
                    }
                    ServerNotification::AgentMessageDelta(delta) => {
                        output.push_str(&delta.delta);
                    }
                    ServerNotification::ItemCompleted(payload) => {
                        output.push_str(&format!("\n[Item completed: {:?}]", payload.item));
                    }
                    other => {
                        debug!("Ignoring notification: {:?}", other);
                    }
                }
            }
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Call turn/steer.
    async fn call_turn_steer(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_turn_steer_params(args)?;
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "turn/steer".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("turn/steer request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call turn/interrupt.
    async fn call_turn_interrupt(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let params = parse_turn_interrupt_params(args)?;
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "turn/interrupt".to_string(),
            params: Some(params),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("turn/interrupt request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Config Tools
    // ============================================================

    async fn call_config_list(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let cwd = args.get("cwd").and_then(|v| v.as_str());

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "config/list".to_string(),
            params: Some(json!({ "cwd": cwd })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("config/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    async fn call_config_read(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let cwd = args.get("cwd").and_then(|v| v.as_str());
        let layer = args.get("layer").and_then(|v| v.as_str());

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "config/read".to_string(),
            params: Some(json!({ "cwd": cwd, "layer": layer })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("config/read request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    async fn call_config_write(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let layer = args
            .get("layer")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing layer"))?;
        let cwd = args.get("cwd").and_then(|v| v.as_str());
        let config = args
            .get("config")
            .ok_or_else(|| anyhow::anyhow!("missing config"))?;

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "config/write".to_string(),
            params: Some(json!({
                "layer": layer,
                "cwd": cwd,
                "config": config
            })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("config/write request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Model Tools
    // ============================================================

    async fn call_model_list(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let model_provider = args.get("modelProvider").and_then(|v| v.as_str());

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "model/list".to_string(),
            params: Some(json!({ "modelProvider": model_provider })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("model/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Review Tools
    // ============================================================

    async fn call_review_start(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let thread_id = args
            .get("threadId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing threadId"))?;
        let target = args
            .get("target")
            .ok_or_else(|| anyhow::anyhow!("missing target"))?;
        let delivery = args.get("delivery").and_then(|v| v.as_str());

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "review/start".to_string(),
            params: Some(json!({
                "threadId": thread_id,
                "target": target,
                "delivery": delivery
            })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("review/start request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Skills Tools
    // ============================================================

    async fn call_skills_list(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let cwds = args
            .get("cwds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let force_reload = args
            .get("forceReload")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "skills/list".to_string(),
            params: Some(json!({
                "cwds": cwds,
                "forceReload": force_reload
            })),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("skills/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Apps Tools
    // ============================================================

    /// Call apps/list.
    async fn call_apps_list(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "apps/list".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("apps/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Account Tools
    // ============================================================

    /// Call account/get.
    async fn call_account_get(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "account/get".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("account/get request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call account/rateLimits.
    async fn call_account_rate_limits(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "account/rateLimits".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("account/rateLimits request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Experimental Feature Tools
    // ============================================================

    /// Call experimentalFeature/list.
    async fn call_experimental_feature_list(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "experimentalFeature/list".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("experimentalFeature/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Collaboration Mode Tools
    // ============================================================

    /// Call collaborationMode/list.
    async fn call_collaboration_mode_list(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "collaborationMode/list".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("collaborationMode/list request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // MCP Server Tools
    // ============================================================

    /// Call mcpServer/status.
    async fn call_mcp_server_status(
        &mut self,
        _args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "mcpServer/status".to_string(),
            params: Some(json!({})),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("mcpServer/status request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Feedback Tools
    // ============================================================

    /// Call feedback/upload.
    async fn call_feedback_upload(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "feedback/upload".to_string(),
            params: Some(json!(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("feedback/upload request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Skills Advanced Tools
    // ============================================================

    /// Call skills/remoteRead.
    async fn call_skills_remote_read(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "skills/remoteRead".to_string(),
            params: Some(json!(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("skills/remoteRead request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call skills/remoteWrite.
    async fn call_skills_remote_write(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "skills/remoteWrite".to_string(),
            params: Some(json!(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("skills/remoteWrite request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    /// Call skills/configWrite.
    async fn call_skills_config_write(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "skills/configWrite".to_string(),
            params: Some(json!(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("skills/configWrite request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // Command Tools
    // ============================================================

    /// Call command/exec.
    async fn call_command_exec(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<CallToolResult> {
        let request_id = self.client.next_request_id();
        let request = JSONRPCRequest {
            id: request_id,
            method: "command/exec".to_string(),
            params: Some(json!(args)),
            trace: None,
        };

        let response: serde_json::Value = self
            .client
            .send_request(&request)
            .await
            .context("command/exec request failed")?;

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&response)?,
        )]))
    }

    // ============================================================
    // MCP Response Helpers
    // ============================================================

    /// Send an MCP response.
    async fn send_mcp_response(
        &self,
        request_id: McpRequestId,
        result: serde_json::Value,
    ) -> anyhow::Result<()> {
        let response = JsonRpcResponse {
            jsonrpc: JsonRpcVersion2_0,
            id: request_id,
            result,
        };
        let json = serde_json::to_string(&response)?;
        self.outgoing_tx.send(json)?;
        Ok(())
    }

    /// Send an MCP error response.
    async fn send_mcp_error(
        &self,
        request_id: McpRequestId,
        code: ErrorCode,
        message: String,
    ) -> anyhow::Result<()> {
        let error = JsonRpcError {
            jsonrpc: JsonRpcVersion2_0,
            id: request_id,
            error: ErrorData::new(code, message, None),
        };
        let json = serde_json::to_string(&error)?;
        self.outgoing_tx.send(json)?;
        Ok(())
    }

    /// Handle an incoming MCP notification.
    pub fn handle_mcp_notification(
        &mut self,
        notification: JsonRpcNotification<rmcp::model::ClientNotification>,
    ) {
        match notification.notification {
            rmcp::model::ClientNotification::InitializedNotification(_) => {
                info!("MCP client initialized");
            }
            other => {
                debug!("Ignoring MCP notification: {:?}", other);
            }
        }
    }
}

// ============================================================
// Tool Schema Definitions
// ============================================================

fn make_thread_start_tool() -> Tool {
    Tool::new(
        "thread_start",
        "Start a new thread/conversation on the remote app-server",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "model": {
                    "type": "string",
                    "description": "Model to use for this thread"
                },
                "modelProvider": {
                    "type": "string",
                    "description": "Model provider (e.g., 'openai', 'anthropic')"
                },
                "serviceTier": {
                    "type": "string",
                    "enum": ["auto", "default", "flex", "priority"],
                    "description": "Service tier for API usage"
                },
                "cwd": {
                    "type": "string",
                    "description": "Working directory for the thread"
                },
                "approvalPolicy": {
                    "type": "string",
                    "enum": ["untrusted", "on-failure", "on-request", "never"],
                    "description": "Approval policy for tool calls"
                },
                "sandbox": {
                    "type": "string",
                    "enum": ["read-only", "workspace-write", "danger-full-access"],
                    "description": "Sandbox mode for file system access"
                },
                "config": {
                    "type": "object",
                    "description": "Configuration overrides as key-value pairs"
                },
                "serviceName": {
                    "type": "string",
                    "description": "Service name for identification"
                },
                "baseInstructions": {
                    "type": "string",
                    "description": "Base system instructions for the agent"
                },
                "developerInstructions": {
                    "type": "string",
                    "description": "Developer-provided instructions"
                },
                "personality": {
                    "type": "string",
                    "enum": ["default", "concise"],
                    "description": "Agent personality style"
                },
                "ephemeral": {
                    "type": "boolean",
                    "description": "If true, thread will not be persisted"
                },
                "dynamicTools": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "description": {"type": "string"},
                            "inputSchema": {"type": "object"}
                        },
                        "required": ["name", "description", "inputSchema"]
                    },
                    "description": "Dynamic tool definitions to inject"
                },
                "persistExtendedHistory": {
                    "type": "boolean",
                    "description": "Persist additional history for richer thread reconstruction"
                }
            }
        })),
    )
}

fn make_thread_read_tool() -> Tool {
    Tool::new(
        "thread_read",
        "Read a thread's details and optionally its turns",
        make_input_schema(json!({
            "type": "object",
            "required": ["threadId"],
            "properties": {
                "threadId": {
                    "type": "string",
                    "description": "Thread ID to read"
                },
                "includeTurns": {
                    "type": "boolean",
                    "description": "Include turn history in response"
                }
            }
        })),
    )
}

fn make_thread_list_tool() -> Tool {
    Tool::new(
        "thread_list",
        "List threads with filtering and pagination",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "cursor": {
                    "type": "string",
                    "description": "Pagination cursor from previous response"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of threads to return"
                },
                "sortKey": {
                    "type": "string",
                    "enum": ["createdAt", "updatedAt"],
                    "description": "Sort key for ordering results"
                },
                "modelProviders": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by model providers"
                },
                "sourceKinds": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["cli", "app", "api", "cloud"]},
                    "description": "Filter by thread source kinds"
                },
                "archived": {
                    "type": "boolean",
                    "description": "Filter by archived status"
                },
                "cwd": {
                    "type": "string",
                    "description": "Filter by exact working directory"
                },
                "searchTerm": {
                    "type": "string",
                    "description": "Filter by title substring match"
                }
            }
        })),
    )
}

fn make_thread_resume_tool() -> Tool {
    Tool::new(
        "thread_resume",
        "Resume an existing thread with optional configuration overrides",
        make_input_schema(json!({
            "type": "object",
            "required": ["threadId"],
            "properties": {
                "threadId": {
                    "type": "string",
                    "description": "Thread ID to resume"
                },
                "model": {
                    "type": "string",
                    "description": "Override model for resumed thread"
                },
                "modelProvider": {
                    "type": "string",
                    "description": "Override model provider"
                },
                "serviceTier": {
                    "type": "string",
                    "enum": ["auto", "default", "flex", "priority"],
                    "description": "Override service tier"
                },
                "cwd": {
                    "type": "string",
                    "description": "Override working directory"
                },
                "approvalPolicy": {
                    "type": "string",
                    "enum": ["untrusted", "on-failure", "on-request", "never"],
                    "description": "Override approval policy"
                },
                "sandbox": {
                    "type": "string",
                    "enum": ["read-only", "workspace-write", "danger-full-access"],
                    "description": "Override sandbox mode"
                },
                "config": {
                    "type": "object",
                    "description": "Configuration overrides"
                },
                "baseInstructions": {
                    "type": "string",
                    "description": "Override base instructions"
                },
                "developerInstructions": {
                    "type": "string",
                    "description": "Override developer instructions"
                },
                "personality": {
                    "type": "string",
                    "enum": ["default", "concise"],
                    "description": "Override personality"
                },
                "persistExtendedHistory": {
                    "type": "boolean",
                    "description": "Persist extended history"
                }
            }
        })),
    )
}

fn make_turn_start_tool() -> Tool {
    Tool::new(
        "turn_start",
        "Start a new turn in a thread, sending input to the agent",
        make_input_schema(json!({
            "type": "object",
            "required": ["threadId", "input"],
            "properties": {
                "threadId": {
                    "type": "string",
                    "description": "Thread ID to send the input to"
                },
                "input": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "const": "text"},
                                    "text": {"type": "string"}
                                },
                                "required": ["type", "text"]
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "const": "image"},
                                    "url": {"type": "string"}
                                },
                                "required": ["type", "url"]
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "const": "localImage"},
                                    "path": {"type": "string"}
                                },
                                "required": ["type", "path"]
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "const": "skill"},
                                    "name": {"type": "string"},
                                    "path": {"type": "string"}
                                },
                                "required": ["type", "name"]
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "const": "mention"},
                                    "name": {"type": "string"},
                                    "path": {"type": "string"}
                                },
                                "required": ["type", "name", "path"]
                            }
                        ]
                    },
                    "description": "Input items (text, images, skills, mentions)"
                },
                "cwd": {
                    "type": "string",
                    "description": "Override working directory for this turn"
                },
                "approvalPolicy": {
                    "type": "string",
                    "enum": ["untrusted", "on-failure", "on-request", "never"],
                    "description": "Override approval policy for this turn"
                },
                "sandboxPolicy": {
                    "type": "object",
                    "description": "Override sandbox policy for this turn"
                },
                "model": {
                    "type": "string",
                    "description": "Override model for this turn"
                },
                "serviceTier": {
                    "type": "string",
                    "enum": ["auto", "default", "flex", "priority"],
                    "description": "Override service tier for this turn"
                },
                "effort": {
                    "type": "string",
                    "enum": ["minimal", "low", "medium", "high"],
                    "description": "Reasoning effort level"
                },
                "summary": {
                    "type": "string",
                    "enum": ["auto", "concise", "detailed"],
                    "description": "Reasoning summary style"
                },
                "personality": {
                    "type": "string",
                    "enum": ["default", "concise"],
                    "description": "Override personality for this turn"
                },
                "outputSchema": {
                    "type": "object",
                    "description": "JSON Schema to constrain the final assistant message"
                },
                "collaborationMode": {
                    "type": "object",
                    "description": "Pre-set collaboration mode settings (experimental)"
                }
            }
        })),
    )
}

fn make_config_list_tool() -> Tool {
    Tool::new(
        "config_list",
        "List available configuration layers and their sources",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "cwd": {
                    "type": "string",
                    "description": "Working directory to resolve config from"
                }
            }
        })),
    )
}

fn make_config_read_tool() -> Tool {
    Tool::new(
        "config_read",
        "Read the merged configuration for a working directory",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "cwd": {
                    "type": "string",
                    "description": "Working directory to read config for"
                },
                "layer": {
                    "type": "string",
                    "description": "Specific config layer to read (default: merged)"
                }
            }
        })),
    )
}

fn make_config_write_tool() -> Tool {
    Tool::new(
        "config_write",
        "Write configuration to a specific layer",
        make_input_schema(json!({
            "type": "object",
            "required": ["layer", "config"],
            "properties": {
                "layer": {
                    "type": "string",
                    "enum": ["user", "project", "session"],
                    "description": "Config layer to write to"
                },
                "cwd": {
                    "type": "string",
                    "description": "Working directory for project layer"
                },
                "config": {
                    "type": "object",
                    "description": "Configuration values to write"
                }
            }
        })),
    )
}

fn make_model_list_tool() -> Tool {
    Tool::new(
        "model_list",
        "List available models and their capabilities",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "modelProvider": {
                    "type": "string",
                    "description": "Filter by model provider"
                }
            }
        })),
    )
}

fn make_review_start_tool() -> Tool {
    Tool::new(
        "review_start",
        "Start a review of code changes",
        make_input_schema(json!({
            "type": "object",
            "required": ["threadId", "target"],
            "properties": {
                "threadId": {
                    "type": "string",
                    "description": "Thread ID to run review in"
                },
                "target": {
                    "type": "object",
                    "description": "Review target specification",
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "const": "pullRequest"},
                                "url": {"type": "string"}
                            },
                            "required": ["type", "url"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "const": "diff"},
                                "diff": {"type": "string"}
                            },
                            "required": ["type", "diff"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "const": "files"},
                                "paths": {"type": "array", "items": {"type": "string"}}
                            },
                            "required": ["type", "paths"]
                        }
                    ]
                },
                "delivery": {
                    "type": "string",
                    "enum": ["inline", "detached"],
                    "description": "Where to run the review"
                }
            }
        })),
    )
}

fn make_skills_list_tool() -> Tool {
    Tool::new(
        "skills_list",
        "List available skills for a working directory",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "cwds": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Working directories to search for skills"
                },
                "forceReload": {
                    "type": "boolean",
                    "description": "Force reload skills from disk"
                }
            }
        })),
    )
}

fn make_thread_start_simple_tool() -> Tool {
    Tool::new(
        "thread_start_simple",
        "Simple thread start with just cwd and model (backward compatible)",
        make_input_schema(json!({
            "type": "object",
            "properties": {
                "cwd": {
                    "type": "string",
                    "description": "Working directory for the thread"
                },
                "model": {
                    "type": "string",
                    "description": "Model to use (optional)"
                }
            }
        })),
    )
}

fn make_turn_start_simple_tool() -> Tool {
    Tool::new(
        "turn_start_simple",
        "Simple turn start with just thread_id and text message (backward compatible)",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id", "message"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to send the message to"
                },
                "message": {
                    "type": "string",
                    "description": "Text message to send to the agent"
                }
            }
        })),
    )
}

fn make_thread_fork_tool() -> Tool {
    Tool::new(
        "thread_fork",
        "Fork a thread to create a new conversation branch",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to fork"
                },
                "turn_id": {
                    "type": "string",
                    "description": "Optional turn ID to fork from"
                },
                "name": {
                    "type": "string",
                    "description": "Optional name for the forked thread"
                }
            }
        })),
    )
}

fn make_thread_archive_tool() -> Tool {
    Tool::new(
        "thread_archive",
        "Archive a thread",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to archive"
                }
            }
        })),
    )
}

fn make_thread_unarchive_tool() -> Tool {
    Tool::new(
        "thread_unarchive",
        "Unarchive a thread",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to unarchive"
                }
            }
        })),
    )
}

fn make_thread_unsubscribe_tool() -> Tool {
    Tool::new(
        "thread_unsubscribe",
        "Unsubscribe from a thread",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to unsubscribe from"
                }
            }
        })),
    )
}

fn make_thread_set_name_tool() -> Tool {
    Tool::new(
        "thread_set_name",
        "Set the name of a thread",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id", "name"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to rename"
                },
                "name": {
                    "type": "string",
                    "description": "New name for the thread"
                }
            }
        })),
    )
}

fn make_thread_compact_start_tool() -> Tool {
    Tool::new(
        "thread_compact_start",
        "Start compacting a thread to reduce context size",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to compact"
                }
            }
        })),
    )
}

fn make_thread_background_terminals_clean_tool() -> Tool {
    Tool::new(
        "thread_background_terminals_clean",
        "Clean up background terminals for a thread",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to clean terminals for"
                }
            }
        })),
    )
}

fn make_thread_rollback_tool() -> Tool {
    Tool::new(
        "thread_rollback",
        "Rollback a thread to a previous state",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id", "turn_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID to rollback"
                },
                "turn_id": {
                    "type": "string",
                    "description": "Turn ID to rollback to"
                }
            }
        })),
    )
}

fn make_thread_loaded_list_tool() -> Tool {
    Tool::new(
        "thread_loaded_list",
        "List all currently loaded threads in memory",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_turn_steer_tool() -> Tool {
    Tool::new(
        "turn_steer",
        "Steer an ongoing turn with new input",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id", "turn_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID"
                },
                "turn_id": {
                    "type": "string",
                    "description": "Turn ID to steer"
                },
                "input": {
                    "type": "array",
                    "description": "Input items for steering",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["text", "image", "localImage", "skill", "mention"]
                            }
                        }
                    }
                }
            }
        })),
    )
}

fn make_turn_interrupt_tool() -> Tool {
    Tool::new(
        "turn_interrupt",
        "Interrupt an ongoing turn",
        make_input_schema(json!({
            "type": "object",
            "required": ["thread_id", "turn_id"],
            "properties": {
                "thread_id": {
                    "type": "string",
                    "description": "Thread ID"
                },
                "turn_id": {
                    "type": "string",
                    "description": "Turn ID to interrupt"
                }
            }
        })),
    )
}

fn make_skills_remote_read_tool() -> Tool {
    Tool::new(
        "skills_remote_read",
        "Read a remote skill definition",
        make_input_schema(json!({
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL of the remote skill"
                }
            }
        })),
    )
}

fn make_skills_remote_write_tool() -> Tool {
    Tool::new(
        "skills_remote_write",
        "Write a skill to a remote location",
        make_input_schema(json!({
            "type": "object",
            "required": ["url", "content"],
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL to write the skill to"
                },
                "content": {
                    "type": "string",
                    "description": "Skill content to write"
                }
            }
        })),
    )
}

fn make_skills_config_write_tool() -> Tool {
    Tool::new(
        "skills_config_write",
        "Write skill configuration",
        make_input_schema(json!({
            "type": "object",
            "required": ["skills"],
            "properties": {
                "skills": {
                    "type": "array",
                    "description": "Skills configuration",
                    "items": {
                        "type": "object"
                    }
                }
            }
        })),
    )
}

fn make_apps_list_tool() -> Tool {
    Tool::new(
        "apps_list",
        "List available apps",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_account_get_tool() -> Tool {
    Tool::new(
        "account_get",
        "Get current account information",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_account_rate_limits_tool() -> Tool {
    Tool::new(
        "account_rate_limits",
        "Get current account rate limits",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_experimental_feature_list_tool() -> Tool {
    Tool::new(
        "experimental_feature_list",
        "List available experimental features",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_collaboration_mode_list_tool() -> Tool {
    Tool::new(
        "collaboration_mode_list",
        "List available collaboration modes",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_mcp_server_status_tool() -> Tool {
    Tool::new(
        "mcp_server_status",
        "Get MCP server status",
        make_input_schema(json!({
            "type": "object",
            "properties": {}
        })),
    )
}

fn make_feedback_upload_tool() -> Tool {
    Tool::new(
        "feedback_upload",
        "Upload feedback to the server",
        make_input_schema(json!({
            "type": "object",
            "required": ["feedback"],
            "properties": {
                "feedback": {
                    "type": "object",
                    "description": "Feedback data to upload"
                }
            }
        })),
    )
}

fn make_command_exec_tool() -> Tool {
    Tool::new(
        "command_exec",
        "Execute a command through the app-server",
        make_input_schema(json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Command to execute"
                },
                "args": {
                    "type": "array",
                    "description": "Command arguments",
                    "items": {
                        "type": "string"
                    }
                },
                "cwd": {
                    "type": "string",
                    "description": "Working directory"
                }
            }
        })),
    )
}

// ============================================================
// Parameter Parsing Helpers
// ============================================================

fn parse_thread_start_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> ThreadStartParams {
    ThreadStartParams {
        model: args.get("model").and_then(|v| v.as_str()).map(String::from),
        model_provider: args
            .get("modelProvider")
            .and_then(|v| v.as_str())
            .map(String::from),
        service_tier: args
            .get("serviceTier")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        cwd: args.get("cwd").and_then(|v| v.as_str()).map(String::from),
        approval_policy: args
            .get("approvalPolicy")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        sandbox: args.get("sandbox").and_then(|v| v.as_str()).and_then(|s| {
            // Convert kebab-case to snake_case for enum parsing
            let normalized = s.replace('-', "_");
            serde_json::from_value(json!(normalized)).ok()
        }),
        config: args
            .get("config")
            .and_then(|v| v.as_object())
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        service_name: args
            .get("serviceName")
            .and_then(|v| v.as_str())
            .map(String::from),
        base_instructions: args
            .get("baseInstructions")
            .and_then(|v| v.as_str())
            .map(String::from),
        developer_instructions: args
            .get("developerInstructions")
            .and_then(|v| v.as_str())
            .map(String::from),
        personality: args
            .get("personality")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        ephemeral: args.get("ephemeral").and_then(serde_json::Value::as_bool),
        dynamic_tools: args
            .get("dynamicTools")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect()
            }),
        ..Default::default()
    }
}

fn parse_thread_list_params(args: serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    let mut params = serde_json::Map::new();

    if let Some(v) = args.get("cursor").and_then(|v| v.as_str()) {
        params.insert("cursor".to_string(), json!(v));
    }
    if let Some(v) = args.get("limit").and_then(serde_json::Value::as_u64) {
        params.insert("limit".to_string(), json!(v as u32));
    }
    if let Some(v) = args.get("sortKey").and_then(|v| v.as_str()) {
        params.insert("sortKey".to_string(), json!(v));
    }
    if let Some(arr) = args.get("modelProviders").and_then(|v| v.as_array()) {
        params.insert(
            "modelProviders".to_string(),
            json!(arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
        );
    }
    if let Some(arr) = args.get("sourceKinds").and_then(|v| v.as_array()) {
        params.insert(
            "sourceKinds".to_string(),
            json!(arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
        );
    }
    if let Some(v) = args.get("archived").and_then(serde_json::Value::as_bool) {
        params.insert("archived".to_string(), json!(v));
    }
    if let Some(v) = args.get("cwd").and_then(|v| v.as_str()) {
        params.insert("cwd".to_string(), json!(v));
    }
    if let Some(v) = args.get("searchTerm").and_then(|v| v.as_str()) {
        params.insert("searchTerm".to_string(), json!(v));
    }

    json!(params)
}

fn parse_thread_resume_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let mut params = serde_json::Map::new();

    if let Some(v) = args.get("threadId").and_then(|v| v.as_str()) {
        params.insert("threadId".to_string(), json!(v));
    }
    if let Some(v) = args.get("model").and_then(|v| v.as_str()) {
        params.insert("model".to_string(), json!(v));
    }
    if let Some(v) = args.get("modelProvider").and_then(|v| v.as_str()) {
        params.insert("modelProvider".to_string(), json!(v));
    }
    if let Some(v) = args.get("serviceTier").and_then(|v| v.as_str()) {
        params.insert("serviceTier".to_string(), json!(v));
    }
    if let Some(v) = args.get("cwd").and_then(|v| v.as_str()) {
        params.insert("cwd".to_string(), json!(v));
    }
    if let Some(v) = args.get("approvalPolicy").and_then(|v| v.as_str()) {
        params.insert("approvalPolicy".to_string(), json!(v));
    }
    if let Some(v) = args.get("sandbox").and_then(|v| v.as_str()) {
        params.insert("sandbox".to_string(), json!(v.replace('-', "_")));
    }
    if let Some(v) = args.get("config") {
        params.insert("config".to_string(), v.clone());
    }
    if let Some(v) = args.get("baseInstructions").and_then(|v| v.as_str()) {
        params.insert("baseInstructions".to_string(), json!(v));
    }
    if let Some(v) = args.get("developerInstructions").and_then(|v| v.as_str()) {
        params.insert("developerInstructions".to_string(), json!(v));
    }
    if let Some(v) = args.get("personality").and_then(|v| v.as_str()) {
        params.insert("personality".to_string(), json!(v));
    }
    if let Some(v) = args
        .get("persistExtendedHistory")
        .and_then(serde_json::Value::as_bool)
    {
        params.insert("persistExtendedHistory".to_string(), json!(v));
    }

    json!(params)
}

fn parse_turn_start_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<TurnStartParams> {
    let thread_id = args
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing threadId"))?
        .to_string();

    let input: Vec<UserInput> = args
        .get("input")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let item_type = item.get("type")?.as_str()?;
                    match item_type {
                        "text" => Some(UserInput::Text {
                            text: item.get("text")?.as_str()?.to_string(),
                            text_elements: vec![],
                        }),
                        "image" => Some(UserInput::Image {
                            url: item.get("url")?.as_str()?.to_string(),
                        }),
                        "localImage" => Some(UserInput::LocalImage {
                            path: item
                                .get("path")
                                .and_then(|p| p.as_str())
                                .map(std::path::PathBuf::from)?,
                        }),
                        "skill" => Some(UserInput::Skill {
                            name: item.get("name")?.as_str()?.to_string(),
                            path: item
                                .get("path")
                                .and_then(|p| p.as_str())
                                .map(std::path::PathBuf::from)
                                .unwrap_or_default(),
                        }),
                        "mention" => Some(UserInput::Mention {
                            name: item.get("name")?.as_str()?.to_string(),
                            path: item.get("path")?.as_str()?.to_string(),
                        }),
                        _ => None,
                    }
                })
                .collect()
        })
        .ok_or_else(|| anyhow::anyhow!("missing or invalid input"))?;

    Ok(TurnStartParams {
        thread_id,
        input,
        cwd: args
            .get("cwd")
            .and_then(|v| v.as_str())
            .map(std::path::PathBuf::from),
        approval_policy: args
            .get("approvalPolicy")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        sandbox_policy: None,
        model: args.get("model").and_then(|v| v.as_str()).map(String::from),
        service_tier: args
            .get("serviceTier")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        effort: args
            .get("effort")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        summary: args
            .get("summary")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        personality: args
            .get("personality")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_value(json!(s)).ok()),
        output_schema: args.get("outputSchema").cloned(),
        collaboration_mode: None,
    })
}

fn parse_thread_id_params(args: serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    let thread_id = args.get("thread_id").and_then(|v| v.as_str()).unwrap_or("");
    json!({ "threadId": thread_id })
}

fn parse_thread_fork_params(args: serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    let mut params = serde_json::Map::new();
    if let Some(v) = args.get("thread_id").and_then(|v| v.as_str()) {
        params.insert("threadId".to_string(), json!(v));
    }
    if let Some(v) = args.get("turn_id").and_then(|v| v.as_str()) {
        params.insert("turnId".to_string(), json!(v));
    }
    if let Some(v) = args.get("name").and_then(|v| v.as_str()) {
        params.insert("name".to_string(), json!(v));
    }
    json!(params)
}

fn parse_thread_set_name_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let thread_id = args.get("thread_id").and_then(|v| v.as_str()).unwrap_or("");
    let name = args.get("name").and_then(|v| v.as_str()).unwrap_or("");
    json!({ "threadId": thread_id, "name": name })
}

fn parse_thread_rollback_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let thread_id = args.get("thread_id").and_then(|v| v.as_str()).unwrap_or("");
    let turn_id = args.get("turn_id").and_then(|v| v.as_str()).unwrap_or("");
    json!({ "threadId": thread_id, "turnId": turn_id })
}

fn parse_turn_steer_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let thread_id = args
        .get("thread_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing thread_id"))?;
    let turn_id = args
        .get("turn_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing turn_id"))?;

    let input: Vec<UserInput> = args
        .get("input")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let item_type = item.get("type")?.as_str()?;
                    match item_type {
                        "text" => Some(UserInput::Text {
                            text: item.get("text")?.as_str()?.to_string(),
                            text_elements: vec![],
                        }),
                        "image" => Some(UserInput::Image {
                            url: item.get("url")?.as_str()?.to_string(),
                        }),
                        "localImage" => Some(UserInput::LocalImage {
                            path: item
                                .get("path")
                                .and_then(|p| p.as_str())
                                .map(std::path::PathBuf::from)?,
                        }),
                        "skill" => Some(UserInput::Skill {
                            name: item.get("name")?.as_str()?.to_string(),
                            path: item
                                .get("path")
                                .and_then(|p| p.as_str())
                                .map(std::path::PathBuf::from)
                                .unwrap_or_default(),
                        }),
                        "mention" => Some(UserInput::Mention {
                            name: item.get("name")?.as_str()?.to_string(),
                            path: item.get("path")?.as_str()?.to_string(),
                        }),
                        _ => None,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(json!({
        "threadId": thread_id,
        "turnId": turn_id,
        "input": input
    }))
}

fn parse_turn_interrupt_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let thread_id = args
        .get("thread_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing thread_id"))?;
    let turn_id = args
        .get("turn_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing turn_id"))?;

    Ok(json!({
        "threadId": thread_id,
        "turnId": turn_id
    }))
}
