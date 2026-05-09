//! WebSocket client for connecting to a remote app-server.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use futures::SinkExt;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::debug;
use tracing::error;
use tracing::info;
use url::Url;

/// WebSocket client for the app-server JSON-RPC protocol.
pub struct WebSocketClient {
    /// Channel to send outgoing messages.
    sender: mpsc::Sender<String>,
    /// Channel to receive incoming messages.
    receiver: mpsc::Receiver<String>,
    /// Request ID counter for generating unique IDs.
    request_id_counter: AtomicU64,
    /// Pending notifications that arrived while waiting for a response.
    pending_notifications: VecDeque<JSONRPCNotification>,
}

impl WebSocketClient {
    /// Connect to a remote app-server via WebSocket.
    pub async fn connect(url: &str, auth_bearer_token: Option<&str>) -> Result<Self> {
        let parsed = Url::parse(url).with_context(|| format!("invalid websocket URL `{url}`"))?;

        let (ws_stream, _) = if let Some(token) = auth_bearer_token {
            let mut request = parsed
                .as_str()
                .into_client_request()
                .context("failed to build websocket client request")?;
            let auth_value = format!("Bearer {token}")
                .parse()
                .context("failed to parse Authorization header value")?;
            request.headers_mut().insert("Authorization", auth_value);
            tokio_tungstenite::connect_async(request)
                .await
                .with_context(|| format!("failed to connect to websocket app-server at `{url}`"))?
        } else {
            tokio_tungstenite::connect_async(parsed.as_str())
                .await
                .with_context(|| format!("failed to connect to websocket app-server at `{url}`"))?
        };

        info!("Connected to app-server at {url}");

        // Split the websocket into sender and receiver
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<String>(super::CHANNEL_CAPACITY);
        let (incoming_tx, incoming_rx) = mpsc::channel::<String>(super::CHANNEL_CAPACITY);

        // Spawn task to send outgoing messages.
        let url_owned = url.to_string();
        tokio::spawn(async move {
            while let Some(msg) = outgoing_rx.recv().await {
                if let Err(e) = ws_sender.send(Message::Text(msg.into())).await {
                    error!("Failed to send websocket message: {}", e);
                    break;
                }
            }
            debug!("WebSocket sender task finished for {}", url_owned);
        });

        // Spawn task to receive incoming messages.
        let url_owned = url.to_string();
        tokio::spawn(async move {
            while let Some(msg_result) = ws_receiver.next().await {
                match msg_result {
                    Ok(Message::Text(text)) => {
                        if incoming_tx.send(text.to_string()).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        // Respond to ping - tungstenite handles this internally
                        debug!("Received ping: {:?}", payload);
                    }
                    Ok(Message::Pong(_)) => {
                        debug!("Received pong");
                    }
                    Ok(Message::Close(_)) | Ok(Message::Frame(_)) => {
                        info!("WebSocket connection closed for {}", url_owned);
                        break;
                    }
                    Ok(Message::Binary(_)) => {
                        debug!("Received binary message (ignored)");
                    }
                    Err(e) => {
                        error!("WebSocket receive error for {}: {}", url_owned, e);
                        break;
                    }
                }
            }
            debug!("WebSocket receiver task finished for {}", url_owned);
        });

        Ok(Self {
            sender: outgoing_tx,
            receiver: incoming_rx,
            request_id_counter: AtomicU64::new(1),
            pending_notifications: VecDeque::new(),
        })
    }

    /// Generate a unique request ID.
    pub fn next_request_id(&self) -> RequestId {
        let id = self.request_id_counter.fetch_add(1, Ordering::Relaxed);
        RequestId::String(format!("bridge-{id}"))
    }

    /// Send a JSON-RPC request and wait for the response.
    pub async fn send_request<T>(&mut self, request: &JSONRPCRequest) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let request_id = request.id.clone();
        let payload = serde_json::to_string(request).context("failed to serialize request")?;

        debug!("Sending request: {}", payload);
        self.sender
            .send(payload)
            .await
            .context("failed to send request over websocket")?;

        // Wait for the matching response
        loop {
            let raw = self
                .receiver
                .recv()
                .await
                .context("websocket connection closed")?;

            let message: JSONRPCMessage = serde_json::from_str(&raw)
                .with_context(|| format!("failed to parse JSON-RPC message: {raw}"))?;

            match message {
                JSONRPCMessage::Response(JSONRPCResponse { id, result }) => {
                    if id == request_id {
                        return serde_json::from_value(result)
                            .context("failed to deserialize response");
                    }
                    // Not our response, ignore
                    debug!("Received response for different request: {:?}", id);
                }
                JSONRPCMessage::Error(err) => {
                    if err.id == request_id {
                        bail!("request failed: {err:?}");
                    }
                }
                JSONRPCMessage::Notification(notification) => {
                    // Queue notification for later retrieval
                    self.pending_notifications.push_back(notification);
                }
                JSONRPCMessage::Request(_) => {
                    // Server-initiated requests are not expected in this context
                    debug!("Received unexpected server request");
                }
            }
        }
    }

    /// Send a JSON-RPC notification (no response expected).
    pub async fn send_notification(&mut self, notification: &JSONRPCNotification) -> Result<()> {
        let payload =
            serde_json::to_string(notification).context("failed to serialize notification")?;

        debug!("Sending notification: {}", payload);
        self.sender
            .send(payload)
            .await
            .context("failed to send notification over websocket")?;
        Ok(())
    }

    /// Receive the next notification from the server.
    pub async fn recv_notification(&mut self) -> Result<JSONRPCNotification> {
        // Check pending queue first
        if let Some(notification) = self.pending_notifications.pop_front() {
            return Ok(notification);
        }

        // Wait for next message
        loop {
            let raw = self
                .receiver
                .recv()
                .await
                .context("websocket connection closed")?;

            let message: JSONRPCMessage = serde_json::from_str(&raw)
                .with_context(|| format!("failed to parse JSON-RPC message: {raw}"))?;

            match message {
                JSONRPCMessage::Notification(notification) => {
                    return Ok(notification);
                }
                JSONRPCMessage::Response(_) | JSONRPCMessage::Error(_) => {
                    // Ignore stray responses/errors
                    debug!("Received stray response/error");
                }
                JSONRPCMessage::Request(_) => {
                    debug!("Received unexpected server request");
                }
            }
        }
    }

    /// Check if the connection is still active.
    pub fn is_connected(&self) -> bool {
        !self.sender.is_closed()
    }
}
