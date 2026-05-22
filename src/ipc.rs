use crate::coordinator::{Coordinator, CoordinatorError};
use crate::db::NodeDefinition;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::string::FromUtf8Error;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info};
use uuid::Uuid;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] FromUtf8Error),
    #[error("request exceeds {0} bytes")]
    RequestTooLarge(usize),
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct RpcResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcErrorBody>,
}

#[derive(Debug, Serialize)]
struct RpcErrorBody {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RegisterTaskParams {
    goal: String,
    #[serde(default = "empty_object")]
    context: Value,
    #[serde(default)]
    initial_nodes: Vec<NodeDefinition>,
}

#[derive(Debug, Deserialize)]
struct AcquireNextNodeParams {
    task_id: Uuid,
    #[serde(default = "default_lease_owner")]
    lease_owner: String,
}

#[derive(Debug, Deserialize)]
struct CommitEventParams {
    task_id: Uuid,
    node_id: Uuid,
    #[serde(default)]
    lease_id: Option<Uuid>,
    #[serde(default)]
    action_type: Option<String>,
    #[serde(alias = "event_payload")]
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct CompleteNodeParams {
    task_id: Uuid,
    node_id: Uuid,
    lease_id: Uuid,
    #[serde(alias = "payload")]
    result_payload: Value,
}

#[derive(Debug, Deserialize)]
struct HeartbeatNodeParams {
    task_id: Uuid,
    node_id: Uuid,
    lease_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct FailNodeParams {
    task_id: Uuid,
    node_id: Uuid,
    lease_id: Uuid,
    #[serde(alias = "payload")]
    error_payload: Value,
}

#[derive(Debug, Deserialize)]
struct TaskStatusParams {
    task_id: Uuid,
}

pub async fn serve(
    socket_path: impl AsRef<Path>,
    coordinator: Arc<Coordinator>,
) -> Result<(), IpcError> {
    let socket_path = socket_path.as_ref();
    let listener = UnixListener::bind(socket_path)?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o660))?;
    info!(path = %socket_path.display(), "agentd IPC listener started");

    loop {
        let (stream, _addr) = listener.accept().await?;
        let coordinator = Arc::clone(&coordinator);

        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, coordinator).await {
                error!(%err, "IPC client handler failed");
            }
        });
    }
}

async fn handle_client(stream: UnixStream, coordinator: Arc<Coordinator>) -> Result<(), IpcError> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    loop {
        let line = match read_bounded_line(&mut reader).await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(IpcError::RequestTooLarge(max_bytes)) => {
                let response =
                    RpcResponse::error(None, -32001, format!("request exceeds {max_bytes} bytes"));
                write_response(&mut writer, &response).await?;
                break;
            }
            Err(err) => return Err(err),
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        debug!(request = trimmed, "received IPC request");
        let response = match serde_json::from_str::<RpcRequest>(trimmed) {
            Ok(request) => handle_request(request, Arc::clone(&coordinator)).await,
            Err(err) => RpcResponse::error(None, -32700, format!("parse error: {err}")),
        };

        write_response(&mut writer, &response).await?;
    }

    Ok(())
}

async fn read_bounded_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<String>, IpcError> {
    let mut bytes = Vec::with_capacity(256);

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }

        if let Some(newline_index) = available.iter().position(|byte| *byte == b'\n') {
            let consumed = newline_index + 1;
            if bytes.len() + consumed > MAX_REQUEST_BYTES {
                reader.consume(consumed);
                return Err(IpcError::RequestTooLarge(MAX_REQUEST_BYTES));
            }
            bytes.extend_from_slice(&available[..consumed]);
            reader.consume(consumed);
            break;
        }

        if bytes.len() + available.len() > MAX_REQUEST_BYTES {
            let consumed = available.len();
            reader.consume(consumed);
            return Err(IpcError::RequestTooLarge(MAX_REQUEST_BYTES));
        }

        bytes.extend_from_slice(available);
        let consumed = available.len();
        reader.consume(consumed);
    }

    Ok(Some(String::from_utf8(bytes)?))
}

async fn write_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut BufWriter<W>,
    response: &RpcResponse,
) -> Result<(), IpcError> {
    let encoded = serde_json::to_vec(response)?;
    writer.write_all(&encoded).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn handle_request(request: RpcRequest, coordinator: Arc<Coordinator>) -> RpcResponse {
    let id = request.id;
    let result = match request.method.as_str() {
        "DescribeInterface" | "describe_interface" => Ok(describe_interface()),
        "Health" | "health" => health(coordinator).await,
        "RegisterTask" | "register_task" => register_task(request.params, coordinator).await,
        "AcquireNextNode" | "acquire_next_node" => {
            acquire_next_node(request.params, coordinator).await
        }
        "CommitEvent" | "commit_event" => commit_event(request.params, coordinator).await,
        "HeartbeatNode" | "heartbeat_node" => heartbeat_node(request.params, coordinator).await,
        "CompleteNode" | "complete_node" => complete_node(request.params, coordinator).await,
        "FailNode" | "fail_node" => fail_node(request.params, coordinator).await,
        "TaskStatus" | "task_status" | "GetTaskStatus" | "get_task_status" => {
            task_status(request.params, coordinator).await
        }
        other => {
            return RpcResponse::error(id, -32601, format!("unknown method: {other}"));
        }
    };

    match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => error.into_response(id),
    }
}

fn describe_interface() -> Value {
    json!({
        "protocol": "agentd-jsonl-v1",
        "transport": "unix-domain-socket",
        "framing": "one JSON-RPC style request per line; one response per line",
        "state_model": {
            "node_statuses": ["PENDING", "RUNNING", "COMPLETED", "FAILED"],
            "strict_transitions": {
                "AcquireNextNode": "PENDING -> RUNNING",
                "CompleteNode": "RUNNING -> COMPLETED when lease_id matches",
                "FailNode": "RUNNING -> FAILED when lease_id matches",
                "timeout_rollback": "RUNNING -> PENDING"
            }
        },
        "methods": [
            {
                "method": "DescribeInterface",
                "params": {},
                "result": {"protocol": "string", "methods": "array"}
            },
            {
                "method": "Health",
                "params": {},
                "result": {
                    "status": "ok",
                    "database": "ok",
                    "running_timeout_secs": "integer",
                    "context_event_limit": "integer"
                }
            },
            {
                "method": "RegisterTask",
                "params": {
                    "goal": "string",
                    "context": "object",
                    "initial_nodes": [
                        {
                            "id": "optional uuid",
                            "dependencies": ["uuid"],
                            "payload_schema": "object"
                        }
                    ]
                },
                "result": {"task_id": "uuid"}
            },
            {
                "method": "AcquireNextNode",
                "params": {"task_id": "uuid", "lease_owner": "optional string"},
                "result": {
                    "task_id": "uuid",
                    "node_id": "uuid",
                    "lease_id": "uuid",
                    "lease_owner": "string",
                    "lease_expires_at": "unix timestamp",
                    "context": "object, or null when no node is currently runnable"
                }
            },
            {
                "method": "CommitEvent",
                "params": {
                    "task_id": "uuid",
                    "node_id": "uuid",
                    "lease_id": "optional uuid, required while node is RUNNING",
                    "action_type": "optional string",
                    "payload": "object"
                },
                "result": {"event_id": "uuid"}
            },
            {
                "method": "HeartbeatNode",
                "params": {"task_id": "uuid", "node_id": "uuid", "lease_id": "uuid"},
                "result": {"ok": true}
            },
            {
                "method": "CompleteNode",
                "params": {
                    "task_id": "uuid",
                    "node_id": "uuid",
                    "lease_id": "uuid",
                    "result_payload": "object"
                },
                "result": {"event_id": "uuid"}
            },
            {
                "method": "FailNode",
                "params": {
                    "task_id": "uuid",
                    "node_id": "uuid",
                    "lease_id": "uuid",
                    "error_payload": "object"
                },
                "result": {"event_id": "uuid"}
            },
            {
                "method": "TaskStatus",
                "params": {"task_id": "uuid"},
                "result": {
                    "task": "object",
                    "counts": "object keyed by node status",
                    "nodes": "array"
                }
            }
        ]
    })
}

async fn health(coordinator: Arc<Coordinator>) -> Result<Value, RequestError> {
    Ok(serde_json::to_value(coordinator.health().await?)?)
}

async fn register_task(
    params: Value,
    coordinator: Arc<Coordinator>,
) -> Result<Value, RequestError> {
    let params: RegisterTaskParams = serde_json::from_value(params)?;
    let task_id = coordinator
        .register_task(params.goal, params.context, params.initial_nodes)
        .await?;
    Ok(json!({ "task_id": task_id }))
}

async fn acquire_next_node(
    params: Value,
    coordinator: Arc<Coordinator>,
) -> Result<Value, RequestError> {
    let params: AcquireNextNodeParams = serde_json::from_value(params)?;
    let acquired = coordinator
        .acquire_next_node(params.task_id, params.lease_owner)
        .await?;
    Ok(serde_json::to_value(acquired)?)
}

async fn commit_event(params: Value, coordinator: Arc<Coordinator>) -> Result<Value, RequestError> {
    let params: CommitEventParams = serde_json::from_value(params)?;
    let event_id = coordinator
        .commit_event(
            params.task_id,
            params.node_id,
            params.lease_id,
            params.action_type,
            params.payload,
        )
        .await?;
    Ok(json!({ "event_id": event_id }))
}

async fn complete_node(
    params: Value,
    coordinator: Arc<Coordinator>,
) -> Result<Value, RequestError> {
    let params: CompleteNodeParams = serde_json::from_value(params)?;
    let event_id = coordinator
        .complete_node(
            params.task_id,
            params.node_id,
            params.lease_id,
            params.result_payload,
        )
        .await?;
    Ok(json!({ "event_id": event_id }))
}

async fn heartbeat_node(
    params: Value,
    coordinator: Arc<Coordinator>,
) -> Result<Value, RequestError> {
    let params: HeartbeatNodeParams = serde_json::from_value(params)?;
    coordinator
        .heartbeat_node(params.task_id, params.node_id, params.lease_id)
        .await?;
    Ok(json!({ "ok": true }))
}

async fn fail_node(params: Value, coordinator: Arc<Coordinator>) -> Result<Value, RequestError> {
    let params: FailNodeParams = serde_json::from_value(params)?;
    let event_id = coordinator
        .fail_node(
            params.task_id,
            params.node_id,
            params.lease_id,
            params.error_payload,
        )
        .await?;
    Ok(json!({ "event_id": event_id }))
}

async fn task_status(params: Value, coordinator: Arc<Coordinator>) -> Result<Value, RequestError> {
    let params: TaskStatusParams = serde_json::from_value(params)?;
    let snapshot = coordinator.task_status(params.task_id).await?;
    Ok(serde_json::to_value(snapshot)?)
}

#[derive(Debug, Error)]
enum RequestError {
    #[error("invalid params: {0}")]
    InvalidParams(#[from] serde_json::Error),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
}

impl RequestError {
    fn into_response(self, id: Option<Value>) -> RpcResponse {
        match self {
            Self::InvalidParams(err) => {
                RpcResponse::error(id, -32602, format!("invalid params: {err}"))
            }
            Self::Coordinator(err) => RpcResponse::error(id, -32000, err.to_string()),
        }
    }
}

impl RpcResponse {
    fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Option<Value>, code: i64, message: String) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcErrorBody { code, message }),
        }
    }
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

fn default_lease_owner() -> String {
    "anonymous".to_string()
}
