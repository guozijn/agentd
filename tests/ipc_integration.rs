use agentd::coordinator::Coordinator;
use agentd::db::Database;
use agentd::ipc;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use uuid::Uuid;

struct TestDaemon {
    socket_path: PathBuf,
    db_path: PathBuf,
    handle: JoinHandle<()>,
}

impl TestDaemon {
    async fn start(timeout: Duration) -> Self {
        let id = Uuid::new_v4();
        let short_id = id.simple().to_string();
        let socket_path = PathBuf::from(format!("/tmp/ad_{}.sock", &short_id[..12]));
        let db_path = std::env::temp_dir().join(format!("agentd_ipc_test_{id}.db"));
        let database_url = format!("sqlite://{}", db_path.display());
        let db = Database::initialise(&database_url)
            .await
            .expect("test database should initialise");
        let coordinator = Arc::new(Coordinator::new(db, timeout, 10));
        let serve_path = socket_path.clone();
        let handle = tokio::spawn(async move {
            ipc::serve(serve_path, coordinator)
                .await
                .expect("IPC server should run until aborted");
        });

        wait_for_socket(&socket_path).await;
        Self {
            socket_path,
            db_path,
            handle,
        }
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        self.handle.abort();
        let _ = std::fs::remove_file(&self.socket_path);
        cleanup_db(&self.db_path);
    }
}

#[tokio::test]
async fn ipc_contract_supports_multi_worker_dag_flow() {
    let daemon = TestDaemon::start(Duration::from_secs(30)).await;

    let interface = rpc_call(&daemon.socket_path, "DescribeInterface", json!({})).await;
    assert_eq!(interface["protocol"], "agentd-jsonl-v1");
    assert!(interface["methods"]
        .as_array()
        .expect("methods should be an array")
        .iter()
        .any(|method| method["method"] == "Metrics"));
    assert!(interface["methods"]
        .as_array()
        .expect("methods should be an array")
        .iter()
        .any(|method| method["method"] == "AcquireResourceLock"));

    let first_node = Uuid::new_v4();
    let second_node = Uuid::new_v4();
    let final_node = Uuid::new_v4();
    let registered = rpc_call(
        &daemon.socket_path,
        "RegisterTask",
        json!({
            "goal": "integration DAG",
            "context": {"test": "ipc_contract_supports_multi_worker_dag_flow"},
            "initial_nodes": [
                {"id": first_node, "dependencies": [], "payload_schema": {"agent": "a"}},
                {"id": second_node, "dependencies": [], "payload_schema": {"agent": "b"}},
                {"id": final_node, "dependencies": [first_node, second_node], "payload_schema": {"agent": "join"}}
            ]
        }),
    )
    .await;
    let task_id = registered["task_id"]
        .as_str()
        .expect("task_id should exist");

    let first = rpc_call(
        &daemon.socket_path,
        "AcquireNextNode",
        json!({"task_id": task_id, "lease_owner": "worker-a"}),
    )
    .await;
    let second = rpc_call(
        &daemon.socket_path,
        "AcquireNextNode",
        json!({"task_id": task_id, "lease_owner": "worker-b"}),
    )
    .await;
    assert_ne!(first["node_id"], second["node_id"]);
    assert_ne!(first["lease_id"], second["lease_id"]);

    let blocked = rpc_call(
        &daemon.socket_path,
        "AcquireNextNode",
        json!({"task_id": task_id, "lease_owner": "worker-c"}),
    )
    .await;
    assert!(blocked.is_null());

    complete(&daemon.socket_path, task_id, &first).await;
    complete(&daemon.socket_path, task_id, &second).await;

    let joined = rpc_call(
        &daemon.socket_path,
        "AcquireNextNode",
        json!({"task_id": task_id, "lease_owner": "worker-join"}),
    )
    .await;
    assert_eq!(joined["node_id"], final_node.to_string());
    complete(&daemon.socket_path, task_id, &joined).await;

    let metrics = rpc_call(&daemon.socket_path, "Metrics", json!({})).await;
    assert_eq!(metrics["database"]["schema_version"], 2);
    assert_eq!(metrics["database"]["total_tasks"], 1);
    assert_eq!(metrics["database"]["total_nodes"], 3);
    assert_eq!(metrics["runtime"]["completed_nodes"], 3);

    let status = rpc_call(
        &daemon.socket_path,
        "TaskStatus",
        json!({"task_id": task_id}),
    )
    .await;
    assert_eq!(status["counts"]["COMPLETED"], 3);
}

#[tokio::test]
async fn ipc_rejects_stale_lease_after_reacquire() {
    let daemon = TestDaemon::start(Duration::from_secs(0)).await;
    let node_id = Uuid::new_v4();
    let registered = rpc_call(
        &daemon.socket_path,
        "RegisterTask",
        json!({
            "goal": "stale lease",
            "initial_nodes": [{"id": node_id, "dependencies": [], "payload_schema": {}}]
        }),
    )
    .await;
    let task_id = registered["task_id"]
        .as_str()
        .expect("task_id should exist");

    let first = rpc_call(
        &daemon.socket_path,
        "AcquireNextNode",
        json!({"task_id": task_id, "lease_owner": "old-worker"}),
    )
    .await;
    let second = rpc_call(
        &daemon.socket_path,
        "AcquireNextNode",
        json!({"task_id": task_id, "lease_owner": "new-worker"}),
    )
    .await;
    assert_eq!(first["node_id"], second["node_id"]);
    assert_ne!(first["lease_id"], second["lease_id"]);

    let stale = raw_rpc_call(
        &daemon.socket_path,
        "CompleteNode",
        json!({
            "task_id": task_id,
            "node_id": first["node_id"],
            "lease_id": first["lease_id"],
            "result_payload": {"stale": true}
        }),
    )
    .await;
    assert!(stale.get("error").is_some());

    complete(&daemon.socket_path, task_id, &second).await;
}

#[tokio::test]
async fn ipc_rejects_oversized_request_line() {
    let daemon = TestDaemon::start(Duration::from_secs(30)).await;
    let mut stream = UnixStream::connect(&daemon.socket_path)
        .await
        .expect("socket should connect");
    let request = json!({
        "id": 1,
        "method": "Health",
        "params": {"padding": "x".repeat(1024 * 1024)}
    });
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("request should write");

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("response should read");
    let response: Value = serde_json::from_str(&line).expect("response should be JSON");
    assert_eq!(response["error"]["code"], -32001);
}

#[tokio::test]
async fn ipc_resource_locks_coordinate_cross_provider_file_edits() {
    let daemon = TestDaemon::start(Duration::from_secs(30)).await;

    let first = rpc_call(
        &daemon.socket_path,
        "AcquireResourceLock",
        json!({
            "resource_key": "file:src/lib.rs",
            "owner": "codex-cli",
            "provider": "openai",
            "ttl_secs": 30,
            "metadata": {"intent": "edit"}
        }),
    )
    .await;
    assert_eq!(first["resource_key"], "file:src/lib.rs");
    assert_eq!(first["owner"], "codex-cli");
    assert_eq!(first["provider"], "openai");

    let blocked = rpc_call(
        &daemon.socket_path,
        "AcquireResourceLock",
        json!({
            "resource_key": "file:src/lib.rs",
            "owner": "claude-code",
            "provider": "anthropic",
            "ttl_secs": 30
        }),
    )
    .await;
    assert!(blocked.is_null());

    rpc_call(
        &daemon.socket_path,
        "HeartbeatResourceLock",
        json!({
            "resource_key": "file:src/lib.rs",
            "lease_id": first["lease_id"],
            "ttl_secs": 30
        }),
    )
    .await;

    let locks = rpc_call(&daemon.socket_path, "ListResourceLocks", json!({})).await;
    assert_eq!(
        locks["locks"].as_array().expect("locks should array").len(),
        1
    );

    rpc_call(
        &daemon.socket_path,
        "ReleaseResourceLock",
        json!({
            "resource_key": "file:src/lib.rs",
            "lease_id": first["lease_id"]
        }),
    )
    .await;

    let second = rpc_call(
        &daemon.socket_path,
        "AcquireResourceLock",
        json!({
            "resource_key": "file:src/lib.rs",
            "owner": "claude-code",
            "provider": "anthropic",
            "ttl_secs": 30
        }),
    )
    .await;
    assert_eq!(second["owner"], "claude-code");
    assert_ne!(second["lease_id"], first["lease_id"]);
}

async fn complete(socket_path: &Path, task_id: &str, acquired: &Value) {
    rpc_call(
        socket_path,
        "CompleteNode",
        json!({
            "task_id": task_id,
            "node_id": acquired["node_id"],
            "lease_id": acquired["lease_id"],
            "result_payload": {"ok": true}
        }),
    )
    .await;
}

async fn rpc_call(socket_path: &Path, method: &str, params: Value) -> Value {
    let response = raw_rpc_call(socket_path, method, params).await;
    if let Some(error) = response.get("error") {
        panic!("RPC call failed: {error}");
    }
    response
        .get("result")
        .cloned()
        .expect("successful RPC response should include result")
}

async fn raw_rpc_call(socket_path: &Path, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .expect("socket should connect");
    let request = json!({"id": 1, "method": method, "params": params});
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("request should write");

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("response should read");
    serde_json::from_str(&line).expect("response should be JSON")
}

async fn wait_for_socket(socket_path: &Path) {
    for _ in 0..100 {
        if UnixStream::connect(socket_path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "IPC socket did not become ready at {}",
        socket_path.display()
    );
}

fn cleanup_db(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
