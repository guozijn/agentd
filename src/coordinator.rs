use crate::db::{DagNodeRow, Database, DatabaseMetrics, DbError, NodeDefinition, TaskSnapshot};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("task not found: {0}")]
    TaskNotFound(Uuid),
    #[error("node not found: {node_id} for task {task_id}")]
    NodeNotFound { task_id: Uuid, node_id: Uuid },
    #[error("node {node_id} for task {task_id} is not RUNNING")]
    NodeNotRunning { task_id: Uuid, node_id: Uuid },
}

#[derive(Debug, Clone)]
pub struct Coordinator {
    db: Database,
    running_timeout: Duration,
    context_event_limit: u32,
    started_at: Instant,
    metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Serialize)]
pub struct AcquiredNode {
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub lease_id: Uuid,
    pub lease_owner: String,
    pub lease_expires_at: i64,
    pub context: Value,
}

#[derive(Debug, Serialize)]
pub struct HealthStatus {
    pub status: &'static str,
    pub database: &'static str,
    pub running_timeout_secs: u64,
    pub context_event_limit: u32,
}

#[derive(Debug, Default)]
struct RuntimeMetrics {
    registered_tasks: AtomicU64,
    acquire_attempts: AtomicU64,
    acquired_nodes: AtomicU64,
    acquisition_latency_micros_total: AtomicU64,
    committed_events: AtomicU64,
    heartbeats: AtomicU64,
    completed_nodes: AtomicU64,
    failed_nodes: AtomicU64,
    timeout_rollbacks: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub running_timeout_secs: u64,
    pub context_event_limit: u32,
    pub runtime: RuntimeMetricsSnapshot,
    pub database: DatabaseMetrics,
}

#[derive(Debug, Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub registered_tasks: u64,
    pub acquire_attempts: u64,
    pub acquired_nodes: u64,
    pub acquisition_latency_micros_total: u64,
    pub average_acquisition_latency_micros: u64,
    pub committed_events: u64,
    pub heartbeats: u64,
    pub completed_nodes: u64,
    pub failed_nodes: u64,
    pub timeout_rollbacks: u64,
}

impl Coordinator {
    pub fn new(db: Database, running_timeout: Duration, context_event_limit: u32) -> Self {
        Self {
            db,
            running_timeout,
            context_event_limit,
            started_at: Instant::now(),
            metrics: Arc::new(RuntimeMetrics::default()),
        }
    }

    pub async fn register_task(
        &self,
        goal: String,
        context: Value,
        initial_nodes: Vec<NodeDefinition>,
    ) -> Result<Uuid, CoordinatorError> {
        let task_id = self.db.register_task(goal, context, initial_nodes).await?;
        self.metrics
            .registered_tasks
            .fetch_add(1, Ordering::Relaxed);
        Ok(task_id)
    }

    pub async fn acquire_next_node(
        &self,
        task_id: Uuid,
        lease_owner: String,
    ) -> Result<Option<AcquiredNode>, CoordinatorError> {
        let started_at = Instant::now();
        self.metrics
            .acquire_attempts
            .fetch_add(1, Ordering::Relaxed);
        self.ensure_task_exists(task_id).await?;
        let reset_count = self
            .db
            .reset_timed_out_nodes(Some(task_id), self.running_timeout)
            .await?;
        self.record_timeout_rollbacks(reset_count);

        let pending_nodes = self.db.pending_nodes(task_id).await?;
        for node in pending_nodes {
            let dependencies = node.dependencies()?;
            if !self
                .db
                .dependencies_completed(task_id, &dependencies)
                .await?
            {
                continue;
            }

            let node_id = node.id()?;
            if let Some(lease) = self
                .db
                .mark_node_running(task_id, node_id, &lease_owner, self.running_timeout)
                .await?
            {
                self.metrics.acquired_nodes.fetch_add(1, Ordering::Relaxed);
                self.record_acquisition_latency(started_at);
                let running_node = self
                    .db
                    .fetch_node(task_id, node_id)
                    .await?
                    .ok_or(CoordinatorError::NodeNotFound { task_id, node_id })?;
                let context = self.synthesise_context(task_id, &running_node).await?;
                return Ok(Some(AcquiredNode {
                    task_id,
                    node_id,
                    lease_id: lease.lease_id,
                    lease_owner: lease.lease_owner,
                    lease_expires_at: lease.lease_expires_at,
                    context,
                }));
            }
        }

        self.record_acquisition_latency(started_at);
        Ok(None)
    }

    pub async fn commit_event(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Option<Uuid>,
        action_type: Option<String>,
        payload: Value,
    ) -> Result<Uuid, CoordinatorError> {
        self.ensure_node_exists(task_id, node_id).await?;
        let action_type = action_type.unwrap_or_else(|| "COMMIT_EVENT".to_string());
        let event_id = self
            .db
            .append_event(task_id, node_id, lease_id, &action_type, payload)
            .await?;
        self.metrics
            .committed_events
            .fetch_add(1, Ordering::Relaxed);
        Ok(event_id)
    }

    pub async fn heartbeat_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
    ) -> Result<(), CoordinatorError> {
        self.ensure_node_exists(task_id, node_id).await?;
        if self
            .db
            .heartbeat_node(task_id, node_id, lease_id, self.running_timeout)
            .await?
        {
            self.metrics.heartbeats.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(CoordinatorError::NodeNotRunning { task_id, node_id })
        }
    }

    pub async fn complete_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
        result_payload: Value,
    ) -> Result<Uuid, CoordinatorError> {
        self.ensure_node_exists(task_id, node_id).await?;
        let event_id = self
            .db
            .complete_node(task_id, node_id, lease_id, result_payload)
            .await?;
        self.metrics.completed_nodes.fetch_add(1, Ordering::Relaxed);
        Ok(event_id)
    }

    pub async fn fail_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
        error_payload: Value,
    ) -> Result<Uuid, CoordinatorError> {
        self.ensure_node_exists(task_id, node_id).await?;
        let event_id = self
            .db
            .fail_node(task_id, node_id, lease_id, error_payload)
            .await?;
        self.metrics.failed_nodes.fetch_add(1, Ordering::Relaxed);
        Ok(event_id)
    }

    pub async fn task_status(&self, task_id: Uuid) -> Result<TaskSnapshot, CoordinatorError> {
        self.db
            .task_snapshot(task_id)
            .await?
            .ok_or(CoordinatorError::TaskNotFound(task_id))
    }

    pub async fn reset_timed_out_nodes(&self) -> Result<u64, CoordinatorError> {
        let count = self
            .db
            .reset_timed_out_nodes(None, self.running_timeout)
            .await?;
        self.record_timeout_rollbacks(count);
        Ok(count)
    }

    pub async fn health(&self) -> Result<HealthStatus, CoordinatorError> {
        self.db.ping().await?;
        Ok(HealthStatus {
            status: "ok",
            database: "ok",
            running_timeout_secs: self.running_timeout.as_secs(),
            context_event_limit: self.context_event_limit,
        })
    }

    pub const fn running_timeout(&self) -> Duration {
        self.running_timeout
    }

    pub async fn metrics(&self) -> Result<MetricsSnapshot, CoordinatorError> {
        let runtime = self.runtime_metrics_snapshot();
        let database = self.db.metrics().await?;

        Ok(MetricsSnapshot {
            uptime_secs: self.started_at.elapsed().as_secs(),
            running_timeout_secs: self.running_timeout.as_secs(),
            context_event_limit: self.context_event_limit,
            runtime,
            database,
        })
    }

    fn runtime_metrics_snapshot(&self) -> RuntimeMetricsSnapshot {
        let acquire_attempts = self.metrics.acquire_attempts.load(Ordering::Relaxed);
        let acquisition_latency_micros_total = self
            .metrics
            .acquisition_latency_micros_total
            .load(Ordering::Relaxed);
        let average_acquisition_latency_micros = acquisition_latency_micros_total
            .checked_div(acquire_attempts)
            .unwrap_or(0);

        RuntimeMetricsSnapshot {
            registered_tasks: self.metrics.registered_tasks.load(Ordering::Relaxed),
            acquire_attempts,
            acquired_nodes: self.metrics.acquired_nodes.load(Ordering::Relaxed),
            acquisition_latency_micros_total,
            average_acquisition_latency_micros,
            committed_events: self.metrics.committed_events.load(Ordering::Relaxed),
            heartbeats: self.metrics.heartbeats.load(Ordering::Relaxed),
            completed_nodes: self.metrics.completed_nodes.load(Ordering::Relaxed),
            failed_nodes: self.metrics.failed_nodes.load(Ordering::Relaxed),
            timeout_rollbacks: self.metrics.timeout_rollbacks.load(Ordering::Relaxed),
        }
    }

    fn record_acquisition_latency(&self, started_at: Instant) {
        let micros = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.metrics
            .acquisition_latency_micros_total
            .fetch_add(micros, Ordering::Relaxed);
    }

    fn record_timeout_rollbacks(&self, count: u64) {
        if count > 0 {
            self.metrics
                .timeout_rollbacks
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    async fn ensure_task_exists(&self, task_id: Uuid) -> Result<(), CoordinatorError> {
        if self.db.fetch_task(task_id).await?.is_some() {
            Ok(())
        } else {
            Err(CoordinatorError::TaskNotFound(task_id))
        }
    }

    async fn ensure_node_exists(
        &self,
        task_id: Uuid,
        node_id: Uuid,
    ) -> Result<(), CoordinatorError> {
        if self.db.fetch_node(task_id, node_id).await?.is_some() {
            Ok(())
        } else {
            Err(CoordinatorError::NodeNotFound { task_id, node_id })
        }
    }

    async fn synthesise_context(
        &self,
        task_id: Uuid,
        node: &DagNodeRow,
    ) -> Result<Value, CoordinatorError> {
        let task = self
            .db
            .fetch_task(task_id)
            .await?
            .ok_or(CoordinatorError::TaskNotFound(task_id))?;
        let node_id = node.id()?;
        let dependencies = node.dependencies()?;
        let completed_dependencies = self.db.dependency_results(task_id, &dependencies).await?;
        let events = self
            .db
            .events_for_task(task_id, self.context_event_limit)
            .await?;

        Ok(json!({
            "task": {
                "id": task.id()?,
                "goal": task.goal,
                "context": task.context()?,
                "created_at": task.created_at,
            },
            "node": {
                "id": node_id,
                "task_id": node.task_id()?,
                "status": node.status()?.as_str(),
                "dependencies": dependencies,
                "payload_schema": node.payload_schema()?,
                "result_payload": node.result_payload()?,
                "acquired_at": node.acquired_at,
                "lease_id": node.lease_id()?,
                "lease_owner": node.lease_owner,
                "lease_expires_at": node.lease_expires_at,
                "updated_at": node.updated_at,
            },
            "completed_dependencies": completed_dependencies,
            "event_journal_limit": self.context_event_limit,
            "event_journal": events,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, NodeDefinition};
    use serde_json::json;

    async fn test_coordinator(timeout: Duration) -> (Coordinator, std::path::PathBuf) {
        let db_path = std::env::temp_dir().join(format!("agentd_test_{}.db", Uuid::new_v4()));
        let database_url = format!("sqlite://{}", db_path.display());
        let db = Database::initialise(&database_url)
            .await
            .expect("test database should initialise");
        (Coordinator::new(db, timeout, 10), db_path)
    }

    fn one_node() -> Vec<NodeDefinition> {
        vec![NodeDefinition {
            id: None,
            dependencies: Vec::new(),
            payload_schema: json!({ "type": "object" }),
        }]
    }

    #[tokio::test]
    async fn stale_lease_cannot_complete_after_timeout_reacquire() {
        let (coordinator, db_path) = test_coordinator(Duration::from_secs(0)).await;
        let task_id = coordinator
            .register_task("lease test".to_string(), json!({}), one_node())
            .await
            .expect("task should register");

        let first = coordinator
            .acquire_next_node(task_id, "worker-a".to_string())
            .await
            .expect("acquire should succeed")
            .expect("node should be runnable");

        let second = coordinator
            .acquire_next_node(task_id, "worker-b".to_string())
            .await
            .expect("second acquire should trigger timeout rollback")
            .expect("node should be reacquired");

        assert_eq!(first.node_id, second.node_id);
        assert_ne!(first.lease_id, second.lease_id);

        let stale_result = coordinator
            .complete_node(
                task_id,
                first.node_id,
                first.lease_id,
                json!({ "worker": "stale" }),
            )
            .await;
        assert!(stale_result.is_err());

        coordinator
            .complete_node(
                task_id,
                second.node_id,
                second.lease_id,
                json!({ "worker": "current" }),
            )
            .await
            .expect("current lease should complete");

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn concurrent_acquire_returns_distinct_pending_nodes() {
        let (coordinator, db_path) = test_coordinator(Duration::from_secs(60)).await;
        let task_id = coordinator
            .register_task(
                "parallel test".to_string(),
                json!({}),
                vec![
                    NodeDefinition {
                        id: None,
                        dependencies: Vec::new(),
                        payload_schema: json!({ "name": "a" }),
                    },
                    NodeDefinition {
                        id: None,
                        dependencies: Vec::new(),
                        payload_schema: json!({ "name": "b" }),
                    },
                ],
            )
            .await
            .expect("task should register");

        let (first, second) = tokio::join!(
            coordinator.acquire_next_node(task_id, "worker-a".to_string()),
            coordinator.acquire_next_node(task_id, "worker-b".to_string())
        );
        let first = first
            .expect("first acquire should succeed")
            .expect("first node should exist");
        let second = second
            .expect("second acquire should succeed")
            .expect("second node should exist");

        assert_ne!(first.node_id, second.node_id);
        assert_ne!(first.lease_id, second.lease_id);

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn metrics_track_state_machine_activity() {
        let (coordinator, db_path) = test_coordinator(Duration::from_secs(60)).await;
        let task_id = coordinator
            .register_task("metrics test".to_string(), json!({}), one_node())
            .await
            .expect("task should register");
        let acquired = coordinator
            .acquire_next_node(task_id, "metrics-worker".to_string())
            .await
            .expect("acquire should succeed")
            .expect("node should be runnable");

        coordinator
            .commit_event(
                task_id,
                acquired.node_id,
                Some(acquired.lease_id),
                Some("TEST_EVENT".to_string()),
                json!({ "ok": true }),
            )
            .await
            .expect("event should commit");
        coordinator
            .complete_node(
                task_id,
                acquired.node_id,
                acquired.lease_id,
                json!({ "done": true }),
            )
            .await
            .expect("node should complete");

        let metrics = coordinator.metrics().await.expect("metrics should load");
        assert_eq!(metrics.runtime.registered_tasks, 1);
        assert_eq!(metrics.runtime.acquire_attempts, 1);
        assert_eq!(metrics.runtime.acquired_nodes, 1);
        assert_eq!(metrics.runtime.committed_events, 1);
        assert_eq!(metrics.runtime.completed_nodes, 1);
        assert_eq!(metrics.database.total_tasks, 1);
        assert_eq!(metrics.database.total_nodes, 1);
        assert_eq!(metrics.database.total_events, 2);
        assert_eq!(
            metrics.database.node_status_counts.get("COMPLETED"),
            Some(&1)
        );

        let _ = std::fs::remove_file(db_path);
    }
}
