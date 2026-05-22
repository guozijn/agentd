use crate::db::{DagNodeRow, Database, DbError, NodeDefinition, TaskSnapshot};
use serde::Serialize;
use serde_json::{json, Value};
use std::time::Duration;
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

impl Coordinator {
    pub fn new(db: Database, running_timeout: Duration, context_event_limit: u32) -> Self {
        Self {
            db,
            running_timeout,
            context_event_limit,
        }
    }

    pub async fn register_task(
        &self,
        goal: String,
        context: Value,
        initial_nodes: Vec<NodeDefinition>,
    ) -> Result<Uuid, CoordinatorError> {
        Ok(self.db.register_task(goal, context, initial_nodes).await?)
    }

    pub async fn acquire_next_node(
        &self,
        task_id: Uuid,
        lease_owner: String,
    ) -> Result<Option<AcquiredNode>, CoordinatorError> {
        self.ensure_task_exists(task_id).await?;
        self.db
            .reset_timed_out_nodes(Some(task_id), self.running_timeout)
            .await?;

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
        Ok(self
            .db
            .append_event(task_id, node_id, lease_id, &action_type, payload)
            .await?)
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
        Ok(self
            .db
            .complete_node(task_id, node_id, lease_id, result_payload)
            .await?)
    }

    pub async fn fail_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
        error_payload: Value,
    ) -> Result<Uuid, CoordinatorError> {
        self.ensure_node_exists(task_id, node_id).await?;
        Ok(self
            .db
            .fail_node(task_id, node_id, lease_id, error_payload)
            .await?)
    }

    pub async fn task_status(&self, task_id: Uuid) -> Result<TaskSnapshot, CoordinatorError> {
        self.db
            .task_snapshot(task_id)
            .await?
            .ok_or(CoordinatorError::TaskNotFound(task_id))
    }

    pub async fn reset_timed_out_nodes(&self) -> Result<u64, CoordinatorError> {
        Ok(self
            .db
            .reset_timed_out_nodes(None, self.running_timeout)
            .await?)
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
}
