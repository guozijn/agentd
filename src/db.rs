use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{FromRow, SqlitePool};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

pub const LATEST_SCHEMA_VERSION: i64 = 2;

struct Migration {
    version: i64,
    name: &'static str,
    statements: &'static [&'static str],
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial_state_engine",
        statements: &[
            r#"
    CREATE TABLE global_tasks (
        id TEXT PRIMARY KEY NOT NULL,
        goal TEXT NOT NULL,
        context TEXT NOT NULL CHECK (json_valid(context)),
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    "#,
        r#"
    CREATE TABLE dag_nodes (
        id TEXT PRIMARY KEY NOT NULL,
        task_id TEXT NOT NULL REFERENCES global_tasks(id) ON DELETE CASCADE,
        status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
        dependencies TEXT NOT NULL CHECK (json_valid(dependencies) AND json_type(dependencies) = 'array'),
        payload_schema TEXT NOT NULL CHECK (json_valid(payload_schema)),
        result_payload TEXT CHECK (result_payload IS NULL OR json_valid(result_payload)),
        acquired_at INTEGER,
        lease_id TEXT,
        lease_owner TEXT,
        lease_expires_at INTEGER,
        updated_at INTEGER NOT NULL DEFAULT (unixepoch('now')),
        UNIQUE(task_id, id)
    )
    "#,
        r#"
    CREATE TABLE event_journal (
        id TEXT PRIMARY KEY NOT NULL,
        task_id TEXT NOT NULL REFERENCES global_tasks(id) ON DELETE CASCADE,
        node_id TEXT NOT NULL REFERENCES dag_nodes(id) ON DELETE CASCADE,
        action_type TEXT NOT NULL,
        payload TEXT NOT NULL CHECK (json_valid(payload)),
        timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    "#,
            "CREATE INDEX idx_dag_nodes_task_status ON dag_nodes(task_id, status)",
            "CREATE INDEX idx_dag_nodes_running_timeout ON dag_nodes(status, acquired_at)",
            "CREATE INDEX idx_dag_nodes_lease_timeout ON dag_nodes(status, lease_expires_at)",
            "CREATE INDEX idx_event_journal_task_node ON event_journal(task_id, node_id, timestamp)",
        ],
    },
    Migration {
        version: 2,
        name: "resource_locks",
        statements: &[
            r#"
    CREATE TABLE resource_locks (
        resource_key TEXT PRIMARY KEY NOT NULL,
        lease_id TEXT NOT NULL,
        owner TEXT NOT NULL,
        provider TEXT,
        task_id TEXT REFERENCES global_tasks(id) ON DELETE CASCADE,
        node_id TEXT,
        metadata TEXT NOT NULL CHECK (json_valid(metadata)),
        acquired_at INTEGER NOT NULL DEFAULT (unixepoch('now')),
        lease_expires_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL DEFAULT (unixepoch('now'))
    )
    "#,
            "CREATE INDEX idx_resource_locks_lease_timeout ON resource_locks(lease_expires_at)",
            "CREATE INDEX idx_resource_locks_owner ON resource_locks(owner)",
        ],
    },
];

#[derive(Debug, Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("JSON serialisation error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("UUID parse error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone)]
pub struct Database {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl NodeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl TryFrom<&str> for NodeStatus {
    type Error = DbError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(DbError::InvalidState(format!(
                "unknown node status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    #[serde(default)]
    pub dependencies: Vec<Uuid>,
    #[serde(default = "empty_object")]
    pub payload_schema: Value,
}

#[derive(Debug, Clone, FromRow)]
pub struct GlobalTaskRow {
    pub id: String,
    pub goal: String,
    pub context: String,
    pub created_at: String,
}

impl GlobalTaskRow {
    pub fn id(&self) -> Result<Uuid, DbError> {
        Ok(Uuid::parse_str(&self.id)?)
    }

    pub fn context(&self) -> Result<Value, DbError> {
        Ok(serde_json::from_str(&self.context)?)
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct DagNodeRow {
    pub id: String,
    pub task_id: String,
    pub status: String,
    pub dependencies: String,
    pub payload_schema: String,
    pub result_payload: Option<String>,
    pub acquired_at: Option<i64>,
    pub lease_id: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub updated_at: i64,
}

impl DagNodeRow {
    pub fn id(&self) -> Result<Uuid, DbError> {
        Ok(Uuid::parse_str(&self.id)?)
    }

    pub fn task_id(&self) -> Result<Uuid, DbError> {
        Ok(Uuid::parse_str(&self.task_id)?)
    }

    pub fn status(&self) -> Result<NodeStatus, DbError> {
        NodeStatus::try_from(self.status.as_str())
    }

    pub fn dependencies(&self) -> Result<Vec<Uuid>, DbError> {
        Ok(serde_json::from_str(&self.dependencies)?)
    }

    pub fn payload_schema(&self) -> Result<Value, DbError> {
        Ok(serde_json::from_str(&self.payload_schema)?)
    }

    pub fn result_payload(&self) -> Result<Option<Value>, DbError> {
        self.result_payload
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(DbError::from)
    }

    pub fn lease_id(&self) -> Result<Option<Uuid>, DbError> {
        self.lease_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(DbError::from)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DependencyResult {
    pub node_id: Uuid,
    pub result_payload: Option<Value>,
}

#[derive(Debug, Clone, FromRow)]
pub struct EventJournalRow {
    pub id: String,
    pub task_id: String,
    pub node_id: String,
    pub action_type: String,
    pub payload: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalEvent {
    pub id: Uuid,
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub action_type: String,
    pub payload: Value,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskSnapshot {
    pub task: GlobalTaskSnapshot,
    pub counts: BTreeMap<String, u64>,
    pub nodes: Vec<NodeSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GlobalTaskSnapshot {
    pub id: Uuid,
    pub goal: String,
    pub context: Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeSnapshot {
    pub id: Uuid,
    pub task_id: Uuid,
    pub status: NodeStatus,
    pub dependencies: Vec<Uuid>,
    pub payload_schema: Value,
    pub result_payload: Option<Value>,
    pub acquired_at: Option<i64>,
    pub lease_id: Option<Uuid>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LeaseInfo {
    pub lease_id: Uuid,
    pub lease_owner: String,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatabaseMetrics {
    pub schema_version: i64,
    pub latest_schema_version: i64,
    pub total_tasks: i64,
    pub total_nodes: i64,
    pub total_events: i64,
    pub active_resource_locks: i64,
    pub expired_resource_locks: i64,
    pub running_leases: i64,
    pub expired_running_leases: i64,
    pub node_status_counts: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ResourceLockRow {
    pub resource_key: String,
    pub lease_id: String,
    pub owner: String,
    pub provider: Option<String>,
    pub task_id: Option<String>,
    pub node_id: Option<String>,
    pub metadata: String,
    pub acquired_at: i64,
    pub lease_expires_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceLockSnapshot {
    pub resource_key: String,
    pub lease_id: Uuid,
    pub owner: String,
    pub provider: Option<String>,
    pub task_id: Option<Uuid>,
    pub node_id: Option<Uuid>,
    pub metadata: Value,
    pub acquired_at: i64,
    pub lease_expires_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceLockLease {
    pub resource_key: String,
    pub lease_id: Uuid,
    pub owner: String,
    pub provider: Option<String>,
    pub task_id: Option<Uuid>,
    pub node_id: Option<Uuid>,
    pub metadata: Value,
    pub acquired_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct AcquireResourceLock {
    pub resource_key: String,
    pub owner: String,
    pub provider: Option<String>,
    pub lease_ttl: Duration,
    pub task_id: Option<Uuid>,
    pub node_id: Option<Uuid>,
    pub metadata: Value,
}

impl TryFrom<ResourceLockRow> for ResourceLockSnapshot {
    type Error = DbError;

    fn try_from(row: ResourceLockRow) -> Result<Self, Self::Error> {
        Ok(Self {
            resource_key: row.resource_key,
            lease_id: Uuid::parse_str(&row.lease_id)?,
            owner: row.owner,
            provider: row.provider,
            task_id: row.task_id.as_deref().map(Uuid::parse_str).transpose()?,
            node_id: row.node_id.as_deref().map(Uuid::parse_str).transpose()?,
            metadata: serde_json::from_str(&row.metadata)?,
            acquired_at: row.acquired_at,
            lease_expires_at: row.lease_expires_at,
            updated_at: row.updated_at,
        })
    }
}

impl TryFrom<ResourceLockRow> for ResourceLockLease {
    type Error = DbError;

    fn try_from(row: ResourceLockRow) -> Result<Self, Self::Error> {
        let snapshot = ResourceLockSnapshot::try_from(row)?;
        Ok(Self {
            resource_key: snapshot.resource_key,
            lease_id: snapshot.lease_id,
            owner: snapshot.owner,
            provider: snapshot.provider,
            task_id: snapshot.task_id,
            node_id: snapshot.node_id,
            metadata: snapshot.metadata,
            acquired_at: snapshot.acquired_at,
            lease_expires_at: snapshot.lease_expires_at,
        })
    }
}

impl TryFrom<EventJournalRow> for JournalEvent {
    type Error = DbError;

    fn try_from(row: EventJournalRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Uuid::parse_str(&row.id)?,
            task_id: Uuid::parse_str(&row.task_id)?,
            node_id: Uuid::parse_str(&row.node_id)?,
            action_type: row.action_type,
            payload: serde_json::from_str(&row.payload)?,
            timestamp: row.timestamp,
        })
    }
}

impl TryFrom<GlobalTaskRow> for GlobalTaskSnapshot {
    type Error = DbError;

    fn try_from(row: GlobalTaskRow) -> Result<Self, Self::Error> {
        let id = row.id()?;
        let context = row.context()?;

        Ok(Self {
            id,
            goal: row.goal,
            context,
            created_at: row.created_at,
        })
    }
}

impl TryFrom<DagNodeRow> for NodeSnapshot {
    type Error = DbError;

    fn try_from(row: DagNodeRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id()?,
            task_id: row.task_id()?,
            status: row.status()?,
            dependencies: row.dependencies()?,
            payload_schema: row.payload_schema()?,
            result_payload: row.result_payload()?,
            acquired_at: row.acquired_at,
            lease_id: row.lease_id()?,
            lease_owner: row.lease_owner,
            lease_expires_at: row.lease_expires_at,
            updated_at: row.updated_at,
        })
    }
}

impl Database {
    pub async fn initialise(database_url: &str) -> Result<Self, DbError> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;

        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await?;

        initialise_schema(&pool).await?;

        Ok(Self { pool })
    }

    pub async fn ping(&self) -> Result<(), DbError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn schema_version(&self) -> Result<i64, DbError> {
        schema_version(&self.pool).await
    }

    pub async fn metrics(&self) -> Result<DatabaseMetrics, DbError> {
        let schema_version = self.schema_version().await?;
        let total_tasks = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM global_tasks")
            .fetch_one(&self.pool)
            .await?;
        let total_nodes = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM dag_nodes")
            .fetch_one(&self.pool)
            .await?;
        let total_events = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_journal")
            .fetch_one(&self.pool)
            .await?;
        let active_resource_locks = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_locks WHERE lease_expires_at > unixepoch('now')",
        )
        .fetch_one(&self.pool)
        .await?;
        let expired_resource_locks = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_locks WHERE lease_expires_at <= unixepoch('now')",
        )
        .fetch_one(&self.pool)
        .await?;
        let running_leases = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM dag_nodes WHERE status = 'RUNNING' AND lease_id IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        let expired_running_leases = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM dag_nodes
            WHERE status = 'RUNNING'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= unixepoch('now')
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        let status_rows = sqlx::query_as::<_, (String, i64)>(
            "SELECT status, COUNT(*) FROM dag_nodes GROUP BY status",
        )
        .fetch_all(&self.pool)
        .await?;
        let node_status_counts = status_rows.into_iter().collect();

        Ok(DatabaseMetrics {
            schema_version,
            latest_schema_version: LATEST_SCHEMA_VERSION,
            total_tasks,
            total_nodes,
            total_events,
            active_resource_locks,
            expired_resource_locks,
            running_leases,
            expired_running_leases,
            node_status_counts,
        })
    }

    pub async fn register_task(
        &self,
        goal: String,
        context: Value,
        initial_nodes: Vec<NodeDefinition>,
    ) -> Result<Uuid, DbError> {
        let task_id = Uuid::new_v4();
        let nodes = materialise_nodes(initial_nodes)?;
        let context_json = serde_json::to_string(&context)?;

        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO global_tasks (id, goal, context) VALUES (?, ?, ?)")
            .bind(task_id.to_string())
            .bind(goal)
            .bind(context_json)
            .execute(&mut *tx)
            .await?;

        for (node_id, node) in nodes {
            let dependencies_json = serde_json::to_string(&node.dependencies)?;
            let payload_schema_json = serde_json::to_string(&node.payload_schema)?;

            sqlx::query(
                r#"
                INSERT INTO dag_nodes (id, task_id, status, dependencies, payload_schema)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(node_id.to_string())
            .bind(task_id.to_string())
            .bind(NodeStatus::Pending.as_str())
            .bind(dependencies_json)
            .bind(payload_schema_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(task_id)
    }

    pub async fn fetch_task(&self, task_id: Uuid) -> Result<Option<GlobalTaskRow>, DbError> {
        Ok(sqlx::query_as::<_, GlobalTaskRow>(
            "SELECT id, goal, context, created_at FROM global_tasks WHERE id = ?",
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn fetch_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
    ) -> Result<Option<DagNodeRow>, DbError> {
        Ok(sqlx::query_as::<_, DagNodeRow>(
            r#"
            SELECT id, task_id, status, dependencies, payload_schema, result_payload,
                   acquired_at, lease_id, lease_owner, lease_expires_at, updated_at
            FROM dag_nodes
            WHERE task_id = ? AND id = ?
            "#,
        )
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn list_nodes(&self, task_id: Uuid) -> Result<Vec<DagNodeRow>, DbError> {
        Ok(sqlx::query_as::<_, DagNodeRow>(
            r#"
            SELECT id, task_id, status, dependencies, payload_schema, result_payload,
                   acquired_at, lease_id, lease_owner, lease_expires_at, updated_at
            FROM dag_nodes
            WHERE task_id = ?
            ORDER BY rowid ASC
            "#,
        )
        .bind(task_id.to_string())
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn pending_nodes(&self, task_id: Uuid) -> Result<Vec<DagNodeRow>, DbError> {
        Ok(sqlx::query_as::<_, DagNodeRow>(
            r#"
            SELECT id, task_id, status, dependencies, payload_schema, result_payload,
                   acquired_at, lease_id, lease_owner, lease_expires_at, updated_at
            FROM dag_nodes
            WHERE task_id = ? AND status = 'PENDING'
            ORDER BY rowid ASC
            "#,
        )
        .bind(task_id.to_string())
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn dependencies_completed(
        &self,
        task_id: Uuid,
        dependencies: &[Uuid],
    ) -> Result<bool, DbError> {
        for dependency_id in dependencies {
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM dag_nodes WHERE task_id = ? AND id = ?")
                    .bind(task_id.to_string())
                    .bind(dependency_id.to_string())
                    .fetch_optional(&self.pool)
                    .await?;

            if status.as_deref() != Some(NodeStatus::Completed.as_str()) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub async fn mark_node_running(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_owner: &str,
        lease_ttl: Duration,
    ) -> Result<Option<LeaseInfo>, DbError> {
        let lease_id = Uuid::new_v4();
        let lease_ttl_secs = i64::try_from(lease_ttl.as_secs()).unwrap_or(i64::MAX);
        let result = sqlx::query(
            r#"
            UPDATE dag_nodes
            SET status = 'RUNNING',
                acquired_at = unixepoch('now'),
                lease_id = ?,
                lease_owner = ?,
                lease_expires_at = unixepoch('now') + ?,
                updated_at = unixepoch('now')
            WHERE task_id = ? AND id = ? AND status = 'PENDING'
            "#,
        )
        .bind(lease_id.to_string())
        .bind(lease_owner)
        .bind(lease_ttl_secs)
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Ok(None);
        }

        let lease_expires_at = sqlx::query_scalar::<_, i64>(
            "SELECT lease_expires_at FROM dag_nodes WHERE task_id = ? AND id = ?",
        )
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .fetch_one(&self.pool)
        .await?;

        Ok(Some(LeaseInfo {
            lease_id,
            lease_owner: lease_owner.to_string(),
            lease_expires_at,
        }))
    }

    pub async fn append_event(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Option<Uuid>,
        action_type: &str,
        payload: Value,
    ) -> Result<Uuid, DbError> {
        self.ensure_current_lease(task_id, node_id, lease_id)
            .await?;
        let event_id = Uuid::new_v4();
        let payload_json = serde_json::to_string(&payload)?;

        sqlx::query(
            r#"
            INSERT INTO event_journal (id, task_id, node_id, action_type, payload)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(event_id.to_string())
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .bind(action_type)
        .bind(payload_json)
        .execute(&self.pool)
        .await?;

        Ok(event_id)
    }

    pub async fn heartbeat_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
        lease_ttl: Duration,
    ) -> Result<bool, DbError> {
        let lease_ttl_secs = i64::try_from(lease_ttl.as_secs()).unwrap_or(i64::MAX);
        let result = sqlx::query(
            r#"
            UPDATE dag_nodes
            SET acquired_at = unixepoch('now'),
                lease_expires_at = unixepoch('now') + ?,
                updated_at = unixepoch('now')
            WHERE task_id = ? AND id = ? AND status = 'RUNNING' AND lease_id = ?
            "#,
        )
        .bind(lease_ttl_secs)
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .bind(lease_id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn complete_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
        result_payload: Value,
    ) -> Result<Uuid, DbError> {
        let result_json = serde_json::to_string(&result_payload)?;
        let event_payload = serde_json::json!({ "result": result_payload });
        let event_payload_json = serde_json::to_string(&event_payload)?;
        let event_id = Uuid::new_v4();

        let mut tx = self.pool.begin().await?;
        let update = sqlx::query(
            r#"
            UPDATE dag_nodes
            SET status = 'COMPLETED',
                result_payload = ?,
                acquired_at = NULL,
                lease_id = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = unixepoch('now')
            WHERE task_id = ? AND id = ? AND status = 'RUNNING' AND lease_id = ?
            "#,
        )
        .bind(result_json)
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .bind(lease_id.to_string())
        .execute(&mut *tx)
        .await?;

        if update.rows_affected() != 1 {
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM dag_nodes WHERE task_id = ? AND id = ?")
                    .bind(task_id.to_string())
                    .bind(node_id.to_string())
                    .fetch_optional(&mut *tx)
                    .await?;

            return match status {
                Some(status) => Err(DbError::InvalidState(format!(
                    "node {node_id} cannot transition from {status} to COMPLETED"
                ))),
                None => Err(DbError::NotFound(format!(
                    "node {node_id} for task {task_id}"
                ))),
            };
        }

        sqlx::query(
            r#"
            INSERT INTO event_journal (id, task_id, node_id, action_type, payload)
            VALUES (?, ?, ?, 'COMPLETE_NODE', ?)
            "#,
        )
        .bind(event_id.to_string())
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .bind(event_payload_json)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(event_id)
    }

    pub async fn fail_node(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Uuid,
        error_payload: Value,
    ) -> Result<Uuid, DbError> {
        let result_json = serde_json::to_string(&error_payload)?;
        let event_payload = serde_json::json!({ "error": error_payload });
        let event_payload_json = serde_json::to_string(&event_payload)?;
        let event_id = Uuid::new_v4();

        let mut tx = self.pool.begin().await?;
        let update = sqlx::query(
            r#"
            UPDATE dag_nodes
            SET status = 'FAILED',
                result_payload = ?,
                acquired_at = NULL,
                lease_id = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = unixepoch('now')
            WHERE task_id = ?
              AND id = ?
              AND status = 'RUNNING'
              AND lease_id = ?
            "#,
        )
        .bind(result_json)
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .bind(lease_id.to_string())
        .execute(&mut *tx)
        .await?;

        if update.rows_affected() != 1 {
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM dag_nodes WHERE task_id = ? AND id = ?")
                    .bind(task_id.to_string())
                    .bind(node_id.to_string())
                    .fetch_optional(&mut *tx)
                    .await?;

            return match status {
                Some(status) => Err(DbError::InvalidState(format!(
                    "node {node_id} cannot transition from {status} to FAILED"
                ))),
                None => Err(DbError::NotFound(format!(
                    "node {node_id} for task {task_id}"
                ))),
            };
        }

        sqlx::query(
            r#"
            INSERT INTO event_journal (id, task_id, node_id, action_type, payload)
            VALUES (?, ?, ?, 'FAIL_NODE', ?)
            "#,
        )
        .bind(event_id.to_string())
        .bind(task_id.to_string())
        .bind(node_id.to_string())
        .bind(event_payload_json)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(event_id)
    }

    pub async fn reset_timed_out_nodes(
        &self,
        task_id: Option<Uuid>,
        timeout: Duration,
    ) -> Result<u64, DbError> {
        let timeout_secs = i64::try_from(timeout.as_secs()).unwrap_or(i64::MAX);

        let result = if let Some(task_id) = task_id {
            sqlx::query(
                r#"
                UPDATE dag_nodes
                SET status = 'PENDING',
                    acquired_at = NULL,
                    lease_id = NULL,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = unixepoch('now')
                WHERE task_id = ?
                  AND status = 'RUNNING'
                  AND (
                    lease_expires_at <= unixepoch('now')
                    OR (
                        lease_expires_at IS NULL
                        AND acquired_at IS NOT NULL
                        AND acquired_at <= unixepoch('now') - ?
                    )
                  )
                "#,
            )
            .bind(task_id.to_string())
            .bind(timeout_secs)
            .execute(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                UPDATE dag_nodes
                SET status = 'PENDING',
                    acquired_at = NULL,
                    lease_id = NULL,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = unixepoch('now')
                WHERE status = 'RUNNING'
                  AND (
                    lease_expires_at <= unixepoch('now')
                    OR (
                        lease_expires_at IS NULL
                        AND acquired_at IS NOT NULL
                        AND acquired_at <= unixepoch('now') - ?
                    )
                  )
                "#,
            )
            .bind(timeout_secs)
            .execute(&self.pool)
            .await?
        };

        Ok(result.rows_affected())
    }

    pub async fn acquire_resource_lock(
        &self,
        request: AcquireResourceLock,
    ) -> Result<Option<ResourceLockLease>, DbError> {
        let lease_id = Uuid::new_v4();
        let lease_ttl_secs = i64::try_from(request.lease_ttl.as_secs())
            .unwrap_or(i64::MAX)
            .max(1);
        let metadata_json = serde_json::to_string(&request.metadata)?;
        let task_id_string = request.task_id.map(|id| id.to_string());
        let node_id_string = request.node_id.map(|id| id.to_string());

        let mut tx = self.pool.begin().await?;
        let update = sqlx::query(
            r#"
            UPDATE resource_locks
            SET lease_id = ?,
                owner = ?,
                provider = ?,
                task_id = ?,
                node_id = ?,
                metadata = ?,
                acquired_at = unixepoch('now'),
                lease_expires_at = unixepoch('now') + ?,
                updated_at = unixepoch('now')
            WHERE resource_key = ? AND lease_expires_at <= unixepoch('now')
            "#,
        )
        .bind(lease_id.to_string())
        .bind(&request.owner)
        .bind(request.provider.as_deref())
        .bind(task_id_string.as_deref())
        .bind(node_id_string.as_deref())
        .bind(&metadata_json)
        .bind(lease_ttl_secs)
        .bind(&request.resource_key)
        .execute(&mut *tx)
        .await?;

        if update.rows_affected() == 0 {
            sqlx::query(
                r#"
                INSERT OR IGNORE INTO resource_locks (
                    resource_key, lease_id, owner, provider, task_id, node_id, metadata,
                    acquired_at, lease_expires_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, unixepoch('now'), unixepoch('now') + ?, unixepoch('now'))
                "#,
            )
            .bind(&request.resource_key)
            .bind(lease_id.to_string())
            .bind(&request.owner)
            .bind(request.provider.as_deref())
            .bind(task_id_string.as_deref())
            .bind(node_id_string.as_deref())
            .bind(&metadata_json)
            .bind(lease_ttl_secs)
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query_as::<_, ResourceLockRow>(
            r#"
            SELECT resource_key, lease_id, owner, provider, task_id, node_id, metadata,
                   acquired_at, lease_expires_at, updated_at
            FROM resource_locks
            WHERE resource_key = ?
            "#,
        )
        .bind(&request.resource_key)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        if row.lease_id == lease_id.to_string() {
            Ok(Some(ResourceLockLease::try_from(row)?))
        } else {
            Ok(None)
        }
    }

    pub async fn heartbeat_resource_lock(
        &self,
        resource_key: &str,
        lease_id: Uuid,
        lease_ttl: Duration,
    ) -> Result<bool, DbError> {
        let lease_ttl_secs = i64::try_from(lease_ttl.as_secs())
            .unwrap_or(i64::MAX)
            .max(1);
        let result = sqlx::query(
            r#"
            UPDATE resource_locks
            SET lease_expires_at = unixepoch('now') + ?,
                updated_at = unixepoch('now')
            WHERE resource_key = ? AND lease_id = ? AND lease_expires_at > unixepoch('now')
            "#,
        )
        .bind(lease_ttl_secs)
        .bind(resource_key)
        .bind(lease_id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn release_resource_lock(
        &self,
        resource_key: &str,
        lease_id: Uuid,
    ) -> Result<bool, DbError> {
        let result =
            sqlx::query("DELETE FROM resource_locks WHERE resource_key = ? AND lease_id = ?")
                .bind(resource_key)
                .bind(lease_id.to_string())
                .execute(&self.pool)
                .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn list_resource_locks(&self) -> Result<Vec<ResourceLockSnapshot>, DbError> {
        let rows = sqlx::query_as::<_, ResourceLockRow>(
            r#"
            SELECT resource_key, lease_id, owner, provider, task_id, node_id, metadata,
                   acquired_at, lease_expires_at, updated_at
            FROM resource_locks
            WHERE lease_expires_at > unixepoch('now')
            ORDER BY resource_key ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(ResourceLockSnapshot::try_from)
            .collect()
    }

    pub async fn dependency_results(
        &self,
        task_id: Uuid,
        dependencies: &[Uuid],
    ) -> Result<Vec<DependencyResult>, DbError> {
        let mut results = Vec::with_capacity(dependencies.len());

        for dependency_id in dependencies {
            let row: Option<(Option<String>,)> =
                sqlx::query_as("SELECT result_payload FROM dag_nodes WHERE task_id = ? AND id = ?")
                    .bind(task_id.to_string())
                    .bind(dependency_id.to_string())
                    .fetch_optional(&self.pool)
                    .await?;

            if let Some((payload_json,)) = row {
                let result_payload = payload_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?;
                results.push(DependencyResult {
                    node_id: *dependency_id,
                    result_payload,
                });
            }
        }

        Ok(results)
    }

    pub async fn events_for_task(
        &self,
        task_id: Uuid,
        limit: u32,
    ) -> Result<Vec<JournalEvent>, DbError> {
        let rows = sqlx::query_as::<_, EventJournalRow>(
            r#"
            SELECT id, task_id, node_id, action_type, payload, timestamp FROM (
                SELECT id, task_id, node_id, action_type, payload, timestamp, rowid
                FROM event_journal
                WHERE task_id = ?
                ORDER BY timestamp DESC, rowid DESC
                LIMIT ?
            )
            ORDER BY timestamp ASC, rowid ASC
            "#,
        )
        .bind(task_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(JournalEvent::try_from).collect()
    }

    pub async fn task_snapshot(&self, task_id: Uuid) -> Result<Option<TaskSnapshot>, DbError> {
        let Some(task) = self.fetch_task(task_id).await? else {
            return Ok(None);
        };
        let nodes = self.list_nodes(task_id).await?;
        let mut counts = BTreeMap::new();
        let mut snapshots = Vec::with_capacity(nodes.len());

        for node in nodes {
            let snapshot = NodeSnapshot::try_from(node)?;
            *counts
                .entry(snapshot.status.as_str().to_string())
                .or_insert(0) += 1;
            snapshots.push(snapshot);
        }

        Ok(Some(TaskSnapshot {
            task: GlobalTaskSnapshot::try_from(task)?,
            counts,
            nodes: snapshots,
        }))
    }

    async fn ensure_current_lease(
        &self,
        task_id: Uuid,
        node_id: Uuid,
        lease_id: Option<Uuid>,
    ) -> Result<(), DbError> {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, lease_id FROM dag_nodes WHERE task_id = ? AND id = ?")
                .bind(task_id.to_string())
                .bind(node_id.to_string())
                .fetch_optional(&self.pool)
                .await?;

        let Some((status, current_lease_id)) = row else {
            return Err(DbError::NotFound(format!(
                "node {node_id} for task {task_id}"
            )));
        };

        if status != NodeStatus::Running.as_str() {
            return Ok(());
        }

        match (lease_id, current_lease_id.as_deref()) {
            (Some(lease_id), Some(current)) if lease_id.to_string() == current => Ok(()),
            _ => Err(DbError::InvalidState(format!(
                "node {node_id} for task {task_id} requires the current RUNNING lease"
            ))),
        }
    }
}

async fn initialise_schema(pool: &SqlitePool) -> Result<(), DbError> {
    let had_migration_table = table_exists(pool, "agentd_schema_migrations").await?;
    let had_core_schema = table_exists(pool, "global_tasks").await?
        || table_exists(pool, "dag_nodes").await?
        || table_exists(pool, "event_journal").await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS agentd_schema_migrations (
            version INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )
        "#,
    )
    .execute(pool)
    .await?;

    if !had_migration_table && had_core_schema {
        adopt_legacy_schema(pool).await?;
    }

    apply_pending_migrations(pool).await?;
    Ok(())
}

async fn adopt_legacy_schema(pool: &SqlitePool) -> Result<(), DbError> {
    for table in ["global_tasks", "dag_nodes", "event_journal"] {
        if !table_exists(pool, table).await? {
            return Err(DbError::InvalidState(format!(
                "cannot adopt legacy database: missing table {table}"
            )));
        }
    }

    ensure_column(pool, "dag_nodes", "lease_id", "TEXT").await?;
    ensure_column(pool, "dag_nodes", "lease_owner", "TEXT").await?;
    ensure_column(pool, "dag_nodes", "lease_expires_at", "INTEGER").await?;
    ensure_index(
        pool,
        "idx_dag_nodes_task_status",
        "CREATE INDEX IF NOT EXISTS idx_dag_nodes_task_status ON dag_nodes(task_id, status)",
    )
    .await?;
    ensure_index(
        pool,
        "idx_dag_nodes_running_timeout",
        "CREATE INDEX IF NOT EXISTS idx_dag_nodes_running_timeout ON dag_nodes(status, acquired_at)",
    )
    .await?;
    ensure_index(
        pool,
        "idx_dag_nodes_lease_timeout",
        "CREATE INDEX IF NOT EXISTS idx_dag_nodes_lease_timeout ON dag_nodes(status, lease_expires_at)",
    )
    .await?;
    ensure_index(
        pool,
        "idx_event_journal_task_node",
        "CREATE INDEX IF NOT EXISTS idx_event_journal_task_node ON event_journal(task_id, node_id, timestamp)",
    )
    .await?;

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO agentd_schema_migrations (version, name)
        VALUES (1, 'initial_state_engine')
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn apply_pending_migrations(pool: &SqlitePool) -> Result<(), DbError> {
    let applied_versions = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM agentd_schema_migrations ORDER BY version ASC",
    )
    .fetch_all(pool)
    .await?;
    let applied_versions: HashSet<i64> = applied_versions.into_iter().collect();

    for migration in MIGRATIONS {
        if applied_versions.contains(&migration.version) {
            continue;
        }

        let mut tx = pool.begin().await?;
        for statement in migration.statements {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        sqlx::query(
            r#"
            INSERT INTO agentd_schema_migrations (version, name)
            VALUES (?, ?)
            "#,
        )
        .bind(migration.version)
        .bind(migration.name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }

    Ok(())
}

async fn schema_version(pool: &SqlitePool) -> Result<i64, DbError> {
    Ok(
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(version) FROM agentd_schema_migrations")
            .fetch_one(pool)
            .await?
            .unwrap_or(0),
    )
}

async fn table_exists(pool: &SqlitePool, table_name: &str) -> Result<bool, DbError> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;

    Ok(exists == 1)
}

async fn ensure_index(pool: &SqlitePool, index_name: &str, statement: &str) -> Result<(), DbError> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
    )
    .bind(index_name)
    .fetch_one(pool)
    .await?;

    if exists == 0 {
        sqlx::query(statement).execute(pool).await?;
    }

    Ok(())
}

async fn ensure_column(
    pool: &SqlitePool,
    table_name: &str,
    column_name: &str,
    column_type: &str,
) -> Result<(), DbError> {
    let query = format!("SELECT name FROM pragma_table_info('{table_name}')");
    let columns = sqlx::query_scalar::<_, String>(&query)
        .fetch_all(pool)
        .await?;

    if columns.iter().any(|name| name == column_name) {
        return Ok(());
    }

    let statement = format!("ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}");
    sqlx::query(&statement).execute(pool).await?;
    Ok(())
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

fn materialise_nodes(
    initial_nodes: Vec<NodeDefinition>,
) -> Result<Vec<(Uuid, NodeDefinition)>, DbError> {
    let mut seen = HashSet::with_capacity(initial_nodes.len());
    let mut materialised = Vec::with_capacity(initial_nodes.len());

    for mut node in initial_nodes {
        let node_id = node.id.take().unwrap_or_else(Uuid::new_v4);
        if !seen.insert(node_id) {
            return Err(DbError::InvalidState(format!(
                "duplicate node id: {node_id}"
            )));
        }
        materialised.push((node_id, node));
    }

    for (node_id, node) in &materialised {
        for dependency_id in &node.dependencies {
            if dependency_id == node_id {
                return Err(DbError::InvalidState(format!(
                    "node {node_id} cannot depend on itself"
                )));
            }

            if !seen.contains(dependency_id) {
                return Err(DbError::InvalidState(format!(
                    "node {node_id} depends on unknown node {dependency_id}"
                )));
            }
        }
    }

    ensure_acyclic(&materialised)?;
    Ok(materialised)
}

fn ensure_acyclic(nodes: &[(Uuid, NodeDefinition)]) -> Result<(), DbError> {
    let graph: HashMap<Uuid, Vec<Uuid>> = nodes
        .iter()
        .map(|(id, node)| (*id, node.dependencies.clone()))
        .collect();
    let mut permanent = HashSet::with_capacity(nodes.len());
    let mut temporary = HashSet::with_capacity(nodes.len());

    for node_id in graph.keys() {
        visit_node(*node_id, &graph, &mut temporary, &mut permanent)?;
    }

    Ok(())
}

fn visit_node(
    node_id: Uuid,
    graph: &HashMap<Uuid, Vec<Uuid>>,
    temporary: &mut HashSet<Uuid>,
    permanent: &mut HashSet<Uuid>,
) -> Result<(), DbError> {
    if permanent.contains(&node_id) {
        return Ok(());
    }

    if !temporary.insert(node_id) {
        return Err(DbError::InvalidState(format!(
            "cycle detected at node {node_id}"
        )));
    }

    if let Some(dependencies) = graph.get(&node_id) {
        for dependency_id in dependencies {
            visit_node(*dependency_id, graph, temporary, permanent)?;
        }
    }

    temporary.remove(&node_id);
    permanent.insert(node_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_database_applies_latest_schema_version() {
        let db_path = test_db_path();
        let database_url = format!("sqlite://{}", db_path.display());
        let db = Database::initialise(&database_url)
            .await
            .expect("database should initialise");

        assert_eq!(db.schema_version().await.unwrap(), LATEST_SCHEMA_VERSION);
        assert!(table_exists(&db.pool, "agentd_schema_migrations")
            .await
            .unwrap());

        cleanup_db(&db_path);
    }

    #[tokio::test]
    async fn legacy_database_is_adopted_without_losing_state() {
        let db_path = test_db_path();
        let database_url = format!("sqlite://{}", db_path.display());
        create_legacy_database(&database_url).await;

        let db = Database::initialise(&database_url)
            .await
            .expect("legacy database should initialise");
        let columns = table_columns(&db.pool, "dag_nodes").await.unwrap();

        assert_eq!(db.schema_version().await.unwrap(), LATEST_SCHEMA_VERSION);
        assert!(columns.iter().any(|column| column == "lease_id"));
        assert!(columns.iter().any(|column| column == "lease_owner"));
        assert!(columns.iter().any(|column| column == "lease_expires_at"));
        assert!(table_exists(&db.pool, "resource_locks").await.unwrap());

        let metrics = db.metrics().await.expect("metrics should load");
        assert_eq!(metrics.total_tasks, 1);
        assert_eq!(metrics.total_nodes, 1);

        cleanup_db(&db_path);
    }

    async fn create_legacy_database(database_url: &str) {
        let options = SqliteConnectOptions::from_str(database_url)
            .unwrap()
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();

        for statement in [
            r#"
            CREATE TABLE global_tasks (
                id TEXT PRIMARY KEY NOT NULL,
                goal TEXT NOT NULL,
                context TEXT NOT NULL CHECK (json_valid(context)),
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#,
            r#"
            CREATE TABLE dag_nodes (
                id TEXT PRIMARY KEY NOT NULL,
                task_id TEXT NOT NULL REFERENCES global_tasks(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
                dependencies TEXT NOT NULL CHECK (json_valid(dependencies) AND json_type(dependencies) = 'array'),
                payload_schema TEXT NOT NULL CHECK (json_valid(payload_schema)),
                result_payload TEXT CHECK (result_payload IS NULL OR json_valid(result_payload)),
                acquired_at INTEGER,
                updated_at INTEGER NOT NULL DEFAULT (unixepoch('now')),
                UNIQUE(task_id, id)
            )
            "#,
            r#"
            CREATE TABLE event_journal (
                id TEXT PRIMARY KEY NOT NULL,
                task_id TEXT NOT NULL REFERENCES global_tasks(id) ON DELETE CASCADE,
                node_id TEXT NOT NULL REFERENCES dag_nodes(id) ON DELETE CASCADE,
                action_type TEXT NOT NULL,
                payload TEXT NOT NULL CHECK (json_valid(payload)),
                timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#,
        ] {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }

        let task_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        sqlx::query("INSERT INTO global_tasks (id, goal, context) VALUES (?, ?, ?)")
            .bind(task_id.to_string())
            .bind("legacy task")
            .bind("{}")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO dag_nodes (id, task_id, status, dependencies, payload_schema) VALUES (?, ?, 'PENDING', '[]', '{}')",
        )
        .bind(node_id.to_string())
        .bind(task_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    async fn table_columns(pool: &SqlitePool, table_name: &str) -> Result<Vec<String>, DbError> {
        let query = format!("SELECT name FROM pragma_table_info('{table_name}')");
        Ok(sqlx::query_scalar::<_, String>(&query)
            .fetch_all(pool)
            .await?)
    }

    fn test_db_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("agentd_db_test_{}.db", Uuid::new_v4()))
    }

    fn cleanup_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
