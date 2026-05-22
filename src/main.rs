use agentd::coordinator::Coordinator;
use agentd::db::Database;
use agentd::ipc;
use anyhow::{Context, Result};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("agentd=info".parse()?))
        .init();

    let database_url = match std::env::var("AGENTD_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => default_database_url()?,
    };
    let socket_path = PathBuf::from(
        std::env::var("AGENTD_SOCKET_PATH").unwrap_or_else(|_| "/tmp/agentd.sock".into()),
    );
    let timeout_secs = std::env::var("AGENTD_NODE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(300);
    let context_event_limit = std::env::var("AGENTD_CONTEXT_EVENT_LIMIT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(50);

    remove_stale_socket(&socket_path)
        .with_context(|| format!("failed to prepare socket path {}", socket_path.display()))?;

    let db = Database::initialise(&database_url)
        .await
        .with_context(|| format!("failed to initialise database at {database_url}"))?;
    let coordinator = Arc::new(Coordinator::new(
        db,
        Duration::from_secs(timeout_secs),
        context_event_limit,
    ));

    spawn_timeout_sweeper(Arc::clone(&coordinator));

    info!(
        database_url,
        socket_path = %socket_path.display(),
        timeout_secs,
        context_event_limit,
        "starting agentd"
    );

    tokio::select! {
        result = ipc::serve(&socket_path, coordinator) => result?,
        result = tokio::signal::ctrl_c() => {
            result.context("failed to listen for shutdown signal")?;
            info!("shutdown signal received");
        }
    }

    remove_stale_socket(&socket_path)
        .with_context(|| format!("failed to remove socket path {}", socket_path.display()))?;
    Ok(())
}

fn default_database_url() -> Result<String> {
    let agentd_home = match std::env::var_os("AGENTD_HOME") {
        Some(path) => PathBuf::from(path),
        None => {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .context("HOME is not set; set AGENTD_DATABASE_URL or AGENTD_HOME")?;
            home.join(".agentd")
        }
    };

    std::fs::create_dir_all(&agentd_home)
        .with_context(|| format!("failed to create {}", agentd_home.display()))?;

    let database_path = agentd_home.join("agent_state.db");
    Ok(format!("sqlite://{}", database_path.display()))
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => anyhow::bail!(
            "refusing to remove non-socket path {}; choose another AGENTD_SOCKET_PATH",
            path.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn spawn_timeout_sweeper(coordinator: Arc<Coordinator>) {
    let timeout = coordinator.running_timeout();
    let mut interval = tokio::time::interval((timeout / 2).max(Duration::from_secs(1)));

    tokio::spawn(async move {
        loop {
            interval.tick().await;
            match coordinator.reset_timed_out_nodes().await {
                Ok(0) => {}
                Ok(count) => info!(count, "reset timed-out running nodes"),
                Err(err) => error!(%err, "failed to reset timed-out running nodes"),
            }
        }
    });
}
