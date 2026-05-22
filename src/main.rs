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
    let env_file = load_startup_env().context("failed to load startup environment")?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("agentd=info".parse()?))
        .init();

    let database_url = match std::env::var("AGENTD_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => default_database_url()?,
    };
    let socket_path = match std::env::var_os("AGENTD_SOCKET_PATH") {
        Some(path) => PathBuf::from(path),
        None => default_socket_path()?,
    };
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
        env_file = env_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
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

fn load_startup_env() -> Result<Option<PathBuf>> {
    let explicit_env_file = std::env::var_os("AGENTD_ENV_FILE").map(PathBuf::from);
    let env_file = match explicit_env_file {
        Some(path) => {
            if !path.exists() {
                anyhow::bail!("AGENTD_ENV_FILE does not exist: {}", path.display());
            }
            Some(path)
        }
        None => {
            let default_path = default_agentd_home()?.join(".env");
            if default_path.exists() {
                Some(default_path)
            } else {
                let project_path = PathBuf::from(".env");
                project_path.exists().then_some(project_path)
            }
        }
    };

    if let Some(path) = &env_file {
        load_env_file(path)?;
    }

    Ok(env_file)
}

fn load_env_file(path: &Path) -> Result<()> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;

    for (line_number, line) in contents.lines().enumerate() {
        let Some((key, value)) = parse_env_assignment(line) else {
            continue;
        };

        if key.contains('\0') || value.contains('\0') {
            anyhow::bail!(
                "invalid NUL byte in {} at line {}",
                path.display(),
                line_number + 1
            );
        }

        if std::env::var_os(&key).is_none() {
            std::env::set_var(key, value);
        }
    }

    Ok(())
}

fn parse_env_assignment(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let assignment = trimmed.strip_prefix("export ").unwrap_or(trimmed);
    let (raw_key, raw_value) = assignment.split_once('=')?;
    let key = raw_key.trim();
    if key.is_empty()
        || !key
            .chars()
            .all(|character| character == '_' || character.is_ascii_alphanumeric())
        || key
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_digit())
    {
        return None;
    }

    let value = strip_env_quotes(raw_value.trim()).to_string();
    Some((key.to_string(), value))
}

fn strip_env_quotes(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\''))
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn default_database_url() -> Result<String> {
    let agentd_home = ensure_agentd_home()?;
    let database_path = agentd_home.join("agent_state.db");
    Ok(format!("sqlite://{}", database_path.display()))
}

fn default_socket_path() -> Result<PathBuf> {
    Ok(ensure_agentd_home()?.join("agentd.sock"))
}

fn ensure_agentd_home() -> Result<PathBuf> {
    let agentd_home = default_agentd_home()?;
    std::fs::create_dir_all(&agentd_home)
        .with_context(|| format!("failed to create {}", agentd_home.display()))?;
    Ok(agentd_home)
}

fn default_agentd_home() -> Result<PathBuf> {
    match std::env::var_os("AGENTD_HOME") {
        Some(path) => Ok(PathBuf::from(path)),
        None => {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .context("HOME is not set; set AGENTD_DATABASE_URL or AGENTD_HOME")?;
            Ok(home.join(".agentd"))
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_env_assignments() {
        assert_eq!(
            parse_env_assignment("AGENTD_SOCKET_PATH=/tmp/agentd.sock"),
            Some((
                "AGENTD_SOCKET_PATH".to_string(),
                "/tmp/agentd.sock".to_string()
            ))
        );
        assert_eq!(
            parse_env_assignment("export RUST_LOG=\"agentd=debug\""),
            Some(("RUST_LOG".to_string(), "agentd=debug".to_string()))
        );
        assert_eq!(
            parse_env_assignment("DEEPSEEK_MODEL='deepseek-v4-flash'"),
            Some((
                "DEEPSEEK_MODEL".to_string(),
                "deepseek-v4-flash".to_string()
            ))
        );
    }

    #[test]
    fn ignores_non_assignments_and_invalid_keys() {
        assert_eq!(parse_env_assignment(""), None);
        assert_eq!(parse_env_assignment("# comment"), None);
        assert_eq!(parse_env_assignment("1BAD=value"), None);
        assert_eq!(parse_env_assignment("BAD-KEY=value"), None);
        assert_eq!(parse_env_assignment("NO_VALUE"), None);
    }

    #[test]
    fn process_environment_overrides_env_file_values() {
        let key = format!("AGENTD_TEST_OVERRIDE_{}", std::process::id());
        let path = std::env::temp_dir().join(format!("agentd_env_test_{}.env", std::process::id()));
        std::fs::write(&path, format!("{key}=from-file\n")).unwrap();
        std::env::set_var(&key, "from-process");

        load_env_file(&path).unwrap();

        assert_eq!(std::env::var(&key).unwrap(), "from-process");
        std::env::remove_var(&key);
        let _ = std::fs::remove_file(path);
    }
}
