use anyhow::Context;
use std::process::Stdio;
use std::time::Duration;
use strake_common::config::{AppConfig, McpConfig};
use strake_error::{ErrorCode, Result, StrakeError};
use thiserror::Error;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info_span, Instrument};

#[derive(Error, Debug)]
pub enum SidecarError {
    #[error("Python binary not found: {0}")]
    PythonNotFound(String),
    #[error("Failed to get current executable path: {0}")]
    ExecutablePathError(#[from] std::io::Error),
    #[error("Could not locate 'python' directory (searched relative to binary and CWD)")]
    PythonDirNotFound,
    #[error("Failed to join PYTHONPATH: {0}")]
    PythonPathJoinError(#[from] std::env::JoinPathsError),
}

impl From<SidecarError> for StrakeError {
    fn from(err: SidecarError) -> Self {
        StrakeError::new(ErrorCode::SidecarError, err.to_string())
    }
}

/// Handle to the spawned sidecar process
#[derive(Debug)]
pub struct SidecarHandle {
    pub(crate) shutdown_tx: Option<oneshot::Sender<()>>,
    pub(crate) task: Option<JoinHandle<()>>,
}

impl Drop for SidecarHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            tracing::warn!("SidecarHandle dropped without calling shutdown(). Aborting supervisor task to prevent orphaned processes.");
            task.abort();
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()); // Best effort shutdown signal
        }
    }
}

impl SidecarHandle {
    /// Gracefully shuts down the sidecar supervisor and the sidecar process.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            if tx.send(()).is_err() {
                tracing::error!("Failed to send shutdown signal to MCP Sidecar supervisor");
            }
        }
        if let Some(task) = self.task.take() {
            if let Err(e) = task.await {
                tracing::error!(error = %e, "MCP Sidecar supervisor task failed during shutdown");
            }
        }
    }
}

/// Spawns the MCP Python sidecar agent with a supervisor loop.
/// Spawns the MCP sidecar supervisor task.
///
/// The supervisor manages the Python lifecycle, including resolution of the
/// Python binary, path setup, and an active health check loop.
///
/// Circuit breaker: After `max_retries` consecutive failures, enters a
/// cooldown period (configurable via `cooldown_secs`) before attempting restart.
/// This prevents log spam and CPU waste when the Python environment
/// is permanently misconfigured.
#[must_use = "SidecarHandle must be stored and shutdown() called on exit to avoid leaking processes"]
#[tracing::instrument(skip(config), fields(port = config.mcp.port))]
pub async fn spawn_sidecar(config: &AppConfig) -> anyhow::Result<Option<SidecarHandle>> {
    if !config.mcp.enabled {
        return Ok(None);
    }

    let mut mcp_config = config.mcp.clone();
    mcp_config.environment = config.environment;
    let server_addr = config.server.listen_addr.clone();
    let port = mcp_config.port;
    let span = info_span!("mcp_sidecar", port = port);
    let _enter = span.enter();

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(
        async move {
            let mut consecutive_failures = 0;
            let max_retries = mcp_config.max_retries;
            let cooldown_secs = mcp_config.cooldown_secs;
            let strake_url = format!("grpc://{}", server_addr.replace("0.0.0.0", "127.0.0.1"));

            loop {
                // 1. CHECK SHUTDOWN AT TOP OF OUTER LOOP
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                        tracing::info!("Shutdown signal received in supervisor (outer loop). Exiting.");
                        return;
                    }
                    else => {}
                }

                // Circuit Breaker: If we've failed too many times, wait for a cooldown
                if consecutive_failures >= max_retries {
                    tracing::error!(
                        failures = consecutive_failures,
                        cooldown = cooldown_secs,
                        "MCP Sidecar reached max retries. Entering cooldown period."
                    );
                    tokio::select! {
                        biased;
                        _ = &mut shutdown_rx => return,
                        _ = tokio::time::sleep(Duration::from_secs(cooldown_secs)) => {
                            tracing::info!("Cooldown period elapsed. Attempting to restart MCP Sidecar...");
                            consecutive_failures = 0;
                        }
                    }
                }

                let python_bin = match resolve_python_bin(mcp_config.python_bin.as_deref()).await {
                    Ok(bin) => bin,
                    Err(e) => {
                        tracing::error!(error = %e, "Could not resolve Python binary for MCP Sidecar");
                        consecutive_failures += 1;
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                };

                let python_path = match resolve_python_path().await {
                    Ok(path) => path,
                    Err(e) => {
                        tracing::error!(error = %e, "Could not resolve PYTHONPATH for MCP Sidecar");
                        consecutive_failures += 1;
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                };

                tracing::info!(
                    port = mcp_config.port,
                    python = %python_bin,
                    "Starting MCP Sidecar..."
                );

                let mut child = match Command::new(&python_bin)
                    .env("PYTHONPATH", &python_path)
                    .env("STRAKE_URL", &strake_url)
                    .env("STRAKE_ENV", mcp_config.environment.to_string())
                    .env("STRAKE_USE_FIRECRACKER", if mcp_config.use_firecracker { "true" } else { "false" })
                    .arg("-m")
                    .arg("strake.mcp")
                    .arg("--port")
                    .arg(mcp_config.port.to_string())
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .context("Failed to spawn MCP Sidecar process")
                {
                    Ok(child) => child,
                    Err(e) => {
                        tracing::error!(error = %e, "MCP Sidecar spawn failed");
                        consecutive_failures += 1;
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                        continue;
                    }
                };

                let mut health_interval = tokio::time::interval(Duration::from_millis(mcp_config.health_check_interval_ms));
                health_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                // 2. NON-BLOCKING STARTUP DELAY
                // Wait for the startup_delay_ms or a shutdown signal
                if mcp_config.startup_delay_ms > 0 {
                    tokio::select! {
                        biased;
                        _ = &mut shutdown_rx => {
                            tracing::info!("Shutdown signal received during MCP Sidecar startup delay. Terminating...");
                            if let Err(e) = graceful_shutdown(child, &mcp_config).await {
                                tracing::error!(error = %e, "Graceful shutdown of MCP Sidecar failed");
                            }
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(mcp_config.startup_delay_ms)) => {
                            tracing::info!("Startup delay elapsed. Entering health check loop.");
                        }
                    }
                }

                // Skip the first tick since we just waited the startup delay
                health_interval.tick().await;

                let http_client = match reqwest::Client::builder()
                    .timeout(Duration::from_secs(2))
                    .build()
                {
                    Ok(client) => client,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to build HTTP client for MCP health checks. Retrying sidecar spawn...");
                        consecutive_failures += 1;
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                        continue;
                    }
                };

                loop {
                    tokio::select! {
                        biased;
                        _ = &mut shutdown_rx => {
                            tracing::info!("Shutdown signal received. Terminating MCP Sidecar...");
                            if let Err(e) = graceful_shutdown(child, &mcp_config).await {
                                tracing::error!(error = %e, "Graceful shutdown of MCP Sidecar failed");
                            }
                            return;
                        }
                        exit_status = child.wait() => {
                            match exit_status {
                                Ok(status) => tracing::warn!("MCP Sidecar exited with status: {}", status),
                                Err(e) => tracing::error!(error = %e, "MCP Sidecar child wait failed"),
                            }
                            consecutive_failures += 1;
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                            break;
                        }
                        _ = health_interval.tick() => {
                            if let Some(url) = &mcp_config.health_check_url {
                                match tokio::time::timeout(Duration::from_secs(2), http_client.get(url).send()).await {
                                    Ok(Ok(resp)) if resp.status().is_success() => {
                                        consecutive_failures = 0; // Success! Reset counter
                                    }
                                    Ok(Ok(resp)) => {
                                        tracing::warn!(status = %resp.status(), "MCP Sidecar health check failed");
                                    }
                                    Ok(Err(e)) => {
                                        tracing::warn!(error = %e, "MCP Sidecar health check request failed");
                                    }
                                    Err(_) => {
                                        tracing::warn!("MCP Sidecar health check timed out");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        .instrument(span.clone())
    );

    Ok(Some(SidecarHandle {
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
    }))
}

async fn graceful_shutdown(mut child: Child, config: &McpConfig) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = i32::try_from(pid).map_err(std::io::Error::other)?;
            if let Err(e) = signal::kill(Pid::from_raw(pid), Signal::SIGTERM) {
                tracing::error!(error = %e, "Failed to send SIGTERM to MCP Sidecar");
            }

            tokio::select! {
                _ = child.wait() => {
                    tracing::info!("MCP Sidecar exited gracefully.");
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_millis(config.shutdown_timeout_ms)) => {
                    tracing::warn!("MCP Sidecar did not exit gracefully within timeout. Killing.");
                }
            }
        }
    }

    child.kill().await?;
    child.wait().await?;
    Ok(())
}

async fn resolve_python_bin(override_bin: Option<&str>) -> Result<String> {
    if let Some(bin) = override_bin {
        if Command::new(bin).arg("--version").output().await.is_ok() {
            return Ok(bin.to_string());
        }
        return Err(StrakeError::new(
            ErrorCode::ConfigError,
            format!(
                "Configured python_bin '{}' not found or not executable",
                bin
            ),
        ));
    }

    for bin in &["python3", "python"] {
        if Command::new(bin).arg("--version").output().await.is_ok() {
            return Ok(bin.to_string());
        }
    }

    Err(SidecarError::PythonNotFound("tried 'python3' and 'python'".to_string()).into())
}

async fn resolve_python_path() -> Result<String> {
    let current_exe = std::env::current_exe().map_err(SidecarError::ExecutablePathError)?;

    // Attempt to locate 'python' directory relative to the binary or CWD
    let mut paths_to_check = vec![];
    if let Some(parent) = current_exe.parent().and_then(|p| p.parent()) {
        paths_to_check.push(parent.join("python"));
    }
    if let Ok(cwd) = std::env::current_dir() {
        paths_to_check.push(cwd.join("python"));
    }

    let mut found_path = None;
    for path in paths_to_check {
        if tokio::fs::metadata(&path)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            found_path = Some(path);
            break;
        }
    }

    let python_dir = found_path.ok_or(SidecarError::PythonDirNotFound)?;

    let existing_pythonpath = std::env::var("PYTHONPATH").unwrap_or_default();
    if existing_pythonpath.is_empty() {
        Ok(python_dir.to_string_lossy().to_string())
    } else {
        let new_path = std::env::join_paths(
            std::iter::once(python_dir).chain(std::env::split_paths(&existing_pythonpath)),
        )
        .map_err(SidecarError::PythonPathJoinError)?;

        Ok(new_path.to_string_lossy().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strake_common::config::AppConfig;

    #[tokio::test]
    async fn test_resolve_python_bin() {
        let res = resolve_python_bin(None).await;
        match res {
            Ok(bin) => assert!(bin == "python3" || bin == "python"),
            Err(_) => println!("Python not found in this environment"),
        }
    }

    #[tokio::test]
    async fn test_resolve_python_path() {
        let path = resolve_python_path().await;
        match path {
            Ok(p) => {
                println!("Resolved PYTHONPATH: {}", p);
                assert!(p.contains("python"));
            }
            Err(e) => {
                println!(
                    "PYTHONPATH resolution failed (expected if 'python' dir missing): {}",
                    e
                );
            }
        }
    }

    #[tokio::test]
    async fn test_spawn_disabled() {
        let mut config = AppConfig::default();
        config.mcp.enabled = false;
        let handle = spawn_sidecar(&config).await.unwrap();
        assert!(handle.is_none());
    }

    #[tokio::test]
    async fn test_shutdown_lifecycle() {
        let mut config = AppConfig::default();
        config.mcp.enabled = true;
        config.mcp.startup_delay_ms = 10000;

        let handle = spawn_sidecar(&config).await;
        if let Ok(Some(h)) = handle {
            h.shutdown().await;
        }
    }
}
