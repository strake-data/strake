use crate::config::AppConfig;
use anyhow::{anyhow, Context, Result};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::oneshot;

/// Handle to the spawned sidecar process
pub struct SidecarHandle {
    shutdown_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl SidecarHandle {
    /// Gracefully shuts down the sidecar supervisor and the sidecar process.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.task.await;
    }
}

/// Spawns the MCP Python sidecar agent with a supervisor loop.
pub async fn spawn_sidecar(config: &AppConfig) -> Result<Option<SidecarHandle>> {
    if !config.mcp.enabled {
        return Ok(None);
    }

    let mcp_config = config.mcp.clone();
    let server_addr = config.server.listen_addr.clone();

    // Fail-fast: Validate Python environment before spawning the task
    let python_bin = resolve_python_bin(mcp_config.python_bin.as_deref()).await?;
    let python_path = resolve_python_path()?;

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(async move {
        let mut retry_count = 0;
        let strake_url = format!("grpc://{}", server_addr.replace("0.0.0.0", "127.0.0.1"));

        // Initial startup delay to avoid race condition with main server
        if mcp_config.startup_delay_ms > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(mcp_config.startup_delay_ms)) => {}
                _ = &mut shutdown_rx => return,
            }
        }

        loop {
            tracing::info!(
                "Spawning MCP Agent (attempt {}/{}): {} -m strake.mcp --port {}",
                retry_count + 1,
                mcp_config.max_retries + 1,
                python_bin,
                mcp_config.port
            );

            let mut cmd = Command::new(&python_bin);
            cmd.arg("-m")
                .arg("strake.mcp")
                .arg("--transport")
                .arg("sse")
                .arg("--port")
                .arg(mcp_config.port.to_string())
                .env("STRAKE_URL", &strake_url)
                .env("PYTHONPATH", &python_path)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .kill_on_drop(true);

            match cmd.spawn() {
                Ok(mut child) => {
                    tokio::select! {
                        exit_status = child.wait() => {
                            match exit_status {
                                Ok(status) => {
                                    tracing::warn!("MCP Sidecar exited with status: {}", status);
                                }
                                Err(e) => {
                                    tracing::error!("Error waiting for MCP Sidecar: {}", e);
                                }
                            }
                        }
                        _ = &mut shutdown_rx => {
                            tracing::info!("Shutdown signal received. Terminating MCP Sidecar...");
                            // For now, kill_on_drop(true) will handle the SIGKILL when 'child' is dropped.
                            // To be more graceful on Unix:
                            #[cfg(unix)]
                            {
                                if let Some(pid) = child.id() {
                                    use nix::sys::signal::{self, Signal};
                                    use nix::unistd::Pid;
                                    let _ = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM);

                                    // Wait a bit for graceful exit
                                    tokio::select! {
                                        _ = child.wait() => {
                                            tracing::info!("MCP Sidecar exited gracefully.");
                                        }
                                        _ = tokio::time::sleep(Duration::from_millis(mcp_config.shutdown_timeout_ms)) => {
                                            tracing::warn!("MCP Sidecar did not exit gracefully within timeout. Killing.");
                                            let _ = child.kill().await;
                                        }
                                    }
                                }
                            }
                            #[cfg(not(unix))]
                            {
                                let _ = child.kill().await;
                            }
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to spawn MCP Sidecar: {}", e);
                }
            }

            if retry_count < mcp_config.max_retries {
                retry_count += 1;
                let delay = Duration::from_millis(mcp_config.retry_delay_ms * retry_count as u64);
                tracing::info!("Retrying sidecar in {:?}...", delay);

                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = &mut shutdown_rx => return,
                }
            } else {
                tracing::error!(
                    "MCP Sidecar failed after {} retries. Giving up.",
                    mcp_config.max_retries
                );
                break;
            }
        }
    });

    Ok(Some(SidecarHandle { shutdown_tx, task }))
}

async fn resolve_python_bin(override_bin: Option<&str>) -> Result<String> {
    if let Some(bin) = override_bin {
        if Command::new(bin).arg("--version").output().await.is_ok() {
            return Ok(bin.to_string());
        }
        return Err(anyhow!(
            "Configured python_bin '{}' not found or not executable",
            bin
        ));
    }

    for bin in &["python3", "python"] {
        if Command::new(bin).arg("--version").output().await.is_ok() {
            return Ok(bin.to_string());
        }
    }

    Err(anyhow!(
        "Python 3 not found in PATH (tried 'python3' and 'python')"
    ))
}

fn resolve_python_path() -> Result<String> {
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;

    // Attempt to locate 'python' directory relative to the binary or CWD
    let paths_to_check = vec![
        current_exe
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("python")), // bin/../python
        std::env::current_dir().ok().map(|p| p.join("python")),
    ];

    let mut found_path = None;
    for path in paths_to_check.into_iter().flatten() {
        if path.exists() && path.is_dir() {
            found_path = Some(path);
            break;
        }
    }

    let python_dir = found_path.ok_or_else(|| {
        anyhow!("Could not locate 'python' directory (searched relative to binary and CWD)")
    })?;

    let existing_pythonpath = std::env::var("PYTHONPATH").unwrap_or_default();
    if existing_pythonpath.is_empty() {
        Ok(python_dir.to_string_lossy().to_string())
    } else {
        let mut paths = std::env::split_paths(&existing_pythonpath).collect::<Vec<_>>();
        paths.insert(0, python_dir);
        let new_path =
            std::env::join_paths(paths).map_err(|e| anyhow!("Failed to join PYTHONPATH: {}", e))?;
        Ok(new_path.to_string_lossy().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    #[tokio::test]
    async fn test_resolve_python_bin() {
        let res = resolve_python_bin(None).await;
        // In some environments python might not be available, so we just check if it returns as expected
        match res {
            Ok(bin) => assert!(bin == "python3" || bin == "python"),
            Err(_) => println!("Python not found in this environment"),
        }
    }

    #[test]
    fn test_resolve_python_path() {
        // This will only succeed if the 'python' directory exists.
        // During tests, we might be in the root of the repo where 'python' directory exists.
        let path = resolve_python_path();
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
        // Mock a really long startup delay so it doesn't actually try to spawn python which might fail
        config.mcp.startup_delay_ms = 10000;

        let handle = spawn_sidecar(&config).await;
        if let Ok(Some(h)) = handle {
            h.shutdown().await;
        }
    }
}
