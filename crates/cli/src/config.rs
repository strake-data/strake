use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CliConfig {
    #[serde(default = "default_api_url")]
    pub api_url: String,
    pub token: Option<String>,
    pub database_url: Option<String>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            api_url: default_api_url(),
            token: None,
            database_url: None,
        }
    }
}

fn default_api_url() -> String {
    DEFAULT_API_URL.to_string()
}

/// Default API URL for Strake server
pub const DEFAULT_API_URL: &str = "http://localhost:8080/api/v1";

/// Default database URL for local development
pub const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/postgres";

#[derive(Serialize, Deserialize, Default)]
struct ConfigFile {
    #[serde(default)]
    current_profile: Option<String>,
    #[serde(default)]
    profiles: HashMap<String, CliConfig>,
}

/// Load configuration based on profile name, environment variables, and config file.
pub fn load(profile_arg: Option<&str>) -> Result<CliConfig> {
    // 1. Load config file
    let config_path = get_config_path();
    let config_file = if config_path.exists() {
        let content = fs::read_to_string(&config_path)
            .context(format!("Failed to read config file: {:?}", config_path))?;
        serde_yaml::from_str::<ConfigFile>(&content).context("Failed to parse config.yaml")?
    } else {
        ConfigFile::default()
    };

    // 2. Determine profile
    // Priority: Arg > Env > Config File > "default"
    let profile_name = profile_arg
        .map(|s| s.to_string())
        .or_else(|| env::var("STRAKE_PROFILE").ok())
        .or(config_file.current_profile)
        .unwrap_or_else(|| "default".to_string());

    // 3. Get profile config or default
    let mut config = config_file
        .profiles
        .get(&profile_name)
        .cloned()
        .unwrap_or_default();

    // 4. Override with Environment Variables (Highest Priority for individual fields)
    if let Ok(url) = env::var("STRAKE_API_URL") {
        config.api_url = url;
    }
    if let Ok(token) = env::var("STRAKE_TOKEN") {
        config.token = Some(token);
    }
    if let Ok(db_url) = env::var("DATABASE_URL") {
        config.database_url = Some(db_url);
    }
    // Also support legacy STRAKE_URL? Not standard.

    Ok(config)
}

fn get_config_path() -> PathBuf {
    if let Ok(path) = env::var("STRAKE_CONFIG") {
        return PathBuf::from(path);
    }

    // ~/.strake/config.yaml
    if let Some(home) = dirs::home_dir() {
        home.join(".strake").join("config.yaml")
    } else {
        // Fallback to current dir if no home? Or fail?
        // For CLI, home should exist.
        PathBuf::from(".strake/config.yaml")
    }
}
