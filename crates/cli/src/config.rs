use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "backend")]
pub enum MetadataBackendConfig {
    #[serde(rename = "sqlite")]
    Sqlite { path: PathBuf },
    #[serde(rename = "postgres")]
    Postgres { url: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CliConfig {
    #[serde(default = "default_api_url")]
    pub api_url: String,
    pub token: Option<String>,
    pub database_url: Option<String>, // Legacy
    pub metadata: Option<MetadataBackendConfig>,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            api_url: default_api_url(),
            token: None,
            database_url: None,
            metadata: None,
        }
    }
}

fn default_api_url() -> String {
    DEFAULT_API_URL.to_string()
}

/// Default API URL for Strake server
pub const DEFAULT_API_URL: &str = "http://localhost:8080/api/v1";

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

        // Detect if it's a flat CliConfig (has 'metadata' or 'api_url' at top level)
        // or a profiled ConfigFile
        let value: serde_yaml::Value =
            serde_yaml::from_str(&content).context("Failed to parse config file as YAML")?;

        if value.get("metadata").is_some()
            || value.get("api_url").is_some()
            || value.get("profiles").is_none()
        {
            // Treat as flat CliConfig
            let flat_config: CliConfig =
                serde_yaml::from_value(value).context("Failed to parse flat configuration")?;
            let mut cf = ConfigFile::default();
            cf.profiles.insert("default".to_string(), flat_config);
            cf
        } else {
            // Treat as profiled ConfigFile
            serde_yaml::from_str::<ConfigFile>(&content)
                .context("Failed to parse profiled configuration file")?
        }
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
        config.database_url = Some(db_url.clone());
        // Auto-configure metadata to Postgres if env var set and no metadata set
        if config.metadata.is_none() {
            config.metadata = Some(MetadataBackendConfig::Postgres { url: db_url });
        }
    }

    // 5. Default Metadata to SQLite if not set
    if config.metadata.is_none() {
        config.metadata = Some(MetadataBackendConfig::Sqlite {
            path: get_default_sqlite_path(),
        });
    }

    Ok(config)
}

fn get_config_path() -> PathBuf {
    if let Ok(path) = env::var("STRAKE_CONFIG") {
        return PathBuf::from(path);
    }

    // Check for local strake.yaml first
    let local = PathBuf::from("strake.yaml");
    if local.exists() {
        return local;
    }

    // ~/.strake/config.yaml
    if let Some(home) = dirs::home_dir() {
        home.join(".strake").join("config.yaml")
    } else {
        // Fallback to current dir if no home
        PathBuf::from(".strake/config.yaml")
    }
}

fn get_default_sqlite_path() -> PathBuf {
    // Default to .strake/metadata.db in the current directory for project isolation.
    PathBuf::from(".strake").join("metadata.db")
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_sqlite_path() {
        let path = get_default_sqlite_path();
        assert_eq!(path, PathBuf::from(".strake").join("metadata.db"));
    }
}
