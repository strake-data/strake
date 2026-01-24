use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyLogEntry {
    pub domain: String,
    pub version: i32,
    pub user_id: String,
    pub sources_added: serde_json::Value,
    pub sources_deleted: serde_json::Value,
    pub tables_modified: serde_json::Value,
    pub config_hash: String,
    pub config_yaml: String,
    #[serde(skip_deserializing)]
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStatus {
    pub name: String,
    pub version: i32,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct ApplyResult {
    pub sources_added: Vec<String>,
    pub sources_deleted: Vec<String>,
}
