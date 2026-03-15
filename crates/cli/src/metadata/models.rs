//! # Metadata Models
//!
//! Internal data structures for metadata representation.
//!
//! ## Overview
//!
//! Exposes JSON/YAML serializable structs used to exchange config and audit state among
//! CLI commands and drivers.
//!
//! ## Usage
//!
//! ```rust
//! // use crate::metadata::models::*;
//! ```
//!
//! ## Performance Characteristics
//!
//! Lightweight POJOs.
//!
//! ## Safety
//!
//! Standard safe Rust.
//!
//! ## References
//!
//! - Strake Models Reference.

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
