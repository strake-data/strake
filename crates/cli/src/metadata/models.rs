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
//! ```ignore
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

/// Audit log entry for an apply event.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApplyLogEntry {
    /// The domain of the application.
    pub domain: strake_common::models::DomainName,
    /// The version of the domain after application.
    pub version: i32,
    /// The user ID of the actor who performed the action.
    pub user_id: strake_common::models::ActorName,
    /// Summary of sources added.
    pub sources_added: Vec<strake_common::models::SourceName>,
    /// Summary of sources deleted.
    pub sources_deleted: Vec<strake_common::models::SourceName>,
    /// Summary of tables modified.
    pub tables_modified: Vec<String>,
    /// Hash of the configuration at this version.
    pub config_hash: String,
    /// The full YAML configuration.
    pub config_yaml: String,
    /// Timestamp of the event.
    #[serde(skip_deserializing)]
    pub timestamp: Option<DateTime<Utc>>,
}

/// Status of a domain in the metadata store.
#[derive(Debug, Serialize, Deserialize)]
pub struct DomainStatus {
    /// The name of the domain.
    pub name: strake_common::models::DomainName,
    /// Current version of the domain.
    pub version: i32,
    /// When the domain was first created.
    pub created_at: Option<DateTime<Utc>>,
}

/// Result of an apply operation.
#[derive(Debug)]
pub struct ApplyResult {
    /// Names of sources that were successfully added.
    pub sources_added: Vec<strake_common::models::SourceName>,
    /// Names of sources that were successfully deleted.
    pub sources_deleted: Vec<strake_common::models::SourceName>,
}
