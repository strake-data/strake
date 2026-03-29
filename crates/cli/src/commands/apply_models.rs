use chrono::{DateTime, Utc};
use serde::Serialize;
use strake_common::models::{ActorName, DomainName};

/// Represents the structured outcome of an `apply` operation.
///
/// This receipt provides a detailed audit trail of what changed, why, and how long it took.
/// It is the primary data structure for machine-readable output.
#[derive(Serialize, Clone, Debug)]
pub struct ApplyReceipt {
    /// Schema version of the receipt format.
    pub receipt_version: u32,
    /// When the apply operation was completed.
    pub applied_at: DateTime<Utc>,
    /// The user or system account that initiated the apply.
    pub actor: ActorName,
    /// The domain (e.g., "finance") that was targeted.
    pub domain: DomainName,
    /// The version transition resulting from this apply.
    pub version: VersionTransition,
    /// Total duration of the apply operation in milliseconds.
    pub duration_ms: u128,
    /// High-level status of the apply operation.
    pub status: ApplyStatus,
    /// Summary of structural changes to sources, tables, contracts, and policies.
    pub changes: ResourceChanges,
    /// List of warnings that occurred during apply (e.g., connectivity issues).
    pub warnings: Vec<ApplyWarning>,
    /// Whether any drift was detected during the apply process.
    pub drift_detected: bool,
    /// Detailed rejection reason if the status is `Rejected`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection: Option<RejectionDetail>,
    /// List of errors preventing full application.
    pub errors: Vec<ErrorDetail>,
}

/// Represents a change in the domain version.
#[derive(Serialize, Clone, Debug)]
pub struct VersionTransition {
    /// The version before the apply (None if new domain).
    pub previous: Option<i32>,
    /// The version after the apply.
    pub current: i32,
}

/// Outcome status of an apply attempt.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ApplyStatus {
    /// Success: All changes applied cleanly.
    Applied,
    /// Partial Success: Applied but with warnings.
    #[allow(dead_code)]
    AppliedWithWarnings,
    /// Dry Run: Changes computed but not persisted.
    DryRun,
    /// Rejected: Pre-apply checks failed (e.g., version conflict).
    Rejected,
    /// Failed: Technical error during apply.
    Failed,
}

/// Detailed breakdown of changes by resource type.
#[derive(Serialize, Clone, Debug, Default)]
pub struct ResourceChanges {
    /// Changes to data sources.
    pub sources: ChangeSummary,
    /// Changes to tables.
    pub tables: ChangeSummary,
    /// Changes to verification contracts.
    pub contracts: ChangeSummary,
    /// Changes to access policies.
    pub policies: ChangeSummary,
}

/// Statistics about added, removed, and modified items.
#[derive(Serialize, Clone, Debug, Default)]
#[non_exhaustive]
pub struct ChangeSummary {
    /// List of names of added items.
    pub added: Vec<String>,
    /// List of names of removed items.
    pub removed: Vec<String>,
    /// List of names of modified items.
    pub modified: Vec<String>,
}

/// A non-terminal warning encountered during apply.
#[derive(Serialize, Clone, Debug)]
pub struct ApplyWarning {
    /// Machine-readable warning code.
    pub code: String,
    /// The source that triggered the warning.
    pub source: String,
    /// Optional table context.
    pub table: Option<String>,
    /// Optional column context.
    pub column: Option<String>,
    /// Human-readable explanation.
    pub detail: String,
}

/// Details about why an apply was rejected (e.g., version conflict).
#[derive(Serialize, Clone, Debug)]
pub struct RejectionDetail {
    /// Machine-readable rejection code.
    pub reason: String,
    /// The version the actor expected.
    pub expected_version: i32,
    /// The actual version in the database.
    pub actual_version: i32,
    /// Human-readable detail.
    pub detail: String,
}

/// Details about a terminal error during apply.
#[derive(Serialize, Clone, Debug)]
pub struct ErrorDetail {
    /// Machine-readable error code.
    pub code: String,
    /// Optional source context.
    pub source: Option<String>,
    /// Human-readable explanation.
    pub detail: String,
}
