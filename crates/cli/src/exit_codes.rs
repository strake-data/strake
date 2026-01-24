//! Structured exit codes for machine-readable error handling.
//!
//! These codes allow CI/CD pipelines to distinguish between different types of failures.

/// Success (standard convention)
#[allow(dead_code)]
pub const SUCCESS: i32 = 0;

/// General error (fallback for unknown errors)
pub const GENERAL_ERROR: i32 = 1;

/// CLI usage error (invalid arguments, missing flags)
pub const USAGE_ERROR: i32 = 2;

/// Configuration error (YAML parse failure, invalid schema)
pub const CONFIG_ERROR: i32 = 3;

/// Connection error (Database unreachable, timeout, network failure)
pub const CONNECTION_ERROR: i32 = 4;

/// Validation error (Contract violation, missing source/table)
pub const VALIDATION_ERROR: i32 = 5;

/// Conflict error (Optimistic lock failure, state mismatch)
pub const CONFLICT_ERROR: i32 = 6;

/// Permission error (Unauthorized, license check failed)
pub const PERMISSION_ERROR: i32 = 7;

/// Partial failure (Some operations succeeded, others failed)
#[allow(dead_code)]
pub const PARTIAL_FAILURE: i32 = 8;
