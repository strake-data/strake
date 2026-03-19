//! Structured exit codes for machine-readable error handling.
//!
//! These codes allow CI/CD pipelines to distinguish between different types of failures.

/// Success: Command completed successfully. No warnings or issues.
pub const EXIT_OK: i32 = 0;

/// Hard error: Command did not complete.
pub const EXIT_ERROR: i32 = 1;

/// Warning: Command completed successfully, but warnings were raised.
/// (e.g., apply with drift, validation with coercions)
pub const EXIT_WARNINGS: i32 = 2;

/// Dry-run: --dry-run was passed. Nothing written.
pub const EXIT_DRY_RUN: i32 = 3;
