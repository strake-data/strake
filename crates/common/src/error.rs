//! # Error Handling
//!
//! Re-exports and utilities for error handling in Strake.
//!
//! This module provides the [`crate::error::StrakeError`] type and associated [`crate::error::ErrorCode`] and
//! [`crate::error::Result`] types, which are used throughout the crate for consistent error reporting.

/// High-level category of an error (e.g., Auth, Database).
pub use strake_error::ErrorCategory;
/// Machine-readable error code.
pub use strake_error::ErrorCode;
/// Additional context for an error.
pub use strake_error::ErrorContext;
/// Unified result type for Strake operations.
pub use strake_error::Result;
/// Unified error type for Strake operations.
pub use strake_error::StrakeError;
