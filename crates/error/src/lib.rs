//! # strake-error
//!
//! Unified error types for the Strake federated query engine.
//!
//! All errors are designed to be AI-parseable with:
//! - Numeric error codes (STRAKE-XXXX)
//! - Structured JSON context
//! - Actionable hints for self-correction

mod code;
mod context;
mod convert;

pub use code::{ErrorCategory, ErrorCode};
pub use context::ErrorContext;

use serde::{Deserialize, Serialize};
use std::fmt;

/// The unified error type for all Strake operations.
///
/// Designed for consumption by LLM agents via MCP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrakeError {
    /// Numeric error code (e.g., "STRAKE-1001")
    pub code: ErrorCode,

    /// Human-readable error message
    pub message: String,

    /// Structured context for programmatic handling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ErrorContext>,

    /// AI-actionable suggestion for self-correction
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,

    /// Correlation ID for distributed tracing (future: OpenTelemetry)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl StrakeError {
    /// Create a new error with code and message
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            context: None,
            hint: None,
            trace_id: None,
        }
    }

    /// Add structured context
    pub fn with_context(mut self, context: ErrorContext) -> Self {
        self.context = Some(context);
        self
    }

    /// Add an AI-actionable hint
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Add trace ID for correlation
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Serialize to JSON for MCP/API responses
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            tracing::warn!("Failed to serialize StrakeError: {}", e);
            format!(
                r#"{{"code":"{}","message":"Serialization failed"}}"#,
                self.code
            )
        })
    }

    /// Serialize to pretty JSON for logging
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| self.to_json())
    }
}

impl fmt::Display for StrakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;
        if let Some(hint) = &self.hint {
            write!(f, " (Hint: {})", hint)?;
        }
        Ok(())
    }
}

impl std::error::Error for StrakeError {}

/// Result type alias for Strake operations
pub type Result<T> = std::result::Result<T, StrakeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strake_error_builder() {
        let err = StrakeError::new(ErrorCode::SourceNotFound, "Source not found")
            .with_hint("Check catalog")
            .with_trace_id("12345");

        assert_eq!(err.code, ErrorCode::SourceNotFound);
        assert_eq!(err.message, "Source not found");
        assert_eq!(err.hint, Some("Check catalog".to_string()));
        assert_eq!(err.trace_id, Some("12345".to_string()));
        assert!(err.context.is_none());
    }

    #[test]
    fn test_display_implementation() {
        let err =
            StrakeError::new(ErrorCode::SyntaxError, "Unexpected token").with_hint("Remove comma");

        // Should format as "[STRAKE-2001] Unexpected token (Hint: Remove comma)"
        assert_eq!(
            err.to_string(),
            "[STRAKE-2001] Unexpected token (Hint: Remove comma)"
        );

        let err_no_hint = StrakeError::new(ErrorCode::InternalPanic, "Crash");
        assert_eq!(err_no_hint.to_string(), "[STRAKE-5003] Crash");
    }

    #[test]
    fn test_json_output() {
        let err = StrakeError::new(ErrorCode::PoolExhausted, "Too many connections");
        let json = err.to_json();

        assert!(json.contains("\"code\":\"STRAKE-1004\""));
        assert!(json.contains("\"message\":\"Too many connections\""));
    }
}
