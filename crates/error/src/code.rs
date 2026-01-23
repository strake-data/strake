use serde::{Deserialize, Serialize};
use std::fmt;

/// Numeric error codes following STRAKE-XXXX format.
///
/// ## Code Ranges
/// - **1000-1999**: Connection errors
/// - **2000-2999**: Query errors  
/// - **3000-3999**: Configuration errors
/// - **4000-4999**: Authentication/Authorization errors
/// - **5000-5999**: Internal/System errors
///
/// Codes are stable across versions (semver contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
#[non_exhaustive]
pub enum ErrorCode {
    // === Connection Errors (1000-1999) ===
    /// STRAKE-1001: Source not found in catalog
    SourceNotFound = 1001,
    /// STRAKE-1002: Network connection timeout
    ConnectionTimeout = 1002,
    /// STRAKE-1003: SSL/TLS handshake failed
    SslHandshakeFailed = 1003,
    /// STRAKE-1004: Connection pool exhausted
    PoolExhausted = 1004,
    /// STRAKE-1005: Source type not supported
    UnsupportedSourceType = 1005,

    // === Query Errors (2000-2999) ===
    /// STRAKE-2001: SQL syntax error
    SyntaxError = 2001,
    /// STRAKE-2002: Field/column not found
    FieldNotFound = 2002,
    /// STRAKE-2003: Table not found
    TableNotFound = 2003,
    /// STRAKE-2004: Type mismatch in expression
    TypeMismatch = 2004,
    /// STRAKE-2005: Ambiguous column reference
    AmbiguousColumn = 2005,
    /// STRAKE-2006: Query exceeds resource budget
    BudgetExceeded = 2006,
    /// STRAKE-2007: Pushdown not supported for operation
    PushdownUnsupported = 2007,
    /// STRAKE-2008: Query cancelled by user
    QueryCancelled = 2008,

    // === Configuration Errors (3000-3999) ===
    /// STRAKE-3001: Invalid YAML syntax
    InvalidYaml = 3001,
    /// STRAKE-3002: Schema validation failed
    SchemaViolation = 3002,
    /// STRAKE-3003: Missing required field in config
    MissingRequiredField = 3003,
    /// STRAKE-3004: Invalid connection string
    InvalidConnectionString = 3004,

    // === Auth Errors (4000-4999) ===
    /// STRAKE-4001: Authentication failed
    AuthenticationFailed = 4001,
    /// STRAKE-4002: Authorization denied (RBAC)
    AuthorizationDenied = 4002,
    /// STRAKE-4003: API key invalid or expired
    InvalidApiKey = 4003,
    /// STRAKE-4004: Token expired
    TokenExpired = 4004,
    /// STRAKE-4005: Concurrency slot unavailable
    SlotUnavailable = 4005,

    // === Internal Errors (5000-5999) ===
    /// STRAKE-5001: Internal DataFusion error
    DataFusionInternal = 5001,
    /// STRAKE-5002: Serialization/deserialization failed
    SerializationFailed = 5002,
    /// STRAKE-5003: Unexpected internal state
    InternalPanic = 5003,
    /// STRAKE-5004: Feature not implemented
    NotImplemented = 5004,

    /// STRAKE-9999: Unknown/unclassified error
    Unknown = 9999,
}

impl ErrorCode {
    /// Get the numeric code value
    pub fn as_u16(&self) -> u16 {
        *self as u16
    }

    /// Get the formatted code string (e.g., "STRAKE-2002")
    pub fn as_str(&self) -> String {
        format!("STRAKE-{:04}", self.as_u16())
    }

    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        match self.as_u16() {
            1000..=1999 => ErrorCategory::Connection,
            2000..=2999 => ErrorCategory::Query,
            3000..=3999 => ErrorCategory::Config,
            4000..=4999 => ErrorCategory::Auth,
            5000..=5999 => ErrorCategory::Internal,
            _ => ErrorCategory::Internal,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<ErrorCode> for String {
    fn from(code: ErrorCode) -> String {
        code.as_str()
    }
}

impl TryFrom<String> for ErrorCode {
    type Error = String;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        // Parse "STRAKE-XXXX" format
        let num: u16 = s
            .strip_prefix("STRAKE-")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| "Invalid format".to_string())?;
        Self::try_from(num).map_err(|_| "Unknown code".to_string())
    }
}

impl TryFrom<u16> for ErrorCode {
    type Error = String;

    fn try_from(n: u16) -> std::result::Result<Self, Self::Error> {
        match n {
            1001 => Ok(Self::SourceNotFound),
            1002 => Ok(Self::ConnectionTimeout),
            1003 => Ok(Self::SslHandshakeFailed),
            1004 => Ok(Self::PoolExhausted),
            1005 => Ok(Self::UnsupportedSourceType),
            2001 => Ok(Self::SyntaxError),
            2002 => Ok(Self::FieldNotFound),
            2003 => Ok(Self::TableNotFound),
            2004 => Ok(Self::TypeMismatch),
            2005 => Ok(Self::AmbiguousColumn),
            2006 => Ok(Self::BudgetExceeded),
            2007 => Ok(Self::PushdownUnsupported),
            2008 => Ok(Self::QueryCancelled),
            3001 => Ok(Self::InvalidYaml),
            3002 => Ok(Self::SchemaViolation),
            3003 => Ok(Self::MissingRequiredField),
            3004 => Ok(Self::InvalidConnectionString),
            4001 => Ok(Self::AuthenticationFailed),
            4002 => Ok(Self::AuthorizationDenied),
            4003 => Ok(Self::InvalidApiKey),
            4004 => Ok(Self::TokenExpired),
            4005 => Ok(Self::SlotUnavailable),
            5001 => Ok(Self::DataFusionInternal),
            5002 => Ok(Self::SerializationFailed),
            5003 => Ok(Self::InternalPanic),
            5004 => Ok(Self::NotImplemented),
            9999 => Ok(Self::Unknown),
            _ => Err(format!("Unknown error code: {}", n)),
        }
    }
}

/// High-level error category for Python exception mapping
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ErrorCategory {
    Connection,
    Query,
    Config,
    Auth,
    Internal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_formatting() {
        assert_eq!(ErrorCode::SourceNotFound.as_str(), "STRAKE-1001");
        assert_eq!(ErrorCode::SyntaxError.as_str(), "STRAKE-2001");
        assert_eq!(ErrorCode::Unknown.as_str(), "STRAKE-9999");
    }

    #[test]
    fn test_error_code_parsing() {
        assert_eq!(
            ErrorCode::try_from("STRAKE-1001".to_string()).unwrap(),
            ErrorCode::SourceNotFound
        );
        assert_eq!(
            ErrorCode::try_from("STRAKE-9999".to_string()).unwrap(),
            ErrorCode::Unknown
        );
    }

    #[test]
    fn test_error_code_parsing_errors() {
        assert!(ErrorCode::try_from("INVALID".to_string()).is_err());
        assert!(ErrorCode::try_from("STRAKE-0000".to_string()).is_err());
        assert!(ErrorCode::try_from("STRAKE-ABC".to_string()).is_err());
    }

    #[test]
    fn test_error_categories() {
        assert_eq!(
            ErrorCode::SourceNotFound.category(),
            ErrorCategory::Connection
        );
        assert_eq!(ErrorCode::SyntaxError.category(), ErrorCategory::Query);
        assert_eq!(ErrorCode::InvalidYaml.category(), ErrorCategory::Config);
        assert_eq!(
            ErrorCode::AuthenticationFailed.category(),
            ErrorCategory::Auth
        );
        assert_eq!(ErrorCode::InternalPanic.category(), ErrorCategory::Internal);
        assert_eq!(ErrorCode::Unknown.category(), ErrorCategory::Internal);
    }
}
