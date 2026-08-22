//! # Error Conversion
//!
//! ## Overview
//! Converts external errors (`DataFusionError`, `anyhow::Error`, `std::io::Error`)
//! into the `StrakeError` domain type. This module implements structured conversions from third-party
//! and standard library errors to `StrakeError`. To avoid fragile string-based parsing, it walks the causal
//! chain of dynamic errors (e.g. `External` and `anyhow::Error`) to extract structured internal errors.
//!
//! ## Usage
//! ```rust
//! # use anyhow::anyhow;
//! # use strake_error::{StrakeError, ErrorCode};
//! let err = anyhow!("Schema error: No field named iddd.");
//! let strake_err: StrakeError = err.into();
//! assert_eq!(strake_err.code, ErrorCode::FieldNotFound);
//! ```
//!
//! ## Performance Characteristics
//! Downcasting is performed efficiently via reference checks. The `levenshtein` distance
//! algorithm is optimized to O(n·m) time complexity by collecting input strings into
//! `Vec<char>` to avoid O(i) character indexing and checks input lengths to limit space usage.
//!
//! ## Errors
//! Maps external types to `ErrorCode`:
//! - `DataFusionError::SchemaError::FieldNotFound` → `ErrorCode::FieldNotFound`
//! - `DataFusionError::Plan` matching table heuristics → `ErrorCode::TableNotFound`
//! - `DataFusionError::Plan` generic fallback → `ErrorCode::SyntaxError`
//! - `DataFusionError::Execution` → classified or `ErrorCode::DataFusionInternal`
//! - `DataFusionError::Diagnostic` → recursively re-applies this same mapping to the wrapped inner error
//! - `anyhow::Error` walking downcasts → `ErrorCode` from matched internal causes, or classified fallback
//! - `std::io::Error` → `ErrorCode::DataFusionInternal`
//! - `serde_json::Error` → `ErrorCode::SerializationFailed`
//! - `serde_yaml::Error` → `ErrorCode::InvalidYaml`

#[cfg(feature = "datafusion")]
use crate::ErrorContext;
use crate::{ErrorCode, StrakeError};
#[cfg(feature = "datafusion")]
use datafusion::error::DataFusionError;

/// Helper to map common gRPC status codes to Strake ErrorCodes.
#[cfg(feature = "grpc")]
fn map_tonic_code(code: tonic::Code) -> Option<ErrorCode> {
    match code {
        tonic::Code::Unauthenticated => Some(ErrorCode::InvalidApiKey),
        tonic::Code::PermissionDenied => Some(ErrorCode::AuthorizationDenied),
        tonic::Code::NotFound => Some(ErrorCode::SourceNotFound),
        tonic::Code::DeadlineExceeded => Some(ErrorCode::ConnectionTimeout),
        tonic::Code::Internal => Some(ErrorCode::DataFusionInternal),
        _ => None,
    }
}

/// Helper to classify generic error messages into standard `ErrorCode` categories.
/// Returns `None` if the error does not match any recognized SQL planning or execution issues.
fn classify_df_error_msg(msg: &str) -> Option<ErrorCode> {
    let msg_lower = msg.to_ascii_lowercase();
    if (msg_lower.contains("table ") || msg_lower.contains("table'"))
        && msg_lower.contains("not found")
    {
        Some(ErrorCode::TableNotFound)
    } else if msg_lower.contains("no field named")
        || (msg_lower.contains("field") && msg_lower.contains("not found"))
    {
        Some(ErrorCode::FieldNotFound)
    } else if msg_lower.contains("[err_contract_violation]")
        || msg_lower.contains("strict contract violation")
    {
        Some(ErrorCode::BudgetExceeded)
    } else if msg_lower.contains("invalid api key")
        || msg_lower.contains("unauthenticated")
        || msg_lower.contains("authentication credentials")
        || msg_lower.contains("invalid token")
    {
        Some(ErrorCode::InvalidApiKey)
    } else if msg_lower.contains("permission denied") || msg_lower.contains("authorization denied")
    {
        Some(ErrorCode::AuthorizationDenied)
    } else {
        None
    }
}

/// Conversion from a referenced `DataFusionError` to `StrakeError`.
///
/// Walks the internal causal chain of `External` errors to retrieve structured errors,
/// falling back to category-based classification for native DataFusion planner/plan errors.
#[cfg(feature = "datafusion")]
impl From<&DataFusionError> for StrakeError {
    fn from(err: &DataFusionError) -> Self {
        match err {
            DataFusionError::SchemaError(schema_err, _) => match schema_err.as_ref() {
                datafusion::common::SchemaError::FieldNotFound {
                    field,
                    valid_fields,
                } => {
                    let available: Vec<String> =
                        valid_fields.iter().map(|f| f.name.clone()).collect();

                    let hint = find_closest_match(&field.name, &available);

                    let mut error = StrakeError::new(
                        ErrorCode::FieldNotFound,
                        format!("Field '{}' not found", field.name),
                    )
                    .with_context(ErrorContext::FieldNotFound {
                        field: field.name.clone(),
                        table: None,
                        available_fields: available,
                    });

                    if let Some(closest) = hint {
                        error = error.with_hint(format!("Did you mean '{}'?", closest));
                    }
                    error
                }
                _ => {
                    let msg = schema_err.to_string();
                    let code = classify_df_error_msg(&msg).unwrap_or(ErrorCode::DataFusionInternal);
                    StrakeError::new(code, msg)
                }
            },
            DataFusionError::Plan(msg) => {
                let code = classify_df_error_msg(msg).unwrap_or(ErrorCode::SyntaxError);
                StrakeError::new(code, msg.clone())
            }
            DataFusionError::SQL(parse_err, _) => {
                StrakeError::new(ErrorCode::SyntaxError, parse_err.to_string())
            }
            DataFusionError::Execution(msg) => {
                let code = classify_df_error_msg(msg).unwrap_or(ErrorCode::DataFusionInternal);
                StrakeError::new(code, msg.clone())
            }
            DataFusionError::Diagnostic(_, inner_err) => StrakeError::from(inner_err.as_ref()),
            DataFusionError::External(e) => {
                let mut current: Option<&dyn std::error::Error> = Some(e.as_ref());
                while let Some(cause) = current {
                    if let Some(strake_err) = cause.downcast_ref::<StrakeError>() {
                        return strake_err.clone();
                    }
                    if let Some(df_err) = cause.downcast_ref::<DataFusionError>() {
                        return StrakeError::from(df_err);
                    }
                    #[cfg(feature = "grpc")]
                    if let Some((status, code)) = cause
                        .downcast_ref::<tonic::Status>()
                        .and_then(|s| map_tonic_code(s.code()).map(|c| (s, c)))
                    {
                        return StrakeError::new(code, status.message().to_string());
                    }
                    current = cause.source();
                }
                let msg = format!("{:#}", e);
                let code = classify_df_error_msg(&msg).unwrap_or(ErrorCode::DataFusionInternal);
                StrakeError::new(code, msg)
            }
            _ => {
                let msg = err.to_string();
                let code = classify_df_error_msg(&msg).unwrap_or(ErrorCode::DataFusionInternal);
                StrakeError::new(code, msg)
            }
        }
    }
}

/// Conversion from an owned `DataFusionError` to `StrakeError`.
#[cfg(feature = "datafusion")]
impl From<DataFusionError> for StrakeError {
    fn from(err: DataFusionError) -> Self {
        StrakeError::from(&err)
    }
}

/// Conversion from `anyhow::Error` to `StrakeError`.
///
/// Inspects the cause chain using `.chain()` to downcast structured errors
/// (such as `StrakeError`, `DataFusionError`, or `tonic::Status`), falling back to
/// string matching for client/server contract violations.
impl From<anyhow::Error> for StrakeError {
    fn from(err: anyhow::Error) -> Self {
        // Attempt to downcast to a StrakeError or DataFusionError if it's already one in the cause chain
        for cause in err.chain() {
            if let Some(strake_err) = cause.downcast_ref::<StrakeError>() {
                return strake_err.clone();
            }
            #[cfg(feature = "datafusion")]
            if let Some(df_err) = cause.downcast_ref::<DataFusionError>() {
                return StrakeError::from(df_err);
            }
            #[cfg(feature = "grpc")]
            if let Some((status, code)) = cause
                .downcast_ref::<tonic::Status>()
                .and_then(|s| map_tonic_code(s.code()).map(|c| (s, c)))
            {
                return StrakeError::new(code, status.message().to_string());
            }
        }

        let msg = format!("{:#}", err);
        let code = classify_df_error_msg(&msg).unwrap_or(ErrorCode::DataFusionInternal);
        StrakeError::new(code, msg)
    }
}

/// Conversion from `std::io::Error` to `StrakeError`.
impl From<std::io::Error> for StrakeError {
    fn from(err: std::io::Error) -> Self {
        StrakeError::new(ErrorCode::DataFusionInternal, err.to_string())
    }
}

/// Conversion from `serde_json::Error` to `StrakeError`.
impl From<serde_json::Error> for StrakeError {
    fn from(err: serde_json::Error) -> Self {
        StrakeError::new(ErrorCode::SerializationFailed, err.to_string())
    }
}

/// Conversion from `serde_yaml::Error` to `StrakeError`.
impl From<serde_yaml::Error> for StrakeError {
    fn from(err: serde_yaml::Error) -> Self {
        StrakeError::new(ErrorCode::InvalidYaml, err.to_string())
    }
}

// Levenshtein-based suggestion (moved from strake-common)
/// Helper to find the closest match in a list of candidate strings using Levenshtein distance.
///
/// Returns `None` if no match is within a distance threshold of 3.
#[cfg(feature = "datafusion")]
fn find_closest_match(target: &str, options: &[String]) -> Option<String> {
    let mut best_match: Option<&str> = None;
    let mut min_distance = usize::MAX;

    for option in options {
        let distance = levenshtein(target, option);
        if distance < min_distance && distance <= 3 {
            min_distance = distance;
            best_match = Some(option.as_str());
        }
    }

    best_match.map(|s| s.to_string())
}

/// Helper to compute the Levenshtein distance between two strings.
///
/// Implements standard dynamic programming with O(n·m) time complexity and O(min(n, m)) space complexity.
#[cfg(feature = "datafusion")]
fn levenshtein(a: &str, b: &str) -> usize {
    let mut a_chars: Vec<char> = a.chars().collect();
    let mut b_chars: Vec<char> = b.chars().collect();
    if a_chars.len() < b_chars.len() {
        std::mem::swap(&mut a_chars, &mut b_chars);
    }
    let (len_a, len_b) = (a_chars.len(), b_chars.len());
    if len_b == 0 {
        return len_a;
    }
    let mut prev = (0..=len_b).collect::<Vec<_>>();
    let mut curr = vec![0usize; len_b + 1];
    for i in 1..=len_a {
        curr[0] = i;
        for j in 1..=len_b {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = std::cmp::min(
                std::cmp::min(prev[j] + 1, curr[j - 1] + 1),
                prev[j - 1] + cost,
            );
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[len_b]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "datafusion")]
    // Verifies the correct Levenshtein distance results across multiple cases.
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("book", "back"), 2);
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("same", "same"), 0);
    }

    #[test]
    #[cfg(feature = "datafusion")]
    // Verifies that close matches are suggested correctly while distant matches are ignored.
    fn test_find_closest_match() {
        let options = vec![
            "revenue".to_string(),
            "cost".to_string(),
            "profit".to_string(),
        ];

        // Exact matches
        assert_eq!(
            find_closest_match("revenue", &options),
            Some("revenue".to_string())
        );

        // Close matches
        assert_eq!(
            find_closest_match("revenu", &options),
            Some("revenue".to_string())
        );
        assert_eq!(
            find_closest_match("cst", &options),
            Some("cost".to_string())
        );

        // No match (distance > 3)
        assert_eq!(find_closest_match("completely_different", &options), None);
    }

    #[test]
    #[cfg(feature = "datafusion")]
    // Verifies standard conversion mapping from DataFusion errors.
    fn test_datafusion_error_mappings() {
        use datafusion::common::Column;

        // Test Plan mapping
        let plan_err = DataFusionError::Plan("Table not found".to_string());
        let strake_err: StrakeError = plan_err.into();
        assert_eq!(strake_err.code, ErrorCode::TableNotFound);
        assert_eq!(strake_err.message, "Table not found");

        // Test SchemaError mapping with hint
        let field = Column::from_name("revenu"); // Typo
        let valid_fields = vec![Column::from_name("revenue"), Column::from_name("cost")];

        let schema_err = DataFusionError::SchemaError(
            Box::new(datafusion::common::SchemaError::FieldNotFound {
                field: Box::new(field),
                valid_fields: valid_fields.clone(),
            }),
            Box::new(None),
        );

        let strake_err: StrakeError = schema_err.into();
        assert_eq!(strake_err.code, ErrorCode::FieldNotFound);
        assert_eq!(strake_err.message, "Field 'revenu' not found");
        assert_eq!(strake_err.hint, Some("Did you mean 'revenue'?".to_string()));

        // Check context type
        match strake_err.context.as_deref() {
            Some(ErrorContext::FieldNotFound {
                field,
                available_fields,
                ..
            }) => {
                assert_eq!(field, "revenu");
                assert_eq!(available_fields[0], "revenue");
            }
            _ => panic!("Expected FieldNotFound context"),
        }
    }

    #[test]
    // Verifies standard conversion mapping from IO errors.
    fn test_io_error_mapping() {
        let io_err = std::io::Error::other("File error");
        let strake_err: StrakeError = io_err.into();
        assert_eq!(strake_err.code, ErrorCode::DataFusionInternal);
        assert!(strake_err.message.contains("File error"));
    }

    #[test]
    // Verifies anyhow error classification for planning and syntax errors.
    fn test_anyhow_error_planning_mapping() {
        let err1 =
            anyhow::anyhow!("Error during planning: table 'strake.nonexistent.table' not found");
        let strake_err1: StrakeError = err1.into();
        assert_eq!(strake_err1.code, ErrorCode::TableNotFound);

        let err2 = anyhow::anyhow!("Schema error: No field named iddd.");
        let strake_err2: StrakeError = err2.into();
        assert_eq!(strake_err2.code, ErrorCode::FieldNotFound);
    }

    #[test]
    #[cfg(all(feature = "datafusion", feature = "grpc"))]
    // Verifies conversion of tonic::Status error mapping.
    fn test_tonic_status_error_mapping() {
        let status = tonic::Status::new(tonic::Code::PermissionDenied, "Access denied");
        let df_err = DataFusionError::External(Box::new(status));
        let strake_err: StrakeError = df_err.into();
        assert_eq!(strake_err.code, ErrorCode::AuthorizationDenied);
        assert_eq!(strake_err.message, "Access denied");
    }

    #[test]
    #[cfg(feature = "datafusion")]
    fn test_datafusion_diagnostic_error_mapping_recursively() {
        use datafusion::common::Column;

        let field = Column::from_name("revenu");
        let valid_fields = vec![Column::from_name("revenue")];

        let schema_err = DataFusionError::SchemaError(
            Box::new(datafusion::common::SchemaError::FieldNotFound {
                field: Box::new(field),
                valid_fields,
            }),
            Box::new(None),
        );

        let diagnostic = datafusion::common::Diagnostic::new_error("Context message", None);
        let diagnostic_err =
            DataFusionError::Diagnostic(Box::new(diagnostic), Box::new(schema_err));
        let strake_err: StrakeError = diagnostic_err.into();

        assert_eq!(strake_err.code, ErrorCode::FieldNotFound);
        assert_eq!(strake_err.message, "Field 'revenu' not found");
        assert_eq!(strake_err.hint, Some("Did you mean 'revenue'?".to_string()));
    }
}
