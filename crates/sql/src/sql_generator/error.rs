use datafusion::error::DataFusionError;
use sqlparser::parser::ParserError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlGenError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("SQL Parser error: {0}")]
    Parser(#[from] ParserError),

    #[error("Unsupported plan type: {message} (node: {node_type})")]
    UnsupportedPlan { message: String, node_type: String },

    #[error("Ambiguous column reference: {name}. Candidates: {candidates:?}")]
    AmbiguousColumn {
        name: String,
        candidates: Vec<String>,
    },

    #[error("Unsupported expression: {0}")]
    UnsupportedExpr(String),

    #[error("Scope violation: Column '{col}' not found. Node: {node_type}, Available: {available:?}, Stack: {scope_stack:?}")]
    ScopeViolation {
        col: String,
        node_type: &'static str,
        available: Vec<String>,
        scope_stack: Vec<String>,
    },

    #[error("Dialect error: {0}")]
    DialectError(String),

    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),

    #[error("Maximum recursion depth ({0}) exceeded")]
    MaxRecursion(usize),
}

impl SqlGenError {
    pub fn to_strake_error(self, dialect_name: &str) -> strake_error::StrakeError {
        use strake_error::{ErrorCode, ErrorContext, StrakeError};

        match self {
            SqlGenError::DataFusion(e) => StrakeError::new(
                ErrorCode::DataFusionInternal,
                format!("DataFusion error: {}", e),
            )
            .with_hint("Check if the query is valid for DataFusion"),
            SqlGenError::Parser(e) => {
                StrakeError::new(ErrorCode::SyntaxError, format!("SQL Parser error: {}", e))
                    .with_hint("The generated SQL is syntactically invalid for the target dialect")
            }
            SqlGenError::UnsupportedPlan { message, node_type } => {
                let mut data = std::collections::HashMap::new();
                data.insert(
                    "node_type".to_string(),
                    serde_json::Value::String(node_type.clone()),
                );
                data.insert(
                    "dialect".to_string(),
                    serde_json::Value::String(dialect_name.to_string()),
                );

                StrakeError::new(
                    ErrorCode::PushdownUnsupported,
                    format!(
                        "Plan node '{}' not supported for SQL generation: {}",
                        node_type, message
                    ),
                )
                .with_context(ErrorContext::Generic { data })
                .with_hint("Try simplifying the query or disabling pushdown for this source")
            }
            SqlGenError::UnsupportedExpr(expr) => StrakeError::new(
                ErrorCode::NotImplemented,
                format!("Expression not supported for SQL generation: {}", expr),
            )
            .with_hint("This expression might not have a mapping for the target dialect"),
            SqlGenError::AmbiguousColumn { name, candidates } => StrakeError::new(
                ErrorCode::AmbiguousColumn,
                format!(
                    "Ambiguous column reference: {}. Candidates: {:?}",
                    name, candidates
                ),
            )
            .with_hint("Try qualifying the column name with a table or alias"),
            SqlGenError::ScopeViolation {
                col,
                node_type,
                available,
                scope_stack,
            } => {
                let mut data = std::collections::HashMap::new();
                data.insert(
                    "scope_stack".to_string(),
                    serde_json::Value::Array(
                        scope_stack
                            .into_iter()
                            .map(serde_json::Value::String)
                            .collect(),
                    ),
                );

                let context = ErrorContext::FieldNotFound {
                    field: col.clone(),
                    table: Some(node_type.to_string()),
                    available_fields: available,
                };

                StrakeError::new(ErrorCode::FieldNotFound, format!("Column '{}' not found in scope for node '{}'", col, node_type))
                    .with_context(context)
                    .with_hint("This usually indicates a bug in the SQL generator's scope management or an unexpected plan structure")
            }
            SqlGenError::DialectError(e) => {
                let mut data = std::collections::HashMap::new();
                data.insert("error".to_string(), serde_json::Value::String(e.clone()));
                data.insert(
                    "dialect".to_string(),
                    serde_json::Value::String(dialect_name.to_string()),
                );

                let context = ErrorContext::Generic { data };

                StrakeError::new(ErrorCode::InternalPanic, format!("Dialect error: {}", e))
                    .with_context(context)
                    .with_hint("The dialect implementation encountered an error")
            }
            SqlGenError::InvalidIdentifier(e) => StrakeError::new(
                ErrorCode::SyntaxError,
                format!("Invalid SQL identifier: {}", e),
            )
            .with_hint("Identifiers must be sanitized to prevent SQL injection"),
            SqlGenError::MaxRecursion(depth) => StrakeError::new(
                ErrorCode::InternalPanic,
                format!("Maximum recursion depth ({}) exceeded", depth),
            )
            .with_hint("This query might be too deeply nested or circular"),
        }
    }
}
