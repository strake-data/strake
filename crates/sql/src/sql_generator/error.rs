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
            SqlGenError::UnsupportedPlan { message, node_type } => StrakeError::new(
                ErrorCode::PushdownUnsupported,
                format!(
                    "Plan node '{}' not supported for SQL generation: {}",
                    node_type, message
                ),
            )
            .with_hint("Try simplifying the query or disabling pushdown for this source"),
            SqlGenError::UnsupportedExpr(expr) => StrakeError::new(
                ErrorCode::NotImplemented,
                format!("Expression not supported for SQL generation: {}", expr),
            )
            .with_hint("This expression might not have a mapping for the target dialect"),
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
        }
    }
}
