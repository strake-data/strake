use datafusion::error::DataFusionError;
use sqlparser::parser::ParserError;
use thiserror::Error;

/// Errors that can occur during translation from DataFusion plans to SQL queries.
#[derive(Debug, Error)]
pub enum SqlGenError {
    /// Errors propagated from the underlying DataFusion engine.
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    /// Errors encountered during SQL parsing or validation via `sqlparser-rs`.
    #[error("SQL Parser error: {0}")]
    Parser(#[from] ParserError),

    /// Returned when the plan contains a node type that cannot be translated to SQL.
    #[error("Unsupported plan type: {message} (node: {node_type})")]
    UnsupportedPlan {
        /// Descriptive message explaining why the node is unsupported.
        message: String,
        /// The type name of the logical plan node.
        node_type: String,
    },

    /// Returned when a column reference matches multiple entries in the visible scope.
    #[error("Ambiguous column reference: {name}. Candidates: {candidates:?}")]
    AmbiguousColumn {
        /// The name of the ambiguous column.
        name: String,
        /// List of candidate identifiers (e.g. "t0.id", "t1.id").
        candidates: Vec<String>,
    },

    /// Returned when a specific expression (e.g. a complex scalar function) cannot be translated.
    #[error("Unsupported expression: {0}")]
    UnsupportedExpr(String),

    /// Returned when a column reference cannot be resolved in the current scope stack.
    #[error(
        "Scope violation: Column '{col}' not found. Node: {node_type}, Available: {available:?}, Stack: {scope_stack:?}"
    )]
    ScopeViolation {
        /// The name of the missing column.
        col: String,
        /// The type of plan node where resolution failed.
        node_type: &'static str,
        /// List of available column names in the current scope.
        available: Vec<String>,
        /// The current stack of active scope aliases.
        scope_stack: Vec<String>,
    },

    /// Errors specific to dialect-level configuration or mapping.
    #[error("Dialect error: {0}")]
    DialectError(String),

    /// Returned when an identifier (table or column name) contains illegal characters.
    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),

    /// Returned when recursion depth exceeds [`MAX_RECURSION_DEPTH`](crate::sql_generator::translator::MAX_RECURSION_DEPTH).
    #[error("Maximum recursion depth ({0}) exceeded")]
    MaxRecursion(usize),
}

impl SqlGenError {
    /// Converts the `SqlGenError` into a user-facing [`StrakeError`].
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
