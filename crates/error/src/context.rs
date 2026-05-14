//! # Error Contexts
//!
//! Structured metadata for errors to enable programmatic analysis and AI parsing.

use serde::{Deserialize, Serialize};

/// Structured context for AI-parseable errors.
///
/// Each variant provides specific fields relevant to that error type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ErrorContext {
    /// Context for STRAKE-2002 (FieldNotFound).
    FieldNotFound {
        /// The name of the missing field.
        field: String,
        /// The name of the table containing the missing field, if known.
        table: Option<String>,
        /// List of available fields in the schema for suggestions.
        available_fields: Vec<String>,
    },

    /// Context for STRAKE-2003 (TableNotFound).
    TableNotFound {
        /// The name of the missing table.
        table: String,
        /// The catalog name, if applicable.
        catalog: Option<String>,
        /// List of available tables in the source.
        available_tables: Vec<String>,
    },

    /// Context for STRAKE-2001 (SyntaxError).
    SyntaxError {
        /// Byte position in the input string.
        position: usize,
        /// 1-indexed line number.
        line: usize,
        /// 1-indexed column number.
        column: usize,
        /// A snippet of the code where the error occurred.
        snippet: String,
    },

    /// Context for STRAKE-1001 (SourceNotFound).
    SourceNotFound {
        /// The name of the source that was not found.
        source_name: String,
        /// List of registered source names.
        available_sources: Vec<String>,
    },

    /// Context for connection errors (STRAKE-1002, 1003, 1004).
    Connection {
        /// Name of the data source.
        source_name: String,
        /// Type of the source (e.g., "postgres").
        source_type: String,
        /// Hostname or IP address.
        host: Option<String>,
        /// Connection port.
        port: Option<u16>,
    },

    /// Context for STRAKE-3001/3002 (config errors).
    Config {
        /// Path to the configuration file.
        file_path: Option<String>,
        /// Line number in the config file.
        line: Option<usize>,
        /// The configuration field that failed validation.
        field: Option<String>,
    },

    /// Context for STRAKE-4001/4002 (auth errors).
    Auth {
        /// The user attempting the operation.
        user: Option<String>,
        /// The source being accessed.
        source: Option<String>,
        /// The permission string required.
        required_permission: Option<String>,
    },

    /// Context for STRAKE-2007 (PushdownUnsupported).
    Pushdown {
        /// The name of the logical operator.
        operation: String,
        /// The target source type.
        source_type: String,
        /// Detailed reason why pushdown failed.
        reason: String,
    },

    /// Context for STRAKE-2006 (BudgetExceeded).
    BudgetExceeded {
        /// Estimated row count that triggered the violation.
        estimated_rows: usize,
        /// The configured row limit.
        limit: usize,
        /// Suggestion to reduce query scope.
        suggestion: String,
    },

    /// Context for Schema Drift (STRAKE-2009, 2010, 2011).
    ///
    /// Provides detailed diff between the expected schema and the actual source schema.
    SchemaDrift {
        /// Name of the data source.
        source_name: String,
        /// Table identifier.
        table: String,
        /// Columns defined in the catalog.
        expected_columns: Vec<String>,
        /// Columns actually present in the source.
        actual_columns: Vec<String>,
        /// Columns missing from the source.
        missing_columns: Vec<String>,
        /// Columns where types have changed.
        type_mismatches: Vec<String>,
    },

    /// Generic key-value context for extensibility.
    Generic {
        /// Flat map of additional error metadata.
        #[serde(flatten)]
        data: std::collections::HashMap<String, serde_json::Value>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_drift_context_serde_roundtrip() {
        let ctx = ErrorContext::SchemaDrift {
            source_name: "test_source".to_string(),
            table: "test_table".to_string(),
            expected_columns: vec!["a".to_string()],
            actual_columns: vec!["a".to_string(), "b".to_string()],
            missing_columns: vec![],
            type_mismatches: vec![],
        };

        let json = serde_json::to_string(&ctx).unwrap();
        let de: ErrorContext = serde_json::from_str(&json).unwrap();

        match de {
            ErrorContext::SchemaDrift { source_name, .. } => {
                assert_eq!(source_name, "test_source");
            }
            _ => panic!("Wrong variant"),
        }
    }
}
