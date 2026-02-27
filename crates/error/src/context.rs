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
    /// Context for STRAKE-2002 (FieldNotFound)
    FieldNotFound {
        field: String,
        table: Option<String>,
        available_fields: Vec<String>,
    },

    /// Context for STRAKE-2003 (TableNotFound)
    TableNotFound {
        table: String,
        catalog: Option<String>,
        available_tables: Vec<String>,
    },

    /// Context for STRAKE-2001 (SyntaxError)
    SyntaxError {
        position: usize,
        line: usize,
        column: usize,
        snippet: String,
    },

    /// Context for STRAKE-1001 (SourceNotFound)
    SourceNotFound {
        source_name: String,
        available_sources: Vec<String>,
    },

    /// Context for connection errors (STRAKE-1002, 1003, 1004)
    Connection {
        source_name: String,
        source_type: String,
        host: Option<String>,
        port: Option<u16>,
    },

    /// Context for STRAKE-3001/3002 (config errors)
    Config {
        file_path: Option<String>,
        line: Option<usize>,
        field: Option<String>,
    },

    /// Context for STRAKE-4001/4002 (auth errors)  
    Auth {
        user: Option<String>,
        source: Option<String>,
        required_permission: Option<String>,
    },

    /// Context for STRAKE-2007 (PushdownUnsupported)
    Pushdown {
        operation: String,
        source_type: String,
        reason: String,
    },

    /// Context for STRAKE-2006 (BudgetExceeded)
    BudgetExceeded {
        estimated_rows: usize,
        limit: usize,
        suggestion: String,
    },

    /// Context for Schema Drift (STRAKE-2009, 2010, 2011)
    ///
    /// Provides detailed diff between the expected schema and the actual source schema.
    SchemaDrift {
        /// Name of the data source
        source_name: String,
        /// Table identifier
        table: String,
        /// Columns defined in the catalog
        expected_columns: Vec<String>,
        /// Columns actually present in the source
        actual_columns: Vec<String>,
        /// Columns missing from the source
        missing_columns: Vec<String>,
        /// Columns where types have changed
        type_mismatches: Vec<String>,
    },

    /// Generic key-value context for extensibility
    Generic {
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
