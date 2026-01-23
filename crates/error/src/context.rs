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

    /// Generic key-value context for extensibility
    Generic {
        #[serde(flatten)]
        data: std::collections::HashMap<String, serde_json::Value>,
    },
}
