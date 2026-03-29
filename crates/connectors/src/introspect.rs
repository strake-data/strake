use async_trait::async_trait;
use globset::GlobMatcher;
use strake_common::schema::IntrospectedTable;

#[async_trait]
/// Trait for introspecting the schema and tables of a data source.
pub trait SchemaIntrospector: Send + Sync {
    /// Return all table identifiers visible to this connection,
    /// filtered by optional glob pattern.
    async fn list_tables(
        &self,
        pattern: Option<&GlobMatcher>,
    ) -> Result<Vec<TableRef>, IntrospectError>;

    /// Introspect a single table. `full` controls whether precision
    /// types, constraints, and comments are fetched (expensive path).
    async fn introspect_table(
        &self,
        table: &TableRef,
        full: bool,
    ) -> Result<IntrospectedTable, IntrospectError>;
}

/// A reference to a table in a specific schema.
#[derive(Debug, Clone)]
pub struct TableRef {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

/// Errors that can occur during schema introspection.
#[derive(Debug, thiserror::Error)]
pub enum IntrospectError {
    /// Connection failure.
    #[error("Connection failure: {0}")]
    Connection(String),
    /// Permission denied.
    #[error("Permission denied: {0}")]
    Permission(String),
    /// Table not found.
    #[error("Table not found: {0}")]
    NotFound(String),
    /// Query execution error.
    #[error("Query execution error: {0}")]
    Query(String),
    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),
}
