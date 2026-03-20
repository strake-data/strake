use async_trait::async_trait;
use globset::GlobMatcher;
use strake_common::schema::IntrospectedTable;

#[async_trait]
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

#[derive(Debug, Clone)]
pub struct TableRef {
    pub schema: String,
    pub table: String,
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntrospectError {
    #[error("Connection failure: {0}")]
    Connection(String),
    #[error("Permission denied: {0}")]
    Permission(String),
    #[error("Table not found: {0}")]
    NotFound(String),
    #[error("Query execution error: {0}")]
    Query(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}
