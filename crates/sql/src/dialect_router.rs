//! Smart router for SQL dialect translation.
//!
//! Strake supports multiple mechanisms for pushing down queries to remote sources:
//!
//! 1. **Native Unparser**: Uses DataFusion's built-in SQL generator for standard dialects (Postgres, MySQL, SQLite).
//! 2. **Custom Unparser**: Uses Strake's own `UnparserDialect` implementations for complex enterprise sources (Oracle, Snowflake) where DataFusion support is missing or incomplete.
//! 3. **Substrait**: For engines that support binary plan transmission (DuckDB, other DataFusion instances).
//! 4. **No Pushdown**: Fallback to local execution if no dialect is available.
//!
//! # Wrapper Logic
//!
//! The `route_dialect` function is the main entry point, mapping a source type string
//! (e.g., "postgres", "oracle") to the appropriate `DialectPath`.

use datafusion::sql::unparser::dialect::{
    DefaultDialect, Dialect as UnparserDialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};
use std::sync::Arc;

use crate::dialects::{OracleDialect, SnowflakeDialect};

use crate::sql_generator::dialect::{
    DefaultDialectCapabilities, DefaultTypeMapper, DialectCapabilities, PostgreSqlCapabilities,
    TypeMapper,
};

/// Represents the dialect translation path for a source
pub enum DialectPath {
    /// DataFusion built-in Unparser dialects
    Native(
        Arc<dyn UnparserDialect + Send + Sync>,
        Arc<dyn DialectCapabilities>,
        Arc<dyn TypeMapper>,
    ),
    /// Strake custom Unparser dialects with FunctionMapper
    Custom(
        Arc<dyn UnparserDialect + Send + Sync>,
        Arc<dyn DialectCapabilities>,
        Arc<dyn TypeMapper>,
        Option<crate::dialects::FunctionMapper>,
    ),
    /// Substrait binary plan (for DuckDB, remote DataFusion)
    Substrait,
    /// No pushdown — fetch all data and execute locally
    LocalExecution,
}

/// Routes a source type to its appropriate dialect translation path
pub fn route_dialect(source_type: &str) -> DialectPath {
    match source_type.to_lowercase().as_str() {
        // Tier 1: DataFusion built-in dialects
        "postgres" | "postgresql" => DialectPath::Native(
            Arc::new(PostgreSqlDialect {}),
            Arc::new(PostgreSqlCapabilities),
            Arc::new(DefaultTypeMapper),
        ),
        "mysql" | "mariadb" => DialectPath::Native(
            Arc::new(MySqlDialect {}),
            Arc::new(DefaultDialectCapabilities),
            Arc::new(DefaultTypeMapper),
        ),
        "sqlite" => DialectPath::Native(
            Arc::new(SqliteDialect {}),
            Arc::new(DefaultDialectCapabilities),
            Arc::new(DefaultTypeMapper),
        ),

        // Tier 2: Strake custom dialects
        "oracle" => {
            let dialect = OracleDialect::new();
            let mapper = dialect.mapper().clone();
            let dialect_arc = Arc::new(dialect);
            DialectPath::Custom(
                dialect_arc.clone() as Arc<dyn UnparserDialect + Send + Sync>,
                dialect_arc.clone() as Arc<dyn DialectCapabilities>,
                dialect_arc.clone() as Arc<dyn TypeMapper>,
                Some(mapper),
            )
        }
        "snowflake" => {
            let dialect = SnowflakeDialect::new();
            let mapper = dialect.mapper().clone();
            let dialect_arc = Arc::new(dialect);
            DialectPath::Custom(
                dialect_arc.clone() as Arc<dyn UnparserDialect + Send + Sync>,
                dialect_arc.clone() as Arc<dyn DialectCapabilities>,
                dialect_arc.clone() as Arc<dyn TypeMapper>,
                Some(mapper),
            )
        }

        // Tier 3: Substrait-capable engines
        "duckdb" | "datafusion" => DialectPath::Substrait,

        // Tier 4: Unknown — fallback to local execution (no pushdown)
        _ => {
            tracing::warn!(
                source_type = %source_type,
                "No dialect available for source, falling back to local execution"
            );
            DialectPath::LocalExecution
        }
    }
}

/// Returns a default dialect for rendering when no specific dialect is available
pub fn default_dialect() -> Arc<dyn UnparserDialect + Send + Sync> {
    Arc::new(DefaultDialect {})
}

/// Check if a source uses Substrait for plan pushdown
pub fn is_substrait_source(source_type: &str) -> bool {
    matches!(route_dialect(source_type), DialectPath::Substrait)
}

/// Check if a source requires local execution (no pushdown)
pub fn is_local_execution(source_type: &str) -> bool {
    matches!(route_dialect(source_type), DialectPath::LocalExecution)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_dialects() {
        assert!(matches!(route_dialect("postgres"), DialectPath::Native(..)));
        assert!(matches!(
            route_dialect("PostgreSQL"),
            DialectPath::Native(..)
        ));
        assert!(matches!(route_dialect("mysql"), DialectPath::Native(..)));
        assert!(matches!(route_dialect("sqlite"), DialectPath::Native(..)));
    }

    #[test]
    fn test_custom_dialects() {
        assert!(matches!(route_dialect("oracle"), DialectPath::Custom(..)));
        assert!(matches!(
            route_dialect("snowflake"),
            DialectPath::Custom(..)
        ));
    }

    #[test]
    fn test_substrait() {
        assert!(matches!(route_dialect("duckdb"), DialectPath::Substrait));
        assert!(matches!(
            route_dialect("datafusion"),
            DialectPath::Substrait
        ));
    }

    #[test]
    fn test_fallback() {
        assert!(matches!(
            route_dialect("unknown"),
            DialectPath::LocalExecution
        ));
        assert!(matches!(
            route_dialect("foobar"),
            DialectPath::LocalExecution
        ));
    }
}
