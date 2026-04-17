//! SQL Dialect Implementations
//!
//! Custom UnparserDialect implementations for databases not covered by DataFusion's built-in dialects.
//! Uses FunctionMapper for declarative function translation rules.

/// Declarative registry for function translation rules.
mod function_mapper;
/// Oracle SQL dialect implementation.
mod oracle;
/// Snowflake SQL dialect implementation.
mod snowflake;

pub use function_mapper::{FunctionMapper, Translation};
pub use oracle::OracleDialect;
pub use snowflake::SnowflakeDialect;
