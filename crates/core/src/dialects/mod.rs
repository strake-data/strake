//! SQL Dialect Implementations
//!
//! Custom UnparserDialect implementations for databases not covered by DataFusion's built-in dialects.
//! Uses FunctionMapper for declarative function translation rules.

mod function_mapper;
mod oracle;
mod snowflake;

pub use function_mapper::{FunctionMapper, Translation};
pub use oracle::OracleDialect;
pub use snowflake::SnowflakeDialect;
