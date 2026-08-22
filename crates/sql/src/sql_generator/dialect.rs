//! # SQL Generator Dialects
//!
//! Provides traits and implementations for handling dialect-specific SQL generation.
//!
//! ## Overview
//!
//! This module defines the [`DialectCapabilities`] and [`TypeMapper`] traits, which
//! allow the SQL generator to adapt its output for different databases (e.g., DuckDB, Postgres, Snowflake).
//!
//! ## Normalization
//!
//! Dialects can control whether to strip catalog or schema qualifiers, how to map
//! operators and functions, and how to format specific types like intervals.
//!
//! ## Usage
//!
//! ```rust
//! use strake_sql::dialect_router::route_dialect;
//! let dialect = route_dialect("duckdb");
//! ```
//!
//! ## Performance Characteristics
//!
//! Dialect lookups and name normalizations are O(1) or O(N) where N is the length
//! of the identifier. Normalization avoids allocations when possible by checking
//! for existing casing.
//!
//! ## Errors
//!
//! Returns `DialectPath::LocalExecution` for unknown source types, logging a warning.

use crate::dialects::FunctionMapper;
use crate::sql_generator::error::SqlGenError;
use datafusion::arrow::datatypes::DataType as DfDataType;
use datafusion::logical_expr::Operator;
use datafusion::sql::unparser::dialect::Dialect;
use sqlparser::ast::{BinaryOperator, DataType as SqlDataType, TimezoneInfo};

/// Trait for defining the capabilities and behavior of a specific SQL dialect.
pub trait DialectCapabilities: Send + Sync {
    /// Returns true if the dialect supports `DISTINCT ON (...)` syntax.
    fn supports_distinct_on(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports the `VALUES` clause as a table factor.
    fn supports_values_clause(&self) -> bool {
        true
    }
    /// Returns true if the dialect requires `FROM DUAL` for constant queries.
    fn requires_from_dual(&self) -> bool {
        false
    }
    /// Returns true if catalog qualifiers should be stripped from identifiers.
    fn strip_catalog_qualifier(&self) -> bool {
        false
    }
    /// Returns true if schema qualifiers should be stripped from identifiers.
    fn strip_schema_qualifier(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports the `AS` keyword for table aliases.
    fn supports_as_alias_for_tables(&self) -> bool {
        true
    }
    /// Returns true if subqueries should always be wrapped to ensure stable aliasing.
    fn always_wrap_subqueries(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports the `LIMIT` clause.
    fn supports_limit_clause(&self) -> bool {
        true
    }
    /// Returns true if the dialect supports the `FETCH FIRST` clause.
    fn supports_fetch_clause(&self) -> bool {
        false
    }
    /// Returns true if the dialect supports empty SELECT lists.
    fn supports_empty_select(&self) -> bool {
        false
    }
    /// Returns the offset rows preference/suffix style for the dialect.
    fn offset_rows_style(&self) -> sqlparser::ast::OffsetRows {
        sqlparser::ast::OffsetRows::None
    }

    /// Maps a DataFusion [`Operator`] to a SQL [`BinaryOperator`].
    fn map_operator(&self, _op: &Operator) -> Option<BinaryOperator> {
        None
    }
    /// Maps an aggregate function name to its dialect-specific counterpart.
    fn map_aggregate_function(&self, name: &str) -> Option<String> {
        const AGGREGATES: &[&str] = &["sum", "count", "avg", "min", "max", "stddev", "variance"];
        if AGGREGATES.iter().any(|&a| a.eq_ignore_ascii_case(name)) {
            Some(name.to_uppercase())
        } else {
            None
        }
    }
    /// Maps a scalar function name to its dialect-specific counterpart.
    fn map_scalar_function(&self, _name: &str) -> Option<String> {
        None
    }

    /// Format an interval literal for the dialect.
    /// Returns None if the dialect doesn't support the IntervalMonthDayNano type logic.
    fn format_interval(
        &self,
        _months: i32,
        _days: i32,
        _nanos: i64,
    ) -> Option<sqlparser::ast::Expr> {
        None
    }

    /// Format a literal scalar value for the dialect.
    /// Returns None if the dialect relies on standard literal unparsing.
    fn format_literal(
        &self,
        _val: &datafusion::scalar::ScalarValue,
    ) -> Option<sqlparser::ast::Expr> {
        None
    }

    /// Normalize function name for the dialect (e.g. casing).
    fn normalize_function_name(&self, name: &str) -> String {
        const COMMON_FUNCS: &[&str] = &[
            "ln",
            "sum",
            "avg",
            "min",
            "max",
            "abs",
            "exp",
            "sin",
            "cos",
            "tan",
            "log",
            "rank",
            "lead",
            "trim",
            "ceil",
            "sqrt",
            "cast",
            "count",
            "floor",
            "round",
            "trunc",
            "power",
            "upper",
            "lower",
            "stddev",
            "ntile",
            "length",
            "substr",
            "concat",
            "nullif",
            "extract",
            "to_char",
            "to_date",
            "variance",
            "greatest",
            "coalesce",
            "substring",
            "row_number",
            "date_part",
            "dense_rank",
            "last_value",
            "percent_rank",
            "first_value",
            "cume_dist",
            "to_timestamp",
        ];

        if COMMON_FUNCS.iter().any(|&f| f.eq_ignore_ascii_case(name)) {
            name.to_uppercase()
        } else {
            name.to_string()
        }
    }
}

/// Trait for mapping DataFusion types to SQL data types.
pub trait TypeMapper: Send + Sync {
    /// Maps a DataFusion [`DfDataType`] to a SQL [`SqlDataType`].
    fn map_type(&self, df_type: &DfDataType) -> Result<SqlDataType, SqlGenError>;
}

/// Default implementation of [`DialectCapabilities`] with standard SQL behavior.
pub struct DefaultDialectCapabilities;
impl DialectCapabilities for DefaultDialectCapabilities {}

/// PostgreSQL-specific capabilities and formatting.
pub struct PostgreSqlCapabilities;
impl DialectCapabilities for PostgreSqlCapabilities {
    fn supports_distinct_on(&self) -> bool {
        true
    }

    fn strip_catalog_qualifier(&self) -> bool {
        true
    }

    fn strip_schema_qualifier(&self) -> bool {
        true
    }

    fn format_interval(&self, months: i32, days: i32, nanos: i64) -> Option<sqlparser::ast::Expr> {
        let sign = if nanos < 0 { "-" } else { "" };
        let abs_nanos = nanos.unsigned_abs();
        let whole_secs = abs_nanos / 1_000_000_000;
        let rem_nanos = abs_nanos % 1_000_000_000;
        let value = format!(
            "{} months {} days {}{}.{:09} seconds",
            months, days, sign, whole_secs, rem_nanos
        );
        Some(sqlparser::ast::Expr::Interval(sqlparser::ast::Interval {
            value: Box::new(sqlparser::ast::Expr::Value(
                sqlparser::ast::Value::SingleQuotedString(value).into(),
            )),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }))
    }

    fn normalize_function_name(&self, name: &str) -> String {
        name.to_lowercase()
    }
}

/// SQLite-specific capabilities including catalog/schema stripping.
pub struct SqliteCapabilities;
impl DialectCapabilities for SqliteCapabilities {
    fn strip_catalog_qualifier(&self) -> bool {
        true
    }
    fn strip_schema_qualifier(&self) -> bool {
        true
    }
    fn normalize_function_name(&self, name: &str) -> String {
        name.to_uppercase()
    }
}

/// DuckDB-specific capabilities including catalog/schema stripping.
pub struct DuckDBCapabilities;
impl DialectCapabilities for DuckDBCapabilities {
    fn strip_catalog_qualifier(&self) -> bool {
        true
    }
    fn strip_schema_qualifier(&self) -> bool {
        true
    }
    fn supports_empty_select(&self) -> bool {
        true
    }
    fn normalize_function_name(&self, name: &str) -> String {
        if name.chars().all(|c| !c.is_uppercase()) {
            name.to_string()
        } else {
            name.to_lowercase()
        }
    }
}

/// Snowflake-specific capabilities and keyword normalization.
pub struct SnowflakeCapabilities;
impl DialectCapabilities for SnowflakeCapabilities {
    fn supports_distinct_on(&self) -> bool {
        false
    }
    fn supports_values_clause(&self) -> bool {
        true
    }

    fn normalize_function_name(&self, name: &str) -> String {
        name.to_uppercase()
    }
}

/// Standard DataFusion to SQL type mapper.
pub struct DefaultTypeMapper;
impl TypeMapper for DefaultTypeMapper {
    fn map_type(&self, df_type: &DfDataType) -> Result<SqlDataType, SqlGenError> {
        match df_type {
            DfDataType::Int8 => Ok(SqlDataType::TinyInt(None)),
            DfDataType::Int16 => Ok(SqlDataType::SmallInt(None)),
            DfDataType::Int32 => Ok(SqlDataType::Integer(None)),
            DfDataType::Int64 => Ok(SqlDataType::BigInt(None)),
            DfDataType::UInt8 => Ok(SqlDataType::UInt8),
            DfDataType::UInt16 => Ok(SqlDataType::UInt16),
            DfDataType::UInt32 => Ok(SqlDataType::UInt32),
            DfDataType::UInt64 => Ok(SqlDataType::UInt64),
            DfDataType::Float32 => Ok(SqlDataType::Float(sqlparser::ast::ExactNumberInfo::None)),
            DfDataType::Float64 => Ok(SqlDataType::Double(sqlparser::ast::ExactNumberInfo::None)),
            DfDataType::Utf8 | DfDataType::LargeUtf8 | DfDataType::Utf8View => {
                Ok(SqlDataType::Varchar(None))
            }

            DfDataType::Boolean => Ok(SqlDataType::Boolean),
            DfDataType::Date32 => Ok(SqlDataType::Date),
            DfDataType::Timestamp(_, _) => Ok(SqlDataType::Timestamp(None, TimezoneInfo::None)),
            DfDataType::Decimal128(p, s)
            | DfDataType::Decimal64(p, s)
            | DfDataType::Decimal32(p, s) => Ok(SqlDataType::Decimal(
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(*p as u64, *s as i64),
            )),
            _ => Err(SqlGenError::UnsupportedPlan {
                message: format!("Unsupported type for casting: {:?}", df_type),
                node_type: "Cast".to_string(),
            }),
        }
    }
}

/// Dialect configuration used by the [`SqlGenerator`].
pub struct GeneratorDialect<'a> {
    /// The underlying DataFusion unparser dialect.
    pub unparser_dialect: &'a dyn Dialect,
    /// Optional registry for function name/transformation mapping.
    pub function_mapper: Option<&'a FunctionMapper>,
    /// Dialect-specific capabilities and formatting behavior.
    pub capabilities: std::sync::Arc<dyn DialectCapabilities>,
    /// Custom type mapper for CAST operations.
    pub type_mapper: std::sync::Arc<dyn TypeMapper>,
    /// The target source type (e.g. `SourceType::Postgres`).
    pub source_type: strake_common::models::SourceType,
}

impl<'a> GeneratorDialect<'a> {
    /// Creates a new [`GeneratorDialect`].
    pub fn new(
        unparser_dialect: &'a dyn Dialect,
        function_mapper: Option<&'a FunctionMapper>,
        capabilities: std::sync::Arc<dyn DialectCapabilities>,
        type_mapper: std::sync::Arc<dyn TypeMapper>,
        source_type: impl Into<strake_common::models::SourceType>,
    ) -> Self {
        Self {
            unparser_dialect,
            function_mapper,
            capabilities,
            type_mapper,
            source_type: source_type.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgresql_format_interval() {
        let caps = PostgreSqlCapabilities;

        // Positive interval
        let expr = caps.format_interval(1, 2, 3_500_000_000).unwrap();
        assert_eq!(
            expr.to_string(),
            "INTERVAL '1 months 2 days 3.500000000 seconds'"
        );

        // Negative sub-second interval
        let expr = caps.format_interval(0, 0, -500_000_000).unwrap();
        assert_eq!(
            expr.to_string(),
            "INTERVAL '0 months 0 days -0.500000000 seconds'"
        );

        // Negative whole-second and sub-second interval
        let expr = caps.format_interval(0, 0, -1_500_000_000).unwrap();
        assert_eq!(
            expr.to_string(),
            "INTERVAL '0 months 0 days -1.500000000 seconds'"
        );
    }
}
