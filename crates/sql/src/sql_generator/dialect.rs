use crate::dialects::FunctionMapper;
use crate::sql_generator::error::SqlGenError;
use datafusion::arrow::datatypes::DataType as DfDataType;
use datafusion::logical_expr::Operator;
use datafusion::sql::unparser::dialect::Dialect;
use sqlparser::ast::{BinaryOperator, DataType as SqlDataType, TimezoneInfo};

pub trait DialectCapabilities: Send + Sync {
    fn supports_distinct_on(&self) -> bool {
        false
    }
    fn supports_values_clause(&self) -> bool {
        true
    }
    fn requires_from_dual(&self) -> bool {
        false
    }

    // Function & Operator mappings
    fn map_operator(&self, _op: &Operator) -> Option<BinaryOperator> {
        None
    }
    // These could eventually replace or complement FunctionMapper
    fn map_aggregate_function(&self, _name: &str) -> Option<String> {
        const AGGREGATES: &[&str] = &["sum", "count", "avg", "min", "max", "stddev", "variance"];
        if AGGREGATES.contains(&_name.to_lowercase().as_str()) {
            Some(_name.to_uppercase())
        } else {
            None
        }
    }
    fn map_scalar_function(&self, _name: &str) -> Option<String> {
        None
    }

    /// Format an interval literal for the dialect
    /// Returns None if the dialect doesn't support the IntervalMonthDayNano type logic
    fn format_interval(
        &self,
        _months: i32,
        _days: i32,
        _nanos: i64,
    ) -> Option<sqlparser::ast::Expr> {
        None
    }

    /// Normalize function name for the dialect (e.g. casing)
    fn normalize_function_name(&self, name: &str) -> String {
        match name.to_lowercase().as_str() {
            // Aggregate functions
            "sum" | "count" | "avg" | "min" | "max" | "stddev" | "variance" |
            // Window functions  
            "row_number" | "rank" | "dense_rank" | "lead" | "lag" | "first_value" | "last_value" |
            "nth_value" | "cume_dist" | "percent_rank" | "ntile" |
            // Common scalar functions that should be uppercase
            "coalesce" | "nullif" | "greatest" | "least" | "concat" | "substring" | "substr" |
            "length" | "char_length" | "upper" | "lower" | "trim" | "abs" | "ceil" | "floor" |
            "round" | "trunc" | "sqrt" | "power" | "exp" | "ln" | "log" | "sin" | "cos" | "tan" |
            "cast" | "date_part" | "extract" | "to_char" | "to_date" | "to_timestamp" => {
                name.to_uppercase()
            }
            _ => name.to_string(),
        }
    }
}

pub trait TypeMapper: Send + Sync {
    fn map_type(&self, df_type: &DfDataType) -> Result<SqlDataType, SqlGenError>;
}

pub struct DefaultDialectCapabilities;
impl DialectCapabilities for DefaultDialectCapabilities {}

pub struct PostgreSqlCapabilities;
impl DialectCapabilities for PostgreSqlCapabilities {
    fn supports_distinct_on(&self) -> bool {
        true
    }

    fn format_interval(&self, months: i32, days: i32, nanos: i64) -> Option<sqlparser::ast::Expr> {
        // Postgres format: 'X MONTHS Y DAYS Z NANOSECONDS' (as currently implemented)
        // Actually Postgres uses ::interval syntax normally, but the verbose string works too
        let value = format!("{} MONTHS {} DAYS {} NANOSECONDS", months, days, nanos);
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
            DfDataType::Utf8 | DfDataType::LargeUtf8 => Ok(SqlDataType::Varchar(None)),
            DfDataType::Boolean => Ok(SqlDataType::Boolean),
            DfDataType::Date32 => Ok(SqlDataType::Date),
            DfDataType::Timestamp(_, _) => Ok(SqlDataType::Timestamp(None, TimezoneInfo::None)),
            DfDataType::Decimal128(p, s) => Ok(SqlDataType::Decimal(
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(*p as u64, *s as i64),
            )),
            _ => Err(SqlGenError::UnsupportedPlan {
                message: format!("Unsupported type for casting: {:?}", df_type),
                node_type: "Cast".to_string(),
            }),
        }
    }
}

/// dialect configuration for the generator
pub struct GeneratorDialect<'a> {
    pub unparser_dialect: &'a dyn Dialect,
    pub function_mapper: Option<&'a FunctionMapper>,
    pub capabilities: std::sync::Arc<dyn DialectCapabilities>,
    pub type_mapper: std::sync::Arc<dyn TypeMapper>,
    pub dialect_name: &'a str,
}

impl<'a> GeneratorDialect<'a> {
    pub fn new(
        unparser_dialect: &'a dyn Dialect,
        function_mapper: Option<&'a FunctionMapper>,
        capabilities: std::sync::Arc<dyn DialectCapabilities>,
        type_mapper: std::sync::Arc<dyn TypeMapper>,
        dialect_name: &'a str,
    ) -> Self {
        Self {
            unparser_dialect,
            function_mapper,
            capabilities,
            type_mapper,
            dialect_name,
        }
    }
}
