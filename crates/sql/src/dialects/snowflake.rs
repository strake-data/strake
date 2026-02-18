//! Snowflake Dialect
//!
//! Custom UnparserDialect for Snowflake with common function translations.

use super::FunctionMapper;
use datafusion::sql::unparser::dialect::Dialect;
use sqlparser::ast::{Expr as SqlExpr, Value};

/// Snowflake-specific SQL dialect for the Unparser
#[derive(Debug, Clone)]
pub struct SnowflakeDialect {
    mapper: FunctionMapper,
}

impl Default for SnowflakeDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SnowflakeDialect {
    pub fn new() -> Self {
        Self {
            mapper: snowflake_function_rules(),
        }
    }

    /// Access the function mapper for custom translations
    pub fn mapper(&self) -> &FunctionMapper {
        &self.mapper
    }
}

impl Dialect for SnowflakeDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn use_timestamp_for_date64(&self) -> bool {
        true
    }
}

fn null_expr() -> SqlExpr {
    SqlExpr::Value(Value::Null.into())
}

fn str_expr(s: &str) -> SqlExpr {
    SqlExpr::Value(Value::SingleQuotedString(s.to_string()).into())
}

/// Snowflake-specific function translation rules
fn snowflake_function_rules() -> FunctionMapper {
    FunctionMapper::new()
        .rename("length", "LENGTH")
        .rename("substr", "SUBSTR")
        .rename("upper", "UPPER")
        .rename("lower", "LOWER")
        .rename("trim", "TRIM")
        .rename("coalesce", "COALESCE")
        .rename("abs", "ABS")
        .rename("ceil", "CEIL")
        .rename("floor", "FLOOR")
        .rename("round", "ROUND")
        .rename("concat", "CONCAT")
        .rename("to_timestamp", "TO_TIMESTAMP")
        .rename("to_date", "TO_DATE")
        .rename("nvl", "NVL")
        .rename("iff", "IFF")
        // Transforms
        .transform("string_agg", |args| {
            let expr = args.first().cloned().unwrap_or_else(null_expr);
            let sep = args.get(1).cloned().unwrap_or_else(|| str_expr(","));
            FunctionMapper::build_func("LISTAGG", vec![expr, sep])
        })
        .transform("array_agg", |args| {
            let expr = args.first().cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("ARRAY_AGG", vec![expr])
        })
        .transform("current_timestamp", |_| {
            SqlExpr::Function(sqlparser::ast::Function {
                name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new("CURRENT_TIMESTAMP"),
                )]),
                args: sqlparser::ast::FunctionArguments::None,
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: sqlparser::ast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })
        })
        .transform("now", |_| {
            SqlExpr::Function(sqlparser::ast::Function {
                name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new("CURRENT_TIMESTAMP"),
                )]),
                args: sqlparser::ast::FunctionArguments::None,
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: sqlparser::ast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })
        })
        .transform("from_unixtime", |args| {
            let ts = args.first().cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("TO_TIMESTAMP", vec![ts])
        })
}

impl crate::sql_generator::dialect::DialectCapabilities for SnowflakeDialect {
    fn supports_distinct_on(&self) -> bool {
        false
    }
}

impl crate::sql_generator::dialect::TypeMapper for SnowflakeDialect {
    fn map_type(
        &self,
        df_type: &datafusion::arrow::datatypes::DataType,
    ) -> Result<sqlparser::ast::DataType, crate::sql_generator::error::SqlGenError> {
        // Snowflake mapping
        use datafusion::arrow::datatypes::DataType;
        use sqlparser::ast::DataType as SqlDataType;

        match df_type {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(SqlDataType::Varchar(None)),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                Ok(SqlDataType::Numeric(sqlparser::ast::ExactNumberInfo::None))
            }
            DataType::Float64 | DataType::Float32 => {
                Ok(SqlDataType::Float(sqlparser::ast::ExactNumberInfo::None))
            }
            DataType::Boolean => Ok(SqlDataType::Boolean),
            DataType::Date32 => Ok(SqlDataType::Date),
            DataType::Timestamp(_, _) => Ok(SqlDataType::Timestamp(
                None,
                sqlparser::ast::TimezoneInfo::None,
            )),
            _ => crate::sql_generator::dialect::DefaultTypeMapper.map_type(df_type),
        }
    }
}
