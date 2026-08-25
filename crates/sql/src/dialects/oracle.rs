//! # Oracle Dialect Integration
//!
//! Provides a custom `UnparserDialect` implementation specifically tailored for Oracle Database.
//!
//! ## Overview
//!
//! This module extends DataFusion's SQL generation layer by providing translations for DataFusion functions,
//! operators, and custom types mapping to Oracle-compatible syntax. It implements the [`Dialect`] trait
//! for custom unparsing alongside [`DialectCapabilities`] and [`TypeMapper`].
//!
//! ## Usage
//!
//! ```rust
//! use strake_sql::dialects::OracleDialect;
//! let dialect = OracleDialect::new();
//! ```
//!
//! ## Performance Characteristics
//!
//! Parsing and mapping logic operates with zero additional heap allocations for static function translations.
//! Scalar function maps are constructed once globally and accessed via constant reference paths.
//!
//! ## Errors
//!
//! Errors are propagated via the normal [`crate::sql_generator::error::SqlGenError`] variants
//! for unsupported types or configurations during plan translation.
//!
//! ## Safety
//!
//! This module is written using safe Rust constructs.

use super::FunctionMapper;
use datafusion::sql::unparser::dialect::Dialect;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Value};

/// Oracle-specific SQL dialect for the Unparser.
#[derive(Debug, Clone)]
pub struct OracleDialect {
    /// Declarative mapping for renaming or transforming scalar functions.
    mapper: FunctionMapper,
}

impl Default for OracleDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleDialect {
    /// Creates a new [`OracleDialect`] with pre-configured function mappings.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use strake_sql::dialects::OracleDialect;
    /// let dialect = OracleDialect::new();
    /// ```
    pub fn new() -> Self {
        Self {
            mapper: oracle_function_rules(),
        }
    }

    /// Access the function mapper for custom translations.
    pub fn mapper(&self) -> &FunctionMapper {
        &self.mapper
    }
}

impl Dialect for OracleDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn use_timestamp_for_date64(&self) -> bool {
        false
    }
}

fn null_expr() -> SqlExpr {
    SqlExpr::Value(Value::Null.into())
}

fn str_expr(s: &str) -> SqlExpr {
    SqlExpr::Value(Value::SingleQuotedString(s.to_string()).into())
}

fn ident_expr(name: &str) -> SqlExpr {
    SqlExpr::Identifier(sqlparser::ast::Ident::new(name))
}

/// Comprehensive Oracle function translation rules
fn oracle_function_rules() -> FunctionMapper {
    FunctionMapper::new()
        // ========================================
        // NULL handling
        // ========================================
        .rename("coalesce", "NVL")
        .rename("nullif", "NULLIF")
        .transform("ifnull", |args| {
            let a = args.first().cloned().unwrap_or_else(null_expr);
            let b = args.get(1).cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("NVL", vec![a, b])
        })
        // ========================================
        // String functions
        // ========================================
        .rename("length", "LENGTH")
        .rename("char_length", "LENGTH")
        .rename("substr", "SUBSTR")
        .rename("substring", "SUBSTR")
        .rename("upper", "UPPER")
        .rename("lower", "LOWER")
        .rename("trim", "TRIM")
        .rename("ltrim", "LTRIM")
        .rename("rtrim", "RTRIM")
        .transform("concat", |args| {
            if args.is_empty() {
                return null_expr();
            }
            let mut res = args[0].clone();
            for arg in &args[1..] {
                res = SqlExpr::BinaryOp {
                    left: Box::new(res),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(arg.clone()),
                };
            }
            res
        })
        .transform("concat_ws", |args| {
            if args.len() < 2 {
                return null_expr();
            }
            let sep = args[0].clone();
            let mut res = args[1].clone();
            for arg in &args[2..] {
                res = SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::BinaryOp {
                        left: Box::new(res),
                        op: BinaryOperator::StringConcat,
                        right: Box::new(sep.clone()),
                    }),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(arg.clone()),
                };
            }
            res
        })
        .transform("strpos", |args| {
            let a = args.first().cloned().unwrap_or_else(null_expr);
            let b = args.get(1).cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("INSTR", vec![a, b])
        })
        .transform("position", |args| {
            let needle = args.first().cloned().unwrap_or_else(null_expr);
            let haystack = args.get(1).cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("INSTR", vec![haystack, needle])
        })
        .transform("regexp_replace", |args| {
            let a = args.first().cloned().unwrap_or_else(null_expr);
            let b = args.get(1).cloned().unwrap_or_else(null_expr);
            let c = args.get(2).cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("REGEXP_REPLACE", vec![a, b, c])
        })
        .transform("regexp_like", |args| {
            let a = args.first().cloned().unwrap_or_else(null_expr);
            let b = args.get(1).cloned().unwrap_or_else(null_expr);
            FunctionMapper::build_func("REGEXP_LIKE", vec![a, b])
        })
        // ========================================
        // Numeric functions
        // ========================================
        .rename("abs", "ABS")
        .rename("ceil", "CEIL")
        .rename("floor", "FLOOR")
        .rename("round", "ROUND")
        .rename("trunc", "TRUNC")
        .rename("mod", "MOD")
        .rename("power", "POWER")
        .rename("sqrt", "SQRT")
        .transform("random", |_| {
            SqlExpr::CompoundIdentifier(vec![
                sqlparser::ast::Ident::new("DBMS_RANDOM"),
                sqlparser::ast::Ident::new("VALUE"),
            ])
        })
        .transform("rand", |_| {
            SqlExpr::CompoundIdentifier(vec![
                sqlparser::ast::Ident::new("DBMS_RANDOM"),
                sqlparser::ast::Ident::new("VALUE"),
            ])
        })
        // ========================================
        // Date/Time functions
        // ========================================
        .transform("current_timestamp", |_| ident_expr("SYSTIMESTAMP"))
        .transform("current_date", |_| ident_expr("SYSDATE"))
        .transform("now", |_| ident_expr("SYSTIMESTAMP"))
        .transform("extract", |args| {
            let part = args.first().cloned().unwrap_or_else(|| str_expr("YEAR"));
            let source = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| ident_expr("SYSDATE"));
            SqlExpr::Extract {
                field: match part {
                    SqlExpr::Value(sqlparser::ast::ValueWithSpan {
                        value: Value::SingleQuotedString(s),
                        ..
                    }) => sqlparser::ast::DateTimeField::Custom(sqlparser::ast::Ident::new(
                        s.to_uppercase(),
                    )),
                    _ => sqlparser::ast::DateTimeField::Year,
                },
                syntax: sqlparser::ast::ExtractSyntax::From,
                expr: Box::new(source),
            }
        })
        .transform("date_part", |args| {
            let part = args.first().cloned().unwrap_or_else(|| str_expr("YEAR"));
            let source = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| ident_expr("SYSDATE"));
            SqlExpr::Extract {
                field: match part {
                    SqlExpr::Value(sqlparser::ast::ValueWithSpan {
                        value: Value::SingleQuotedString(s),
                        ..
                    }) => sqlparser::ast::DateTimeField::Custom(sqlparser::ast::Ident::new(
                        s.to_uppercase(),
                    )),
                    _ => sqlparser::ast::DateTimeField::Year,
                },
                syntax: sqlparser::ast::ExtractSyntax::From,
                expr: Box::new(source),
            }
        })
        .transform("date_trunc", |args| {
            let part = args.first().cloned().unwrap_or_else(|| str_expr("month"));
            let source = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| ident_expr("SYSDATE"));
            let fmt_str = match part {
                SqlExpr::Value(sqlparser::ast::ValueWithSpan {
                    value: Value::SingleQuotedString(s),
                    ..
                }) => match s.to_lowercase().as_str() {
                    "year" | "yyyy" | "yy" => "YYYY",
                    "quarter" => "Q",
                    "month" | "mm" => "MM",
                    "week" => "IW",
                    "day" | "dd" => "DD",
                    "hour" | "hh" => "HH24",
                    "minute" | "mi" => "MI",
                    _ => "MM",
                },
                _ => "MM",
            };
            FunctionMapper::build_func("TRUNC", vec![source, str_expr(fmt_str)])
        })
        .transform("to_date", |args| {
            let a = args.first().cloned().unwrap_or_else(null_expr);
            let b = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| str_expr("YYYY-MM-DD"));
            FunctionMapper::build_func("TO_DATE", vec![a, b])
        })
        .transform("to_timestamp", |args| {
            let a = args.first().cloned().unwrap_or_else(null_expr);
            if args.len() == 1 {
                FunctionMapper::build_func("TO_TIMESTAMP", vec![a])
            } else {
                let b = args
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| str_expr("YYYY-MM-DD HH24:MI:SS"));
                FunctionMapper::build_func("TO_TIMESTAMP", vec![a, b])
            }
        })
        .transform("from_unixtime", |args| {
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| SqlExpr::Value(Value::Number("0".to_string(), false).into()));
            let start = FunctionMapper::build_func(
                "TO_DATE",
                vec![str_expr("1970-01-01"), str_expr("YYYY-MM-DD")],
            );
            SqlExpr::BinaryOp {
                left: Box::new(start),
                op: BinaryOperator::Plus,
                right: Box::new(SqlExpr::Nested(Box::new(SqlExpr::BinaryOp {
                    left: Box::new(ts),
                    op: BinaryOperator::Divide,
                    right: Box::new(SqlExpr::Value(
                        Value::Number("86400".to_string(), false).into(),
                    )),
                }))),
            }
        })
        // ========================================
        // Aggregate functions
        // ========================================
        .rename("count", "COUNT")
        .rename("sum", "SUM")
        .rename("avg", "AVG")
        .rename("min", "MIN")
        .rename("max", "MAX")
        .transform("string_agg", |args| {
            let expr = args.first().cloned().unwrap_or_else(null_expr);
            let sep = args.get(1).cloned().unwrap_or_else(|| str_expr(","));
            SqlExpr::Function(sqlparser::ast::Function {
                name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new("LISTAGG"),
                )]),
                args: sqlparser::ast::FunctionArguments::List(
                    sqlparser::ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(expr.clone()),
                            ),
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(sep),
                            ),
                        ],
                        clauses: vec![],
                    },
                ),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![sqlparser::ast::OrderByExpr {
                    expr,
                    options: sqlparser::ast::OrderByOptions {
                        asc: None,
                        nulls_first: None,
                    },
                    with_fill: None,
                }],
                parameters: sqlparser::ast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })
        })
}

fn format_oracle_timestamp(secs: i64, nsecs: u32, tz: Option<&str>) -> Option<SqlExpr> {
    if let Some(datetime) = chrono::DateTime::from_timestamp(secs, nsecs) {
        if let Some(_tz_str) = tz {
            let formatted = datetime.format("%Y-%m-%d %H:%M:%S%.6f %z").to_string();
            Some(FunctionMapper::build_func(
                "TO_TIMESTAMP_TZ",
                vec![
                    str_expr(&formatted),
                    str_expr("YYYY-MM-DD HH24:MI:SS.FF TZH:TZM"),
                ],
            ))
        } else {
            let formatted = datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            Some(FunctionMapper::build_func(
                "TO_TIMESTAMP",
                vec![str_expr(&formatted), str_expr("YYYY-MM-DD HH24:MI:SS.FF")],
            ))
        }
    } else {
        None
    }
}

impl crate::sql_generator::dialect::DialectCapabilities for OracleDialect {
    fn supports_distinct_on(&self) -> bool {
        false
    }
    fn supports_window_frame_without_order_by(&self) -> bool {
        false
    }
    fn strip_catalog_qualifier(&self) -> bool {
        true
    }
    fn supports_as_alias_for_tables(&self) -> bool {
        false
    }
    fn always_wrap_subqueries(&self) -> bool {
        true
    }
    fn supports_limit_clause(&self) -> bool {
        false
    }
    fn supports_fetch_clause(&self) -> bool {
        true
    }
    fn supports_values_clause(&self) -> bool {
        false // Oracle uses SELECT ... FROM DUAL UNION ALL ...
    }
    fn requires_from_dual(&self) -> bool {
        true
    }
    fn offset_rows_style(&self) -> sqlparser::ast::OffsetRows {
        sqlparser::ast::OffsetRows::Rows
    }

    fn format_literal(
        &self,
        val: &datafusion::scalar::ScalarValue,
    ) -> Option<sqlparser::ast::Expr> {
        use datafusion::scalar::ScalarValue;
        match val {
            ScalarValue::Date32(Some(days)) => {
                if let Some(date) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).and_then(|epoch| {
                    epoch.checked_add_signed(chrono::Duration::days(*days as i64))
                }) {
                    let formatted = date.format("%Y-%m-%d").to_string();
                    Some(FunctionMapper::build_func(
                        "TO_DATE",
                        vec![str_expr(&formatted), str_expr("YYYY-MM-DD")],
                    ))
                } else {
                    None
                }
            }
            ScalarValue::Date64(Some(ms)) => {
                let secs = ms.div_euclid(1000);
                let nsecs = ms.rem_euclid(1000) * 1_000_000;
                if let Some(datetime) = chrono::DateTime::from_timestamp(secs, nsecs as u32) {
                    let formatted = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
                    Some(FunctionMapper::build_func(
                        "TO_DATE",
                        vec![str_expr(&formatted), str_expr("YYYY-MM-DD HH24:MI:SS")],
                    ))
                } else {
                    None
                }
            }
            ScalarValue::TimestampSecond(Some(secs), tz) => {
                format_oracle_timestamp(*secs, 0, tz.as_deref())
            }
            ScalarValue::TimestampMillisecond(Some(ms), tz) => {
                let secs = ms.div_euclid(1000);
                let nsecs = ms.rem_euclid(1000) * 1_000_000;
                format_oracle_timestamp(secs, nsecs as u32, tz.as_deref())
            }
            ScalarValue::TimestampMicrosecond(Some(us), tz) => {
                let secs = us.div_euclid(1_000_000);
                let nsecs = us.rem_euclid(1_000_000) * 1000;
                format_oracle_timestamp(secs, nsecs as u32, tz.as_deref())
            }
            ScalarValue::TimestampNanosecond(Some(ns), tz) => {
                let secs = ns.div_euclid(1_000_000_000);
                let nsecs = ns.rem_euclid(1_000_000_000);
                format_oracle_timestamp(secs, nsecs as u32, tz.as_deref())
            }
            _ => None,
        }
    }

    fn format_interval(&self, months: i32, days: i32, nanos: i64) -> Option<sqlparser::ast::Expr> {
        if months != 0 && days == 0 && nanos == 0 {
            let sign = if months < 0 { "-" } else { "" };
            let abs_months = months.abs();
            let value = format!("{}{}", sign, abs_months);
            Some(SqlExpr::Interval(sqlparser::ast::Interval {
                value: Box::new(str_expr(&value)),
                leading_field: Some(sqlparser::ast::DateTimeField::Month),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            }))
        } else if months == 0 {
            let sign = if days < 0 || nanos < 0 { "-" } else { "" };
            let abs_days = days.abs();
            let abs_nanos = nanos.unsigned_abs();
            let secs = abs_nanos / 1_000_000_000;
            let rem_nanos = abs_nanos % 1_000_000_000;
            let hours = secs / 3600;
            let mins = (secs % 3600) / 60;
            let secs_rem = secs % 60;

            let value = if rem_nanos > 0 {
                format!(
                    "{}{} {:02}:{:02}:{:02}.{:06}",
                    sign,
                    abs_days,
                    hours,
                    mins,
                    secs_rem,
                    rem_nanos / 1000
                )
            } else if hours > 0 || mins > 0 || secs_rem > 0 {
                format!(
                    "{}{} {:02}:{:02}:{:02}",
                    sign, abs_days, hours, mins, secs_rem
                )
            } else {
                format!("{}{}", sign, abs_days)
            };

            let leading_field = Some(sqlparser::ast::DateTimeField::Day);
            let last_field = if hours > 0 || mins > 0 || secs_rem > 0 || rem_nanos > 0 {
                Some(sqlparser::ast::DateTimeField::Second)
            } else {
                None
            };

            Some(SqlExpr::Interval(sqlparser::ast::Interval {
                value: Box::new(str_expr(&value)),
                leading_field,
                leading_precision: None,
                last_field,
                fractional_seconds_precision: None,
            }))
        } else {
            None
        }
    }
}

impl crate::sql_generator::dialect::TypeMapper for OracleDialect {
    fn map_type(
        &self,
        df_type: &datafusion::arrow::datatypes::DataType,
    ) -> Result<sqlparser::ast::DataType, crate::sql_generator::error::SqlGenError> {
        use datafusion::arrow::datatypes::DataType;
        use sqlparser::ast::DataType as SqlDataType;

        match df_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(SqlDataType::Custom(
                sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new("VARCHAR2"),
                )]),
                vec![],
            )),
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Ok(SqlDataType::Custom(
                sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new("RAW"),
                )]),
                vec![],
            )),
            DataType::Int64 | DataType::UInt64 | DataType::Int32 | DataType::UInt32 => {
                Ok(SqlDataType::Numeric(sqlparser::ast::ExactNumberInfo::None))
            }
            DataType::Float64 => Ok(SqlDataType::Double(sqlparser::ast::ExactNumberInfo::None)),
            DataType::Float32 => Ok(SqlDataType::Float(sqlparser::ast::ExactNumberInfo::None)),
            DataType::Boolean => Ok(SqlDataType::Numeric(
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(1, 0),
            )),
            DataType::Date32 => Ok(SqlDataType::Date),
            DataType::Timestamp(_, _) => Ok(SqlDataType::Timestamp(
                None,
                sqlparser::ast::TimezoneInfo::None,
            )),
            _ => crate::sql_generator::dialect::DefaultTypeMapper.map_type(df_type),
        }
    }
}
