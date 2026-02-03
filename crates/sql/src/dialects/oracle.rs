//! Oracle Dialect
//!
//! Custom UnparserDialect for Oracle Database with comprehensive function translations.

use super::FunctionMapper;
use datafusion::sql::unparser::dialect::Dialect;
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Value};

/// Oracle-specific SQL dialect for the Unparser

#[derive(Debug, Clone)]
pub struct OracleDialect {
    mapper: FunctionMapper,
}

impl Default for OracleDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleDialect {
    pub fn new() -> Self {
        Self {
            mapper: oracle_function_rules(),
        }
    }

    /// Access the function mapper for custom translations
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
                        s.to_uppercase().trim_matches('\'').to_string(),
                    )),
                    _ => sqlparser::ast::DateTimeField::Year,
                },
                syntax: sqlparser::ast::ExtractSyntax::From,
                expr: Box::new(source),
            }
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
