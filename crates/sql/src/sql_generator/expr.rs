//! # SQL Expression Translator
//!
//! Translates DataFusion [`Expr`] variants into [`sqlparser::ast::Expr`] nodes.
//!
//! ## Overview
//!
//! The [`ExprTranslator`] uses the [`GeneratorContext`] to resolve column references
//! and the [`GeneratorDialect`] to handle dialect-specific function and operator mappings.
//!
//! Expressions not handled by specialized match arms fall through to DataFusion's
//! built-in [`Unparser`], with the result parsed back into a `sqlparser` AST for
//! structural consistency.
//!
//! ## Errors
//!
//! - [`SqlGenError::ScopeViolation`]: Returned when a column reference cannot be resolved in the current context.
//! - [`SqlGenError::UnsupportedPlan`]: Returned for expressions that cannot be translated to the target dialect.

use crate::sql_generator::context::GeneratorContext;
use crate::sql_generator::dialect::GeneratorDialect;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::sanitize::{safe_ident, safe_ident_unquoted};
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, ObjectName,
    ObjectNamePart, WindowSpec, WindowType,
};
use sqlparser::parser::Parser;
use std::sync::OnceLock;

/// Translates DataFusion logical expressions into SQL AST expressions.
pub struct ExprTranslator<'a, 'b> {
    /// Reference to the generator context for column resolution.
    pub context: &'a mut GeneratorContext,
    /// Reference to the generator dialect for function mapping.
    pub dialect: &'b GeneratorDialect<'b>,
    /// Internal DataFusion unparser used as a fallback.
    unparser: OnceLock<Unparser<'b>>,
}

impl<'a, 'b> ExprTranslator<'a, 'b> {
    /// Creates a new [`ExprTranslator`].
    pub fn new(context: &'a mut GeneratorContext, dialect: &'b GeneratorDialect<'b>) -> Self {
        Self {
            context,
            dialect,
            unparser: OnceLock::new(),
        }
    }

    /// Translates a DataFusion [`Expr`] into a `sqlparser` [`Expr`].
    pub fn expr_to_sql(&mut self, expr: &Expr) -> Result<sqlparser::ast::Expr, SqlGenError> {
        match expr {
            Expr::Column(col) => match self.context.resolve_column(col, "Column") {
                Ok(entry) => Ok(sqlparser::ast::Expr::CompoundIdentifier(vec![
                    safe_ident(entry.source_alias.as_ref())?,
                    safe_ident(entry.name.as_ref())?,
                ])),
                Err(e) => Err(e),
            },

            Expr::ScalarFunction(func) => self.translate_function(func.name(), &func.args, None),

            Expr::AggregateFunction(func) => {
                self.translate_function(func.func.name(), &func.params.args, None)
            }

            Expr::WindowFunction(window) => {
                let over = WindowSpec {
                    partition_by: window
                        .params
                        .partition_by
                        .iter()
                        .map(|e| self.expr_to_sql(e))
                        .collect::<Result<Vec<_>, SqlGenError>>()?,
                    order_by: window
                        .params
                        .order_by
                        .iter()
                        .map(|e| {
                            let sql_expr = self.expr_to_sql(&e.expr)?;
                            Ok(sqlparser::ast::OrderByExpr {
                                expr: sql_expr,
                                options: sqlparser::ast::OrderByOptions {
                                    asc: Some(e.asc),
                                    nulls_first: Some(e.nulls_first),
                                },
                                with_fill: None,
                            })
                        })
                        .collect::<Result<Vec<_>, SqlGenError>>()?,
                    window_frame: Some(self.translate_window_frame(&window.params.window_frame)?),
                    window_name: None,
                };

                self.translate_function(
                    window.fun.name(),
                    &window.params.args,
                    Some(sqlparser::ast::WindowType::WindowSpec(over)),
                )
            }

            // Fallback to DataFusion Unparser for everything else
            Expr::BinaryExpr(bin) => {
                let left = self.expr_to_sql(&bin.left)?;
                let right = self.expr_to_sql(&bin.right)?;
                let op = self.translate_binary_op(bin.op)?;
                Ok(sqlparser::ast::Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }

            Expr::Literal(val, _) => self.translate_literal(val),

            Expr::Alias(alias) => {
                // If we hit an alias in an expression, we usually just want the inner expression
                // unless we're in a projection (which handles aliases separately)
                self.expr_to_sql(&alias.expr)
            }

            Expr::Not(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::UnaryOp {
                    op: sqlparser::ast::UnaryOperator::Not,
                    expr: Box::new(sql_inner),
                })
            }

            Expr::IsNotNull(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::IsNotNull(Box::new(sql_inner)))
            }

            Expr::IsNull(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::IsNull(Box::new(sql_inner)))
            }

            Expr::Between(between) => {
                let expr = self.expr_to_sql(&between.expr)?;
                let low = self.expr_to_sql(&between.low)?;
                let high = self.expr_to_sql(&between.high)?;
                Ok(sqlparser::ast::Expr::Between {
                    expr: Box::new(expr),
                    negated: between.negated,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }

            Expr::InList(in_list) => {
                let expr = self.expr_to_sql(&in_list.expr)?;
                let list = in_list
                    .list
                    .iter()
                    .map(|e| self.expr_to_sql(e))
                    .collect::<Result<Vec<_>, SqlGenError>>()?;
                Ok(sqlparser::ast::Expr::InList {
                    expr: Box::new(expr),
                    list,
                    negated: in_list.negated,
                })
            }

            Expr::Cast(cast) => {
                let sql_inner = self.expr_to_sql(&cast.expr)?;
                let sql_type = self.dialect.type_mapper.map_type(cast.field.data_type())?;
                Ok(sqlparser::ast::Expr::Cast {
                    expr: Box::new(sql_inner),
                    data_type: sql_type,
                    format: None,
                    kind: sqlparser::ast::CastKind::Cast,
                    array: false,
                })
            }

            Expr::TryCast(cast) => {
                let sql_inner = self.expr_to_sql(&cast.expr)?;
                let sql_type = self.dialect.type_mapper.map_type(cast.field.data_type())?;
                Ok(sqlparser::ast::Expr::Cast {
                    expr: Box::new(sql_inner),
                    data_type: sql_type,
                    format: None,
                    kind: sqlparser::ast::CastKind::TryCast,
                    array: false,
                })
            }

            Expr::Case(case) => {
                let operand = case
                    .expr
                    .as_ref()
                    .map(|e| self.expr_to_sql(e))
                    .transpose()?
                    .map(Box::new);
                let conditions = case
                    .when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        Ok(sqlparser::ast::CaseWhen {
                            condition: self.expr_to_sql(w)?,
                            result: self.expr_to_sql(t)?,
                        })
                    })
                    .collect::<Result<Vec<_>, SqlGenError>>()?;
                let else_result = case
                    .else_expr
                    .as_ref()
                    .map(|e| self.expr_to_sql(e))
                    .transpose()?
                    .map(Box::new);
                Ok(sqlparser::ast::Expr::Case {
                    operand,
                    conditions,
                    else_result,
                    case_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
                    end_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
                })
            }

            Expr::Like(like) => {
                let expr = self.expr_to_sql(&like.expr)?;
                let pattern = self.expr_to_sql(&like.pattern)?;
                let escape_char = like
                    .escape_char
                    .map(|c| sqlparser::ast::Value::SingleQuotedString(c.to_string()));

                if like.case_insensitive {
                    Ok(sqlparser::ast::Expr::ILike {
                        negated: like.negated,
                        expr: Box::new(expr),
                        pattern: Box::new(pattern),
                        escape_char,
                        any: false,
                    })
                } else {
                    Ok(sqlparser::ast::Expr::Like {
                        negated: like.negated,
                        expr: Box::new(expr),
                        pattern: Box::new(pattern),
                        escape_char,
                        any: false,
                    })
                }
            }

            Expr::Negative(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::UnaryOp {
                    op: sqlparser::ast::UnaryOperator::Minus,
                    expr: Box::new(sql_inner),
                })
            }

            Expr::IsTrue(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::IsTrue(Box::new(sql_inner)))
            }

            Expr::IsFalse(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::IsFalse(Box::new(sql_inner)))
            }

            Expr::IsUnknown(e) => {
                let sql_inner = self.expr_to_sql(e)?;
                Ok(sqlparser::ast::Expr::IsUnknown(Box::new(sql_inner)))
            }

            _ => {
                let unparser = self
                    .unparser
                    .get_or_init(|| Unparser::new(self.dialect.unparser_dialect));
                let sql_str = unparser
                    .expr_to_sql(expr)
                    .map_err(|e| SqlGenError::UnsupportedPlan {
                        message: format!("Unparser failed: {e}"),
                        node_type: "Expr".to_string(),
                    })?
                    .to_string();

                // Parse back to AST to maintain structural integrity
                let dialect = sqlparser::dialect::GenericDialect {};
                let parser = Parser::new(&dialect);
                parser
                    .try_with_sql(&sql_str)
                    .map_err(SqlGenError::Parser)?
                    .parse_expr()
                    .map_err(SqlGenError::Parser)
            }
        }
    }
    fn translate_window_frame(
        &self,
        frame: &datafusion::logical_expr::WindowFrame,
    ) -> Result<sqlparser::ast::WindowFrame, SqlGenError> {
        use datafusion::logical_expr::WindowFrameUnits;
        use sqlparser::ast::WindowFrameUnits as SqlUnits;

        let units = match frame.units {
            WindowFrameUnits::Rows => SqlUnits::Rows,
            WindowFrameUnits::Range => SqlUnits::Range,
            WindowFrameUnits::Groups => SqlUnits::Groups,
        };

        let start_bound = self.translate_window_bound(&frame.start_bound)?;
        let end_bound = self.translate_window_bound(&frame.end_bound)?;

        Ok(sqlparser::ast::WindowFrame {
            units,
            start_bound,
            end_bound: Some(end_bound),
        })
    }

    fn translate_window_bound(
        &self,
        bound: &datafusion::logical_expr::WindowFrameBound,
    ) -> Result<sqlparser::ast::WindowFrameBound, SqlGenError> {
        use datafusion::logical_expr::WindowFrameBound;
        use datafusion::scalar::ScalarValue;
        use sqlparser::ast::WindowFrameBound as SqlBound;

        match bound {
            WindowFrameBound::Preceding(val) => match val {
                ScalarValue::UInt64(Some(v)) => Ok(SqlBound::Preceding(Some(Box::new(
                    sqlparser::ast::Expr::Value(
                        sqlparser::ast::Value::Number(v.to_string(), false).into(),
                    ),
                )))),
                ScalarValue::Int64(Some(v)) => Ok(SqlBound::Preceding(Some(Box::new(
                    sqlparser::ast::Expr::Value(
                        sqlparser::ast::Value::Number(v.to_string(), false).into(),
                    ),
                )))),
                ScalarValue::IntervalMonthDayNano(Some(v)) => {
                    let months = v.months;
                    let days = v.days;
                    let nanos = v.nanoseconds;

                    if let Some(expr) = self
                        .dialect
                        .capabilities
                        .format_interval(months, days, nanos)
                    {
                        Ok(SqlBound::Preceding(Some(Box::new(expr))))
                    } else {
                        Err(SqlGenError::UnsupportedPlan {
                            message: "Dialect does not support IntervalMonthDayNano".to_string(),
                            node_type: "WindowBound".to_string(),
                        })
                    }
                }
                ScalarValue::UInt64(None)
                | ScalarValue::Int64(None)
                | ScalarValue::IntervalMonthDayNano(None) => Ok(SqlBound::Preceding(None)), // UNBOUNDED PRECEDING
                ScalarValue::Null => Err(SqlGenError::UnsupportedPlan {
                    message: "NULL is not a valid window frame bound".to_string(),
                    node_type: "WindowBound".to_string(),
                }),
                _ => Err(SqlGenError::UnsupportedPlan {
                    message: format!("Unsupported window bound value: {:?}", val),
                    node_type: "WindowBound".to_string(),
                }),
            },
            WindowFrameBound::CurrentRow => Ok(SqlBound::CurrentRow),
            WindowFrameBound::Following(val) => match val {
                ScalarValue::UInt64(Some(v)) => Ok(SqlBound::Following(Some(Box::new(
                    sqlparser::ast::Expr::Value(
                        sqlparser::ast::Value::Number(v.to_string(), false).into(),
                    ),
                )))),
                ScalarValue::Int64(Some(v)) => Ok(SqlBound::Following(Some(Box::new(
                    sqlparser::ast::Expr::Value(
                        sqlparser::ast::Value::Number(v.to_string(), false).into(),
                    ),
                )))),
                ScalarValue::IntervalMonthDayNano(Some(v)) => {
                    let months = v.months;
                    let days = v.days;
                    let nanos = v.nanoseconds;

                    if let Some(expr) = self
                        .dialect
                        .capabilities
                        .format_interval(months, days, nanos)
                    {
                        Ok(SqlBound::Following(Some(Box::new(expr))))
                    } else {
                        Err(SqlGenError::UnsupportedPlan {
                            message: "Dialect does not support IntervalMonthDayNano".to_string(),
                            node_type: "WindowBound".to_string(),
                        })
                    }
                }
                ScalarValue::UInt64(None)
                | ScalarValue::Int64(None)
                | ScalarValue::IntervalMonthDayNano(None) => Ok(SqlBound::Following(None)), // UNBOUNDED FOLLOWING
                ScalarValue::Null => Err(SqlGenError::UnsupportedPlan {
                    message: "NULL is not a valid window frame bound".to_string(),
                    node_type: "WindowBound".to_string(),
                }),
                _ => Err(SqlGenError::UnsupportedPlan {
                    message: format!("Unsupported window bound value: {:?}", val),
                    node_type: "WindowBound".to_string(),
                }),
            },
        }
    }

    fn translate_binary_op(
        &self,
        op: datafusion::logical_expr::Operator,
    ) -> Result<sqlparser::ast::BinaryOperator, SqlGenError> {
        // Check dialect-specific mapping first
        if let Some(sql_op) = self.dialect.capabilities.map_operator(&op) {
            return Ok(sql_op);
        }

        use datafusion::logical_expr::Operator;
        match op {
            Operator::Eq => Ok(sqlparser::ast::BinaryOperator::Eq),
            Operator::NotEq => Ok(sqlparser::ast::BinaryOperator::NotEq),
            Operator::Gt => Ok(sqlparser::ast::BinaryOperator::Gt),
            Operator::GtEq => Ok(sqlparser::ast::BinaryOperator::GtEq),
            Operator::Lt => Ok(sqlparser::ast::BinaryOperator::Lt),
            Operator::LtEq => Ok(sqlparser::ast::BinaryOperator::LtEq),
            Operator::Plus => Ok(sqlparser::ast::BinaryOperator::Plus),
            Operator::Minus => Ok(sqlparser::ast::BinaryOperator::Minus),
            Operator::Multiply => Ok(sqlparser::ast::BinaryOperator::Multiply),
            Operator::Divide => Ok(sqlparser::ast::BinaryOperator::Divide),
            Operator::Modulo => Ok(sqlparser::ast::BinaryOperator::Modulo),
            Operator::And => Ok(sqlparser::ast::BinaryOperator::And),
            Operator::Or => Ok(sqlparser::ast::BinaryOperator::Or),
            Operator::StringConcat => Ok(sqlparser::ast::BinaryOperator::StringConcat),
            Operator::BitwiseAnd => Ok(sqlparser::ast::BinaryOperator::BitwiseAnd),
            Operator::BitwiseOr => Ok(sqlparser::ast::BinaryOperator::BitwiseOr),
            Operator::BitwiseXor => Ok(sqlparser::ast::BinaryOperator::BitwiseXor),
            Operator::BitwiseShiftLeft => Ok(sqlparser::ast::BinaryOperator::PGBitwiseShiftLeft),
            Operator::BitwiseShiftRight => Ok(sqlparser::ast::BinaryOperator::PGBitwiseShiftRight),
            _ => Err(SqlGenError::UnsupportedPlan {
                message: format!("Binary operator: {:?}", op),
                node_type: "BinaryExpr".to_string(),
            }),
        }
    }

    fn translate_literal(
        &self,
        val: &datafusion::scalar::ScalarValue,
    ) -> Result<sqlparser::ast::Expr, SqlGenError> {
        use datafusion::scalar::ScalarValue;
        let sql_value = match val {
            ScalarValue::Int8(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::Int16(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::Int32(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::Int64(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::UInt8(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::UInt16(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::UInt32(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::UInt64(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::Float32(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::Float64(Some(v)) => {
                sqlparser::ast::Value::Number(v.to_string(), false).into()
            }
            ScalarValue::Utf8(Some(v)) => {
                sqlparser::ast::Value::SingleQuotedString(v.clone()).into()
            }
            ScalarValue::Boolean(Some(v)) => sqlparser::ast::Value::Boolean(*v).into(),
            ScalarValue::Decimal128(val, _p, s) => {
                if let Some(v) = val {
                    let s = *s as usize;
                    let v_str = v.abs().to_string();
                    let mut formatted = if s > 0 {
                        if v_str.len() > s {
                            let split_at = v_str.len() - s;
                            format!("{}.{}", &v_str[..split_at], &v_str[split_at..])
                        } else {
                            format!("0.{:0>width$}", v_str, width = s)
                        }
                    } else {
                        v_str
                    };
                    if *v < 0 {
                        formatted.insert(0, '-');
                    }
                    sqlparser::ast::Value::Number(formatted, false).into()
                } else {
                    sqlparser::ast::Value::Null.into()
                }
            }
            ScalarValue::Null => sqlparser::ast::Value::Null.into(),
            _ => {
                return Err(SqlGenError::UnsupportedPlan {
                    message: format!("Literal value: {:?}", val),
                    node_type: "Literal".to_string(),
                });
            }
        };
        Ok(sqlparser::ast::Expr::Value(sql_value))
    }

    /// Translates a function call, applying dialect-specific name mappings and stripping qualifiers if needed.
    pub fn translate_function(
        &mut self,
        name: &str,
        args: &[Expr],
        over: Option<WindowType>,
    ) -> Result<sqlparser::ast::Expr, SqlGenError> {
        // Check FunctionMapper first
        if let Some(mapper) = self.dialect.function_mapper {
            let mut sql_args = Vec::new();
            for arg in args {
                sql_args.push(self.expr_to_sql(arg)?);
            }

            if let Some(translated) = mapper.translate(name, &sql_args) {
                let mut expr = translated;
                if let sqlparser::ast::Expr::Function(ref mut f) = expr
                    && f.over.is_none()
                {
                    f.over = over;
                }
                return Ok(expr);
            }
        }

        // Check DialectCapabilities mappings next
        let mapped_name = if over.is_some()
            || name.eq_ignore_ascii_case("count")
            || name.eq_ignore_ascii_case("sum")
        {
            // Likely an aggregate or window function
            self.dialect.capabilities.map_aggregate_function(name)
        } else {
            self.dialect.capabilities.map_scalar_function(name)
        };

        let final_name =
            mapped_name.unwrap_or_else(|| self.dialect.capabilities.normalize_function_name(name));

        let mut sql_args = Vec::new();
        for arg in args {
            sql_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                self.expr_to_sql(arg)?,
            )));
        }

        let func_args = FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: sql_args,
            clauses: vec![],
        });

        Ok(sqlparser::ast::Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(safe_ident_unquoted(
                &final_name,
            )?)]),
            args: func_args,
            filter: None,
            null_treatment: None,
            over,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        }))
    }
}
