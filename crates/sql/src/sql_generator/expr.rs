use crate::sql_generator::context::GeneratorContext;
use crate::sql_generator::dialect::GeneratorDialect;
use crate::sql_generator::error::SqlGenError;
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, WindowSpec, WindowType,
};
use sqlparser::parser::Parser;

pub struct ExprTranslator<'a> {
    context: &'a mut GeneratorContext,
    dialect: &'a GeneratorDialect<'a>,
}

impl<'a> ExprTranslator<'a> {
    pub fn new(context: &'a mut GeneratorContext, dialect: &'a GeneratorDialect<'a>) -> Self {
        Self { context, dialect }
    }

    pub fn expr_to_sql(&mut self, expr: &Expr) -> Result<sqlparser::ast::Expr, SqlGenError> {
        match expr {
            Expr::Column(col) => {
                let (scope_alias, col_name) = self.context.resolve_column(col, "Expression")?;
                Ok(sqlparser::ast::Expr::CompoundIdentifier(vec![
                    Ident::with_quote('"', scope_alias),
                    Ident::with_quote('"', col_name),
                ]))
            }

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

            Expr::Cast(cast) => {
                let sql_inner = self.expr_to_sql(&cast.expr)?;
                // Minimal cast support for now
                Ok(sqlparser::ast::Expr::Cast {
                    expr: Box::new(sql_inner),
                    data_type: sqlparser::ast::DataType::Custom(
                        sqlparser::ast::ObjectName(vec![
                            sqlparser::ast::ObjectNamePart::Identifier(Ident::new(
                                cast.data_type.to_string(),
                            )),
                        ]),
                        vec![],
                    ),
                    format: None,
                    kind: sqlparser::ast::CastKind::Cast,
                })
            }

            _ => {
                let unparser = Unparser::new(self.dialect.unparser_dialect);
                let sql_str = unparser
                    .expr_to_sql(expr)
                    .map_err(SqlGenError::DataFusion)?;

                // Parse back to AST to maintain structural integrity
                let dialect = sqlparser::dialect::GenericDialect {};
                let parser = Parser::new(&dialect);
                parser
                    .try_with_sql(&sql_str.to_string())
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
                ScalarValue::UInt64(None) | ScalarValue::Int64(None) => {
                    Ok(SqlBound::Preceding(None))
                } // UNBOUNDED PRECEDING
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
                ScalarValue::UInt64(None) | ScalarValue::Int64(None) => {
                    Ok(SqlBound::Following(None))
                } // UNBOUNDED FOLLOWING
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
            ScalarValue::Int8(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::Int16(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::Int32(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::Int64(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::UInt8(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::UInt16(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::UInt32(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::UInt64(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::Float32(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::Float64(Some(v)) => sqlparser::ast::Value::Number(v.to_string(), false),
            ScalarValue::Utf8(Some(v)) => sqlparser::ast::Value::SingleQuotedString(v.clone()),
            ScalarValue::Boolean(Some(v)) => sqlparser::ast::Value::Boolean(*v),
            ScalarValue::Null => sqlparser::ast::Value::Null,
            _ => {
                return Err(SqlGenError::UnsupportedPlan {
                    message: format!("Literal value: {:?}", val),
                    node_type: "Literal".to_string(),
                })
            }
        };
        Ok(sqlparser::ast::Expr::Value(sql_value.into()))
    }

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
                if let sqlparser::ast::Expr::Function(ref mut f) = expr {
                    if f.over.is_none() {
                        f.over = over;
                    }
                }
                return Ok(expr);
            }
        }

        // Standard function handling with normalized name
        let normalized_name = self.normalize_function_name(name);

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
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                normalized_name,
            ))]),
            args: func_args,
            filter: None,
            null_treatment: None,
            over,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        }))
    }

    fn normalize_function_name(&self, name: &str) -> String {
        match self.dialect.dialect_name.to_lowercase().as_str() {
            "postgres" | "postgresql" => name.to_lowercase(),
            "oracle" | "snowflake" => name.to_uppercase(),
            _ => {
                // Default behavior for other dialects
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
    }
}
