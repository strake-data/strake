//! Handles `Union`, `Distinct`, `Limit`, and other set-based logical plan nodes.
//!
//! ## Usage
//!
//! Handled internal by [`SqlGenerator`].
//!
//! ## Performance Characteristics
//!
//! Set operations are generally O(N) where N is the number of inputs. `DistinctOn`
//! translation may involve subquery wrapping which adds planning complexity.

use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use datafusion::logical_expr::{Distinct, Expr};
use sqlparser::ast::{
    Expr as SqlExpr, Ident, ObjectName, ObjectNamePart, Offset, SetExpr, TableAlias,
    TableWithJoins, Value, WildcardAdditionalOptions,
};
use std::sync::Arc;

pub(crate) fn handle_union(
    generator: &mut SqlGenerator,
    union: &datafusion::logical_expr::Union,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    if union.inputs.is_empty() {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Union with no inputs".to_string(),
            node_type: "Union".to_string(),
        });
    }

    let mut queries = Vec::with_capacity(union.inputs.len());

    for input in &union.inputs {
        let checkpoint = generator.context.checkpoint();
        let query = match generator.plan_to_query(input) {
            Ok(q) => q,
            Err(e) => {
                generator.context.rollback(checkpoint);
                return Err(e);
            }
        };
        queries.push(query);
        // Inputs are subqueries, their internal scopes are not needed by the union itself
        // but we need to ensure we don't leak them.
        generator.context.rollback(checkpoint);
    }

    let mut combined_query = queries.remove(0);

    for next_query in queries {
        let left_body = *combined_query.body;
        let right_body = *next_query.body;

        combined_query.body = Box::new(SetExpr::SetOperation {
            op: sqlparser::ast::SetOperator::Union,
            set_quantifier: sqlparser::ast::SetQuantifier::All,
            left: Box::new(left_body),
            right: Box::new(right_body),
        });
    }

    let union_alias = generator.context.next_alias();
    let columns: Arc<[ColumnEntry]> = Arc::from(
        union
            .schema
            .fields()
            .iter()
            .map(|f| ColumnEntry {
                name: Arc::from(f.name().as_str()),
                name_lower: Arc::from(f.name().to_lowercase().as_str()),
                data_type: f.data_type().clone(),
                source_alias: union_alias.clone().into(),
                provenance: vec![union_alias.clone()],
                unique_id: generator.context.next_column_id(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    generator
        .context
        .enter_scope(union_alias, columns, vec![])
        .commit();

    Ok(combined_query)
}

pub(crate) fn handle_distinct(
    generator: &mut SqlGenerator,
    distinct: &datafusion::logical_expr::Distinct,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let input = match distinct {
        Distinct::All(input) => input,
        Distinct::On(on) => &on.input,
    };
    let mut query = generator.plan_to_query(input)?;
    if let SetExpr::Select(ref mut select) = *query.body {
        match distinct {
            Distinct::All(_) => {
                select.distinct = Some(sqlparser::ast::Distinct::Distinct);
            }
            Distinct::On(on) => {
                if generator.dialect.capabilities.supports_distinct_on() {
                    let mut translator =
                        ExprTranslator::new(&mut generator.context, &generator.dialect);
                    let on_exprs = on
                        .on_expr
                        .iter()
                        .map(|e| translator.expr_to_sql(e))
                        .collect::<Result<Vec<_>, SqlGenError>>()?;
                    select.distinct = Some(sqlparser::ast::Distinct::On(on_exprs));
                } else {
                    return rewrite_distinct_on_to_row_number(generator, on);
                }
            }
        }
    } else {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Distinct on non-Select".to_string(),
            node_type: "Distinct".to_string(),
        });
    }
    Ok(query)
}

pub(crate) fn handle_limit(
    generator: &mut SqlGenerator,
    limit: &datafusion::logical_expr::Limit,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = generator.plan_to_query(&limit.input)?;
    let mut translator = ExprTranslator::new(&mut generator.context, &generator.dialect);

    let limit_expr = if let Some(fetch) = &limit.fetch {
        validate_limit_offset_expr(fetch)?;
        Some(translator.expr_to_sql(fetch)?)
    } else {
        None
    };

    let offset_expr = if let Some(skip) = &limit.skip {
        validate_limit_offset_expr(skip)?;
        Some(Offset {
            value: translator.expr_to_sql(skip)?,
            rows: translator.dialect.capabilities.offset_rows_style(),
        })
    } else {
        None
    };

    if limit_expr.is_some() || offset_expr.is_some() {
        generator.apply_limit_offset(&mut query, limit_expr, offset_expr);
    }

    Ok(query)
}

pub(crate) fn handle_empty_relation(
    generator: &mut SqlGenerator,
    empty: &datafusion::logical_expr::EmptyRelation,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut select = generator.create_skeleton_select();

    // Test expects SELECT NULL for each column
    let mut projection = empty
        .schema
        .fields()
        .iter()
        .map(|_| {
            sqlparser::ast::SelectItem::UnnamedExpr(SqlExpr::Value(
                sqlparser::ast::Value::Null.into(),
            ))
        })
        .collect::<Vec<_>>();

    if projection.is_empty() {
        projection.push(sqlparser::ast::SelectItem::UnnamedExpr(SqlExpr::Value(
            sqlparser::ast::Value::Null.into(),
        )));
    }

    select.projection = projection;
    select.from = vec![]; // No FROM clause by default

    // Dialect-specific: Oracle needs FROM DUAL
    if generator.dialect.capabilities.requires_from_dual() {
        select.from = vec![TableWithJoins {
            relation: sqlparser::ast::TableFactor::Table {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("DUAL"))]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                with_ordinality: false,
                partitions: vec![],
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            joins: vec![],
        }];
    }

    select.selection = Some(SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::Value(Value::Number("1".to_string(), false).into())),
        op: sqlparser::ast::BinaryOperator::Eq,
        right: Box::new(SqlExpr::Value(Value::Number("0".to_string(), false).into())),
    });

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));

    // Generating a dummy alias for consistency with how scopes work in other nodes,
    // though it's less critical for an empty relation.
    let alias = generator.context.next_alias();
    let columns = empty
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: Arc::from(f.name().as_str()),
            name_lower: Arc::from(f.name().to_lowercase().as_str()),
            data_type: f.data_type().clone(),
            source_alias: alias.clone().into(),
            provenance: vec![alias.clone()],
            unique_id: generator.context.next_column_id(),
        })
        .collect::<Vec<_>>()
        .into();
    generator
        .context
        .enter_scope(alias, columns, vec![])
        .commit();

    Ok(query)
}

pub(crate) fn handle_values(
    generator: &mut SqlGenerator,
    values: &datafusion::logical_expr::Values,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    if values.values.is_empty() {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Empty Values".to_string(),
            node_type: "Values".to_string(),
        });
    }

    let mut translator = ExprTranslator::new(&mut generator.context, &generator.dialect);
    let mut sql_rows = Vec::new();
    for row in &values.values {
        let mut sql_row = Vec::new();
        for expr in row {
            sql_row.push(translator.expr_to_sql(expr)?);
        }
        sql_rows.push(sql_row);
    }

    let combined_query;
    if generator.dialect.capabilities.supports_values_clause() {
        let mut query = generator.create_skeleton_query();
        query.body = Box::new(SetExpr::Values(sqlparser::ast::Values {
            explicit_row: false,
            rows: sql_rows,
            value_keyword: true,
        }));
        combined_query = query;
    } else {
        // Fallback to UNION ALL
        let mut selects = Vec::new();
        for row in sql_rows {
            let mut select = generator.create_skeleton_select();
            select.projection = row
                .into_iter()
                .enumerate()
                .map(|(i, expr)| {
                    let field = values.schema.field(i);
                    sqlparser::ast::SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::new(field.name().clone()),
                    }
                })
                .collect();
            select.from = vec![];

            if generator.dialect.capabilities.requires_from_dual() {
                select.from = vec![TableWithJoins {
                    relation: sqlparser::ast::TableFactor::Table {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("DUAL"))]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        with_ordinality: false,
                        partitions: vec![],
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    },
                    joins: vec![],
                }];
            }

            selects.push(SetExpr::Select(Box::new(select)));
        }

        let mut body = selects.remove(0);
        for next_body in selects {
            body = SetExpr::SetOperation {
                op: sqlparser::ast::SetOperator::Union,
                set_quantifier: sqlparser::ast::SetQuantifier::All,
                left: Box::new(body),
                right: Box::new(next_body),
            };
        }

        let mut query = generator.create_skeleton_query();
        query.body = Box::new(body);
        combined_query = query;
    }

    let alias = generator.context.next_alias();
    let columns = values
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: Arc::from(f.name().as_str()),
            name_lower: Arc::from(f.name().to_lowercase().as_str()),
            data_type: f.data_type().clone(),
            source_alias: alias.clone().into(),
            provenance: vec![alias.clone()],
            unique_id: generator.context.next_column_id(),
        })
        .collect::<Vec<_>>()
        .into();
    generator
        .context
        .enter_scope(alias, columns, vec![])
        .commit();

    Ok(combined_query)
}

fn rewrite_distinct_on_to_row_number(
    generator: &mut SqlGenerator,
    on: &datafusion::logical_expr::DistinctOn,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // 1. Get input query
    let checkpoint = generator.context.checkpoint();
    let mut input_query = generator.plan_to_query(&on.input)?;
    let input_relation = generator.extract_relation(&mut input_query, None, checkpoint)?;
    let input_scope =
        generator
            .context
            .current_scope()
            .cloned()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing scope for DistinctOn input".to_string(),
                node_type: "DistinctOn".to_string(),
            })?;
    generator.context.pop_scope();

    let mut translator = ExprTranslator::new(&mut generator.context, &generator.dialect);

    // 2. Build Window Function: ROW_NUMBER() OVER (PARTITION BY [on_exprs] ORDER BY [sort_exprs])
    let partition_by = on
        .on_expr
        .iter()
        .map(|e| translator.expr_to_sql(e))
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let order_by = if let Some(sort_exprs) = &on.sort_expr {
        sort_exprs
            .iter()
            .map(|e| {
                let sql_expr = translator.expr_to_sql(&e.expr)?;
                Ok(sqlparser::ast::OrderByExpr {
                    expr: sql_expr,
                    options: sqlparser::ast::OrderByOptions {
                        asc: Some(e.asc),
                        nulls_first: Some(e.nulls_first),
                    },
                    with_fill: None,
                })
            })
            .collect::<Result<Vec<_>, SqlGenError>>()?
    } else {
        // Fallback: order by the same expressions we partition by to ensure determinism
        partition_by
            .iter()
            .map(|e| sqlparser::ast::OrderByExpr {
                expr: e.clone(),
                options: sqlparser::ast::OrderByOptions {
                    asc: Some(true),
                    nulls_first: Some(false),
                },
                with_fill: None,
            })
            .collect()
    };

    let row_num_expr = SqlExpr::Function(sqlparser::ast::Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("ROW_NUMBER"))]),
        args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: Some(sqlparser::ast::WindowType::WindowSpec(
            sqlparser::ast::WindowSpec {
                partition_by,
                order_by,
                window_frame: None,
                window_name: None,
            },
        )),
        within_group: vec![],
        parameters: sqlparser::ast::FunctionArguments::None,
        uses_odbc_syntax: false,
    });

    // 3. Create inner SELECT with ROW_NUMBER()
    let mut inner_select = generator.create_skeleton_select();
    inner_select.from = vec![TableWithJoins {
        relation: input_relation,
        joins: vec![],
    }];

    let row_num_alias = "__strake_row_num";
    let mut projection = input_scope
        .columns
        .iter()
        .map(|entry| {
            Ok(sqlparser::ast::SelectItem::ExprWithAlias {
                expr: SqlExpr::CompoundIdentifier(vec![
                    sqlparser::ast::Ident::new(entry.source_alias.as_ref()),
                    sqlparser::ast::Ident::new(entry.name.as_ref()),
                ]),
                alias: Ident::new(entry.name.as_ref()),
            })
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    projection.push(sqlparser::ast::SelectItem::ExprWithAlias {
        expr: row_num_expr,
        alias: Ident::new(row_num_alias),
    });
    inner_select.projection = projection;

    // 4. Wrap in Outer SELECT with WHERE row_num = 1
    let inner_alias = generator.context.next_alias();
    let mut outer_select = generator.create_skeleton_select();
    outer_select.from = vec![TableWithJoins {
        relation: sqlparser::ast::TableFactor::Derived {
            lateral: false,
            subquery: Box::new(sqlparser::ast::Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(inner_select))),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            }),
            alias: Some(TableAlias {
                name: Ident::new(inner_alias.clone()),
                columns: vec![],
                explicit: generator
                    .dialect
                    .capabilities
                    .supports_as_alias_for_tables(),
            }),
            sample: None,
        },
        joins: vec![],
    }];

    outer_select.projection = input_scope
        .columns
        .iter()
        .map(|entry| sqlparser::ast::SelectItem::ExprWithAlias {
            expr: SqlExpr::CompoundIdentifier(vec![
                Ident::new(inner_alias.clone()),
                Ident::new(entry.name.as_ref()),
            ]),
            alias: Ident::new(entry.name.as_ref()),
        })
        .collect();

    outer_select.selection = Some(SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::CompoundIdentifier(vec![
            Ident::new(inner_alias.clone()),
            Ident::new(row_num_alias),
        ])),
        op: sqlparser::ast::BinaryOperator::Eq,
        right: Box::new(SqlExpr::Value(Value::Number("1".to_string(), false).into())),
    });

    // 5. Setup final scope
    let final_alias = generator.context.next_alias();
    let final_columns = input_scope
        .columns
        .iter()
        .map(|e| {
            let mut e = e.clone();
            e.source_alias = final_alias.clone().into();
            e.provenance.push(final_alias.clone());
            e
        })
        .collect::<Vec<_>>()
        .into();

    generator
        .context
        .enter_scope(final_alias, final_columns, input_scope.qualifiers.clone())
        .commit();

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(outer_select)));
    Ok(query)
}

pub(crate) fn handle_recursive_query(
    generator: &mut SqlGenerator,
    recursive: &datafusion::logical_expr::RecursiveQuery,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let static_query = generator.plan_to_query(&recursive.static_term)?;
    let recursive_term_query = generator.plan_to_query(&recursive.recursive_term)?;

    let cte_name = recursive.name.clone();
    let cte = sqlparser::ast::Cte {
        alias: TableAlias {
            name: Ident::new(cte_name.clone()),
            columns: vec![],
            explicit: generator
                .dialect
                .capabilities
                .supports_as_alias_for_tables(),
        },
        query: Box::new(sqlparser::ast::Query {
            with: None,
            body: Box::new(sqlparser::ast::SetExpr::SetOperation {
                op: sqlparser::ast::SetOperator::Union,
                set_quantifier: if recursive.is_distinct {
                    sqlparser::ast::SetQuantifier::Distinct
                } else {
                    sqlparser::ast::SetQuantifier::All
                },
                left: Box::new(*static_query.body),
                right: Box::new(*recursive_term_query.body),
            }),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: vec![],
        }),
        from: None,
        materialized: None,
        closing_paren_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
    };

    let mut query = generator.create_skeleton_query();
    query.with = Some(sqlparser::ast::With {
        with_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
        recursive: true,
        cte_tables: vec![cte],
    });

    if let SetExpr::Select(ref mut select) = *query.body {
        select.from = vec![TableWithJoins {
            relation: sqlparser::ast::TableFactor::Table {
                name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    Ident::new(cte_name.clone()),
                )]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                with_ordinality: false,
                partitions: vec![],
                json_path: None,
                sample: None,
                index_hints: vec![],
            },
            joins: vec![],
        }];
        select.projection = vec![sqlparser::ast::SelectItem::Wildcard(
            WildcardAdditionalOptions::default(),
        )];
    }

    let alias = generator.context.next_alias();
    let columns = recursive
        .static_term
        .schema()
        .fields()
        .iter()
        .map(
            |f: &std::sync::Arc<datafusion::arrow::datatypes::Field>| ColumnEntry {
                name: Arc::from(f.name().as_str()),
                name_lower: Arc::from(f.name().to_lowercase().as_str()),
                data_type: f.data_type().clone(),
                source_alias: alias.clone().into(),
                provenance: vec![alias.clone()],
                unique_id: generator.context.next_column_id(),
            },
        )
        .collect::<Vec<_>>()
        .into();
    generator
        .context
        .enter_scope(alias, columns, vec![])
        .commit();

    Ok(query)
}

fn validate_limit_offset_expr(_expr: &Expr) -> Result<(), SqlGenError> {
    // Dynamic LIMIT support: We allow any expression that DataFusion supports in LIMIT/OFFSET.
    // The translator will attempt to convert it to SQL. If the SQL dialect doesn't support
    // complex expressions in LIMIT, the database will return an error, but we shouldn't block it here.
    // Previous strict validation prevented arithmetic (e.g. limit 10+5) and subqueries.
    Ok(())
}
