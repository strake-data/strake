use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use datafusion::logical_expr::{Distinct, Expr};
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{
    Expr as SqlExpr, Ident, LimitClause, Offset, SelectItem, SetExpr, TableAlias, TableFactor,
    TableWithJoins, WildcardAdditionalOptions,
};

pub(crate) fn handle_union(
    gen: &mut SqlGenerator,
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
        let checkpoint = gen.context.checkpoint();
        let query = match gen.plan_to_query(input) {
            Ok(q) => q,
            Err(e) => {
                gen.context.rollback(checkpoint);
                return Err(e);
            }
        };
        queries.push(query);
        // Inputs are subqueries, their internal scopes are not needed by the union itself
        // but we need to ensure we don't leak them.
        gen.context.rollback(checkpoint);
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

    let union_alias = gen.context.next_alias();
    let columns = union
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(union_alias.as_str()),
        })
        .collect::<Vec<_>>()
        .into();
    gen.context
        .enter_scope(union_alias, columns, vec![])
        .commit();

    Ok(combined_query)
}

pub(crate) fn handle_distinct(
    gen: &mut SqlGenerator,
    distinct: &datafusion::logical_expr::Distinct,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let input = match distinct {
        Distinct::All(input) => input,
        Distinct::On(on) => &on.input,
    };
    let mut query = gen.plan_to_query(input)?;
    if let SetExpr::Select(ref mut select) = *query.body {
        match distinct {
            Distinct::All(_) => {
                select.distinct = Some(sqlparser::ast::Distinct::Distinct);
            }
            Distinct::On(on) => {
                let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
                let on_exprs = on
                    .on_expr
                    .iter()
                    .map(|e| translator.expr_to_sql(e))
                    .collect::<Result<Vec<_>, SqlGenError>>()?;
                select.distinct = Some(sqlparser::ast::Distinct::On(on_exprs));
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
    gen: &mut SqlGenerator,
    limit: &datafusion::logical_expr::Limit,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = gen.plan_to_query(&limit.input)?;
    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);

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
            rows: sqlparser::ast::OffsetRows::None,
        })
    } else {
        None
    };

    if limit_expr.is_some() || offset_expr.is_some() {
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit: limit_expr,
            offset: offset_expr,
            limit_by: vec![],
        });
    }

    Ok(query)
}

pub(crate) fn handle_empty_relation(
    gen: &mut SqlGenerator,
    empty: &datafusion::logical_expr::EmptyRelation,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut select = gen.create_skeleton_select();
    select.projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];

    // Use SELECT NULL WHERE 1=0 as a portable empty relation
    let mut dummy_select = gen.create_skeleton_select();
    dummy_select.projection = vec![SelectItem::UnnamedExpr(SqlExpr::Value(
        sqlparser::ast::Value::Null.into(),
    ))];
    dummy_select.from = vec![];

    // Some dialects need FROM DUAL or similar.
    // For now, we use a subquery with no FROM which is common but maybe not universal.
    // Future work: use dialect-specific "dual" table if needed.

    let mut dummy_query = gen.create_skeleton_query();
    dummy_query.body = Box::new(SetExpr::Select(Box::new(dummy_select)));

    let alias = gen.context.next_alias();
    select.from = vec![TableWithJoins {
        relation: TableFactor::Derived {
            lateral: false,
            subquery: Box::new(dummy_query),
            alias: Some(TableAlias {
                name: Ident::with_quote('"', alias.clone()),
                columns: vec![],
            }),
        },
        joins: vec![],
    }];

    select.selection = Some(SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::Value(
            sqlparser::ast::Value::Number("1".to_string(), false).into(),
        )),
        op: sqlparser::ast::BinaryOperator::Eq,
        right: Box::new(SqlExpr::Value(
            sqlparser::ast::Value::Number("0".to_string(), false).into(),
        )),
    });

    let mut query = gen.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));

    let columns = empty
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(alias.as_str()),
        })
        .collect::<Vec<_>>()
        .into();
    gen.context.enter_scope(alias, columns, vec![]).commit();

    Ok(query)
}

pub(crate) fn handle_values(
    gen: &mut SqlGenerator,
    values: &datafusion::logical_expr::Values,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    if values.values.is_empty() {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Empty Values".to_string(),
            node_type: "Values".to_string(),
        });
    }

    let mut rows_translated = Vec::new();
    {
        let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
        for row in &values.values {
            let mut row_translated = Vec::new();
            for (i, expr) in row.iter().enumerate() {
                let sql_expr = translator.expr_to_sql(expr)?;
                let field = values.schema.field(i);
                row_translated.push((sql_expr, field.name().clone()));
            }
            rows_translated.push(row_translated);
        }
    }

    let mut queries = Vec::new();
    for row_translated in rows_translated {
        let mut select = gen.create_skeleton_select();
        let mut select_items = Vec::new();

        for (sql_expr, name) in row_translated {
            select_items.push(SelectItem::ExprWithAlias {
                expr: sql_expr,
                alias: Ident::new(name),
            });
        }

        select.projection = select_items;
        select.from = vec![];

        let mut query = gen.create_skeleton_query();
        query.body = Box::new(SetExpr::Select(Box::new(select)));
        queries.push(query);
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

    let alias = gen.context.next_alias();
    let columns = values
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(alias.as_str()),
        })
        .collect::<Vec<_>>()
        .into();
    gen.context.enter_scope(alias, columns, vec![]).commit();

    Ok(combined_query)
}

pub(crate) fn handle_recursive_query(
    gen: &mut SqlGenerator,
    recursive: &datafusion::logical_expr::RecursiveQuery,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let static_query = gen.plan_to_query(&recursive.static_term)?;
    let recursive_term_query = gen.plan_to_query(&recursive.recursive_term)?;

    let cte_name = recursive.name.clone();
    let cte = sqlparser::ast::Cte {
        alias: TableAlias {
            name: Ident::new(cte_name.clone()),
            columns: vec![],
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

    let mut query = gen.create_skeleton_query();
    query.with = Some(sqlparser::ast::With {
        with_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
        recursive: true,
        cte_tables: vec![cte],
    });

    if let SetExpr::Select(ref mut select) = *query.body {
        select.from = vec![TableWithJoins {
            relation: TableFactor::Table {
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
        select.projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
    }

    let alias = gen.context.next_alias();
    let columns = recursive
        .static_term
        .schema()
        .fields()
        .iter()
        .map(
            |f: &std::sync::Arc<datafusion::arrow::datatypes::Field>| ColumnEntry {
                name: std::sync::Arc::from(f.name().as_ref()),
                data_type: f.data_type().clone(),
                source_alias: std::sync::Arc::from(alias.as_str()),
            },
        )
        .collect::<Vec<_>>()
        .into();
    gen.context.enter_scope(alias, columns, vec![]).commit();

    Ok(query)
}

fn validate_limit_offset_expr(expr: &Expr) -> Result<(), SqlGenError> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0 => Ok(()),
        Expr::Literal(ScalarValue::UInt64(Some(_)), _) => Ok(()),
        Expr::Placeholder(_) => Ok(()),
        _ => Err(SqlGenError::UnsupportedPlan {
            message: format!(
                "LIMIT/OFFSET must be non-negative integer or placeholder, got {}",
                expr
            ),
            node_type: "Limit".to_string(),
        }),
    }
}
