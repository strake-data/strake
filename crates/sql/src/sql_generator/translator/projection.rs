use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use datafusion::logical_expr::Expr;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};

pub(crate) fn handle_projection(
    gen: &mut SqlGenerator,
    proj: &datafusion::logical_expr::Projection,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = gen.plan_to_query(&proj.input)?;

    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
    let select_items = proj
        .expr
        .iter()
        .map(|e| {
            let sql_expr = translator.expr_to_sql(e)?;
            Ok(SelectItem::UnnamedExpr(sql_expr))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    if !matches!(*query.body, SetExpr::Select(_)) {
        // Wrap non-select body (e.g. Values, SetOperation) in a subquery
        let sub_alias = gen.context.next_alias();
        let derived = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query),
            alias: Some(TableAlias {
                name: safe_ident(&sub_alias)?,
                columns: vec![],
            }),
        };
        let mut select = gen.create_skeleton_select();
        select.from = vec![TableWithJoins {
            relation: derived,
            joins: vec![],
        }];

        let mut new_query = gen.create_skeleton_query();
        new_query.body = Box::new(SetExpr::Select(Box::new(select)));
        query = new_query;
    }

    if let SetExpr::Select(ref mut select) = *query.body {
        select.projection = select_items;
    } else {
        unreachable!("Just ensured it's a Select");
    }

    let input_qualifiers = gen
        .context
        .current_scope()
        .map(|s| s.qualifiers.clone())
        .unwrap_or_default();
    gen.context.pop_scope();

    let proj_alias = gen.context.next_alias();
    let input_scope = gen.context.current_scope().cloned();
    let new_columns = proj
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let mut provenance = vec![proj_alias.clone()];
            if let Some(Expr::Column(col)) = proj.expr.get(i) {
                if let Some(scope) = &input_scope {
                    // Match on (qualifier, name) to disambiguate columns that share a
                    // name across different relations (e.g. after a self-join).
                    let entry = col
                        .relation
                        .as_ref()
                        .and_then(|rel| {
                            scope.columns.iter().find(|e| {
                                e.name.as_ref() == col.name
                                    && e.provenance.iter().any(|p| p == rel.table())
                            })
                        })
                        .or_else(|| {
                            // Fallback: unique name match (safe when no ambiguity)
                            let matches: Vec<_> = scope
                                .columns
                                .iter()
                                .filter(|e| e.name.as_ref() == col.name)
                                .collect();
                            if matches.len() == 1 {
                                Some(matches[0])
                            } else {
                                None
                            }
                        });
                    if let Some(entry) = entry {
                        provenance.extend(entry.provenance.clone());
                    }
                }
            }

            ColumnEntry {
                name: std::sync::Arc::from(f.name().as_ref()),
                data_type: f.data_type().clone(),
                source_alias: std::sync::Arc::from(proj_alias.as_str()),
                provenance,
                unique_id: gen.context.next_column_id(),
            }
        })
        .collect::<Vec<_>>()
        .into();

    gen.context
        .enter_scope(proj_alias, new_columns, input_qualifiers)
        .commit();

    Ok(query)
}

pub(crate) fn handle_filter(
    gen: &mut SqlGenerator,
    filter: &datafusion::logical_expr::Filter,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = gen.plan_to_query(&filter.input)?;
    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
    let sql_expr = translator.expr_to_sql(&filter.predicate)?;

    if !matches!(*query.body, SetExpr::Select(_)) {
        // Wrap non-select body in a subquery to allow filtering.
        // We project columns explicitly (not SELECT *) for portability and to
        // preserve provenance tracking through the scope system.
        let sub_alias = gen.context.next_alias();
        let explicit_projection = if let Some(scope) = gen.context.current_scope() {
            scope
                .columns
                .iter()
                .map(|entry| {
                    Ok(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                        safe_ident(entry.source_alias.as_ref())?,
                        safe_ident(entry.name.as_ref())?,
                    ])))
                })
                .collect::<Result<Vec<_>, SqlGenError>>()?
        } else {
            vec![SelectItem::Wildcard(
                sqlparser::ast::WildcardAdditionalOptions::default(),
            )]
        };
        let derived = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query),
            alias: Some(TableAlias {
                name: safe_ident(&sub_alias)?,
                columns: vec![],
            }),
        };
        let mut select = gen.create_skeleton_select();
        select.from = vec![TableWithJoins {
            relation: derived,
            joins: vec![],
        }];
        select.projection = explicit_projection;

        let mut new_query = gen.create_skeleton_query();
        new_query.body = Box::new(SetExpr::Select(Box::new(select)));
        query = new_query;
    }

    if let SetExpr::Select(ref mut select) = *query.body {
        if let Some(existing) = &select.selection {
            select.selection = Some(SqlExpr::BinaryOp {
                left: Box::new(existing.clone()),
                op: BinaryOperator::And,
                right: Box::new(sql_expr),
            });
        } else {
            select.selection = Some(sql_expr);
        }
    } else {
        unreachable!("Just ensured it's a Select");
    }

    Ok(query)
}

pub(crate) fn handle_subquery_alias(
    gen: &mut SqlGenerator,
    alias: &datafusion::logical_expr::SubqueryAlias,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let inner_query = gen.plan_to_query(&alias.input)?;
    gen.context.pop_scope();

    let subquery_alias = gen.context.next_alias();
    let input_scope = gen.context.current_scope().cloned();
    let columns: std::sync::Arc<[ColumnEntry]> = alias
        .schema
        .fields()
        .iter()
        .map(|f| {
            let mut provenance = vec![alias.alias.to_string(), subquery_alias.clone()];
            // Inherit provenance from input scope. Guard against ambiguous name
            // matches (e.g. self-joins) by only inheriting when exactly one column
            // in the input scope carries this name.
            if let Some(scope) = &input_scope {
                let matches: Vec<_> = scope
                    .columns
                    .iter()
                    .filter(|e| e.name.as_ref() == f.name())
                    .collect();
                if matches.len() == 1 {
                    provenance.extend(matches[0].provenance.clone());
                }
            }

            ColumnEntry {
                name: std::sync::Arc::from(f.name().as_ref()),
                data_type: f.data_type().clone(),
                source_alias: std::sync::Arc::from(subquery_alias.as_str()),
                provenance,
                unique_id: gen.context.next_column_id(),
            }
        })
        .collect::<Vec<_>>()
        .into();
    let qualifiers = vec![alias.alias.to_string()];
    gen.context
        .enter_scope(subquery_alias.clone(), columns.clone(), qualifiers)
        .commit();

    let derived = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(inner_query),
        alias: Some(TableAlias {
            name: safe_ident(&subquery_alias)?,
            columns: vec![],
        }),
    };

    let mut select = gen.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: derived,
        joins: vec![],
    }];
    select.projection = columns
        .iter()
        .map(|entry| {
            Ok(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                safe_ident(entry.source_alias.as_ref())?,
                safe_ident(entry.name.as_ref())?,
            ])))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut query = gen.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}
