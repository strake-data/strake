use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Ident, SelectItem, SetExpr, TableAlias, TableFactor,
    TableWithJoins,
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

    if let SetExpr::Select(ref mut select) = *query.body {
        select.projection = select_items;
    } else {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Projection on non-Select".to_string(),
            node_type: "Projection".to_string(),
        });
    }

    let input_qualifiers = gen
        .context
        .current_scope()
        .map(|s| s.qualifiers.clone())
        .unwrap_or_default();
    gen.context.pop_scope();

    let proj_alias = gen.context.next_alias();
    let new_columns = proj
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(proj_alias.as_str()),
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
        return Err(SqlGenError::UnsupportedPlan {
            message: "Filter on non-Select".to_string(),
            node_type: "Filter".to_string(),
        });
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
    let columns: std::sync::Arc<[ColumnEntry]> = alias
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(subquery_alias.as_str()),
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
            name: Ident::with_quote('"', subquery_alias.clone()),
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
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                Ident::with_quote('"', entry.source_alias.to_string()),
                Ident::with_quote('"', entry.name.to_string()),
            ]))
        })
        .collect();

    let mut query = gen.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}
