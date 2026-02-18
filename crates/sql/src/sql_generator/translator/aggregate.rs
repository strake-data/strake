use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use sqlparser::ast::{GroupByExpr, SelectItem, SetExpr};

pub(crate) fn handle_aggregate(
    gen: &mut SqlGenerator,
    agg: &datafusion::logical_expr::Aggregate,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = gen.plan_to_query(&agg.input)?;

    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);

    // Group BY expressions
    let group_by = GroupByExpr::Expressions(
        agg.group_expr
            .iter()
            .map(|e| translator.expr_to_sql(e))
            .collect::<Result<Vec<_>, SqlGenError>>()?,
        vec![], // modifiers
    );

    // Aggregate schema order: group_expr then aggr_expr
    let mut select_items = Vec::new();
    for e in &agg.group_expr {
        select_items.push(SelectItem::UnnamedExpr(translator.expr_to_sql(e)?));
    }
    for e in &agg.aggr_expr {
        select_items.push(SelectItem::UnnamedExpr(translator.expr_to_sql(e)?));
    }

    if let SetExpr::Select(ref mut select) = *query.body {
        select.projection = select_items;
        select.group_by = group_by;
    } else {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Aggregate on non-Select".to_string(),
            node_type: "Aggregate".to_string(),
        });
    }

    let input_qualifiers = gen
        .context
        .current_scope()
        .map(|s| s.qualifiers.clone())
        .unwrap_or_default();
    gen.context.pop_scope();
    let alias = gen.context.next_alias();
    let columns = agg
        .schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(alias.as_str()),
            provenance: vec![alias.clone()],
            unique_id: gen.context.next_column_id(),
        })
        .collect::<Vec<_>>()
        .into();
    gen.context
        .enter_scope(alias, columns, input_qualifiers)
        .commit();

    Ok(query)
}

pub(crate) fn handle_window(
    gen: &mut SqlGenerator,
    window: &datafusion::logical_expr::Window,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = gen.plan_to_query(&window.input)?;

    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
    let mut select_items = Vec::new();

    // Keep input columns
    for (qualifier, field) in window.input.schema().iter() {
        let col = datafusion::common::Column::new(qualifier.cloned(), field.name());
        select_items.push(SelectItem::UnnamedExpr(
            translator.expr_to_sql(&datafusion::prelude::Expr::Column(col))?,
        ));
    }

    // Add window expressions
    for e in &window.window_expr {
        select_items.push(SelectItem::UnnamedExpr(translator.expr_to_sql(e)?));
    }

    if let SetExpr::Select(ref mut select) = *query.body {
        select.projection = select_items.clone();
    } else {
        return Err(SqlGenError::UnsupportedPlan {
            message: "Window on non-Select".to_string(),
            node_type: "Window".to_string(),
        });
    }

    let input_qualifiers = gen
        .context
        .current_scope()
        .map(|s| s.qualifiers.clone())
        .unwrap_or_default();
    gen.context.pop_scope();
    let alias = gen.context.next_alias();
    let input_scope = gen.context.current_scope().cloned();
    let columns = window
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let mut provenance = vec![alias.clone()];
            // First N columns are from input
            if i < window.input.schema().fields().len() {
                if let Some(scope) = &input_scope {
                    if let Some(entry) = scope.columns.get(i) {
                        provenance.extend(entry.provenance.clone());
                    }
                }
            }

            ColumnEntry {
                name: std::sync::Arc::from(f.name().as_ref()),
                data_type: f.data_type().clone(),
                source_alias: std::sync::Arc::from(alias.as_str()),
                provenance,
                unique_id: gen.context.next_column_id(),
            }
        })
        .collect::<Vec<_>>()
        .into();
    gen.context
        .enter_scope(alias, columns, input_qualifiers)
        .commit();

    Ok(query)
}
