//! # Sort Translator
//!
//! Translates DataFusion `Sort` logical plan nodes into SQL `ORDER BY` clauses.
//!
//! ## Usage
//! Handled internally by [`SqlGenerator`](crate::sql_generator::translator::SqlGenerator); not intended for direct use.
//!
//! ## Performance Characteristics
//! - **Complexity:** Linearly iterates sort expressions O(N).
//!
//! ## Errors
//! - [`SqlGenError::ScopeViolation`]: If an `ORDER BY` expression references undefined columns.

use super::SqlGenerator;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use sqlparser::ast::{
    Expr as SqlExpr, OrderBy, OrderByExpr, OrderByKind, SelectItem, SetExpr, TableWithJoins,
};

pub(crate) fn handle_sort(
    generator: &mut SqlGenerator,
    sort: &datafusion::logical_expr::Sort,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // 1. Get a stable inner query for the sort's input.
    let checkpoint = generator.context.checkpoint();
    let mut inner_query = generator.plan_to_stable_query(&sort.input, Some(checkpoint))?;

    // 2. Wrap the inner query as a derived table with a stable alias via extract_relation.
    //    This guarantees that the scope alias and the FROM-clause alias are identical,
    //    preventing ORDER BY from referencing a "phantom" alias (e.g. "rel_2") that does
    //    not exist in the outer SELECT's FROM clause (which uses "rel_1").
    //
    //    Background: nodes like Projection and Aggregate call next_alias() for their OUTPUT
    //    scope but their SELECT body uses the INPUT relation alias. If we skip this step,
    //    ORDER BY resolves against the output scope (rel_N+1) while the FROM clause only
    //    exposes the input alias (rel_N), causing "no such column: rel_N+1.col" errors.
    let inner_relation = generator.extract_relation(&mut inner_query, None, Some(checkpoint))?;

    let current_scope =
        generator
            .context
            .current_scope()
            .cloned()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing scope after extract_relation in Sort".to_string(),
                node_type: "Sort".to_string(),
            })?;

    // 3. Translate ORDER BY expressions against the stabilized scope.
    let (ctx, dial) = (&mut generator.context, &generator.dialect);
    let mut translator = ExprTranslator::new(ctx, dial);
    let order_by_exprs = sort
        .expr
        .iter()
        .map(|e| {
            let asc = e.asc;
            let nulls_first = e.nulls_first;
            let sql_expr = translator.expr_to_sql(&e.expr)?;
            Ok(OrderByExpr {
                expr: sql_expr,
                options: sqlparser::ast::OrderByOptions {
                    asc: Some(asc),
                    nulls_first: Some(nulls_first),
                },
                with_fill: None,
            })
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    // 4. Build the outer SELECT that wraps the inner relation.
    //    Project all columns from the stable scope so downstream nodes can reference them.
    let mut select = generator.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: inner_relation,
        joins: vec![],
    }];
    select.projection = current_scope
        .columns
        .iter()
        .map(|c| {
            Ok(SelectItem::ExprWithAlias {
                expr: SqlExpr::CompoundIdentifier(vec![
                    safe_ident(c.source_alias.as_ref())?,
                    safe_ident(c.name.as_ref())?,
                ]),
                alias: safe_ident(c.name.as_ref())?,
            })
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut out_query = generator.create_skeleton_query();
    out_query.body = Box::new(SetExpr::Select(Box::new(select)));
    out_query.order_by = Some(OrderBy {
        kind: OrderByKind::Expressions(order_by_exprs),
        interpolate: None,
    });

    // 5. Handle pushed-down fetch (LIMIT pushed from Sort node).
    if let Some(fetch) = sort.fetch {
        let limit_expr =
            SqlExpr::Value(sqlparser::ast::Value::Number(fetch.to_string(), false).into());
        generator.apply_limit_offset(&mut out_query, Some(limit_expr), None);
    }

    Ok(out_query)
}
