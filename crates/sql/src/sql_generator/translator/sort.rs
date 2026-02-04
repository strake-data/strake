use super::SqlGenerator;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use sqlparser::ast::{Expr as SqlExpr, LimitClause, OrderBy, OrderByExpr, OrderByKind};

pub(crate) fn handle_sort(
    gen: &mut SqlGenerator,
    sort: &datafusion::logical_expr::Sort,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = gen.plan_to_query(&sort.input)?;

    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
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

    query.order_by = Some(OrderBy {
        kind: OrderByKind::Expressions(order_by_exprs),
        interpolate: None,
    });

    // Handle pushed-down fetch (limit)
    if let Some(fetch) = sort.fetch {
        let limit_expr =
            SqlExpr::Value(sqlparser::ast::Value::Number(fetch.to_string(), false).into());
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit: Some(limit_expr),
            offset: None,
            limit_by: vec![],
        });
    }

    Ok(query)
}
