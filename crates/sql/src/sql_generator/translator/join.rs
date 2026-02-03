use super::SqlGenerator;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use datafusion::logical_expr::JoinType;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Ident, Join as SqlJoin, JoinConstraint, JoinOperator,
    SelectItem, SetExpr, TableWithJoins,
};

pub(crate) fn handle_join(
    gen: &mut SqlGenerator,
    join: &datafusion::logical_expr::Join,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let left_query = gen.plan_to_query(&join.left)?;
    let right_query = gen.plan_to_query(&join.right)?;

    let left_relation = gen.extract_relation(left_query)?;
    let right_relation = gen.extract_relation(right_query)?;

    let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
    let mut on_expr: Option<SqlExpr> = None;

    for (l, r) in &join.on {
        let l_sql = translator.expr_to_sql(l)?;
        let r_sql = translator.expr_to_sql(r)?;
        let eq = SqlExpr::BinaryOp {
            left: Box::new(l_sql),
            op: BinaryOperator::Eq,
            right: Box::new(r_sql),
        };

        on_expr = match on_expr {
            Some(e) => Some(SqlExpr::BinaryOp {
                left: Box::new(e),
                op: BinaryOperator::And,
                right: Box::new(eq),
            }),
            None => Some(eq),
        };
    }

    if let Some(filter) = &join.filter {
        let f_sql = translator.expr_to_sql(filter)?;
        on_expr = match on_expr {
            Some(e) => Some(SqlExpr::BinaryOp {
                left: Box::new(e),
                op: BinaryOperator::And,
                right: Box::new(f_sql),
            }),
            None => Some(f_sql),
        };
    }

    let join_constraint =
        JoinConstraint::On(on_expr.ok_or_else(|| SqlGenError::UnsupportedPlan {
            message: "Join without ON condition".to_string(),
            node_type: "Join".to_string(),
        })?);

    let join_op = match join.join_type {
        JoinType::Inner => JoinOperator::Inner(join_constraint),
        JoinType::Left => JoinOperator::LeftOuter(join_constraint),
        JoinType::Right => JoinOperator::RightOuter(join_constraint),
        JoinType::Full => JoinOperator::FullOuter(join_constraint),
        _ => {
            return Err(SqlGenError::UnsupportedPlan {
                message: format!("Join type: {:?}", join.join_type),
                node_type: "Join".to_string(),
            })
        }
    };

    let join_node = SqlJoin {
        global: false,
        relation: right_relation,
        join_operator: join_op,
    };

    let right_scope = gen
        .context
        .current_scope()
        .ok_or_else(|| SqlGenError::UnsupportedPlan {
            message: "Join missing right input scope".to_string(),
            node_type: "Join".to_string(),
        })?
        .clone();
    gen.context.pop_scope();
    let left_scope = gen
        .context
        .current_scope()
        .ok_or_else(|| SqlGenError::UnsupportedPlan {
            message: "Join missing left input scope".to_string(),
            node_type: "Join".to_string(),
        })?
        .clone();
    gen.context.pop_scope();

    let merged_columns: std::sync::Arc<[crate::sql_generator::context::ColumnEntry]> = left_scope
        .columns
        .iter()
        .chain(right_scope.columns.iter())
        .cloned()
        .collect::<Vec<_>>()
        .into();

    let mut merged_qualifiers = left_scope.qualifiers.clone();
    merged_qualifiers.extend(right_scope.qualifiers.clone());

    let join_alias = gen.context.next_alias();
    gen.context
        .enter_scope(join_alias, merged_columns.clone(), merged_qualifiers)
        .commit();

    let mut select = gen.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: left_relation,
        joins: vec![join_node],
    }];
    select.projection = merged_columns
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
