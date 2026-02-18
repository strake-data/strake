use super::SqlGenerator;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use datafusion::logical_expr::JoinType;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Join as SqlJoin, JoinConstraint, JoinOperator, SelectItem,
    SetExpr, TableWithJoins,
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

    let join_alias = gen.context.next_alias();

    let merged_columns: std::sync::Arc<[crate::sql_generator::context::ColumnEntry]> = left_scope
        .columns
        .iter()
        .chain(right_scope.columns.iter())
        .map(|e| {
            let mut e = e.clone();
            e.provenance.push(join_alias.clone());
            e
        })
        .collect::<Vec<_>>()
        .into();

    let mut merged_qualifiers = left_scope.qualifiers.clone();
    merged_qualifiers.extend(right_scope.qualifiers.clone());

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

pub(crate) fn handle_nary_join(
    gen: &mut SqlGenerator,
    nary: &crate::optimizer::join_flattener::NaryJoinNode,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // Process all joins inside a rollback-guarded block.
    // This allows us to catch errors during join processing and cleanly restore the scope stack
    // (removing any partially added scopes) before returning the error.
    // On success, we also rollback (to pop all individual scopes) and then enter a single merged scope.
    let (base_relation, sql_joins, merged_columns, merged_qualifiers) = {
        let checkpoint = gen.context.checkpoint();

        // Execute join logic...
        let result = (|| -> Result<_, SqlGenError> {
            let base_query = gen.plan_to_query(&nary.base)?;
            let base_relation = gen.extract_relation(base_query)?;

            let base_scope = gen
                .context
                .current_scope()
                .ok_or_else(|| SqlGenError::UnsupportedPlan {
                    message: "NaryJoin missing base scope".to_string(),
                    node_type: "NaryJoin".to_string(),
                })?
                .clone();

            let mut sql_joins = Vec::new();
            let mut merged_columns = base_scope.columns.to_vec();
            let mut merged_qualifiers = base_scope.qualifiers.to_vec();

            for branch in &nary.branches {
                let branch_query = gen.plan_to_query(&branch.input)?;
                let branch_relation = gen.extract_relation(branch_query)?;

                let branch_scope = gen
                    .context
                    .current_scope()
                    .ok_or_else(|| SqlGenError::UnsupportedPlan {
                        message: "NaryJoin missing branch scope".to_string(),
                        node_type: "NaryJoin".to_string(),
                    })?
                    .clone();

                let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
                let mut on_expr: Option<SqlExpr> = None;

                for (l, r) in &branch.on {
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

                if let Some(filter) = &branch.filter {
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

                let join_op = match branch.join_type {
                    JoinType::Inner => JoinOperator::Inner(join_constraint),
                    JoinType::Left => JoinOperator::LeftOuter(join_constraint),
                    JoinType::Right => JoinOperator::RightOuter(join_constraint),
                    JoinType::Full => JoinOperator::FullOuter(join_constraint),
                    _ => {
                        return Err(SqlGenError::UnsupportedPlan {
                            message: format!("Join type: {:?}", branch.join_type),
                            node_type: "NaryJoin".to_string(),
                        })
                    }
                };

                sql_joins.push(SqlJoin {
                    global: false,
                    relation: branch_relation,
                    join_operator: join_op,
                });

                merged_columns.extend(branch_scope.columns.to_vec());
                merged_qualifiers.extend(branch_scope.qualifiers.clone());
            }
            Ok((base_relation, sql_joins, merged_columns, merged_qualifiers))
        })();

        // Always rollback - if success, we want to pop the component scopes and merge them.
        // If error, we want to cleanup.
        gen.context.rollback(checkpoint);
        result?
    };

    let join_alias = gen.context.next_alias();
    let final_columns: std::sync::Arc<[crate::sql_generator::context::ColumnEntry]> =
        merged_columns
            .into_iter()
            .map(|mut e| {
                e.provenance.push(join_alias.clone());
                e
            })
            .collect::<Vec<_>>()
            .into();

    gen.context
        .enter_scope(join_alias, final_columns.clone(), merged_qualifiers)
        .commit();

    let mut select = gen.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: base_relation,
        joins: sql_joins,
    }];
    select.projection = final_columns
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
