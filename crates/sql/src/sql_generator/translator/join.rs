//! # Join Translator
//!
//! Handles translation of DataFusion `Join` logical plan nodes into SQL `JOIN` constraints.
//!
//! ## Usage
//! Handled internally by [`SqlGenerator`](crate::sql_generator::translator::SqlGenerator); not intended for direct use.
//!
//! ## Performance Characteristics
//! - **Complexity:** O(N) where N is the number of join constraints and filters.
//! - **Allocation:** Efficiently handles scope manipulation, repushing left and right scopes to avoid leaks.
//!
//! ## Errors
//!
//! - [`SqlGenError::UnsupportedPlan`]: Returned when an unsupported join type (e.g. cross join if not handled) is encountered.
//! - [`SqlGenError::ScopeViolation`]: Propagated from expression translation if a column reference cannot be resolved.

use super::SqlGenerator;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use datafusion::logical_expr::JoinType;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Join as SqlJoin, JoinConstraint, JoinOperator, SelectItem,
    SetExpr, TableAlias, TableFactor, TableWithJoins,
};

pub(crate) fn handle_join(
    generator: &mut SqlGenerator,
    join: &datafusion::logical_expr::Join,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut left_query = generator.plan_to_query(&join.left)?;
    let left_relation = generator.extract_relation(&mut left_query, None)?;

    let mut right_query = generator.plan_to_query(&join.right)?;
    let right_relation = generator.extract_relation(&mut right_query, None)?;

    let mut on_expr: Option<SqlExpr> = None;

    for (l, r) in &join.on {
        // Translation of join expressions must happen in isolation.
        // We use a ScopeHolder RAII guard to ensure the scope stack is restored on any error path.
        let mut holder = crate::sql_generator::context::ScopeHolder::new(&mut generator.context);

        // Pop Right
        let right_scope = holder.pop()?;

        let l_sql = {
            let mut translator = ExprTranslator::new(holder.ctx_mut(), &generator.dialect);
            translator.expr_to_sql(l)?
        };

        // Pop Left
        let left_scope = holder.pop()?;

        // Restore right for r_sql translation
        holder.repush(right_scope);

        let r_sql = {
            let mut translator = ExprTranslator::new(holder.ctx_mut(), &generator.dialect);
            translator.expr_to_sql(r)?
        };

        // Re-pop right, then restore left+right order
        let right_scope = holder.pop()?;
        holder.repush(left_scope);
        holder.repush(right_scope);

        // Success path: commit the holder to prevent restoration on drop
        // We commit here to prevent the ScopeHolder from rolling back the stack on successful translation.
        holder.commit();

        let eq = crate::sql_generator::expr::make_binary_op(l_sql, BinaryOperator::Eq, r_sql);

        on_expr = match on_expr {
            Some(e) => Some(crate::sql_generator::expr::make_binary_op(
                e,
                BinaryOperator::And,
                eq,
            )),
            None => Some(eq),
        };
    }

    if let Some(filter) = &join.filter {
        let (ctx, dial) = (&mut generator.context, &generator.dialect);
        let mut translator = ExprTranslator::new(ctx, dial);
        let f_sql = translator.expr_to_sql(filter)?;
        on_expr = match on_expr {
            Some(e) => Some(crate::sql_generator::expr::make_binary_op(
                e,
                BinaryOperator::And,
                f_sql,
            )),
            None => Some(f_sql),
        };
    }

    let is_semi_or_anti = matches!(
        join.join_type,
        JoinType::LeftSemi | JoinType::RightSemi | JoinType::LeftAnti | JoinType::RightAnti
    );

    if is_semi_or_anti {
        let right_scope = generator
            .context
            .current_scope()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Semi/anti join missing right input scope".to_string(),
                node_type: "Join".to_string(),
            })?
            .clone();
        generator.context.pop_scope();

        let left_scope = generator
            .context
            .current_scope()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Semi/anti join missing left input scope".to_string(),
                node_type: "Join".to_string(),
            })?
            .clone();
        generator.context.pop_scope();

        let is_left = matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti);
        let is_anti = matches!(join.join_type, JoinType::LeftAnti | JoinType::RightAnti);

        let (mut outer_query, mut subquery, outer_scope, _sub_scope) = if is_left {
            (left_query, right_query, left_scope, right_scope)
        } else {
            (right_query, left_query, right_scope, left_scope)
        };

        // Attach correlation condition to subquery WHERE selection
        if let SetExpr::Select(ref mut select) = *subquery.body {
            if let Some(cond) = on_expr {
                if let Some(existing) = &select.selection {
                    select.selection = Some(SqlExpr::BinaryOp {
                        left: Box::new(existing.clone()),
                        op: BinaryOperator::And,
                        right: Box::new(cond),
                    });
                } else {
                    select.selection = Some(cond);
                }
            }
            select.projection = vec![SelectItem::UnnamedExpr(SqlExpr::Value(
                sqlparser::ast::Value::Number("1".to_string(), false).into(),
            ))];
        }

        let exists_expr = SqlExpr::Exists {
            subquery: Box::new(subquery),
            negated: is_anti,
        };

        if !matches!(*outer_query.body, SetExpr::Select(_)) {
            let sub_alias = generator.context.next_alias();
            let derived = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(outer_query),
                alias: Some(TableAlias {
                    name: safe_ident(&sub_alias)?,
                    columns: vec![],
                    explicit: generator
                        .dialect
                        .capabilities
                        .supports_as_alias_for_tables(),
                }),
                sample: None,
            };
            let mut select = generator.create_skeleton_select();
            select.from = vec![TableWithJoins {
                relation: derived,
                joins: vec![],
            }];
            let mut new_query = generator.create_skeleton_query();
            new_query.body = Box::new(SetExpr::Select(Box::new(select)));
            outer_query = new_query;
        }

        if let SetExpr::Select(ref mut select) = *outer_query.body {
            if let Some(existing) = &select.selection {
                select.selection = Some(SqlExpr::BinaryOp {
                    left: Box::new(existing.clone()),
                    op: BinaryOperator::And,
                    right: Box::new(exists_expr),
                });
            } else {
                select.selection = Some(exists_expr);
            }
        }

        generator
            .context
            .enter_scope(
                outer_scope.alias.clone(),
                outer_scope.columns.clone(),
                outer_scope.qualifiers.clone(),
            )
            .commit();

        return Ok(outer_query);
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
            });
        }
    };

    let join_node = SqlJoin {
        global: false,
        relation: right_relation,
        join_operator: join_op,
    };

    let right_scope = generator
        .context
        .current_scope()
        .ok_or_else(|| SqlGenError::UnsupportedPlan {
            message: "Join missing right input scope".to_string(),
            node_type: "Join".to_string(),
        })?
        .clone();
    generator.context.pop_scope();
    let left_scope = generator
        .context
        .current_scope()
        .ok_or_else(|| SqlGenError::UnsupportedPlan {
            message: "Join missing left input scope".to_string(),
            node_type: "Join".to_string(),
        })?
        .clone();
    generator.context.pop_scope();

    let join_alias = generator.context.next_alias();

    let merged_columns: std::sync::Arc<[crate::sql_generator::context::ColumnEntry]> = left_scope
        .columns
        .iter()
        .chain(right_scope.columns.iter())
        // Build output names and commit the scope change.
        .map(|e| {
            let mut e = e.clone();
            e.provenance.push(join_alias.clone());
            e
        })
        .collect::<Vec<_>>()
        .into();

    let mut merged_qualifiers = left_scope.qualifiers.clone();
    merged_qualifiers.extend(right_scope.qualifiers.clone());

    generator
        .context
        .enter_scope(join_alias, merged_columns.clone(), merged_qualifiers)
        .commit();

    let mut select = generator.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: left_relation,
        joins: vec![join_node],
    }];
    select.projection = merged_columns
        .iter()
        .map(|entry| {
            Ok(sqlparser::ast::SelectItem::UnnamedExpr(
                SqlExpr::CompoundIdentifier(vec![
                    safe_ident(entry.source_alias.as_ref())?,
                    safe_ident(entry.name.as_ref())?,
                ]),
            ))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}

pub(crate) fn handle_nary_join(
    generator: &mut SqlGenerator,
    nary: &crate::optimizer::join_flattener::NaryJoinNode,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // Process all joins inside a rollback-guarded block.
    // This allows us to catch errors during join processing and cleanly restore the scope stack
    // (removing any partially added scopes) before returning the error.
    // On success, we also rollback (to pop all individual scopes) and then enter a single merged scope.
    let (base_relation, sql_joins, merged_columns, merged_qualifiers) = {
        let checkpoint = generator.context.checkpoint();

        // Execute join logic...
        let result = (|| -> Result<_, SqlGenError> {
            let mut base_query = generator.plan_to_query(&nary.base)?;
            let base_relation = generator.extract_relation(&mut base_query, None)?;

            let base_scope = generator
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
                let mut branch_query = generator.plan_to_query(&branch.input)?;
                let branch_relation = generator.extract_relation(&mut branch_query, None)?;

                let branch_scope = generator
                    .context
                    .current_scope()
                    .ok_or_else(|| SqlGenError::UnsupportedPlan {
                        message: "NaryJoin missing branch scope".to_string(),
                        node_type: "NaryJoin".to_string(),
                    })?
                    .clone();

                let mut on_expr: Option<SqlExpr> = None;

                for (l, r) in &branch.on {
                    // Similar P0 Fix for NaryJoin using ScopeHolder RAII guards
                    let mut holder =
                        crate::sql_generator::context::ScopeHolder::new(&mut generator.context);

                    // Pop current branch
                    let current_branch_scope = holder.pop()?;

                    let l_sql = {
                        let mut translator =
                            ExprTranslator::new(holder.ctx_mut(), &generator.dialect);
                        translator.expr_to_sql(l)?
                    };

                    // Pop merged so far
                    let merged_scope_so_far = holder.pop()?;

                    // Restore current branch for r_sql translation
                    holder.repush(current_branch_scope);

                    let r_sql = {
                        let mut translator =
                            ExprTranslator::new(holder.ctx_mut(), &generator.dialect);
                        translator.expr_to_sql(r)?
                    };

                    // Restore stack: [..., MERGED_SO_FAR, CURRENT_BRANCH]
                    let current_branch_scope = holder.pop()?;
                    holder.repush(merged_scope_so_far);
                    holder.repush(current_branch_scope);

                    // Commit all successful operations
                    // Commit the scope change to prevent rollback on successful translation.
                    holder.commit();

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
                    let (ctx, dial) = (&mut generator.context, &generator.dialect);
                    let mut translator = ExprTranslator::new(ctx, dial);
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
                        });
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
        // If error, we want to cleanup. Our robust rollback() now handles restoration of pops.
        generator.context.rollback(checkpoint);
        result?
    };

    let join_alias = generator.context.next_alias();
    let final_columns: std::sync::Arc<[crate::sql_generator::context::ColumnEntry]> =
        merged_columns
            .into_iter()
            .map(|mut e| {
                e.provenance.push(join_alias.clone());
                e
            })
            .collect::<Vec<_>>()
            .into();

    generator
        .context
        .enter_scope(join_alias, final_columns.clone(), merged_qualifiers)
        .commit();

    let mut select = generator.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: base_relation,
        joins: sql_joins,
    }];
    select.projection = final_columns
        .iter()
        .map(|entry| {
            Ok(sqlparser::ast::SelectItem::UnnamedExpr(
                SqlExpr::CompoundIdentifier(vec![
                    safe_ident(entry.source_alias.as_ref())?,
                    safe_ident(entry.name.as_ref())?,
                ]),
            ))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}
