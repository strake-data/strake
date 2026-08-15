//! # Aggregate and Window Translator
//!
//! Handles translation of DataFusion `Aggregate` and `Window` logical plan nodes.
//!
//! ## Overview
//!
//! Aggregations are translated by extracting the input relation as a subquery,
//! processing group-by and aggregate expressions, and wrapping the result in a
//! fresh scope to ensure stable aliasing for parent nodes.
//!
//! ## Usage
//! Handled internally by [`SqlGenerator`](crate::sql_generator::translator::SqlGenerator); not intended for direct use.
//!
//! ## Performance Characteristics
//! - **Complexity:** O(N) in the number of aggregate/window expressions.
//! - **Allocation:** Pre-allocates vectors for select items and group by expressions based on input lengths.
//!
//! ## Errors
//! - [`SqlGenError::UnsupportedPlan`]: If input scope is missing or the plan is invalid.
//! - [`SqlGenError::ScopeViolation`]: If column resolution fails in group-by or aggregate expressions.

use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use datafusion::logical_expr::Aggregate;
use sqlparser::ast::{GroupByExpr as GroupByExpression, SelectItem, SetExpr};
use std::sync::Arc;

pub(crate) fn handle_aggregate(
    generator: &mut SqlGenerator,
    agg: &Aggregate,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // 1. Get a stable relation for the input
    let checkpoint = generator.context.checkpoint();
    let mut input_query = generator.plan_to_query(&agg.input)?;
    let input_relation = generator.extract_relation(&mut input_query, None, checkpoint)?;
    debug_assert!(
        generator.context.scope_stack_len() > 0,
        "extract_relation contract: must pop one scope and push its replacement"
    );

    let input_scope =
        generator
            .context
            .current_scope()
            .cloned()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing input scope in Aggregate".to_string(),
                node_type: "Aggregate".to_string(),
            })?;

    let mut select_items = Vec::new();
    let mut group_by_exprs = Vec::new();
    let mut output_names: Vec<Arc<str>> = Vec::new();

    // 2. Process Group By expressions against the input scope (which is now exactly what we select from)
    for e in &agg.group_expr {
        let sql_expr = {
            let mut expr_translator =
                ExprTranslator::new(&mut generator.context, &generator.dialect);
            expr_translator.expr_to_sql(e)?
        };
        group_by_exprs.push(sql_expr.clone());

        let stable_name: Arc<str> = super::derive_bare_name(&e.schema_name().to_string());
        output_names.push(stable_name.clone());
        select_items.push(SelectItem::ExprWithAlias {
            expr: sql_expr,
            alias: safe_ident(&stable_name)?,
        });
    }

    // 3. Process Aggregate expressions against the input scope
    for e in &agg.aggr_expr {
        let sql_expr = {
            let mut expr_translator =
                ExprTranslator::new(&mut generator.context, &generator.dialect);
            let mut sql = expr_translator.expr_to_sql(e)?;

            // Explicitly cast arguments for SUM/AVG to DOUBLE for DuckDB
            if generator.dialect.source_type == strake_common::models::SourceType::Duckdb
                && let sqlparser::ast::Expr::Function(ref mut f) = sql
            {
                let name = f.name.to_string().to_lowercase();
                if (name == "sum" || name == "avg")
                    && let sqlparser::ast::FunctionArguments::List(list) = &mut f.args
                {
                    for arg in &mut list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner_expr),
                        ) = arg
                        {
                            // Wrap the inner expression in a CAST
                            let wrapped = sqlparser::ast::Expr::Cast {
                                expr: Box::new(inner_expr.clone()),
                                data_type: sqlparser::ast::DataType::Double(
                                    sqlparser::ast::ExactNumberInfo::None,
                                ),
                                format: None,
                                kind: sqlparser::ast::CastKind::Cast,
                                array: false,
                            };
                            *inner_expr = wrapped;
                        }
                    }
                }
            }
            Ok::<_, SqlGenError>(sql)
        }?;

        let stable_name: Arc<str> = super::derive_bare_name(&e.schema_name().to_string());
        output_names.push(stable_name.clone());
        select_items.push(SelectItem::ExprWithAlias {
            expr: sql_expr,
            alias: safe_ident(&stable_name)?,
        });
    }

    // 4. Create a completely new SELECT for the aggregate output
    let mut select = generator.create_skeleton_select();
    select.from = vec![sqlparser::ast::TableWithJoins {
        relation: input_relation,
        joins: vec![],
    }];
    select.group_by = GroupByExpression::Expressions(group_by_exprs, vec![]);
    select.projection = select_items;

    // Note: We used to pop_scope() here, but extract_relation already did that
    // and correctly pushed it back. We should NOT pop it again until we are
    // ready to replace it with the Aggregate's output scope.

    // 5. Register the output scope using a NEW alias
    let output_alias = generator.context.next_alias();
    tracing::debug!(target: "sql_generator", alias = %output_alias, "Registering aggregate output scope");

    let columns_vec: Vec<ColumnEntry> = (0..output_names.len())
        .map(|i| {
            let (qualifier, field) = agg.schema.qualified_field(i);
            let name = output_names[i].clone();
            let mut provenance = vec![output_alias.clone()];
            if let Some(q) = qualifier {
                provenance.push(q.to_string());
            }
            ColumnEntry {
                name: name.clone(),
                name_lower: Arc::from(name.to_lowercase().as_str()),
                data_type: field.data_type().clone(),
                source_alias: output_alias.clone().into(),
                provenance,
                unique_id: generator.context.next_column_id(),
            }
        })
        .collect();
    let columns = Arc::<[ColumnEntry]>::from(columns_vec.into_boxed_slice());

    generator
        .context
        .enter_scope(output_alias, columns, input_scope.qualifiers.clone())
        .commit();

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}

pub(crate) fn handle_window(
    generator: &mut SqlGenerator,
    window: &datafusion::logical_expr::Window,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // 1. Get inner query
    let checkpoint = generator.context.checkpoint();
    let mut input_query: sqlparser::ast::Query = generator.plan_to_query(&window.input)?;

    // 2. Extract stable relation from inner query
    let input_relation = generator.extract_relation(&mut input_query, None, checkpoint)?;

    let input_scope =
        generator
            .context
            .current_scope()
            .cloned()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing input scope in Window".to_string(),
                node_type: "Window".to_string(),
            })?;

    let mut select_items = Vec::new();
    let mut output_names: Vec<Arc<str>> = Vec::new();

    // 3. Project input columns
    for entry in input_scope.columns.iter() {
        output_names.push(entry.name.clone());
        select_items.push(SelectItem::UnnamedExpr(
            sqlparser::ast::Expr::CompoundIdentifier(vec![
                safe_ident(entry.source_alias.as_ref())?,
                safe_ident(entry.name.as_ref())?,
            ]),
        ));
    }

    // 4. Add window expressions
    for e in &window.window_expr {
        let sql_expr = {
            let mut expr_translator =
                ExprTranslator::new(&mut generator.context, &generator.dialect);
            expr_translator.expr_to_sql(e)?
        };

        let stable_name: Arc<str> = super::derive_bare_name(&e.schema_name().to_string());
        output_names.push(stable_name.clone());
        select_items.push(SelectItem::ExprWithAlias {
            expr: sql_expr,
            alias: safe_ident(&stable_name)?,
        });
    }

    // 5. Build Select
    let mut select = generator.create_skeleton_select();
    select.from = vec![sqlparser::ast::TableWithJoins {
        relation: input_relation,
        joins: vec![],
    }];
    select.projection = select_items;

    // 6. Register output scope with NEW alias
    let output_alias = generator.context.next_alias();
    let columns_vec: Vec<ColumnEntry> = (0..output_names.len())
        .map(|i| {
            let (qualifier, field) = window.schema.qualified_field(i);
            let name = output_names[i].clone();
            let mut provenance = vec![output_alias.clone()];
            if let Some(q) = qualifier {
                provenance.push(q.to_string());
            }
            ColumnEntry {
                name: name.clone(),
                name_lower: Arc::from(name.to_lowercase().as_str()),
                data_type: field.data_type().clone(),
                source_alias: Arc::from(output_alias.as_ref()),
                provenance,
                unique_id: generator.context.next_column_id(),
            }
        })
        .collect();
    let columns = Arc::<[ColumnEntry]>::from(columns_vec.into_boxed_slice());

    generator
        .context
        .enter_scope(output_alias, columns, input_scope.qualifiers.clone())
        .commit();

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}
