//! # Projection and Filter Translator
//!
//! Handles translation of `Projection`, `Filter`, and `SubqueryAlias` logical plan nodes.
//!
//! ## Usage
//! Handled internally by [`SqlGenerator`](crate::sql_generator::translator::SqlGenerator); not intended for direct use.
//!
//! ## Performance Characteristics
//! - **Complexity:** Applies O(N) translation matching DataFusion expressions.
//! - **Allocation:** May allocate intermediate schema layouts for table alias generation.
//!
//! ## Errors
//! - [`SqlGenError::UnsupportedPlan`]: If input scope is missing for projection handling.
//! - [`SqlGenError::ScopeViolation`]: Propagated from expression translation.

use super::SqlGenerator;
use super::derive_bare_name;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
    WildcardAdditionalOptions,
};
use std::sync::Arc;

pub(crate) fn handle_projection(
    generator: &mut SqlGenerator,
    proj: &datafusion::logical_expr::Projection,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    // 1. Get a stable relation for the input
    let mut input_query = generator.plan_to_query(&proj.input)?;
    let input_relation = generator.extract_relation(&mut input_query, None)?;
    let input_scope =
        generator
            .context
            .current_scope()
            .cloned()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing input scope in Projection".to_string(),
                node_type: "Projection".to_string(),
            })?;

    // 2. Process projection expressions against the input scope (which is now exactly what we select from)
    let (ctx, dial) = (&mut generator.context, &generator.dialect);
    let mut translator = ExprTranslator::new(ctx, dial);
    let mut output_names: Vec<Arc<str>> = Vec::new();
    let select_items = proj
        .expr
        .iter()
        .map(|e| {
            let sql_expr = translator.expr_to_sql(e)?;
            let stable_name = super::derive_bare_name(&e.schema_name().to_string());
            output_names.push(stable_name.clone());
            Ok(SelectItem::ExprWithAlias {
                expr: sql_expr,
                alias: safe_ident(&stable_name)?,
            })
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    // 3. Create a completely new SELECT for the projection output
    let mut select = generator.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation: input_relation,
        joins: vec![],
    }];
    select.projection = select_items;

    // Note: extract_relation already handled input scope popping and re-pushing.
    // We should NOT pop it again until we register our new projection output scope.

    // 4. Register the output scope using a NEW alias
    let proj_alias = generator.context.next_alias();
    let new_columns_vec: Vec<ColumnEntry> = (0..output_names.len())
        .map(|i| {
            let (qualifier, _field) = proj.schema.qualified_field(i);
            let name = output_names[i].clone();
            let mut provenance = vec![proj_alias.clone()];
            if let Some(q) = qualifier {
                provenance.push(q.to_string());
            }
            ColumnEntry {
                name: name.clone(),
                name_lower: Arc::from(name.to_lowercase().as_str()),
                data_type: proj.schema.field(i).data_type().clone(),
                source_alias: Arc::from(proj_alias.as_str()),
                provenance,
                unique_id: generator.context.next_column_id(),
            }
        })
        .collect();
    let new_columns = std::sync::Arc::<[ColumnEntry]>::from(new_columns_vec.into_boxed_slice());

    generator
        .context
        .enter_scope(proj_alias, new_columns, input_scope.qualifiers.clone())
        .commit();

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}

pub(crate) fn handle_filter(
    generator: &mut SqlGenerator,
    filter: &datafusion::logical_expr::Filter,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let mut query = generator.plan_to_stable_query(&filter.input)?;
    let mut translator = ExprTranslator::new(&mut generator.context, &generator.dialect);
    let sql_expr = translator.expr_to_sql(&filter.predicate)?;

    if !matches!(*query.body, SetExpr::Select(_)) {
        let sub_alias = generator.context.next_alias();
        let explicit_projection = if let Some(scope) = generator.context.current_scope() {
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
            vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())]
        };
        let derived = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query),
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
        select.projection = explicit_projection;

        let mut new_query = generator.create_skeleton_query();
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
    generator: &mut SqlGenerator,
    alias: &datafusion::logical_expr::SubqueryAlias,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let inner_query = generator.plan_to_query(&alias.input)?;

    let _input_scope =
        generator
            .context
            .current_scope()
            .cloned()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing input scope in SubqueryAlias".to_string(),
                node_type: "SubqueryAlias".to_string(),
            })?;

    generator.context.pop_scope();

    let subquery_alias = generator.context.next_alias();
    // Use derive_bare_name for consistent lowercased names
    let columns_vec: Vec<ColumnEntry> = (0..alias.schema.fields().len())
        .map(|i| {
            let (qualifier, field) = alias.schema.qualified_field(i);
            let name = derive_bare_name(field.name());
            let mut provenance = vec![subquery_alias.clone(), alias.alias.to_string()];
            if let Some(q) = qualifier {
                provenance.push(q.to_string());
            }
            ColumnEntry {
                name: name.clone(),
                name_lower: Arc::from(name.to_lowercase().as_str()),
                data_type: field.data_type().clone(),
                source_alias: subquery_alias.clone().into(),
                provenance,
                unique_id: generator.context.next_column_id(),
            }
        })
        .collect();
    let columns: std::sync::Arc<[ColumnEntry]> = Arc::from(columns_vec.into_boxed_slice());
    let qualifiers = vec![alias.alias.to_string()];

    generator
        .context
        .enter_scope(subquery_alias.clone(), columns.clone(), qualifiers)
        .commit();

    let derived = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(inner_query),
        alias: Some(TableAlias {
            name: safe_ident(&subquery_alias)?,
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
    select.projection = columns
        .iter()
        .map(|entry| {
            Ok(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                safe_ident(entry.source_alias.as_ref())?,
                safe_ident(entry.name.as_ref())?,
            ])))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(SetExpr::Select(Box::new(select)));
    Ok(query)
}
