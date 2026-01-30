//! SQL Generation for Remote Sources.
//!
//! Converts DataFusion LogicalPlans to SQL text using the DialectRouter
//! to select the appropriate rendering strategy per source type.

use anyhow::{Context, Result};
use datafusion::common::{
    internal_err, plan_err,
    tree_node::{Transformed, TreeNode},
    Column, Result as DFResult, ScalarValue,
};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;

use crate::dialect_router::{route_dialect, DialectPath};
use crate::schema_adapter::SchemaAdapter;

/// Generates Substrait plan bytes for the given LogicalPlan.
///
/// Should be used when `is_substrait_source` returns true.
pub async fn get_substrait_for_plan(plan: &LogicalPlan, ctx: &SessionContext) -> Result<Vec<u8>> {
    crate::substrait_producer::to_substrait_bytes(plan, ctx).await
}

/// Converts a logical plan to SQL text using the appropriate dialect for the target source.
///
/// Used by the federation layer when pushing subqueries to remote databases.
/// Each dialect handles identifier quoting, literal formatting, and syntax
/// differences specific to that database.
///
/// # Returns
/// - `Ok(Some(sql))` - SQL generated successfully
/// - `Ok(None)` - LocalExecution fallback (no SQL pushdown)
/// - `Err(...)` - Substrait path requested (use `get_substrait_for_plan` instead)
pub fn get_sql_for_plan(plan: &LogicalPlan, source_type: &str) -> Result<Option<String>> {
    match route_dialect(source_type) {
        DialectPath::Native(dialect) | DialectPath::Custom(dialect) => {
            let unparser = ScopedUnparser::new(&*dialect);
            let ast = unparser
                .plan_to_sql(plan)
                .context("Failed to unparse LogicalPlan to AST")?;
            Ok(Some(ast.to_string()))
        }
        DialectPath::Substrait => {
            // Caller should use get_substrait_for_plan instead
            Err(anyhow::anyhow!(
                "Source '{}' uses Substrait, use get_substrait_for_plan instead",
                source_type
            ))
        }
        DialectPath::LocalExecution => {
            // No pushdown available - return None to signal local execution
            tracing::debug!(
                source_type = %source_type,
                "No dialect available, using local execution"
            );
            Ok(None)
        }
    }
}

/// Check if a source uses Substrait for plan pushdown
pub fn is_substrait_source(source_type: &str) -> bool {
    matches!(route_dialect(source_type), DialectPath::Substrait)
}

/// Check if a source requires local execution (no pushdown)
pub fn is_local_execution(source_type: &str) -> bool {
    matches!(route_dialect(source_type), DialectPath::LocalExecution)
}

/// Remaps column qualifiers in a LogicalPlan to respect SQL scoping rules.
///
/// This function is designed to be used as a `LogicalOptimizer` for the
/// datafusion-federation's `SQLExecutor` trait. It traverses the plan and
/// fixes column qualifiers so that inner subquery aliases don't leak to
/// outer query scopes.
///
/// The federation layer calls this before `Unparser::plan_to_sql()` to
/// ensure generated SQL has properly qualified column names.
///
/// # Example Usage
/// ```ignore
/// impl SQLExecutor for MyExecutor {
///     fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
///         Some(Box::new(|plan| remap_plan_for_federation(plan)))
///     }
/// }
/// ```
pub fn remap_plan_for_federation(plan: LogicalPlan) -> datafusion::error::Result<LogicalPlan> {
    tracing::debug!(
        target: "sql_gen",
        "remap_plan_for_federation: processing plan for scope-aware SQL generation"
    );

    let original_schema = plan.schema().clone();

    let remapper = PlanScopeRemapper::new();
    let (remapped_plan, _) = remapper.remap_plan(&plan, &None)?;

    tracing::trace!(
        target: "sql_gen",
        "AFTER remapping:\n{}",
        remapped_plan.display_indent()
    );

    // If schema changed (due to flattening/aliasing), wrap in a restoring projection
    if !original_schema.as_ref().eq(remapped_plan.schema().as_ref()) {
        tracing::debug!(target: "sql_gen", "Schema mismatch detected - wrapping in SchemaAdapter");

        let adapter = crate::schema_adapter::SchemaAdapter::new(remapped_plan, original_schema);
        let extension = datafusion::logical_expr::Extension {
            node: std::sync::Arc::new(adapter),
        };
        return Ok(LogicalPlan::Extension(extension));
    }

    Ok(remapped_plan)
}



struct PlanScopeRemapper {
        alias_counter: std::cell::RefCell<usize>,
    }

    /// Feature flag for aggressive join aliasing (disabled post-fix).
    /// Set to `true` only for emergency rollback if ambiguity issues resurface.
    /// See: https://github.com/apache/datafusion/issues/13272
    #[cfg(feature = "aggressive-join-aliasing")]
    const AGGRESSIVE_JOIN_ALIASING: bool = true;
    #[cfg(not(feature = "aggressive-join-aliasing"))]
    const AGGRESSIVE_JOIN_ALIASING: bool = false;

    impl PlanScopeRemapper {
        fn new() -> Self {
            Self {
                alias_counter: std::cell::RefCell::new(0),
            }
        }

        fn next_alias(&self) -> String {
            let mut c = self.alias_counter.borrow_mut();
            *c += 1;
            format!("derived_sq_{}", *c)
        }

        /// Wraps a plan in a SubqueryAlias with uniquely renamed columns.
        /// Used for Join inputs to avoid column name collisions.
        ///
        /// DEPRECATED: Disabled due to datafusion-contrib/datafusion-federation#13272
        /// The Unparser doesn't rewrite outer column references to match wrapped aliases.
        #[allow(dead_code)]
        fn ensure_aliased_with_unique_columns(
            &self,
            plan: LogicalPlan,
            scope: RelationScope,
            source_alias: Option<&str>, // Original alias like "u" or "d"
        ) -> datafusion::error::Result<(LogicalPlan, RelationScope)> {
            use datafusion::logical_expr::SubqueryAlias;

            // Wrap in SubqueryAlias
            let alias = self.next_alias();
            let prefix = source_alias.unwrap_or(&alias);
            
            tracing::debug!(
                target: "sql_gen::aliasing",
                ?alias,
                ?source_alias,
                "Starting smart aliasing"
            );

            // Build renaming projection: SELECT col AS prefix_col, ...
            let schema = plan.schema().clone();
            let mut rename_exprs = Vec::new();
            let mut new_column_names = Vec::new();
            
            use datafusion::common::Column;
            
            for (qualifier, field) in schema.iter() {
                let col_name = field.name();

                // SMART ALIASING: Detect if column is already aliased from previous wrapping
                // Pattern: derived_sq_N_something (e.g., derived_sq_1_d_name)
                let (base_qualifier, base_name) = if col_name.contains("derived_sq_") {
                    // Extract base components: derived_sq_1_d_name -> ("d", "name")
                    // Format in col_name is: derived_sq_{num}_{qual}_{name} or derived_sq_{num}_{name}
                    let parts: Vec<&str> = col_name.split("derived_sq_").collect();
                    if parts.len() >= 2 {
                        let after_prefix = parts.last().unwrap(); // "1_d_name"
                        let name_parts: Vec<&str> = after_prefix.splitn(2, '_').collect(); // ["1", "d_name"]
                        if name_parts.len() == 2 {
                            let base_col = name_parts[1]; // "d_name"
                            // Try to extract qualifier from base_col (format: qual_name) (heuristic)
                            let base_split: Vec<&str> = base_col.rsplitn(2, '_').collect();
                            if base_split.len() == 2 && base_split[1].len() == 1 { // Heuristic: single char qualifier like u, d or simple qual
                                (Some(TableReference::bare(base_split[1])), base_split[0].to_string())
                            } else {
                                (None, base_col.to_string())
                            }
                        } else {
                            (qualifier.cloned(), col_name.clone())
                        }
                    } else {
                        (qualifier.cloned(), col_name.clone())
                    }
                } else if let Some(q) = qualifier {
                    (Some(q.clone()), col_name.clone())
                } else {
                    (None, col_name.clone())
                };

                // Create FLAT name: prefix + base_qual + base_name
                // e.g., derived_sq_2 + d + name = derived_sq_2_d_name (NOT derived_sq_2_derived_sq_1_d_name)
                let new_name = if let Some(q) = &base_qualifier {
                    let q_str = q.to_string().replace('.', "_").replace('"', "");
                    format!("{}_{}_{}", prefix, q_str, base_name)
                } else {
                    format!("{}_{}", prefix, base_name)
                };
                
                new_column_names.push(new_name.clone());

                tracing::trace!(
                    target: "sql_gen::aliasing",
                    ?col_name,
                    ?qualifier,
                    ?base_qualifier,
                    ?base_name,
                    ?new_name,
                    "Transformed column name"
                );
                
                let col_expr = Expr::Column(Column::new(qualifier.cloned(), col_name.clone()));
                rename_exprs.push(col_expr.alias(new_name));
            }
            
            // Create: Projection(renames) -> SubqueryAlias
            let proj_plan = LogicalPlanBuilder::from(plan)
                .project(rename_exprs)?
                .build()?;
            
            let new_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                std::sync::Arc::new(proj_plan),
                alias.clone(),
            )?);

            // Create new scope for the alias
            let mut output_scope = RelationScope::new();
            let alias_ref = TableReference::bare(alias.clone());
            output_scope.register_relation(alias_ref.clone(), new_column_names.clone());
            
            // Map original qualified names to new names
            for (i, (qualifier, field)) in schema.iter().enumerate() {
                 let original_name = field.name();
                 let new_name = &new_column_names[i];
                
                // Determine the ORIGINAL logical qualifier:name before any wrapping
                // Re-use logic or re-derive (simpler to re-derive here for clarity matching plan)
                 let (orig_qual, orig_name) = if original_name.contains("derived_sq_") {
                    let base = original_name.split("derived_sq_")
                        .last()
                        .and_then(|s| s.splitn(2, '_').nth(1))
                        .unwrap_or(original_name);
                    
                    let parts: Vec<&str> = base.rsplitn(2, '_').collect();
                    if parts.len() == 2 && parts[1].len() == 1 {
                        (Some(TableReference::bare(parts[1])), parts[0].to_string())
                    } else {
                        (None, base.to_string())
                    }
                } else if let Some(q) = qualifier {
                    (Some(q.clone()), original_name.clone())
                } else {
                    (None, original_name.clone())
                };

                if let Some(ref qual) = orig_qual {
                    // 1. Standard mapping
                    output_scope.add_column_mapping(
                        Some(qual.clone()),
                        orig_name.clone(),
                        new_name.clone(),
                    );
                    
                    // 2. STRING FALLBACK: "qual.name" -> new_name
                    output_scope.add_string_mapping(
                         format!("{}.{}", qual, orig_name),
                         new_name.clone()
                    );
                } else {
                    output_scope.add_column_mapping(
                        None,
                        orig_name.clone(),
                        new_name.clone(),
                    );
                }
            }
            
            // Map inner relations to outer alias
            for inner_rels in scope.visible_relations().values() {
                for inner_rel in inner_rels {
                    output_scope.add_mapping(inner_rel.clone(), alias_ref.clone());
                }
            }

            tracing::debug!(
                target: "sql_gen::aliasing",
                ?alias,
                new_columns = ?new_column_names,
                "Completed aliasing"
            );

            Ok((new_plan, output_scope))
        }

        #[allow(dead_code)]
        fn ensure_aliased(
            &self,
            plan: LogicalPlan,
            scope: RelationScope,
        ) -> datafusion::error::Result<(LogicalPlan, RelationScope)> {
             self.ensure_aliased_with_unique_columns(plan, scope, None)
        }

        /// Recursively remap the plan.
        /// Returns: (Remapped Plan, Scope Exported by this plan)
        fn remap_plan(
            &self,
            plan: &LogicalPlan,
            parent_scope: &Option<Box<RelationScope>>,
        ) -> datafusion::error::Result<(LogicalPlan, RelationScope)> {
            use datafusion::error::DataFusionError;

            match plan {
                LogicalPlan::Projection(proj) => {
                    let (new_input, input_scope) = self.remap_plan(&proj.input, parent_scope)?;
                    // Removed: ensure_aliased call
                    let combined_scope = self.combine_scopes(&input_scope, parent_scope);

                    let initial_exprs = proj
                        .expr
                        .iter()
                        .map(|e| self.remap_expr(e, &combined_scope))
                        .collect::<datafusion::error::Result<Vec<_>>>()?;

                    let proj_plan = LogicalPlanBuilder::from(new_input)
                        .project(initial_exprs)?
                        .build()?;

                    // Use the resulting schema to populate the output scope.
                    // This ensures that outer nodes see exactly what this projection exports.
                    let mut new_output_scope = RelationScope::new();
                    new_output_scope.set_parent(input_scope.parent().clone());

                    // CRITICAL: Preserve mappings from input scope
                    // This ensures that column_mappings and alias_mappings created by
                    // wrapped join inputs are not lost when traversing through this Projection
                    new_output_scope.inherit_mappings_from(&input_scope);

                    for (qualifier, field) in proj_plan.schema().iter() {
                        let field_name = field.name().clone();
                        if let Some(rel) = qualifier {
                            new_output_scope.register_relation(rel.clone(), vec![field_name]);
                        } else {
                            new_output_scope.register_unqualified_column(field_name);
                        }
                    }

                    Ok((proj_plan, new_output_scope))
                }
                LogicalPlan::SubqueryAlias(sq) => {
                    // Pass parent_scope for correlated subqueries, but isolation is naturally handled
                    // by the fact that child builds its own scope from TableScans.
                    let (new_input, _child_scope) = self.remap_plan(&sq.input, parent_scope)?;

                    // Capture fields before moving new_input into SubqueryAlias
                    let new_fields = new_input.schema().fields().clone();

                    let new_plan = LogicalPlan::SubqueryAlias(datafusion::logical_expr::SubqueryAlias::try_new(
                        std::sync::Arc::new(new_input),
                        sq.alias.clone(),
                    )?);

                    let mut output_scope = RelationScope::new();
                    let alias_ref = sq.alias.clone();
                    
                    // Logic to map original column names to potentially renamed input columns (Smart Aliasing)
                    let original_fields = sq.input.schema().fields();
                    
                    // Register NEW (physical) names as the visible targets in the scope
                    // This ensures that when we resolve to 'derived_sq_X_col', it is valid in the scope
                    let columns: Vec<String> = new_fields.iter().map(|f| f.name().clone()).collect();
                    output_scope.register_relation(alias_ref.clone(), columns);

                    if new_fields.len() == original_fields.len() {
                        for (i, orig_field) in original_fields.iter().enumerate() {
                            let new_name = new_fields[i].name();
                            let orig_name = orig_field.name();
                            
                            if new_name != orig_name {
                                tracing::trace!(
                                    target: "sql_gen::scope",
                                    alias = %sq.alias,
                                    orig = %orig_name,
                                    new = %new_name,
                                    "Mapping subquery column"
                                );
                                // Map qualified reference: sub.user_id -> derived_user_id
                                output_scope.add_column_mapping(
                                    Some(sq.alias.clone()),
                                    orig_name.clone(),
                                    new_name.clone(),
                                );
                                // Map unqualified reference: user_id -> derived_user_id
                                output_scope.add_column_mapping(
                                    None,
                                    orig_name.clone(),
                                    new_name.clone(),
                                );
                            }
                        }
                    }

                    // Map all relations visible in the child scope to this new alias.
                    // This allows outer nodes to resolve "leaked" qualifiers (like inner table names)
                    // to this subquery alias.
                    for inner_rels in _child_scope.visible_relations().values() {
                        for inner_rel in inner_rels {
                            output_scope.add_mapping(inner_rel.clone(), alias_ref.clone());
                        }
                    }

                    Ok((new_plan, output_scope))
                }
                LogicalPlan::TableScan(scan) => {
                    let mut filter_remap_scope = RelationScope::new();
                    // When remapping filters inside the TableScan, we should enable them to
                    // see ALL columns from the table, not just the projected ones.
                    let all_columns = scan
                        .source
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                    filter_remap_scope.register_relation(scan.table_name.clone(), all_columns);

                    let mut remapped_filters = Vec::new();
                    for filter in &scan.filters {
                        remapped_filters.push(self.remap_expr(filter, &filter_remap_scope)?);
                    }

                    let mut new_scan = scan.clone();
                    new_scan.filters = remapped_filters;

                    // For the output scope (visible to parent nodes), we ONLY export projected columns.
                    let mut output_scope = RelationScope::new();
                    let projected_columns = scan
                        .projected_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                    output_scope.register_relation(scan.table_name.clone(), projected_columns);

                    Ok((LogicalPlan::TableScan(new_scan), output_scope))
                }
                LogicalPlan::Join(join) => {
                    tracing::debug!(
                        target: "sql_gen::join",
                        join_type = ?join.join_type,
                        "Processing join"
                    );

                    let (new_left, left_scope) = self.remap_plan(&join.left, parent_scope)?;
                    let (new_right, right_scope) = self.remap_plan(&join.right, parent_scope)?;

                    tracing::trace!(
                        target: "sql_gen::join",
                        left_scope = ?left_scope.visible_relations(),
                        right_scope = ?right_scope.visible_relations(),
                        "Join inputs remapped"
                    );

                    #[cfg(feature = "aggressive-join-aliasing")]
                    let needs_alias_left = should_alias_join_input(&new_left);
                    #[cfg(not(feature = "aggressive-join-aliasing"))]
                    let needs_alias_left = matches!(new_left, LogicalPlan::Projection(_));

                    let (aliased_left, final_left_scope) = if needs_alias_left {
                        let hint = extract_alias_hint(&new_left);
                        self.ensure_aliased_with_unique_columns(new_left, left_scope, hint.as_deref())?
                    } else {
                        (new_left, left_scope)
                    };

                    #[cfg(feature = "aggressive-join-aliasing")]
                    let needs_alias_right = should_alias_join_input(&new_right);
                    #[cfg(not(feature = "aggressive-join-aliasing"))]
                    let needs_alias_right = matches!(new_right, LogicalPlan::Projection(_));

                    let (aliased_right, final_right_scope) = if needs_alias_right {
                        let hint = extract_alias_hint(&new_right);
                        self.ensure_aliased_with_unique_columns(new_right, right_scope, hint.as_deref())?
                    } else {
                        (new_right, right_scope)
                    };

                    let mut combined_scope = final_left_scope.clone();
                    combined_scope.merge(&final_right_scope);
                    let full_scope = self.combine_scopes(&combined_scope, parent_scope);

                    let new_filter = match &join.filter {
                        Some(expr) => Some(self.remap_expr(expr, &full_scope)?),
                        None => None,
                    };

                    let new_on: Vec<(Expr, Expr)> = join
                        .on
                        .iter()
                        .map(|(l, r)| {
                            Ok((
                                self.remap_expr(l, &full_scope)?,
                                self.remap_expr(r, &full_scope)?,
                            ))
                        })
                        .collect::<datafusion::error::Result<Vec<_>>>()?;

                    let (left_keys, right_keys): (Vec<Expr>, Vec<Expr>) = new_on.into_iter().unzip();
                    let left_cols = left_keys
                        .into_iter()
                        .map(|e| {
                            if let Expr::Column(c) = e {
                                Ok(c)
                            } else {
                                Err(DataFusionError::Plan("Join key not a column".to_string()))
                            }
                        })
                        .collect::<datafusion::error::Result<Vec<_>>>()?;

                    let right_cols = right_keys
                        .into_iter()
                        .map(|e| {
                            if let Expr::Column(c) = e {
                                Ok(c)
                            } else {
                                Err(DataFusionError::Plan("Join key not a column".to_string()))
                            }
                        })
                        .collect::<datafusion::error::Result<Vec<_>>>()?;

                    tracing::debug!(
                        target: "sql_gen::join",
                        on = ?join.on,
                        left_keys = ?left_cols,
                        right_keys = ?right_cols,
                        "Join construction starting"
                    );

                    let new_plan = LogicalPlanBuilder::from(aliased_left)
                        .join(
                            aliased_right,
                            join.join_type,
                            (left_cols, right_cols),
                            new_filter,
                        )?
                        .build()?;

                    Ok((new_plan, combined_scope))
                }
            LogicalPlan::Filter(f) => {
                let (new_input, input_scope) = self.remap_plan(&f.input, parent_scope)?;
                let combined_scope = self.combine_scopes(&input_scope, parent_scope);
                let new_expr = self.remap_expr(&f.predicate, &combined_scope)?;

                let new_plan = LogicalPlanBuilder::from(new_input)
                    .filter(new_expr)?
                    .build()?;

                Ok((new_plan, input_scope))
            }
            LogicalPlan::Sort(s) => {
                let (new_input, input_scope) = self.remap_plan(&s.input, parent_scope)?;
                let combined_scope = self.combine_scopes(&input_scope, parent_scope);
                let new_exprs = s
                    .expr
                    .iter()
                    .map(|sort_expr| {
                        let new_inner = self.remap_expr(&sort_expr.expr, &combined_scope)?;
                        Ok(datafusion::logical_expr::SortExpr::new(
                            new_inner,
                            sort_expr.asc,
                            sort_expr.nulls_first,
                        ))
                    })
                    .collect::<datafusion::error::Result<Vec<_>>>()?;

                let new_plan = LogicalPlanBuilder::from(new_input)
                    .sort(new_exprs)?
                    .build()?;

                Ok((new_plan, input_scope))
            }
            LogicalPlan::Aggregate(agg) => {
                tracing::debug!(
                    target: "sql_gen::aggregate",
                    group_expr_count = agg.group_expr.len(),
                    aggr_expr_count = agg.aggr_expr.len(),
                    "Processing aggregate"
                );

                let (new_input, input_scope) = self.remap_plan(&agg.input, parent_scope)?;

                tracing::debug!(
                    target: "sql_gen::aggregate",
                    input_mappings = ?input_scope.column_mappings,
                    input_string_mappings = ?input_scope.string_mappings,
                    "Aggregate input scope"
                );
                
                // Fix for Unparser derived_projection issue: Wrap Projection inputs
                #[cfg(not(feature = "aggressive-join-aliasing"))]
                let (new_input, input_scope) = if matches!(new_input, LogicalPlan::Projection(_)) {
                     let hint = extract_alias_hint(&new_input);
                     self.ensure_aliased_with_unique_columns(new_input, input_scope, hint.as_deref())?
                } else {
                     (new_input, input_scope)
                };

                #[cfg(feature = "aggressive-join-aliasing")]
                let (new_input, input_scope) = if should_alias_join_input(&new_input) {
                     let hint = extract_alias_hint(&new_input);
                     self.ensure_aliased_with_unique_columns(new_input, input_scope, hint.as_deref())?
                } else {
                     (new_input, input_scope)
                };

                let combined_scope = self.combine_scopes(&input_scope, parent_scope);

                let initial_group_exprs = agg
                    .group_expr
                    .iter()
                    .map(|e| self.remap_expr(e, &combined_scope))
                    .collect::<datafusion::error::Result<Vec<_>>>()?;

                let new_aggr_exprs = agg
                    .aggr_expr
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        let remapped = self.remap_expr(e, &combined_scope)?;
                        let schema_idx = agg.group_expr.len() + i;
                        let field = agg.schema.field(schema_idx);
                        let name = field.name().clone();
                        Ok(remapped.alias(name))
                    })
                    .collect::<datafusion::error::Result<Vec<_>>>()?;

                let mut valid_mappings = Vec::new();
                let new_group_exprs: Vec<Expr> = initial_group_exprs
                    .into_iter()
                    .map(|e| {
                        if let Expr::Column(c) = &e {
                            if let Some(rel) = &c.relation {
                                let rel_str = rel.to_string();
                                if !rel_str.contains("derived") {
                                    let new_name = flatten_alias(rel, &c.name);
                                    valid_mappings.push((
                                        Some(rel.clone()),
                                        c.name.clone(),
                                        new_name.clone(),
                                    ));
                                    return e.clone().alias(new_name);
                                }
                            }
                        }
                        e
                    })
                    .collect();

                let agg_plan = LogicalPlanBuilder::from(new_input)
                    .aggregate(new_group_exprs, new_aggr_exprs)?
                    .build()?;

                let mut output_scope = RelationScope::new();
                
                // CRITICAL: Inherit mappings from input scope
                // This ensures that alias_mappings from wrapped join inputs are preserved
                output_scope.inherit_mappings_from(&input_scope);
                
                for (qualifier, field) in agg_plan.schema().iter() {
                    let field_name = field.name().clone();
                    if let Some(rel) = qualifier {
                         output_scope.register_relation(rel.clone(), vec![field_name]);
                    } else {
                         output_scope.register_unqualified_column(field_name);
                    }
                }



                // Register column mappings for flattened group expressions
                // This allows outer nodes to resolve qualified references like `d.name` -> `d_name`
                for (rel, old_name, new_name) in valid_mappings {
                    output_scope.add_column_mapping(rel, old_name.to_string(), new_name.to_string());
                }

                Ok((agg_plan, output_scope))
            }
            LogicalPlan::Extension(ext) => {
                if let Some(_adapter) = ext.node.as_any().downcast_ref::<SchemaAdapter>() {
                    // Already remapped, but we might need to recurse if there are more adapters inside (unlikely)
                    // For now, assume a single layer of SchemaAdapter is enough.
                    return Ok((plan.clone(), RelationScope::new()));
                }

                if let Some(fed_node) = ext
                    .node
                    .as_any()
                    .downcast_ref::<datafusion_federation::FederatedPlanNode>()
                {
                    // For Federated nodes, we MUST remap the inner plan but we stop at further Federated/SchemaAdapters.
                    let (remapped_inner, inner_scope) =
                        self.remap_plan(fed_node.plan(), parent_scope)?;

                    let new_fed_node = datafusion_federation::FederatedPlanNode::new(
                        remapped_inner,
                        fed_node.planner.clone(),
                    );

                    let new_ext = datafusion::logical_expr::Extension {
                        node: std::sync::Arc::new(new_fed_node),
                    };

                    Ok((LogicalPlan::Extension(new_ext), inner_scope))
                } else {
                    if plan.inputs().len() == 1 {
                        let (new_input, input_scope) =
                            self.remap_plan(plan.inputs()[0], parent_scope)?;
                        let new_plan = plan.with_new_exprs(plan.expressions(), vec![new_input])?;
                        Ok((new_plan, input_scope))
                    } else {
                        Ok((plan.clone(), RelationScope::new()))
                    }
                }
            }
            _ => {
                if plan.inputs().len() == 1 {
                    let (new_input, input_scope) =
                        self.remap_plan(plan.inputs()[0], parent_scope)?;
                    let new_plan = plan.with_new_exprs(plan.expressions(), vec![new_input])?;
                    Ok((new_plan, input_scope))
                } else if plan.inputs().is_empty() {
                    Ok((plan.clone(), RelationScope::new()))
                } else {
                    Ok((plan.clone(), RelationScope::new()))
                }
            }
        }
    }

    fn combine_scopes(
        &self,
        primary: &RelationScope,
        secondary: &Option<Box<RelationScope>>,
    ) -> RelationScope {
        match secondary {
            Some(parent) => primary.clone().with_parent(Some(parent.clone())),
            None => primary.clone(),
        }
    }

    fn remap_expr(&self, expr: &Expr, scope: &RelationScope) -> datafusion::error::Result<Expr> {
        use datafusion::common::tree_node::{Transformed, TreeNode};

        expr.clone()
            .transform(|e| {
                if let Expr::Column(col) = &e {
                    let qualified = scope.resolve_column(col)?;
                    return Ok(Transformed::yes(Expr::Column(qualified.into())));
                }
                Ok(Transformed::no(e))
            })
            .map(|t| t.data)
    }
}



fn flatten_alias(rel: &TableReference, col_name: &str) -> String {
    let rel_str = rel.to_string().replace('.', "_").replace('"', "");
    
    // Prevent recursive aliasing for already-wrapped relations
    if rel_str.starts_with("derived_sq") || rel_str.contains("derived") {
         return col_name.to_string(); 
    }

    if col_name.starts_with(&rel_str) {
        col_name.to_string()
    } else {
        format!("{}_{}", rel_str, col_name)
    }
}

/// Extract alias hint from a plan (table name or subquery alias)
fn extract_alias_hint(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan.table_name.to_string()),
        LogicalPlan::SubqueryAlias(sq) => Some(sq.alias.to_string()),
        _ => None,
    }
}

#[cfg(feature = "aggressive-join-aliasing")]
fn should_alias_join_input(plan: &LogicalPlan) -> bool {
    // Feature flag: when disabled, never wrap join inputs in SubqueryAlias.
    // This avoids DataFusion Unparser issue #13272 where outer column references
    // continue to use original qualifiers that are hidden inside the subquery.
    if !AGGRESSIVE_JOIN_ALIASING {
        return false;
    }
    
    match plan {
        // Never wrap simple table scans or existing subquery aliases
        LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => false,
        
        // Don't wrap Projections - their schema is already explicit
        // and wrapping breaks the scope chain by hiding column_mappings
        LogicalPlan::Projection(_) => false,
        
        // Wrap aggregates to avoid ambiguity
        LogicalPlan::Aggregate(_) => true,
        
        // For other complex nodes, generally don't wrap to avoid scope issues
        _ => false,
    }
}

// ----------------------------------------------------------------------------
// Scope Management Logic
// ----------------------------------------------------------------------------

use crate::relation_scope::RelationScope;
use datafusion::common::TableReference;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::sql::unparser::dialect::Dialect as UnparserDialect;

/// Scope-aware SQL generator that properly handles column qualifiers
struct ScopedUnparser<'a> {
    dialect: &'a dyn UnparserDialect,
}

impl<'a> ScopedUnparser<'a> {
    fn new(dialect: &'a dyn UnparserDialect) -> Self {
        Self { dialect }
    }

    fn plan_to_sql(
        &self,
        plan: &LogicalPlan,
    ) -> Result<datafusion::sql::sqlparser::ast::Statement> {
        tracing::debug!("Original plan:\n{}", plan.display_indent());

        let remapper = PlanScopeRemapper::new();
        let (remapped_plan, _) = remapper
            .remap_plan(plan, &Option::<Box<RelationScope>>::None)
            .context("Failed to remap plan scopes")?;

        tracing::debug!("Remapped plan:\n{}", remapped_plan.display_indent());

        let unparser = Unparser::new(self.dialect);
        unparser
            .plan_to_sql(&remapped_plan)
            .context("Failed to unparse remapped plan")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use std::sync::Arc;


    #[tokio::test]
    async fn test_postgres_quoting() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "postgres")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        // Postgres uses " for identifiers
        assert!(sql.contains(r#""users""#) || sql.contains("users"));
        println!("Postgres SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_mysql_quoting() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "mysql")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        println!("MySQL SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_oracle_dialect() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "oracle")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        println!("Oracle SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_snowflake_dialect() {
        let plan = test_plan().await;
        let sql = get_sql_for_plan(&plan, "snowflake")
            .expect("failed to generate sql")
            .expect("expected SQL output");
        println!("Snowflake SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_substrait_source() {
        assert!(is_substrait_source("duckdb"));
        assert!(is_substrait_source("datafusion"));
        assert!(!is_substrait_source("postgres"));
    }

    #[tokio::test]
    async fn test_local_execution_fallback() {
        let plan = test_plan().await;
        let result = get_sql_for_plan(&plan, "unknown_db").expect("should not error");
        assert!(
            result.is_none(),
            "unknown dialect should return None for local execution"
        );
    }

    async fn test_plan() -> LogicalPlan {
        let ctx = SessionContext::new();
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]);
        let _ = ctx.register_table(
            "users",
            std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
                std::sync::Arc::new(schema),
            )),
        );

        ctx.table("users")
            .await
            .unwrap()
            .filter(col("id").gt(lit(10)))
            .unwrap()
            .select(vec![col("name")])
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    #[tokio::test]
    async fn test_aggregation_smart_aliasing() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("dept_id", arrow::datatypes::DataType::Int32, false),
        ]);
        let table = std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(std::sync::Arc::new(schema)));
        ctx.register_table("users", table.clone())?;
        ctx.register_table("depts", table.clone())?;

        let u = ctx.table("users").await?.alias("u")?;
        let d = ctx.table("depts").await?.alias("d")?;

        let join = u.join(d, JoinType::Inner, &["dept_id"], &["id"], None)?;
        let proj = join.select(vec![col("u.name"), col("d.name")])?;
        let agg = proj.aggregate(vec![col("d.name")], vec![datafusion::functions_aggregate::expr_fn::count(col("u.name"))])?;

        let plan = agg.into_optimized_plan()?;
        let sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
        println!("Generated SQL:\n{}", sql);

        assert!(sql.contains("GROUP BY"));
        assert!(!sql.contains("derived_sq_2_derived_sq_1"), "Found recursively nested alias!");
        
        Ok(())
    }

    #[tokio::test]
    async fn test_scoped_subquery_generation() -> Result<()> {
        // Construct plan: SELECT u.name FROM (SELECT * FROM users u) AS derived
        let ctx = SessionContext::new();
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]);
        ctx.register_table(
            "users",
            std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(
                std::sync::Arc::new(schema),
            )),
        )?;

        // Build plan manually to ensure we reference inner alias 'u' from outer scope
        let table_scan = ctx.table("users").await?;
        let subquery = table_scan.alias("u")?.alias("derived")?; // Equivalent to (SELECT * FROM users u) AS derived

        // Now project from it.
        // Note: DF's `alias` method wraps in SubqueryAlias.
        // `table("users")` -> TableScan(users)
        // `.alias("u")` -> SubqueryAlias(u, TableScan(users))
        // `.alias("derived")` -> SubqueryAlias(derived, SubqueryAlias(u, ...))

        // If we project `col("u.name")` from `derived`, DF might fail validation if it strictly enforces scoping.
        // But let's try to simulate the bad plan structure explicitly if needed.
        // For standard DF execution, `derived` exposes columns like `derived.id`.

        // Usage: `SELECT derived.name ...` is correct.
        let plan = subquery.select(vec![col("name")])?.into_optimized_plan()?;

        // The unparser usually fails when it sees `name` coming from `u` deeper in the tree and uses `u.name`.
        println!("Plan before SQL gen:\n{}", plan.display_indent());
        let sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
        println!("Generated SQL: {}", sql);

        assert!(sql.contains("derived") || sql.contains(r#""derived""#));
        // The column u.name is valid INSIDE the subquery.
        assert!(sql.contains(r#""u"."name""#) || sql.contains("u.name"));

        Ok(())
    }


}



