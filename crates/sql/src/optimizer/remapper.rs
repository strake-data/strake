//! Federation Scope Remapper
//!
//! Transforms LogicalPlans to ensure column qualifiers respect SQL scoping rules
//! when unparsing to SQL for remote execution. Handles:
//! - Subquery alias boundary enforcement
//! - Column name collision avoidance via smart aliasing
//! - Join input wrapping for unparser compatibility
//!
//! See: <https://github.com/apache/datafusion/issues/13272> for context on
//! why aggressive aliasing is required for some DataFusion versions.

use datafusion::common::{Column, Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, SubqueryAlias};
use std::cell::Cell;
use std::sync::Arc;

use crate::relation_scope::{DerivedNameParser, RelationScope};
use crate::schema_adapter::SchemaAdapter;

/// Optimizer pass that remaps column qualifiers in a LogicalPlan to respect SQL scoping rules.
pub struct FederationScopeOptimizer;

/// Remaps column qualifiers in a LogicalPlan to respect SQL scoping rules.
///
/// This function is designed to be used as a `LogicalOptimizer` for the
/// datafusion-federation's `SQLExecutor` trait. It traverses the plan and
/// fixes column qualifiers so that inner subquery aliases don't leak to
/// outer query scopes.
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

        let adapter = SchemaAdapter::new(remapped_plan, original_schema);
        let extension = datafusion::logical_expr::Extension {
            node: Arc::new(adapter),
        };
        return Ok(LogicalPlan::Extension(extension));
    }

    Ok(remapped_plan)
}

#[doc(hidden)]
pub struct PlanScopeRemapper {
    pub(crate) alias_counter: Cell<usize>,
}

/// Feature flag for aggressive join aliasing (disabled post-fix).
/// Set to `true` only for emergency rollback if ambiguity issues resurface.
/// See: https://github.com/apache/datafusion/issues/13272
const AGGRESSIVE_JOIN_ALIASING: bool = true;

impl Default for PlanScopeRemapper {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanScopeRemapper {
    pub fn new() -> Self {
        Self {
            alias_counter: Cell::new(1),
        }
    }

    fn next_alias(&self) -> String {
        let val = self.alias_counter.get();
        self.alias_counter.set(val + 1);
        format!("derived_sq_{}", val)
    }

    /// Wraps a plan in a SubqueryAlias with uniquely renamed columns.
    /// Used for Join inputs to avoid column name collisions.
    fn ensure_aliased_with_unique_columns(
        &self,
        plan: LogicalPlan,
        scope: RelationScope,
        source_alias: Option<&str>,
    ) -> Result<(LogicalPlan, RelationScope)> {
        let alias = self.next_alias();
        let prefix = source_alias.unwrap_or(&alias);

        tracing::debug!(
            target: "sql_gen::aliasing",
            ?alias,
            ?source_alias,
            "Starting smart aliasing"
        );

        // 1. Build renaming projection
        let (rename_exprs, new_column_names) = self.build_rename_projection(&plan, prefix)?;

        // 2. Create the aliased plan
        let new_plan = self.create_aliased_plan(plan, &alias, rename_exprs)?;

        // 3. Build output scope with mappings
        let output_scope = self.build_output_scope(
            &scope,
            &alias,
            &new_column_names,
            schema_at_boundary(&new_plan)?,
        )?;

        Ok((new_plan, output_scope))
    }

    fn build_rename_projection(
        &self,
        plan: &LogicalPlan,
        prefix: &str,
    ) -> Result<(Vec<Expr>, Vec<String>)> {
        let schema = plan.schema();
        let mut rename_exprs = Vec::new();
        let mut new_column_names = Vec::new();

        for (qualifier, field) in schema.iter() {
            let col_name = field.name();

            let (base_qualifier, base_name) = DerivedNameParser::parse(col_name)
                .unwrap_or_else(|| (qualifier.cloned(), col_name.clone()));

            // Create FLAT name: prefix + base_qual + base_name
            let new_name = if let Some(q) = &base_qualifier {
                let q_str = q.to_string().replace('.', "_").replace('"', "");
                format!("{}_{}_{}", prefix, q_str, base_name)
            } else {
                format!("{}_{}", prefix, base_name)
            };

            new_column_names.push(new_name.clone());

            let col_expr = Expr::Column(Column::new(qualifier.cloned(), col_name.clone()));
            rename_exprs.push(col_expr.alias(new_name));
        }

        Ok((rename_exprs, new_column_names))
    }

    fn create_aliased_plan(
        &self,
        plan: LogicalPlan,
        alias: &str,
        rename_exprs: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let proj_plan = LogicalPlanBuilder::from(plan)
            .project(rename_exprs)?
            .build()?;

        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(proj_plan),
            alias.to_string(),
        )?))
    }

    fn build_output_scope(
        &self,
        inner_scope: &RelationScope,
        alias: &str,
        new_column_names: &[String],
        schema: &datafusion::common::DFSchema,
    ) -> Result<RelationScope> {
        let mut output_scope = RelationScope::new();
        let alias_ref = TableReference::bare(alias);
        output_scope.register_relation(alias_ref.clone(), new_column_names.to_vec());

        // Map original qualified names to new names
        for (i, (qualifier, field)) in schema.iter().enumerate() {
            let column_name = field.name();
            let new_name = &new_column_names[i];

            let (orig_qual, orig_name) = DerivedNameParser::parse(column_name)
                .unwrap_or_else(|| (qualifier.cloned(), column_name.clone()));

            if let Some(ref qual) = orig_qual {
                output_scope.add_column_mapping(
                    Some(qual.clone()),
                    orig_name.clone(),
                    new_name.clone(),
                );
                output_scope
                    .add_string_mapping(format!("{}.{}", qual, orig_name), new_name.clone());
            } else {
                output_scope.add_column_mapping(None, orig_name.clone(), new_name.clone());
            }
        }

        // Map inner relations to outer alias
        for inner_rels in inner_scope.visible_relations().values() {
            for inner_rel in inner_rels {
                output_scope.add_mapping(inner_rel.clone(), alias_ref.clone());
            }
        }

        Ok(output_scope)
    }

    /// Recursively remap the plan.
    /// Returns: (Remapped Plan, Scope Exported by this plan)
    pub fn remap_plan(
        &self,
        plan: &LogicalPlan,
        parent_scope: &Option<Box<RelationScope>>,
    ) -> Result<(LogicalPlan, RelationScope)> {
        use datafusion::error::DataFusionError;

        match plan {
            LogicalPlan::Projection(proj) => {
                let (new_input, input_scope) = self.remap_plan(&proj.input, parent_scope)?;
                let combined_scope = self.combine_scopes(&input_scope, parent_scope);

                let initial_exprs = proj
                    .expr
                    .iter()
                    .map(|e| self.remap_expr(e, &combined_scope))
                    .collect::<datafusion::error::Result<Vec<_>>>()?;

                let proj_plan = LogicalPlanBuilder::from(new_input)
                    .project(initial_exprs)?
                    .build()?;

                let mut new_output_scope = RelationScope::new();
                new_output_scope.set_parent(input_scope.parent().clone());
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
                let (new_input, _child_scope) = self.remap_plan(&sq.input, parent_scope)?;
                let new_fields = new_input.schema().fields().clone();

                let new_plan =
                    LogicalPlan::SubqueryAlias(datafusion::logical_expr::SubqueryAlias::try_new(
                        Arc::new(new_input),
                        sq.alias.clone(),
                    )?);

                let mut output_scope = RelationScope::new();
                let alias_ref = sq.alias.clone();
                let original_fields = sq.input.schema().fields();

                let columns: Vec<String> = new_fields.iter().map(|f| f.name().clone()).collect();
                output_scope.register_relation(alias_ref.clone(), columns);

                tracing::debug!(
                    target: "sql_gen::debug",
                    "SubqueryAlias {}: mapped fields count: {}/{}",
                    sq.alias, new_fields.len(), original_fields.len()
                );

                if new_fields.len() == original_fields.len() {
                    for (i, orig_field) in original_fields.iter().enumerate() {
                        let new_name = new_fields[i].name();
                        let orig_name = orig_field.name();

                        tracing::debug!(
                            target: "sql_gen::debug",
                            "SubqueryAlias {}: mapping {} -> {}",
                            sq.alias, orig_name, new_name
                        );

                        // Always add mapping, even if names are identical, so we can resolve qualified names
                        output_scope.add_column_mapping(
                            Some(sq.alias.clone()),
                            orig_name.clone(),
                            new_name.clone(),
                        );
                        output_scope.add_column_mapping(None, orig_name.clone(), new_name.clone());
                    }
                }

                for inner_rels in _child_scope.visible_relations().values() {
                    for inner_rel in inner_rels {
                        output_scope.add_mapping(inner_rel.clone(), alias_ref.clone());
                    }
                }

                Ok((new_plan, output_scope))
            }
            LogicalPlan::TableScan(scan) => {
                let mut filter_remap_scope = RelationScope::new();
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
                let (new_left, left_scope) = self.remap_plan(&join.left, parent_scope)?;
                let (new_right, right_scope) = self.remap_plan(&join.right, parent_scope)?;

                let needs_alias_left = should_alias_join_input(&new_left);
                let (aliased_left, final_left_scope): (LogicalPlan, RelationScope) =
                    if needs_alias_left {
                        let hint = extract_alias_hint(&new_left);
                        self.ensure_aliased_with_unique_columns(
                            new_left,
                            left_scope,
                            hint.as_deref(),
                        )?
                    } else {
                        (new_left, left_scope)
                    };

                let needs_alias_right = should_alias_join_input(&new_right);
                let (aliased_right, final_right_scope): (LogicalPlan, RelationScope) =
                    if needs_alias_right {
                        let hint = extract_alias_hint(&new_right);
                        self.ensure_aliased_with_unique_columns(
                            new_right,
                            right_scope,
                            hint.as_deref(),
                        )?
                    } else {
                        (new_right, right_scope)
                    };

                let mut combined_scope = final_left_scope.clone();
                combined_scope.merge(&final_right_scope);

                // DEBUG
                tracing::debug!(
                    target: "sql_gen::debug",
                    "Join Scope Debug: Left mappings: {:?}\nRight mappings: {:?}\nCombined mappings: {:?}",
                    final_left_scope.column_mappings.keys().map(|(q, n)| format!("{:?}.{}", q, n)).collect::<Vec<_>>(),
                    final_right_scope.column_mappings.keys().map(|(q, n)| format!("{:?}.{}", q, n)).collect::<Vec<_>>(),
                    combined_scope.column_mappings.keys().map(|(q, n)| format!("{:?}.{}", q, n)).collect::<Vec<_>>()
                );

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
            LogicalPlan::Union(union) => {
                let mut new_inputs = Vec::new();
                let mut merged_scope = RelationScope::new();

                for input in &union.inputs {
                    let (new_input, scope) = self.remap_plan(input, parent_scope)?;
                    new_inputs.push(new_input);
                    merged_scope.merge(&scope);
                }

                let new_plan = LogicalPlan::Union(datafusion::logical_expr::Union {
                    inputs: new_inputs.into_iter().map(Arc::new).collect(),
                    schema: union.schema.clone(),
                });

                Ok((new_plan, merged_scope))
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
                let (new_input, input_scope) = self.remap_plan(&agg.input, parent_scope)?;

                let (new_input, input_scope) = if should_alias_join_input(&new_input) {
                    let hint = extract_alias_hint(&new_input);
                    self.ensure_aliased_with_unique_columns(
                        new_input,
                        input_scope,
                        hint.as_deref(),
                    )?
                } else {
                    (new_input, input_scope)
                };

                let combined_scope = self.combine_scopes(&input_scope, parent_scope);

                let initial_group_exprs = agg
                    .group_expr
                    .iter()
                    .map(|e| self.remap_expr(e, &combined_scope))
                    .collect::<datafusion::error::Result<Vec<_>>>()?;

                let aggr_exprs_with_info = agg
                    .aggr_expr
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        let remapped = self.remap_expr(e, &combined_scope)?;
                        let schema_idx = agg.group_expr.len() + i;
                        let field = agg.schema.field(schema_idx);
                        let name = field.name().clone();
                        tracing::debug!(
                            target: "sql_gen::debug",
                            "Aggregate: aliasing aggr expr {} to {}",
                            remapped, name
                        );

                        // Also map the original mangled name to this alias to help parent nodes (like Filters)
                        // that might still use the DataFusion-generated name.
                        let mangled = format!("{}", e);
                        if mangled != name {
                            tracing::debug!(
                                target: "sql_gen::debug",
                                "Aggregate: mapping mangled name {} -> {}",
                                mangled, name
                            );
                            // We'll add this to the output_scope later
                        }

                        Ok((remapped.alias(name.clone()), Some(mangled), name))
                    })
                    .collect::<datafusion::error::Result<Vec<_>>>()?;

                let mut agg_expr_mappings: Vec<(String, String)> = Vec::new();
                let mut final_aggr_exprs = Vec::new();
                for (expr, mangled, alias) in aggr_exprs_with_info {
                    final_aggr_exprs.push(expr);
                    if let Some(m) = mangled {
                        agg_expr_mappings.push((m, alias));
                    }
                }

                let agg_plan = LogicalPlanBuilder::from(new_input)
                    .aggregate(initial_group_exprs, final_aggr_exprs)?
                    .build()?;

                let mut output_scope = RelationScope::new();
                output_scope.inherit_mappings_from(&input_scope);

                // Add the aggregate expression mappings
                for (mangled, alias) in agg_expr_mappings {
                    output_scope.add_column_mapping(None, mangled.clone(), alias.clone());
                    // Also as a string mapping for fallback
                    output_scope.add_string_mapping(mangled, alias);
                }

                // Register group expr columns with their ORIGINAL qualifiers
                for (i, group_expr) in agg.group_expr.iter().enumerate() {
                    if let Expr::Column(c) = group_expr {
                        let field_name = agg_plan.schema().field(i).name();
                        // Map the original table qualifier to the output field
                        if let Some(rel) = &c.relation {
                            output_scope.add_column_mapping(
                                Some(rel.clone()),
                                c.name.clone(),
                                field_name.clone(),
                            );
                        }
                        // Also Add unqualified mapping
                        output_scope.add_column_mapping(None, c.name.clone(), field_name.clone());
                    }
                }

                tracing::debug!(
                    target: "sql_gen::debug",
                    "Aggregate output scope has {} column mappings",
                    output_scope.column_mappings.len()
                );

                for (qualifier, field) in agg_plan.schema().iter() {
                    let field_name = field.name().clone();
                    if let Some(rel) = qualifier {
                        output_scope.register_relation(rel.clone(), vec![field_name]);
                    } else {
                        output_scope.register_unqualified_column(field_name);
                    }
                }

                Ok((agg_plan, output_scope))
            }
            LogicalPlan::Extension(ext) => {
                let node = ext.node.as_any();
                if let Some(adapter) = node.downcast_ref::<SchemaAdapter>() {
                    // Recover scope of the inner plan
                    let (new_inner, inner_scope) = self.remap_plan(&adapter.input, parent_scope)?;

                    let new_adapter = SchemaAdapter::new(new_inner, Arc::clone(&adapter.schema));
                    let new_plan = LogicalPlan::Extension(datafusion::logical_expr::Extension {
                        node: Arc::new(new_adapter),
                    });
                    return Ok((new_plan, inner_scope));
                }

                if let Some(fed_node) =
                    node.downcast_ref::<datafusion_federation::FederatedPlanNode>()
                {
                    let (remapped_inner, inner_scope) =
                        self.remap_plan(fed_node.plan(), parent_scope)?;

                    let new_fed_node = datafusion_federation::FederatedPlanNode::new(
                        remapped_inner,
                        Arc::clone(&fed_node.planner),
                    );

                    let new_ext = datafusion::logical_expr::Extension {
                        node: Arc::new(new_fed_node),
                    };

                    Ok((LogicalPlan::Extension(new_ext), inner_scope))
                } else if plan.inputs().len() == 1 {
                    let (new_input, input_scope) =
                        self.remap_plan(plan.inputs()[0], parent_scope)?;
                    let new_plan = plan.with_new_exprs(plan.expressions(), vec![new_input])?;
                    Ok((new_plan, input_scope))
                } else {
                    Ok((plan.clone(), RelationScope::new()))
                }
            }
            _ => {
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

    pub fn remap_expr(
        &self,
        expr: &Expr,
        scope: &RelationScope,
    ) -> datafusion::error::Result<Expr> {
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

fn extract_alias_hint(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan.table_name.to_string()),
        LogicalPlan::SubqueryAlias(sq) => Some(sq.alias.to_string()),
        _ => None,
    }
}

fn should_alias_join_input(plan: &LogicalPlan) -> bool {
    if !AGGRESSIVE_JOIN_ALIASING {
        return false;
    }

    match plan {
        LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => false,
        // Wrap aggregates, joins, unions and projections to avoid ambiguity and unparser failures.
        // Projections are wrapped to ensure qualifiers are properly scoped at boundaries.
        LogicalPlan::Aggregate(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Projection(_) => true,
        _ => false,
    }
}

fn schema_at_boundary(plan: &LogicalPlan) -> Result<&datafusion::common::DFSchema> {
    Ok(plan.schema())
}
