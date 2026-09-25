//! # Interval Domain Propagation Optimizer Rule
//!
//! Provides a logical optimizer rule ([`IntervalDomainPropagationRule`]) that infers and
//! propagates transitive interval and range constraints across join boundaries.
//!
//! ## Overview
//!
//! In federated query processing, joining a large entity or interval dataset (Source A)
//! with a time-bounded event dataset (Source B) often suffers from severe over-fetching
//! because predicates on Source B are not pushed down to Source A across the federation boundary.
//!
//! When an inner join condition specifies an interval relation (e.g.
//! `event_time >= start_time AND event_time <= end_time`) and one branch contains a constant
//! domain predicate (e.g. `event_time BETWEEN T0 AND T1`), this rule computes the transitive
//! mathematical implications on the other branch (`start_time <= T1` and `end_time >= T0`)
//! and injects them as pre-filters directly into the logical plan prior to federated SQL pushdown.
//!
//! ## Extension Point
//!
//! This rule implements DataFusion's `OptimizerRule` trait and operates on `LogicalPlan`
//! trees during logical optimization.
//!
//! ## Soundness & Boundary Algebra
//!
//! Boundary inference preserves exact inclusive and exclusive bounds:
//! - If `L >= R` and `R >= B_low` (inclusive) $\implies L \ge B_low$ (inclusive).
//! - If `L > R` and `R >= B_low` $\implies L > B_low$ (exclusive).
//! - If `L <= R` and `R <= B_high` (inclusive) $\implies L \le B_high$ (inclusive).
//! - If `L < R` and `R <= B_high` $\implies L < B_high$ (exclusive).
//! - If `L = R` (equality), all bounds are bidirectional with matching inclusive/exclusive semantics.
//!
//! Three-valued SQL NULL logic is strictly preserved:
//! - If a contributing bound evaluates to `NULL` or is non-constant, bound derivation is deferred/skipped.
//! - Predicates are only pushed through inner joins (`JoinType::Inner`); outer joins are preserved untouched
//!   to avoid altering null-generation semantics.
//!
//! ## Applicability Guards
//!
//! Propagation is skipped whenever a bound cannot be attributed soundly. Each
//! guard trades coverage for correctness:
//!
//! - **Joins with an `Aggregate`, `Distinct` or `Window` on either side are left
//!   alone.** This rule runs in the same optimizer fixpoint as
//!   `DISTINCT_DECORRELATION_RULE` and `FLATTEN_FEDERATED_JOIN`, which can
//!   rewrite a branch into an aggregate after a bound has been derived against
//!   it; a pre-filter derived here would then filter a post-aggregation output.
//! - **Projections are only crossed for identity and rename mappings.** For
//!   `SELECT a AS b, b AS a FROM t`, the output name `b` denotes the child column
//!   `a`, so a name-only re-resolution would attribute a bound to the wrong
//!   column. Computed outputs (casts, function calls, arithmetic) are left
//!   unmapped, and any predicate referencing them is dropped.
//! - **Columns must agree on their qualifier exactly.** An unqualified column is
//!   never treated as equal to a qualified one with the same name, so a bound
//!   found on `t2.lo` cannot be applied to `t1.lo`.
//! - **Only plan-time constants may be baked into a bound.** See
//!   [`is_valid_constant`] and [`DETERMINISTIC_SCALAR_FNS`]: DataFusion's
//!   `ScalarUDF` has no determinism flag, so volatile zero-argument functions
//!   such as `now()`, `random()` and `uuid()` require an explicit allowlist
//!   rather than an "all arguments are constant" test.
//!
//! ## Performance Characteristics
//!
//! - **Time Complexity**: $\mathcal{O}(J \times C)$ where $J$ is the number of join nodes and $C$
//!   is the number of filter conjuncts.
//! - **Fixpoint Idempotence**: The rule is strictly idempotent; re-applying the rule to its own output
//!   produces zero transformations in subsequent iterations of the optimizer fixpoint loop.
//!
//! ## Examples
//!
//! ```rust
//! use strake_sql::optimizer::interval_propagation::IntervalDomainPropagationRule;
//! use datafusion::optimizer::optimizer::OptimizerRule;
//!
//! let rule = IntervalDomainPropagationRule::new();
//! assert_eq!(rule.name(), "interval_domain_propagation_rule");
//! ```

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::{
    BinaryExpr, Expr, JoinType, LogicalPlan, Operator, Projection,
    logical_plan::builder::LogicalPlanBuilder,
};
use datafusion::optimizer::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::scalar::ScalarValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Rule to infer and propagate transitive range and interval domain constraints across inner joins.
#[derive(Debug, Default)]
pub struct IntervalDomainPropagationRule;

impl IntervalDomainPropagationRule {
    /// Creates a new [`IntervalDomainPropagationRule`].
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BoundType {
    LowerInclusive, // col >= bound
    LowerExclusive, // col > bound
    UpperInclusive, // col <= bound
    UpperExclusive, // col < bound
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColumnBound {
    column: Column,
    bound_type: BoundType,
    bound_expr: Expr,
}

impl ColumnBound {
    #[must_use]
    fn to_expr(&self) -> Expr {
        let col_expr = Expr::Column(self.column.clone());
        let op = match self.bound_type {
            BoundType::LowerInclusive => Operator::GtEq,
            BoundType::LowerExclusive => Operator::Gt,
            BoundType::UpperInclusive => Operator::LtEq,
            BoundType::UpperExclusive => Operator::Lt,
        };
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col_expr),
            op,
            Box::new(self.bound_expr.clone()),
        ))
    }
}

/// Returns true if a literal is safe to bake into a pushed-down predicate.
#[must_use]
fn is_safe_literal(scalar: &ScalarValue) -> bool {
    // Three-valued NULL guard: never derive bounds for NULL constants.
    if scalar.is_null() {
        return false;
    }
    // Timezone safety guard: defer on non-UTC timezone-aware timestamps to avoid
    // remote TZ drift. Checked on every literal in the tree, not just the root,
    // so a `CAST('...' AS TIMESTAMPTZ)` cannot bypass it.
    !matches!(
        scalar,
        ScalarValue::TimestampSecond(_, Some(tz))
        | ScalarValue::TimestampMillisecond(_, Some(tz))
        | ScalarValue::TimestampMicrosecond(_, Some(tz))
        | ScalarValue::TimestampNanosecond(_, Some(tz))
            if tz.as_ref() != "+00:00" && tz.as_ref() != "UTC"
    )
}

/// Scalar functions whose result is a deterministic function of their
/// arguments, and which may therefore appear in a derived bound.
///
/// DataFusion's `ScalarUDF` carries no determinism flag, so this has to be an
/// explicit allowlist. A bare "are all the arguments constant?" test accepts
/// *every* zero-argument function, which includes the non-deterministic `now()`,
/// `random()` and `uuid()`. Those must not be baked into a filter that is pushed
/// below a join and into remote SQL, where they would be evaluated at a
/// different time and against different session state.
const DETERMINISTIC_SCALAR_FNS: &[&str] = &[
    "arrow_cast",
    "date_add",
    "date_part",
    "date_sub",
    "date_trunc",
    "to_timestamp",
];

/// Checks whether an expression is a plan-time constant that is safe to derive
/// bounds from.
///
/// Matches positively on the expressions that are genuinely constant at plan
/// time. A negative test ("contains no column references") is unsound:
/// `expr_to_columns` only collects [`Expr::Column`], so aggregates without
/// column arguments, window functions, `ScalarVariable`s, and volatile scalar
/// functions such as `now()`, `random()` and `uuid()` all pass it. Those must not
/// be baked into a filter that is pushed below a join and into remote SQL, where
/// they would be evaluated at a different time and against different session
/// state.
#[must_use]
fn is_valid_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => is_safe_literal(scalar),
        // Transparent wrappers: the constant-ness of the operand carries over.
        Expr::Negative(inner) => is_valid_constant(inner),
        Expr::Cast(cast) => is_valid_constant(&cast.expr),
        // A scalar function is constant only if it is deterministic *and* every
        // argument is.
        Expr::ScalarFunction(func) => {
            DETERMINISTIC_SCALAR_FNS.contains(&func.name().to_ascii_lowercase().as_str())
                && func.args.iter().all(is_valid_constant)
        }
        // Everything else -- columns, placeholders, aggregates, window
        // functions, scalar variables, volatile functions, binary arithmetic --
        // is rejected.
        _ => false,
    }
}

fn split_conjunction<'a>(expr: &'a Expr, acc: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            split_conjunction(left, acc);
            split_conjunction(right, acc);
        }
        _ => acc.push(expr),
    }
}

#[must_use]
fn expr_to_column(expr: &Expr) -> Option<Column> {
    match expr {
        Expr::Column(c) => Some(c.clone()),
        _ => None,
    }
}

/// Rewrites predicates expressed in a projection's input namespace into the
/// projection's output namespace.
///
/// Crossing a projection changes what a column *name* denotes, so a predicate
/// found below a projection cannot be re-resolved against the parent's schema by
/// name alone: for `SELECT a AS b, b AS a FROM t`, the bound on the child column
/// `b` would be attributed to the output column `b`, which is really `a`.
///
/// Only outputs produced by a bare column reference are translatable. Any other
/// output (a cast, a function call, arithmetic) is a computed value whose
/// relationship to the input cannot be established, so it is left unmapped and
/// any predicate referencing it is dropped. Dropping is always sound; guessing
/// is not.
fn translate_through_projection(proj: &Projection, preds: Vec<Expr>) -> Vec<Expr> {
    let out_schema = proj.schema.as_ref();

    // child column name (lowercased) -> parent column name
    let mut map: HashMap<String, String> = HashMap::new();
    for (idx, expr) in proj.expr.iter().enumerate() {
        let Some(field) = out_schema.field(idx).into() else {
            continue;
        };
        let source = match unwrap_alias(expr) {
            Some(Expr::Column(c)) => c,
            _ => continue,
        };
        map.entry(source.name.to_lowercase())
            .or_insert_with(|| field.name().clone());
    }

    if map.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(preds.len());
    for pred in preds {
        let mut unresolvable = false;
        let Ok(rewritten) = pred.transform(&mut |e| {
            if let Expr::Column(c) = &e {
                if let Some(new_name) = map.get(&c.name.to_lowercase()) {
                    // Drop the qualifier: the parent's schema is authoritative
                    // and `normalize_bounds_for_schema` re-resolves it there.
                    let new_name = new_name.clone();
                    return Ok(Transformed::yes(Expr::Column(Column::new(
                        None::<datafusion::common::TableReference>,
                        new_name,
                    ))));
                }
                unresolvable = true;
            }
            Ok(Transformed::no(e))
        }) else {
            continue;
        };
        if !unresolvable {
            out.push(rewritten.data);
        }
    }
    out
}

/// Strips a single level of [`Expr::Alias`], if present.
fn unwrap_alias(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Alias(alias) => Some(&alias.expr),
        other => Some(other),
    }
}

/// Collects predicates from a subtree.
///
/// **Invariant:** every predicate in `acc` is expressed in the namespace of
/// `plan`'s *own output schema* -- the namespace the parent sees. This is what
/// makes it safe for the caller to re-resolve columns with
/// `normalize_bounds_for_schema` against the join branch's schema.
fn collect_plan_predicates(plan: &LogicalPlan, acc: &mut Vec<Expr>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            // A Filter is schema-preserving, so its predicate is already in the
            // namespace `plan` exports.
            let mut exprs = Vec::new();
            split_conjunction(&filter.predicate, &mut exprs);
            for e in exprs {
                acc.push(e.clone());
            }
            collect_plan_predicates(filter.input.as_ref(), acc);
        }
        LogicalPlan::TableScan(scan) => {
            for filter in &scan.filters {
                let mut exprs = Vec::new();
                split_conjunction(filter, &mut exprs);
                for e in exprs {
                    acc.push(e.clone());
                }
            }
        }
        LogicalPlan::Projection(proj) => {
            let mut child_preds = Vec::new();
            collect_plan_predicates(proj.input.as_ref(), &mut child_preds);
            acc.extend(translate_through_projection(proj, child_preds));
        }
        // A SubqueryAlias preserves column names and only changes the relation
        // qualifier, which `normalize_bounds_for_schema` re-resolves.
        LogicalPlan::SubqueryAlias(alias) => {
            collect_plan_predicates(alias.input.as_ref(), acc);
        }
        _ => {}
    }
}

/// Returns true if a subtree contains a node that changes what its input columns
/// mean, so a derived pre-filter would land on the wrong rows.
///
/// `Aggregate`, `Distinct` and `Window` are all barriers: pushing a pre-filter
/// below one of them -- or deriving bounds from columns a decorrelation /
/// flattening rule in the same optimizer fixpoint may rewrite into an aggregate --
/// risks filtering a post-aggregation output rather than a base scan.
#[must_use]
fn contains_barrier_node(plan: &LogicalPlan) -> bool {
    plan.apply(|node| {
        if matches!(
            node,
            LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) | LogicalPlan::Window(_)
        ) {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .is_ok_and(|recursion| recursion == TreeNodeRecursion::Stop)
}

fn extract_bounds_from_expr(expr: &Expr, acc: &mut Vec<ColumnBound>) {
    let mut conjuncts = Vec::new();
    split_conjunction(expr, &mut conjuncts);

    for conj in conjuncts {
        match conj {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if let Some(col) = expr_to_column(left)
                    && is_valid_constant(right)
                {
                    match op {
                        Operator::GtEq => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::LowerInclusive,
                            bound_expr: (**right).clone(),
                        }),
                        Operator::Gt => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::LowerExclusive,
                            bound_expr: (**right).clone(),
                        }),
                        Operator::LtEq => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::UpperInclusive,
                            bound_expr: (**right).clone(),
                        }),
                        Operator::Lt => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::UpperExclusive,
                            bound_expr: (**right).clone(),
                        }),
                        Operator::Eq => {
                            acc.push(ColumnBound {
                                column: col.clone(),
                                bound_type: BoundType::LowerInclusive,
                                bound_expr: (**right).clone(),
                            });
                            acc.push(ColumnBound {
                                column: col,
                                bound_type: BoundType::UpperInclusive,
                                bound_expr: (**right).clone(),
                            });
                        }
                        _ => {}
                    }
                } else if let Some(col) = expr_to_column(right)
                    && is_valid_constant(left)
                {
                    // e.g. T0 <= event_time  => event_time >= T0
                    match op {
                        Operator::GtEq => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::UpperInclusive,
                            bound_expr: (**left).clone(),
                        }),
                        Operator::Gt => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::UpperExclusive,
                            bound_expr: (**left).clone(),
                        }),
                        Operator::LtEq => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::LowerInclusive,
                            bound_expr: (**left).clone(),
                        }),
                        Operator::Lt => acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::LowerExclusive,
                            bound_expr: (**left).clone(),
                        }),
                        Operator::Eq => {
                            acc.push(ColumnBound {
                                column: col.clone(),
                                bound_type: BoundType::LowerInclusive,
                                bound_expr: (**left).clone(),
                            });
                            acc.push(ColumnBound {
                                column: col,
                                bound_type: BoundType::UpperInclusive,
                                bound_expr: (**left).clone(),
                            });
                        }
                        _ => {}
                    }
                }
            }
            Expr::Between(between) if !between.negated => {
                if let Some(col) = expr_to_column(&between.expr) {
                    if is_valid_constant(&between.low) {
                        acc.push(ColumnBound {
                            column: col.clone(),
                            bound_type: BoundType::LowerInclusive,
                            bound_expr: (*between.low).clone(),
                        });
                    }
                    if is_valid_constant(&between.high) {
                        acc.push(ColumnBound {
                            column: col,
                            bound_type: BoundType::UpperInclusive,
                            bound_expr: (*between.high).clone(),
                        });
                    }
                }
            }
            Expr::InList(in_list) if !in_list.negated && in_list.list.len() == 1 => {
                if let Some(col) = expr_to_column(&in_list.expr)
                    && is_valid_constant(&in_list.list[0])
                {
                    acc.push(ColumnBound {
                        column: col.clone(),
                        bound_type: BoundType::LowerInclusive,
                        bound_expr: in_list.list[0].clone(),
                    });
                    acc.push(ColumnBound {
                        column: col,
                        bound_type: BoundType::UpperInclusive,
                        bound_expr: in_list.list[0].clone(),
                    });
                }
            }
            _ => {}
        }
    }
}

/// Returns true if two columns denote the same source column.
///
/// Requires *exact* qualifier agreement. Treating an unqualified column as equal
/// to any qualified one with the same name would let a bound discovered on
/// `t2.lo` be applied to `t1.lo`; `find_column_in_schema` returns an
/// unqualified column for subquery aliases and self-joins, which is exactly
/// where that conflation is reachable. Unqualified-to-unqualified comparison is
/// still allowed, and is safe because `find_column_in_schema` only returns an
/// unqualified column when the name is unique in that schema.
#[must_use]
fn column_matches(c1: &Column, c2: &Column) -> bool {
    if !c1.name.eq_ignore_ascii_case(&c2.name) {
        return false;
    }
    match (&c1.relation, &c2.relation) {
        (Some(r1), Some(r2)) => r1.to_string().eq_ignore_ascii_case(&r2.to_string()),
        (None, None) => true,
        // Never conflate a qualified column with an unqualified one.
        _ => false,
    }
}

#[must_use]
fn schema_has_column(schema: &DFSchema, col: &Column) -> bool {
    find_column_in_schema(schema, col).is_some()
}

#[must_use]
fn find_column_in_schema(schema: &DFSchema, col: &Column) -> Option<Column> {
    if schema.qualified_field_from_column(col).is_ok() {
        return Some(col.clone());
    }
    let mut matches = schema
        .iter()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(&col.name))
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()));
    let first = matches.next()?;
    if matches.next().is_some() {
        None
    } else {
        Some(first)
    }
}

fn normalize_bounds_for_schema(bounds: &mut [ColumnBound], schema: &DFSchema) {
    for bound in bounds {
        if let Some(column) = find_column_in_schema(schema, &bound.column) {
            bound.column = column;
        }
    }
}

impl OptimizerRule for IntervalDomainPropagationRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(&|node| {
            if let LogicalPlan::Join(join) = &node {
                // Only inner joins preserve interval and range implications without null-generation semantics
                if join.join_type != JoinType::Inner {
                    return Ok(Transformed::no(node));
                }

                // Skip joins whose sides contain Aggregate/Distinct/Window. This
                // rule runs in the same optimizer fixpoint as
                // DISTINCT_DECORRELATION_RULE and FLATTEN_FEDERATED_JOIN, which
                // can rewrite a branch into an aggregate after we have derived a
                // bound against it. In that case a pre-filter derived here would
                // end up filtering a post-aggregation output. Correctness over
                // coverage: nested cases simply lose propagation.
                if contains_barrier_node(join.left.as_ref())
                    || contains_barrier_node(join.right.as_ref())
                {
                    return Ok(Transformed::no(node));
                }

                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                // 1. Collect known predicates & bounds on left and right branches
                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();
                collect_plan_predicates(join.left.as_ref(), &mut left_predicates);
                collect_plan_predicates(join.right.as_ref(), &mut right_predicates);

                let mut left_bounds = Vec::new();
                let mut right_bounds = Vec::new();
                for pred in &left_predicates {
                    extract_bounds_from_expr(pred, &mut left_bounds);
                }
                for pred in &right_predicates {
                    extract_bounds_from_expr(pred, &mut right_bounds);
                }
                normalize_bounds_for_schema(&mut left_bounds, left_schema);
                normalize_bounds_for_schema(&mut right_bounds, right_schema);

                // 2. Collect comparison conjuncts from join.filter
                let mut join_conjuncts = Vec::new();
                if let Some(ref filter_expr) = join.filter {
                    split_conjunction(filter_expr, &mut join_conjuncts);
                }

                let mut new_left_bounds = Vec::new();
                let mut new_right_bounds = Vec::new();

                // 3. Infer transitive bounds from join comparison filters
                for conj in join_conjuncts {
                    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = conj {
                        let col_a = expr_to_column(left);
                        let col_b = expr_to_column(right);

                        if let (Some(ca), Some(cb)) = (col_a, col_b) {
                            let ca_in_left = schema_has_column(left_schema, &ca);
                            let ca_in_right = schema_has_column(right_schema, &ca);
                            let cb_in_left = schema_has_column(left_schema, &cb);
                            let cb_in_right = schema_has_column(right_schema, &cb);

                            let resolve_column = |schema: &DFSchema, col: &Column| {
                                find_column_in_schema(schema, col).unwrap_or_else(|| col.clone())
                            };
                            let (left_side_col, right_side_col, rel_op) =
                                if ca_in_left && cb_in_right {
                                    (
                                        resolve_column(left_schema, &ca),
                                        resolve_column(right_schema, &cb),
                                        *op,
                                    )
                                } else if cb_in_left && ca_in_right {
                                    let inv_op = match op {
                                        Operator::GtEq => Operator::LtEq,
                                        Operator::Gt => Operator::Lt,
                                        Operator::LtEq => Operator::GtEq,
                                        Operator::Lt => Operator::Gt,
                                        Operator::Eq => Operator::Eq,
                                        _ => *op,
                                    };
                                    (
                                        resolve_column(left_schema, &cb),
                                        resolve_column(right_schema, &ca),
                                        inv_op,
                                    )
                                } else {
                                    continue;
                                };

                            // Boundary algebraic inference:
                            // Case A: L >= R  (meaning R <= L)
                            // If R has lower bound R >= L_bound => L >= L_bound (derive for L)
                            // If L has upper bound L <= U_bound => R <= U_bound (derive for R)
                            match rel_op {
                                Operator::GtEq => {
                                    // Derive for Left: if R >= bound => L >= bound
                                    for rb in &right_bounds {
                                        if column_matches(&rb.column, &right_side_col) {
                                            if rb.bound_type == BoundType::LowerInclusive {
                                                new_left_bounds.push(ColumnBound {
                                                    column: left_side_col.clone(),
                                                    bound_type: BoundType::LowerInclusive,
                                                    bound_expr: rb.bound_expr.clone(),
                                                });
                                            } else if rb.bound_type == BoundType::LowerExclusive {
                                                new_left_bounds.push(ColumnBound {
                                                    column: left_side_col.clone(),
                                                    bound_type: BoundType::LowerExclusive,
                                                    bound_expr: rb.bound_expr.clone(),
                                                });
                                            }
                                        }
                                    }
                                    // Derive for Right: if L <= bound => R <= bound
                                    for lb in &left_bounds {
                                        if column_matches(&lb.column, &left_side_col) {
                                            if lb.bound_type == BoundType::UpperInclusive {
                                                new_right_bounds.push(ColumnBound {
                                                    column: right_side_col.clone(),
                                                    bound_type: BoundType::UpperInclusive,
                                                    bound_expr: lb.bound_expr.clone(),
                                                });
                                            } else if lb.bound_type == BoundType::UpperExclusive {
                                                new_right_bounds.push(ColumnBound {
                                                    column: right_side_col.clone(),
                                                    bound_type: BoundType::UpperExclusive,
                                                    bound_expr: lb.bound_expr.clone(),
                                                });
                                            }
                                        }
                                    }
                                }
                                Operator::Gt => {
                                    for rb in &right_bounds {
                                        if column_matches(&rb.column, &right_side_col)
                                            && (rb.bound_type == BoundType::LowerInclusive
                                                || rb.bound_type == BoundType::LowerExclusive)
                                        {
                                            new_left_bounds.push(ColumnBound {
                                                column: left_side_col.clone(),
                                                bound_type: BoundType::LowerExclusive,
                                                bound_expr: rb.bound_expr.clone(),
                                            });
                                        }
                                    }
                                    for lb in &left_bounds {
                                        if column_matches(&lb.column, &left_side_col)
                                            && (lb.bound_type == BoundType::UpperInclusive
                                                || lb.bound_type == BoundType::UpperExclusive)
                                        {
                                            new_right_bounds.push(ColumnBound {
                                                column: right_side_col.clone(),
                                                bound_type: BoundType::UpperExclusive,
                                                bound_expr: lb.bound_expr.clone(),
                                            });
                                        }
                                    }
                                }
                                Operator::LtEq => {
                                    // L <= R
                                    // If R <= bound => L <= bound
                                    // If L >= bound => R >= bound
                                    for rb in &right_bounds {
                                        if column_matches(&rb.column, &right_side_col) {
                                            if rb.bound_type == BoundType::UpperInclusive {
                                                new_left_bounds.push(ColumnBound {
                                                    column: left_side_col.clone(),
                                                    bound_type: BoundType::UpperInclusive,
                                                    bound_expr: rb.bound_expr.clone(),
                                                });
                                            } else if rb.bound_type == BoundType::UpperExclusive {
                                                new_left_bounds.push(ColumnBound {
                                                    column: left_side_col.clone(),
                                                    bound_type: BoundType::UpperExclusive,
                                                    bound_expr: rb.bound_expr.clone(),
                                                });
                                            }
                                        }
                                    }
                                    for lb in &left_bounds {
                                        if column_matches(&lb.column, &left_side_col) {
                                            if lb.bound_type == BoundType::LowerInclusive {
                                                new_right_bounds.push(ColumnBound {
                                                    column: right_side_col.clone(),
                                                    bound_type: BoundType::LowerInclusive,
                                                    bound_expr: lb.bound_expr.clone(),
                                                });
                                            } else if lb.bound_type == BoundType::LowerExclusive {
                                                new_right_bounds.push(ColumnBound {
                                                    column: right_side_col.clone(),
                                                    bound_type: BoundType::LowerExclusive,
                                                    bound_expr: lb.bound_expr.clone(),
                                                });
                                            }
                                        }
                                    }
                                }
                                Operator::Lt => {
                                    for rb in &right_bounds {
                                        if column_matches(&rb.column, &right_side_col)
                                            && (rb.bound_type == BoundType::UpperInclusive
                                                || rb.bound_type == BoundType::UpperExclusive)
                                        {
                                            new_left_bounds.push(ColumnBound {
                                                column: left_side_col.clone(),
                                                bound_type: BoundType::UpperExclusive,
                                                bound_expr: rb.bound_expr.clone(),
                                            });
                                        }
                                    }
                                    for lb in &left_bounds {
                                        if column_matches(&lb.column, &left_side_col)
                                            && (lb.bound_type == BoundType::LowerInclusive
                                                || lb.bound_type == BoundType::LowerExclusive)
                                        {
                                            new_right_bounds.push(ColumnBound {
                                                column: right_side_col.clone(),
                                                bound_type: BoundType::LowerExclusive,
                                                bound_expr: lb.bound_expr.clone(),
                                            });
                                        }
                                    }
                                }
                                Operator::Eq => {
                                    for rb in &right_bounds {
                                        if column_matches(&rb.column, &right_side_col) {
                                            new_left_bounds.push(ColumnBound {
                                                column: left_side_col.clone(),
                                                bound_type: rb.bound_type,
                                                bound_expr: rb.bound_expr.clone(),
                                            });
                                        }
                                    }
                                    for lb in &left_bounds {
                                        if column_matches(&lb.column, &left_side_col) {
                                            new_right_bounds.push(ColumnBound {
                                                column: right_side_col.clone(),
                                                bound_type: lb.bound_type,
                                                bound_expr: lb.bound_expr.clone(),
                                            });
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // 4. Deduplicate and filter out any bounds that already exist on the target side
                let mut left_exprs_to_add = Vec::new();
                let mut seen_left_bounds = HashSet::new();
                for b in new_left_bounds {
                    let exists = left_bounds.iter().any(|existing| {
                        column_matches(&existing.column, &b.column)
                            && existing.bound_type == b.bound_type
                            && existing.bound_expr == b.bound_expr
                    });
                    if exists {
                        continue;
                    }
                    if let Some(target_col) = find_column_in_schema(left_schema, &b.column) {
                        let bound = ColumnBound {
                            column: target_col,
                            bound_type: b.bound_type,
                            bound_expr: b.bound_expr,
                        };
                        let expr = bound.to_expr();
                        if seen_left_bounds.insert(format!("{:?}", expr)) {
                            left_exprs_to_add.push(expr);
                        }
                    }
                }

                let mut right_exprs_to_add = Vec::new();
                let mut seen_right_bounds = HashSet::new();
                for b in new_right_bounds {
                    let exists = right_bounds.iter().any(|existing| {
                        column_matches(&existing.column, &b.column)
                            && existing.bound_type == b.bound_type
                            && existing.bound_expr == b.bound_expr
                    });
                    if exists {
                        continue;
                    }
                    if let Some(target_col) = find_column_in_schema(right_schema, &b.column) {
                        let bound = ColumnBound {
                            column: target_col,
                            bound_type: b.bound_type,
                            bound_expr: b.bound_expr,
                        };
                        let expr = bound.to_expr();
                        if seen_right_bounds.insert(format!("{:?}", expr)) {
                            right_exprs_to_add.push(expr);
                        }
                    }
                }

                if left_exprs_to_add.is_empty() && right_exprs_to_add.is_empty() {
                    return Ok(Transformed::no(node));
                }

                let mut new_left = join.left.as_ref().clone();
                for expr in left_exprs_to_add {
                    new_left = LogicalPlanBuilder::from(new_left).filter(expr)?.build()?;
                }

                let mut new_right = join.right.as_ref().clone();
                for expr in right_exprs_to_add {
                    new_right = LogicalPlanBuilder::from(new_right).filter(expr)?.build()?;
                }

                let mut new_join = join.clone();
                new_join.left = Arc::new(new_left);
                new_join.right = Arc::new(new_right);

                return Ok(Transformed::yes(LogicalPlan::Join(new_join)));
            }

            Ok(Transformed::no(node))
        })
    }

    fn name(&self) -> &str {
        "interval_domain_propagation_rule"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::{Between, col, lit};
    use datafusion::optimizer::optimizer::OptimizerContext;
    use datafusion::scalar::ScalarValue;
    use proptest::prelude::*;

    fn make_table_scan(table_name: &str, fields: Vec<Field>) -> Result<LogicalPlan> {
        let schema = Arc::new(Schema::new(fields));
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        LogicalPlanBuilder::scan(table_name, table_source, None)?.build()
    }

    #[test]
    fn test_interval_join_propagation_oracle_pattern() -> Result<()> {
        let interval_plan = make_table_scan(
            "interval_table",
            vec![
                Field::new(
                    "start_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "end_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("entity_id", DataType::Int64, false),
            ],
        )?;

        let event_plan = make_table_scan(
            "event_table",
            vec![
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("entity_id", DataType::Int64, false),
            ],
        )?;

        let t0 = ScalarValue::TimestampMicrosecond(Some(100_000_000), None);
        let t1 = ScalarValue::TimestampMicrosecond(Some(200_000_000), None);

        let filtered_events = LogicalPlanBuilder::from(event_plan)
            .filter(Expr::Between(Between {
                expr: Box::new(col("event_time")),
                negated: false,
                low: Box::new(lit(t0.clone())),
                high: Box::new(lit(t1.clone())),
            }))?
            .build()?;

        let join_filter = col("event_table.event_time")
            .gt_eq(col("interval_table.start_time"))
            .and(col("event_table.event_time").lt_eq(col("interval_table.end_time")));

        let plan = LogicalPlanBuilder::from(interval_plan)
            .join(
                filtered_events,
                JoinType::Inner,
                (vec!["entity_id"], vec!["entity_id"]),
                Some(join_filter),
            )?
            .build()?;

        let rule = IntervalDomainPropagationRule::new();
        let optimized = rule.rewrite(plan, &OptimizerContext::new())?;

        assert!(optimized.transformed, "Plan should be transformed");
        let plan_str = format!("{:?}", optimized.data);

        assert!(
            plan_str.contains("start_time") && plan_str.contains("TimestampMicrosecond(200000000"),
            "Expected start_time and T1 (200000000) in plan: {}",
            plan_str
        );
        assert!(
            plan_str.contains("end_time") && plan_str.contains("TimestampMicrosecond(100000000"),
            "Expected end_time and T0 (100000000) in plan: {}",
            plan_str
        );

        Ok(())
    }

    #[test]
    fn test_idempotence_rewrite_twice_yields_identical_plan() -> Result<()> {
        let interval_plan = make_table_scan(
            "interval_table",
            vec![
                Field::new("start_time", DataType::Int64, false),
                Field::new("end_time", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
            ],
        )?;

        let event_plan = make_table_scan(
            "event_table",
            vec![
                Field::new("event_time", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
            ],
        )?;

        let filtered_events = LogicalPlanBuilder::from(event_plan)
            .filter(
                col("event_time")
                    .gt_eq(lit(100i64))
                    .and(col("event_time").lt_eq(lit(200i64))),
            )?
            .build()?;

        let join_filter = col("event_table.event_time")
            .gt_eq(col("interval_table.start_time"))
            .and(col("event_table.event_time").lt_eq(col("interval_table.end_time")));

        let plan = LogicalPlanBuilder::from(interval_plan)
            .join(
                filtered_events,
                JoinType::Inner,
                (vec!["id"], vec!["id"]),
                Some(join_filter),
            )?
            .build()?;

        let rule = IntervalDomainPropagationRule::new();
        let config = OptimizerContext::new();

        let once = rule.rewrite(plan, &config)?;
        assert!(once.transformed, "First rewrite should transform plan");

        let twice = rule.rewrite(once.data.clone(), &config)?;
        assert!(
            !twice.transformed,
            "Second rewrite should be idempotent (no transformations)"
        );
        assert_eq!(format!("{:?}", once.data), format!("{:?}", twice.data));

        Ok(())
    }

    #[test]
    fn test_truth_table_matrix_all_operators_and_nulls() -> Result<()> {
        // Operators: <, <=, >, >=, Between, InList, and Null constant
        let table_a = make_table_scan(
            "table_a",
            vec![
                Field::new("val_a", DataType::Int64, true),
                Field::new("id", DataType::Int64, false),
            ],
        )?;
        let table_b = make_table_scan(
            "table_b",
            vec![
                Field::new("val_b", DataType::Int64, true),
                Field::new("id", DataType::Int64, false),
            ],
        )?;

        // 1. Strict inequality (val_b > 50) + join on (val_a >= val_b) => val_a > 50
        let b_filtered = LogicalPlanBuilder::from(table_b.clone())
            .filter(col("val_b").gt(lit(50i64)))?
            .build()?;
        let join_filter = col("table_a.val_a").gt_eq(col("table_b.val_b"));
        let plan = LogicalPlanBuilder::from(table_a.clone())
            .join(
                b_filtered,
                JoinType::Inner,
                (vec!["id"], vec!["id"]),
                Some(join_filter),
            )?
            .build()?;

        let rule = IntervalDomainPropagationRule::new();
        let opt = rule.rewrite(plan, &OptimizerContext::new())?;
        assert!(opt.transformed);
        let plan_str = format!("{:?}", opt.data);
        assert!(
            plan_str.contains("val_a") && plan_str.contains("50"),
            "Expected val_a and 50 in plan: {}",
            plan_str
        );

        // 2. NULL constant guard: filter on (val_b >= NULL) must NOT derive any bounds on A
        let b_null = LogicalPlanBuilder::from(table_b.clone())
            .filter(col("val_b").gt_eq(lit(ScalarValue::Int64(None))))?
            .build()?;
        let join_filter = col("table_a.val_a").gt_eq(col("table_b.val_b"));
        let plan_null = LogicalPlanBuilder::from(table_a)
            .join(
                b_null,
                JoinType::Inner,
                (vec!["id"], vec!["id"]),
                Some(join_filter),
            )?
            .build()?;
        let opt_null = rule.rewrite(plan_null, &OptimizerContext::new())?;
        assert!(
            !opt_null.transformed,
            "NULL bound constants must not derive invalid bounds"
        );

        Ok(())
    }

    #[test]
    fn test_interval_join_propagation_skip_outer_join() -> Result<()> {
        let interval_plan = make_table_scan(
            "interval_table",
            vec![
                Field::new("start_time", DataType::Int64, false),
                Field::new("end_time", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
            ],
        )?;

        let event_plan = make_table_scan(
            "event_table",
            vec![
                Field::new("event_time", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
            ],
        )?;

        let filtered_events = LogicalPlanBuilder::from(event_plan)
            .filter(
                col("event_time")
                    .gt_eq(lit(100i64))
                    .and(col("event_time").lt_eq(lit(200i64))),
            )?
            .build()?;

        let join_filter = col("event_table.event_time")
            .gt_eq(col("interval_table.start_time"))
            .and(col("event_table.event_time").lt_eq(col("interval_table.end_time")));

        let plan = LogicalPlanBuilder::from(interval_plan)
            .join(
                filtered_events,
                JoinType::Left,
                (vec!["id"], vec!["id"]),
                Some(join_filter),
            )?
            .build()?;

        let rule = IntervalDomainPropagationRule::new();
        let optimized = rule.rewrite(plan, &OptimizerContext::new())?;

        assert!(
            !optimized.transformed,
            "Left join should not be transformed"
        );

        Ok(())
    }

    /// Builds the canonical interval-join shape used by the differential
    /// property: an `intervals` table inner-joined to a domain-filtered `events`
    /// table on a range comparison.
    ///
    /// ```text
    ///   intervals (lo, hi, k)  INNER JOIN
    ///     Filter(k IS NOT NULL AND t BETWEEN [low, high]) events (t, x, k)
    ///   ON events.k = intervals.k
    ///      AND events.t <op1> intervals.lo
    ///      AND events.t <op2> intervals.hi
    /// ```
    ///
    /// **Both** sides are materialised memory tables. This matters: with an
    /// empty `intervals` side, every pre-filter the rule derives on `lo`/`hi` is
    /// a no-op, so a differential result comparison would pass no matter how
    /// wrong the derived bounds were. Real rows on both sides make those
    /// pre-filters load-bearing.
    #[allow(clippy::too_many_arguments)]
    fn build_interval_join_plan(
        interval_rows: Vec<(i64, i64, i64)>,
        event_rows: Vec<(i64, i64, Option<i64>)>,
        low: i64,
        high: i64,
        lower_op: Operator,
        upper_op: Operator,
    ) -> Result<LogicalPlan> {
        fn mem_scan(
            name: &str,
            schema: SchemaRef,
            batch: RecordBatch,
        ) -> Result<LogicalPlanBuilder> {
            let source = Arc::new(DefaultTableSource::new(Arc::new(
                datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?,
            )));
            LogicalPlanBuilder::scan(name, source, None)
        }

        let interval_schema = Arc::new(Schema::new(vec![
            Field::new("lo", DataType::Int64, false),
            Field::new("hi", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
        ]));
        let interval_batch = RecordBatch::try_new(
            Arc::clone(&interval_schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    interval_rows.iter().map(|r| r.0),
                )),
                Arc::new(Int64Array::from_iter_values(
                    interval_rows.iter().map(|r| r.1),
                )),
                Arc::new(Int64Array::from_iter_values(
                    interval_rows.iter().map(|r| r.2),
                )),
            ],
        )?;
        let intervals = mem_scan("intervals", interval_schema, interval_batch)?.build()?;

        let event_schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
        ]));
        let event_batch = RecordBatch::try_new(
            Arc::clone(&event_schema),
            vec![
                Arc::new(Int64Array::from_iter_values(event_rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter_values(event_rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter(event_rows.iter().map(|r| r.2))),
            ],
        )?;
        let events = mem_scan("events", event_schema, event_batch)?.build()?;

        let (lo, hi) = if low <= high {
            (low, high)
        } else {
            (high, low)
        };
        let filtered_events = LogicalPlanBuilder::from(events)
            .filter(
                col("k")
                    .is_not_null()
                    .and(col("t").gt_eq(lit(lo)))
                    .and(col("t").lt_eq(lit(hi))),
            )?
            .build()?;

        let join_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("events.t")),
            lower_op,
            Box::new(col("intervals.lo")),
        ))
        .and(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("events.t")),
            upper_op,
            Box::new(col("intervals.hi")),
        )));

        LogicalPlanBuilder::from(intervals)
            .join(
                filtered_events,
                JoinType::Inner,
                (vec!["k"], vec!["k"]),
                Some(join_filter),
            )?
            .project(vec![
                col("intervals.lo"),
                col("intervals.hi"),
                col("events.x"),
            ])?
            .build()
    }

    /// Collects and sorts the plan's output rows so two plans can be compared
    /// order-independently.
    async fn collect_sorted(plan: &LogicalPlan) -> Result<Vec<String>> {
        let ctx = datafusion::execution::context::SessionContext::new();
        let df = ctx.execute_logical_plan(plan.clone()).await?;
        let batches = df.collect().await?;
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| {
                    (0..b.num_columns())
                        .map(|c| {
                            format!(
                                "{:?}",
                                arrow::util::display::array_value_to_string(&b.column(c), i).ok()
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
            })
            .collect();
        rows.sort();
        Ok(rows)
    }

    fn run_differential(
        interval_rows: Vec<(i64, i64, i64)>,
        event_rows: Vec<(i64, i64, Option<i64>)>,
        low: i64,
        high: i64,
        lower_op: Operator,
        upper_op: Operator,
    ) -> std::result::Result<(), String> {
        let plan =
            build_interval_join_plan(interval_rows, event_rows, low, high, lower_op, upper_op)
                .map_err(|e| e.to_string())?;

        let optimized = IntervalDomainPropagationRule::new()
            .rewrite(plan.clone(), &OptimizerContext::new())
            .map_err(|e| e.to_string())?
            .data;

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|e| e.to_string())?;
        let expected = rt
            .block_on(collect_sorted(&plan))
            .map_err(|e| e.to_string())?;
        let actual = rt
            .block_on(collect_sorted(&optimized))
            .map_err(|e| e.to_string())?;

        if expected == actual {
            Ok(())
        } else {
            Err(format!(
                "rule changed the result set (low={low}, high={high}, lower_op={lower_op:?}, upper_op={upper_op:?})\n  before: {expected:?}\n  after:  {actual:?}"
            ))
        }
    }

    proptest! {
        /// End-to-end differential property: the rule may add pre-filters, but
        /// must never change the result set.
        ///
        /// The previous property test was tautological -- it extracted two
        /// bounds and then re-evaluated the same comparison by hand, never
        /// invoking `rewrite`. A regression that flipped `LowerExclusive` to
        /// `LowerInclusive` in the `Operator::Gt` arm would have left it green.
        /// This one executes both plans and compares their output, and
        /// parameterises the join comparison operators so every arm of the
        /// boundary algebra is exercised.
        #[test]
        fn prop_rewrite_preserves_inner_join_results(
            low in -20i64..20,
            high in -20i64..20,
            lower_op in prop::sample::select(vec![
                Operator::GtEq, Operator::Gt, Operator::LtEq, Operator::Lt
            ]),
            upper_op in prop::sample::select(vec![
                Operator::GtEq, Operator::Gt, Operator::LtEq, Operator::Lt
            ]),
            interval_rows in prop::collection::vec((any::<i64>(), any::<i64>(), any::<i64>()), 0..24),
            event_rows in prop::collection::vec((any::<i64>(), any::<i64>(), any::<Option<i64>>()), 0..24),
        ) {
            let (elo, ehi) = if low <= high { (low, high) } else { (high, low) };

            // Squash arbitrary i64s into the same small domain as the event
            // filter. Without this, raw `any::<i64>()` timestamps are outside
            // [low, high] almost always, the events filter removes every row,
            // and the join is empty -- so the comparison would be vacuous and
            // pass no matter how wrong the derived bounds were.
            let narrow = |v: i64| v.rem_euclid(40) - 20;
            let mut interval_rows: Vec<_> = interval_rows
                .into_iter()
                .map(|(lo, hi, k)| (narrow(lo), narrow(hi), k))
                .collect();
            let mut event_rows: Vec<_> = event_rows
                .into_iter()
                .map(|(t, x, k)| (narrow(t), x, k))
                .collect();

            // Pin rows exactly onto the event domain's endpoints. This is what
            // makes an inclusive/exclusive mix-up observable: a derived
            // `hi >= low` versus `hi > low` only differs when some interval's
            // `hi` lands exactly on `low` and an event sits exactly on `low`.
            // Purely uniform random values almost never do either, so the
            // property would pass regardless of the boundary algebra.
            for k in [0i64, 1, 2, 3] {
                interval_rows.push((elo, elo, k));          // hi == low exactly
                interval_rows.push((ehi, ehi, k));          // lo == high exactly
                interval_rows.push((elo, ehi, k));          // exactly the domain
                interval_rows.push((elo - 1, ehi + 1, k));  // strictly outside
                event_rows.push((elo, 1, Some(k)));         // event on the low edge
                event_rows.push((ehi, 2, Some(k)));         // event on the high edge
            }

            run_differential(interval_rows, event_rows, low, high, lower_op, upper_op)
                .map_err(TestCaseError::fail)?;
        }
    }

    #[test]
    fn test_rewrite_does_not_cross_aliasing_projection() -> Result<()> {
        // `SELECT a AS b, b AS a FROM t` under a filter on the *child* column
        // `b`. The child bound is on `a`; attributing it to the output column
        // `b` would push a filter for `a`'s domain onto the wrong column.
        let base = make_table_scan(
            "t",
            vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
                Field::new("k", DataType::Int64, false),
            ],
        )?;

        let aliasing = LogicalPlanBuilder::from(base)
            .project(vec![
                col("a").alias("b"),
                col("b").alias("a"),
                col("k").alias("k"),
            ])?
            .filter(col("b").gt_eq(lit(1000i64)))?
            .build()?;

        let events = make_table_scan(
            "events",
            vec![
                Field::new("t", DataType::Int64, false),
                Field::new("k", DataType::Int64, false),
            ],
        )?;
        let filtered_events = LogicalPlanBuilder::from(events)
            .filter(col("t").gt_eq(lit(50i64)).and(col("t").lt_eq(lit(150i64))))?
            .build()?;

        let join_filter = col("events.t")
            .gt_eq(col("alias.b"))
            .and(col("events.t").lt_eq(col("alias.a")));

        let plan =
            LogicalPlanBuilder::from(LogicalPlanBuilder::from(aliasing).alias("alias")?.build()?)
                .join(
                    filtered_events,
                    JoinType::Inner,
                    (vec!["k"], vec!["k"]),
                    Some(join_filter),
                )?
                .build()?;

        let out = IntervalDomainPropagationRule::new().rewrite(plan, &OptimizerContext::new())?;
        let plan_str = format!("{:?}", out.data);

        // The aliasing branch must not receive a pre-filter derived from the
        // child's `b` column, which the output schema names `a`.
        assert!(
            !plan_str.contains("a: Int64 >= 50") && !plan_str.contains("a: Int64 <= 150"),
            "must not attribute child-column bounds across an aliasing projection: {plan_str}"
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_skips_joins_with_aggregate_side() -> Result<()> {
        // The derived filter must not be pushed into a branch that an
        // aggregate owns, since the optimizer fixpoint may rewrite the branch
        // into a post-aggregation shape.
        let intervals = make_table_scan(
            "intervals",
            vec![
                Field::new("lo", DataType::Int64, false),
                Field::new("hi", DataType::Int64, false),
                Field::new("k", DataType::Int64, false),
            ],
        )?;
        let aggregated = LogicalPlanBuilder::from(intervals)
            .aggregate(
                vec![col("k")],
                vec![Expr::AggregateFunction(
                    datafusion::logical_expr::expr::AggregateFunction::new_udf(
                        datafusion::functions_aggregate::sum::sum_udaf(),
                        vec![col("lo")],
                        false,
                        None,
                        vec![],
                        None,
                    ),
                )],
            )?
            .build()?;

        let events = make_table_scan(
            "events",
            vec![
                Field::new("t", DataType::Int64, false),
                Field::new("k", DataType::Int64, false),
            ],
        )?;
        let filtered_events = LogicalPlanBuilder::from(events)
            .filter(col("t").gt_eq(lit(10i64)).and(col("t").lt_eq(lit(20i64))))?
            .build()?;

        // DataFusion names the aggregate output `sum(intervals.lo)`.
        let join_filter = col("events.t")
            .gt_eq(col("sum(intervals.lo)"))
            .and(col("events.t").lt_eq(col("events.t")));

        let plan = LogicalPlanBuilder::from(aggregated)
            .join(
                filtered_events,
                JoinType::Inner,
                (vec!["k"], vec!["k"]),
                Some(join_filter),
            )?
            .build()?;

        let out = IntervalDomainPropagationRule::new().rewrite(plan, &OptimizerContext::new())?;
        assert!(
            !out.transformed,
            "joins with an Aggregate on either side must be left alone"
        );
        Ok(())
    }

    #[test]
    fn test_is_valid_constant_rejects_non_plan_time_expressions() {
        // Volatile / context-dependent / aggregate expressions must not be
        // baked into a pushed-down filter.
        assert!(!is_valid_constant(&col("t")));
        assert!(!is_valid_constant(&Expr::Placeholder(
            datafusion::logical_expr::expr::Placeholder::new_with_field("p".to_string(), None)
        )));
        assert!(!is_valid_constant(&lit(ScalarValue::Int64(None))));
        assert!(is_valid_constant(&lit(42i64)));

        // now() / random() carry no column args, so they passed the old
        // "contains no column references" test *and* a naive "all args are
        // constant" test -- an empty argument list is vacuously constant.
        let now_fn = datafusion::functions::expr_fn::now();
        let scalar_fn = match now_fn {
            Expr::ScalarFunction(f) => f,
            _ => panic!("expected a scalar function"),
        };
        assert!(
            !is_valid_constant(&Expr::ScalarFunction(scalar_fn)),
            "now() must not be treated as a plan-time constant"
        );

        // An aggregate with no column arguments is likewise column-free.
        let agg =
            Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
                datafusion::functions_aggregate::count::count_udaf(),
                vec![],
                false,
                None,
                vec![],
                None,
            ));
        assert!(!is_valid_constant(&agg), "COUNT(*) must be rejected");

        // The timezone guard must apply to literals nested inside a CAST, not
        // just to a literal at the root.
        let tz_literal = lit(ScalarValue::TimestampMicrosecond(
            Some(0),
            Some("America/New_York".into()),
        ));
        assert!(!is_valid_constant(&tz_literal));
        let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(lit(1i64)),
            DataType::Int64,
        ));
        assert!(
            is_valid_constant(&cast),
            "CAST of a safe literal is still safe"
        );
    }

    #[test]
    fn test_column_matches_requires_exact_qualifier_agreement() {
        let q = |r: &str, n: &str| Column::new(Some(r), n);
        assert!(column_matches(&q("t1", "x"), &q("t1", "X")));
        assert!(!column_matches(&q("t1", "x"), &q("t2", "x")));
        // The bug: an unqualified column matched any qualified one.
        assert!(!column_matches(
            &q("t1", "x"),
            &Column::new(None::<datafusion::common::TableReference>, "x")
        ));
        assert!(!column_matches(
            &Column::new(None::<datafusion::common::TableReference>, "x"),
            &q("t2", "x")
        ));
        let u = Column::new(None::<datafusion::common::TableReference>, "x");
        assert!(column_matches(&u, &u));
    }
}
