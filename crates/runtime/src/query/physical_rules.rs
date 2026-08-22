#![allow(deprecated)]
//! Custom physical query optimization rules for Strake.
//!
//! Provides physical-level rewrites to optimize join layouts, aggregation phases,
//! and merge operators on local and remote execution trees.

use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{Distribution, ExecutionPlan, ExecutionPlanProperties};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use std::sync::Arc;

/// Hardened Broadcast Join optimizer rule that converts small joins into CollectLeft broadcast mode.
#[derive(Debug)]
pub struct StrakeBroadcastJoinRule {
    max_rows: usize,
    max_bytes: usize,
}

impl StrakeBroadcastJoinRule {
    /// Create a new broadcast join rule with the given threshold limits.
    pub fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows,
            max_bytes,
        }
    }

    /// Recursively strips repartitioning from a physical plan branch.
    /// Traversal halts if an operator is reached that strictly requires partitioned inputs.
    fn strip_unnecessary_repartitions(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        let reqs = plan.required_input_distribution();

        let new_children = children
            .into_iter()
            .enumerate()
            .map(|(i, child)| {
                let req = &reqs[i];
                if matches!(req, Distribution::UnspecifiedDistribution) {
                    // Safe to recursively strip Repartition and CoalesceBatches on this branch!
                    self.strip_branch(Arc::clone(child))
                } else {
                    // Unsafe to strip! Keep the child subtree distribution intact.
                    Ok(Arc::clone(child))
                }
            })
            .collect::<DFResult<Vec<_>>>()?;

        plan.with_new_children(new_children)
    }

    fn strip_branch(&self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        if let Some(repart) = plan.downcast_ref::<RepartitionExec>() {
            return self.strip_branch(repart.children()[0].clone());
        }
        if let Some(coalesce) = plan.downcast_ref::<CoalesceBatchesExec>() {
            return self.strip_branch(coalesce.children()[0].clone());
        }

        let children = plan.children();
        let new_children = children
            .into_iter()
            .map(|c| self.strip_branch(c.clone()))
            .collect::<DFResult<Vec<_>>>()?;
        plan.with_new_children(new_children)
    }

    /// Estimates build side size, falling back safely to local metadata scan size-on-disk.
    fn is_small_enough(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        let stats = plan.partition_statistics(None).ok();

        if let Some(ref s) = stats {
            if s.num_rows
                .get_value()
                .is_some_and(|rows| *rows > self.max_rows)
            {
                return false;
            }

            if let Some(bytes) = s.total_byte_size.get_value() {
                return *bytes <= self.max_bytes;
            }
        }

        // Fallback: Check local DataSourceExec configurations for file sizes
        if let Some(estimated_size) = self.estimate_bytes_from_files(plan) {
            return estimated_size <= self.max_bytes;
        }

        false
    }

    fn estimate_bytes_from_files(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
        if let Some((base_config, _)) = plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|ds| ds.downcast_to_file_source::<ParquetSource>())
        {
            let mut total_size = 0;
            for file_group in &base_config.file_groups {
                for file in file_group.files() {
                    total_size += file.object_meta.size as usize;
                }
            }
            return Some(total_size);
        }

        let children = plan.children();
        if children.is_empty() {
            return None;
        }

        let mut total = 0;
        let mut found_any = false;
        for child in children {
            if let Some(size) = self.estimate_bytes_from_files(child) {
                total += size;
                found_any = true;
            }
        }
        if found_any { Some(total) } else { None }
    }
}

impl PhysicalOptimizerRule for StrakeBroadcastJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        let optimized_children = children
            .into_iter()
            .map(|child| self.optimize(child.clone(), _config))
            .collect::<DFResult<Vec<_>>>()?;
        let plan = plan.with_new_children(optimized_children)?;

        if let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() {
            let left = hash_join.left();
            let right = hash_join.right();

            if self.is_small_enough(left) {
                let left_input = if left.output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(Arc::clone(left)))
                        as Arc<dyn ExecutionPlan>
                } else {
                    Arc::clone(left)
                };
                let new_join = HashJoinExec::try_new(
                    left_input,
                    self.strip_unnecessary_repartitions(Arc::clone(right))?,
                    hash_join.on().to_vec(),
                    hash_join.filter().cloned(),
                    hash_join.join_type(),
                    hash_join.projection.as_ref().map(|p| p.to_vec()),
                    PartitionMode::CollectLeft,
                    hash_join.null_equality(),
                    hash_join.null_aware,
                )?;
                return Ok(Arc::new(new_join));
            } else if self.is_small_enough(right)
                && matches!(hash_join.join_type(), JoinType::Inner)
                && hash_join.filter().is_none()
            {
                let left_schema = left.schema();
                let right_schema = right.schema();
                let left_fields = left_schema.fields();
                let right_fields = right_schema.fields();
                let l_len = left_fields.len();
                let r_len = right_fields.len();

                let original_indices: Vec<usize> = match &hash_join.projection {
                    Some(indices) => indices.to_vec(),
                    None => (0..(l_len + r_len)).collect(),
                };

                let hash_join_schema = hash_join.schema();
                let mut projection_exprs = Vec::with_capacity(original_indices.len());
                for (proj_idx, &idx) in original_indices.iter().enumerate() {
                    let field = hash_join_schema.field(proj_idx);
                    let field_name = field.name();
                    let swapped_idx = if idx < l_len {
                        r_len + idx
                    } else {
                        idx - l_len
                    };
                    let physical_col =
                        Arc::new(Column::new(field_name, swapped_idx)) as Arc<dyn PhysicalExpr>;
                    projection_exprs.push((physical_col, field_name.clone()));
                }

                let new_left_input = if right.output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(Arc::clone(right)))
                        as Arc<dyn ExecutionPlan>
                } else {
                    Arc::clone(right)
                };

                let new_right_input = self.strip_unnecessary_repartitions(Arc::clone(left))?;

                let swapped_on: Vec<_> = hash_join
                    .on()
                    .iter()
                    .map(|(l, r)| (r.clone(), l.clone()))
                    .collect();

                let new_join = HashJoinExec::try_new(
                    new_left_input,
                    new_right_input,
                    swapped_on,
                    None,
                    hash_join.join_type(),
                    None,
                    PartitionMode::CollectLeft,
                    hash_join.null_equality(),
                    hash_join.null_aware,
                )?;

                let projection = ProjectionExec::try_new(projection_exprs, Arc::new(new_join))?;
                return Ok(Arc::new(projection));
            }
        }

        Ok(plan)
    }

    fn name(&self) -> &str {
        "strake_broadcast_join_rule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Rule to force single-phase aggregation when query inputs have exactly 1 partition.
#[derive(Debug, Default)]
pub struct SingleNodeAggregationRule;

impl PhysicalOptimizerRule for SingleNodeAggregationRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Optimize children first (from leaf up to root)
        let children = plan.children();
        let optimized_children = children
            .into_iter()
            .map(|child| self.optimize(child.clone(), config))
            .collect::<DFResult<Vec<_>>>()?;
        let plan = plan.with_new_children(optimized_children)?;

        // Look for the Final phase of a two-phase aggregate
        if let Some(final_agg) = plan.downcast_ref::<AggregateExec>().filter(|a| {
            *a.mode() == AggregateMode::Final || *a.mode() == AggregateMode::FinalPartitioned
        }) {
            let mut child = Arc::clone(final_agg.input());

            // Traverse down the intermediate partition/merge operators
            while let Some(merge) = child.downcast_ref::<SortPreservingMergeExec>() {
                child = merge.children()[0].clone();
            }
            while let Some(coalesce) = child.downcast_ref::<CoalescePartitionsExec>() {
                child = coalesce.children()[0].clone();
            }
            while let Some(repart) = child.downcast_ref::<RepartitionExec>() {
                child = repart.children()[0].clone();
            }

            // If the underlying source is a Partial aggregate, collapse it
            if let Some(partial_agg) = child
                .downcast_ref::<AggregateExec>()
                .filter(|a| *a.mode() == AggregateMode::Partial)
            {
                let input = partial_agg.input();
                let input_partitions = input.properties().partitioning.partition_count();
                let is_single_partition = input_partitions == 1;
                let target_partitions = config.execution.target_partitions;
                let is_forced_single = target_partitions == 1;

                if is_single_partition || is_forced_single {
                    // Collapse to Single Aggregate directly over Partial's input
                    let single_agg = AggregateExec::try_new(
                        AggregateMode::Single,
                        partial_agg.group_expr().clone(),
                        partial_agg.aggr_expr().to_vec(),
                        partial_agg.filter_expr().to_vec(),
                        Arc::clone(input),
                        partial_agg.input_schema(),
                    )?;
                    return Ok(Arc::new(single_agg));
                }
            }
        }

        Ok(plan)
    }

    fn name(&self) -> &str {
        "single_node_aggregation_rule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Rule to recursively strip away redundant partition management operators when running in single partition mode.
#[derive(Debug, Default)]
pub struct StrakeSinglePartitionOptimizer;

impl PhysicalOptimizerRule for StrakeSinglePartitionOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        let optimized_children = children
            .into_iter()
            .map(|child| self.optimize(child.clone(), config))
            .collect::<DFResult<Vec<_>>>()?;
        let plan = plan.with_new_children(optimized_children)?;

        let target_partitions = config.execution.target_partitions;

        if target_partitions == 1 {
            // Strip SortPreservingMergeExec
            if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
                return Ok(merge.children()[0].clone());
            }
            // Strip CoalescePartitionsExec
            if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
                return Ok(coalesce.children()[0].clone());
            }
            // Strip RepartitionExec
            if let Some(repart) = plan.downcast_ref::<RepartitionExec>() {
                return Ok(repart.children()[0].clone());
            }
        } else {
            // Strip if child only has 1 partition anyway
            if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
                let child = &merge.children()[0];
                if child.properties().partitioning.partition_count() == 1 {
                    return Ok(Arc::clone(child));
                }
            }
            if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
                let child = &coalesce.children()[0];
                if child.properties().partitioning.partition_count() == 1 {
                    return Ok(Arc::clone(child));
                }
            }
        }

        Ok(plan)
    }

    fn name(&self) -> &str {
        "strake_single_partition_optimizer"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Physical optimizer rule that pushes physical `FilterExec` predicates down to Parquet scans.
#[derive(Debug, Default)]
pub struct PushDownFilter;

impl PushDownFilter {
    /// Create a new `PushDownFilter` rule.
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for PushDownFilter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Optimize children first
        let children = plan.children();
        let optimized_children = children
            .into_iter()
            .map(|child| self.optimize(child.clone(), _config))
            .collect::<DFResult<Vec<_>>>()?;
        let plan = plan.with_new_children(optimized_children)?;

        // Check if current plan is FilterExec
        if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            let predicate = filter.predicate();
            let input = filter.input();

            // Try to recursively push down the predicate to any underlying DataSourceExec
            if let Some(new_input) = push_down_filter_to_scan(input.clone(), predicate) {
                // The filter is successfully pushed down completely!
                // We can replace the FilterExec with the new optimized input.
                return Ok(new_input);
            }
        }

        Ok(plan)
    }

    fn name(&self) -> &str {
        "push_down_filter"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Recursively traverses the physical plan tree, pushing down the predicate to Parquet scan nodes.
fn push_down_filter_to_scan(
    plan: Arc<dyn ExecutionPlan>,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Option<Arc<dyn ExecutionPlan>> {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;

    if let Some(ds_exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some((base_config, parquet_source)) =
            ds_exec.downcast_to_file_source::<ParquetSource>()
    {
        // Combine existing filter with new predicate
        let combined_predicate = if let Some(existing) = parquet_source.filter() {
            Arc::new(BinaryExpr::new(
                Arc::clone(&existing),
                Operator::And,
                Arc::clone(predicate),
            )) as Arc<dyn PhysicalExpr>
        } else {
            Arc::clone(predicate)
        };

        // Rebuild ParquetSource
        let new_source = parquet_source.clone().with_predicate(combined_predicate);

        // Estimate new statistics
        let original_stats = ds_exec
            .partition_statistics(None)
            .map(|s| s.as_ref().clone())
            .unwrap_or_default();
        let selectivity = 0.1; // Simple selectivity heuristic for pushed filters
        let mut new_stats = original_stats.clone();

        if let Precision::Exact(rows) | Precision::Inexact(rows) = original_stats.num_rows {
            let new_rows = (rows as f64 * selectivity).round() as usize;
            new_stats.num_rows = Precision::Inexact(new_rows.max(1));
        }
        if let Precision::Exact(bytes) | Precision::Inexact(bytes) = original_stats.total_byte_size
        {
            let new_bytes = (bytes as f64 * selectivity).round() as usize;
            new_stats.total_byte_size = Precision::Inexact(new_bytes.max(1));
        }

        // Rebuild FileScanConfig
        let new_config = FileScanConfigBuilder::from(base_config.clone())
            .with_source(Arc::new(new_source))
            .with_statistics(new_stats)
            .build();

        // Rebuild DataSourceExec
        return Some(Arc::new(DataSourceExec::new(Arc::new(new_config))));
    }

    // Try pushing down to children recursively
    let children = plan.children();
    if children.is_empty() {
        return None;
    }

    let mut new_children = Vec::new();
    let mut changed = false;
    for child in children {
        if let Some(new_child) = push_down_filter_to_scan(child.clone(), predicate) {
            new_children.push(new_child);
            changed = true;
        } else {
            new_children.push(child.clone());
        }
    }

    if changed {
        plan.with_new_children(new_children).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn create_mem_table(num_partitions: usize) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let mut partitions = vec![];
        for _ in 0..num_partitions {
            partitions.push(vec![batch.clone()]);
        }

        Arc::new(MemTable::try_new(schema, partitions).unwrap())
    }

    #[tokio::test]
    async fn test_single_node_aggregation_rule() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_table("t1", create_mem_table(1)).unwrap();

        let df = ctx.sql("SELECT id, SUM(val) FROM t1 GROUP BY id").await?;
        let plan = df.create_physical_plan().await?;

        // Verify that the optimizer rule collapses the aggregation
        let rule = SingleNodeAggregationRule;
        let optimized = rule.optimize(plan, ctx.state().config_options())?;

        // The optimized plan should contain exactly AggregateMode::Single
        let plan_str = format!("{:?}", optimized);
        assert!(
            plan_str.contains("Single"),
            "Expected single-phase aggregation: {}",
            plan_str
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_single_partition_optimizer() -> DFResult<()> {
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 1;

        let ctx = SessionContext::new_with_config(config.clone().into());
        ctx.register_table("t1", create_mem_table(1)).unwrap();

        let df = ctx.sql("SELECT * FROM t1 ORDER BY id").await?;
        let plan = df.create_physical_plan().await?;

        let rule = StrakeSinglePartitionOptimizer;
        let optimized = rule.optimize(plan, &config)?;

        // Repartitions/SortPreservingMerges should be stripped out
        let plan_str = format!("{:?}", optimized);
        assert!(
            !plan_str.contains("SortPreservingMergeExec"),
            "Expected SortPreservingMergeExec to be stripped: {}",
            plan_str
        );

        Ok(())
    }
}
