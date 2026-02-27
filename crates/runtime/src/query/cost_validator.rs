//! Budget-based query validation.
//!
//! Provides a `CostBasedValidator` rule that rejects execution plans
//! estimated to process too many rows or bytes. This acts as a safety
//! net for expensive queries in multi-tenant environments.
//!
//! # Usage
//!
//! ```rust
//! // use strake_runtime::query::cost_validator::CostBasedValidator;
//! // let validator = CostBasedValidator::new(Some(1_000_000), Some(100_000_000));
//! ```

use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};
use std::sync::Arc;

/// A validator that rejects queries estimated to process too many rows or bytes.
#[derive(Debug)]
pub struct CostBasedValidator {
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
}

impl CostBasedValidator {
    pub fn new(max_rows: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            max_rows,
            max_bytes,
        }
    }
}

impl PhysicalOptimizerRule for CostBasedValidator {
    fn name(&self) -> &str {
        "cost_based_validator"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut cost_visitor = CostVisitor::default();

        // Visit the plan to calculate total cost
        datafusion::physical_plan::accept(plan.as_ref(), &mut cost_visitor)?;

        // Validate against thresholds
        if let Some(max_rows) = self.max_rows {
            if cost_visitor.max_rows > max_rows {
                let context = strake_error::ErrorContext::BudgetExceeded {
                    estimated_rows: cost_visitor.max_rows,
                    limit: max_rows,
                    suggestion:
                        "Add a LIMIT clause or increase the 'max_output_rows' in query limits"
                            .to_string(),
                };

                return Err(DataFusionError::External(Box::new(
                    strake_error::StrakeError::new(
                        strake_error::ErrorCode::BudgetExceeded,
                        format!(
                            "Query estimated {} rows exceeds limit of {}",
                            cost_visitor.max_rows, max_rows
                        ),
                    )
                    .with_context(context),
                )));
            }
        }

        if let Some(max_bytes) = self.max_bytes {
            if cost_visitor.max_bytes > max_bytes {
                // Note: BudgetExceeded context for bytes could be added to ErrorContext in future
                // For now using the same error code but generic message
                return Err(DataFusionError::External(Box::new(
                    strake_error::StrakeError::new(
                        strake_error::ErrorCode::BudgetExceeded,
                        format!(
                            "Query estimated {} bytes exceeds limit of {}",
                            cost_visitor.max_bytes, max_bytes
                        ),
                    )
                    .with_hint("Refine your query to select fewer columns or use filters"),
                )));
            }
        }

        Ok(plan)
    }
}

/// Helper visitor to calculate max statistics from the plan.
///
/// Instead of summing row counts (which would double-count data in pipelines),
/// we track the maximum observed row count across all nodes. This provides
/// a more accurate representation of the "largest" stage of processing.
#[derive(Default)]
struct CostVisitor {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl ExecutionPlanVisitor for CostVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool> {
        // Access statistics from the physical plan node
        #[allow(deprecated)]
        if let Ok(stats) = plan.statistics() {
            // Treat as max encountered in the pipeline rather than sum to avoid double counting
            if let Some(rows) = stats.num_rows.get_value() {
                self.max_rows = self.max_rows.max(*rows);
            }
            if let Some(bytes) = stats.total_byte_size.get_value() {
                self.max_bytes = self.max_bytes.max(*bytes);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use datafusion::common::stats::Precision;
    use datafusion::physical_plan::Statistics;
    use datafusion::physical_plan::{DisplayAs, PlanProperties};
    use std::any::Any;

    // Fix imports based on likely locations in DataFusion v51
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::Partitioning;

    #[derive(Debug)]
    struct MockExec {
        schema: Arc<Schema>,
        stats: Statistics,
        cache: PlanProperties,
    }

    impl MockExec {
        fn new(schema: Arc<Schema>, stats: Statistics) -> Self {
            let cache = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                schema,
                stats,
                cache,
            }
        }
    }

    impl DisplayAs for MockExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "MockExec")
        }
    }

    impl ExecutionPlan for MockExec {
        fn name(&self) -> &str {
            "MockExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
        fn properties(&self) -> &PlanProperties {
            &self.cache
        }
        // children returns references in recent DF
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        // with_new_children takes Arc<Self>
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(MockExec::new(
                self.schema.clone(),
                self.stats.clone(),
            )))
        }
        fn execute(
            &self,
            _: usize,
            _: Arc<datafusion::execution::TaskContext>,
        ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
            unimplemented!()
        }
        fn statistics(&self) -> Result<Statistics> {
            Ok(self.stats.clone())
        }
    }

    #[test]
    fn test_validator_rejects_rows() {
        let schema = Arc::new(Schema::empty());
        let stats = Statistics {
            num_rows: Precision::Exact(1_000_000),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        let plan = Arc::new(MockExec::new(schema, stats));

        let validator = CostBasedValidator::new(Some(500_000), None);
        let config = ConfigOptions::default();

        let result = validator.optimize(plan, &config);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeds limit of 500000"));
    }

    #[test]
    fn test_validator_passes_under_limit() {
        let schema = Arc::new(Schema::empty());
        let stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        let plan = Arc::new(MockExec::new(schema, stats));

        let validator = CostBasedValidator::new(Some(500_000), None);
        let config = ConfigOptions::default();

        let result = validator.optimize(plan, &config);

        assert!(result.is_ok());
    }
}
