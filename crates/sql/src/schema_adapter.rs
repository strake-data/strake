use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Result as DFResult};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

/// A logical node that adapts the schema of its input to a target schema.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SchemaAdapter {
    pub input: LogicalPlan,
    pub schema: DFSchemaRef,
}

impl SchemaAdapter {
    pub fn new(input: LogicalPlan, schema: DFSchemaRef) -> Self {
        Self { input, schema }
    }

    /// Converts this adapter to a standard DataFusion Projection plan.
    /// This is useful for the Unparser which doesn't know how to handle Extension nodes.
    pub fn to_projection(&self) -> Result<LogicalPlan> {
        use datafusion::logical_expr::LogicalPlanBuilder;
        let input_schema = self.input.schema();
        let mut exprs = Vec::new();

        for (i, (qualifier, _field)) in input_schema.iter().enumerate() {
            if i >= self.schema.fields().len() {
                break;
            }
            let target_field = self.schema.field(i);
            let input_field = input_schema.field(i);

            let expr = Expr::Column(datafusion::common::Column::new(
                qualifier.cloned(),
                input_field.name().clone(),
            ))
            .alias(target_field.name().clone());
            exprs.push(expr);
        }

        LogicalPlanBuilder::from(self.input.clone())
            .project(exprs)?
            .build()
    }
}

impl UserDefinedLogicalNode for SchemaAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "SchemaAdapter"
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "SchemaAdapter")
    }

    fn check_invariants(&self, _check: InvariantLevel) -> DFResult<()> {
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(SchemaAdapter {
            input: inputs.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal("SchemaAdapter requires exactly one input".to_string())
            })?,
            schema: self.schema.clone(),
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map(|o| self.schema.to_string().cmp(&o.schema.to_string()))
    }
}

/// Physical planner for SchemaAdapter that wraps the input in a projection
/// to rename columns from physical names to logical names.
pub struct SchemaAdapterPlanner;

#[async_trait]
impl ExtensionPlanner for SchemaAdapterPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let adapter = match node.as_any().downcast_ref::<SchemaAdapter>() {
            Some(a) => a,
            None => return Ok(None),
        };

        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "SchemaAdapter expects exactly one input".to_string(),
            ));
        }

        let input = &physical_inputs[0];
        let input_schema = input.schema();
        let target_schema = adapter.schema.as_arrow();

        tracing::debug!(
            "SchemaAdapter: Mapping physical {:?} to logical {:?}",
            input_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
            target_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        // Build projection expressions mapping physical columns to logical names
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::expressions::Column as PhysColumn;
        use datafusion::physical_plan::projection::ProjectionExec;

        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

        for (logical_idx, logical_field) in target_schema.fields().iter().enumerate() {
            let logical_name = logical_field.name();

            // Strategy: Find physical column by position first, then by name matching.
            // Using field indices is preferred for performance in high-throughput adapters.
            let physical_idx = if logical_idx < input_schema.fields().len() {
                // Positional match (most reliable for adapters)
                Some(logical_idx)
            } else {
                // Fallback: search by name (avoiding format! in loop)
                let suffix = format!("_{}", logical_name);
                input_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == logical_name || f.name().ends_with(&suffix))
            };

            let physical_idx =
                physical_idx.ok_or_else(|| {
                    DataFusionError::Internal(format!(
                    "SchemaAdapter: Cannot map logical field '{}' (index {}) to physical schema. \
                     Physical has {} fields: {:?}",
                    logical_name,
                    logical_idx,
                    input_schema.fields().len(),
                    input_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ))
                })?;

            let physical_field = input_schema.field(physical_idx);
            let physical_col = PhysColumn::new(physical_field.name(), physical_idx);

            projection_exprs.push((
                Arc::new(physical_col) as Arc<dyn PhysicalExpr>,
                logical_name.clone(),
            ));
        }

        let projection = ProjectionExec::try_new(projection_exprs, input.clone())?;

        tracing::debug!(
            "SchemaAdapter: Created projection with output schema {:?}",
            projection
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        Ok(Some(Arc::new(projection)))
    }
}
