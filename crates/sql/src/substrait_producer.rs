//! Substrait Plan Producer.
//!
//! Responsible for converting internal DataFusion `LogicalPlan`s into the standard
//! Substrait binary format. This is primarily used for the DuckDB connector, which
//! consumes Substrait plans directly instead of SQL text.
use anyhow::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer;
use prost::Message;

/// Converts a DataFusion LogicalPlan to Substrait Plan bytes.
/// This uses the `datafusion-substrait` crate functionality integrated into `datafusion`.
pub async fn to_substrait_bytes(plan: &LogicalPlan, ctx: &SessionContext) -> Result<Vec<u8>> {
    // Generate Substrait Plan from LogicalPlan
    let substrait_plan = producer::to_substrait_plan(plan, &ctx.state())?;

    // Encode to bytes (Protobuf)
    // prost::Message trait provides encode_to_vec
    Ok(substrait_plan.encode_to_vec())
}
