#[macro_use]
pub mod macros;

pub use datafusion::arrow::datatypes::{DataType, Field, Schema};
pub use datafusion::common::Result;
pub use datafusion::prelude::*;
pub use std::sync::Arc;

pub async fn setup_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = datafusion::arrow::record_batch::RecordBatch::new_empty(schema.clone());
    ctx.register_batch("users", batch)?;
    Ok(ctx)
}
