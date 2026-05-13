use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

fn main() {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let _ = state.catalog_list();
}
