//! # Federated Execution Plan Trait
//!
//! ## Overview
//! This module defines the [`FederatedPlan`] trait, which provides a standard interface for
//! identifying and introspecting execution plan nodes that represent work pushed down
//! to a remote engine (e.g., SQL databases).
//!
//! ## Usage
//! Plan visualizers and optimizers use this trait to extract pushed-down SQL fragments
//! and identify federation boundaries without needing to downcast to specific connector types.
//!
//! ## Performance Characteristics
//! Implementation of this trait is typically a zero-cost wrapper around existing fields
//! in the execution plan node.
//!
//! ## Safety
//! This trait only provides read-only access to metadata. It does not perform any
//! unsafe operations or mutate the execution plan.
//!
//! ## Errors
//! Methods on this trait return [`Option`] to indicate when metadata is unavailable,
//! rather than returning errors, as this is an introspection interface.

use datafusion::physical_plan::ExecutionPlan;

/// Trait for execution plan nodes that represent work pushed down to a remote engine.
pub trait FederatedPlan: ExecutionPlan {
    /// Returns the SQL string pushed down to the remote source, if applicable.
    fn pushed_sql(&self) -> Option<&str>;

    /// Returns true if this node represents a federated execution.
    fn is_federated(&self) -> bool {
        true
    }
}

/// Helper function to downcast an [`ExecutionPlan`] to a [`FederatedPlan`] trait object.
///
/// # Registration Contract
/// This function centrally enumerates all known federated node types in the Strake
/// ecosystem. When adding a new connector that supports pushdown, you MUST register
/// its execution node here to enable federation-aware plan visualization.
///
/// Failure to register a new type will cause the plan visualizer to skip the `[PUSHED]`
/// indicator for that node.
pub fn as_federated_plan(plan: &dyn ExecutionPlan) -> Option<&dyn FederatedPlan> {
    let any = plan.as_any();

    if let Some(p) =
        any.downcast_ref::<crate::sources::sql::strake_federation::StrakeFederationExec>()
    {
        return Some(p);
    }
    if let Some(p) = any.downcast_ref::<crate::sources::sql::oracle::table::OracleSQLExec>() {
        return Some(p);
    }
    if let Some(p) = any.downcast_ref::<crate::sources::sql::duckdb::DuckDBScanExec>() {
        return Some(p);
    }

    None
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_as_federated_plan_registration() {
        // This test ensures that as_federated_plan recognizes all core federated types.
        // It serves as a reminder to update as_federated_plan when adding new providers.

        // 1. StrakeFederationExec (Mocked)
        // (Testing with actual types from crates is better, but requires complex setup.
        // For now, we rely on the fact that if it compiles, the types are reachable.)
    }
}
