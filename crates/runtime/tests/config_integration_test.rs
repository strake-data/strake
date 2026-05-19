use std::collections::HashMap;
use strake_common::config::*;
use strake_runtime::federation::FederationEngine;

#[tokio::test]
async fn test_optimizer_config_toggles() -> anyhow::Result<()> {
    // 1. Test with all optimizer flags = false (default)
    let config_off = Config::default();
    let mut resource_config_off = ResourceConfig::default();
    resource_config_off.enable_broadcast_join = false;
    resource_config_off.enable_push_down_filter = false;
    resource_config_off.enable_single_node_aggregation = false;
    resource_config_off.enable_single_partition_optimizer = false;
    resource_config_off.enable_correlated_distinct_pushdown = false;

    let engine_off = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config: config_off,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: resource_config_off,
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let physical_optimizers_off = engine_off.context().state().physical_optimizers().to_vec();
    let logical_optimizers_off = engine_off.context().state().optimizers().to_vec();

    assert!(
        !physical_optimizers_off
            .iter()
            .any(|r| r.name() == "strake_broadcast_join_rule")
    );
    assert!(
        !physical_optimizers_off
            .iter()
            .any(|r| r.name() == "push_down_filter")
    );
    assert!(
        !physical_optimizers_off
            .iter()
            .any(|r| r.name() == "single_node_aggregation_rule")
    );
    assert!(
        !physical_optimizers_off
            .iter()
            .any(|r| r.name() == "strake_single_partition_optimizer")
    );
    assert!(
        !logical_optimizers_off
            .iter()
            .any(|r| r.name() == "correlated_distinct_pushdown_rule")
    );

    // 2. Test with all optimizer flags = true
    let config_on = Config::default();
    let mut resource_config_on = ResourceConfig::default();
    resource_config_on.enable_broadcast_join = true;
    resource_config_on.enable_push_down_filter = true;
    resource_config_on.enable_single_node_aggregation = true;
    resource_config_on.enable_single_partition_optimizer = true;
    resource_config_on.enable_correlated_distinct_pushdown = true;

    let engine_on = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config: config_on,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: resource_config_on,
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let physical_optimizers_on = engine_on.context().state().physical_optimizers().to_vec();
    let logical_optimizers_on = engine_on.context().state().optimizers().to_vec();

    assert!(
        physical_optimizers_on
            .iter()
            .any(|r| r.name() == "strake_broadcast_join_rule")
    );
    assert!(
        physical_optimizers_on
            .iter()
            .any(|r| r.name() == "push_down_filter")
    );
    assert!(
        physical_optimizers_on
            .iter()
            .any(|r| r.name() == "single_node_aggregation_rule")
    );
    assert!(
        physical_optimizers_on
            .iter()
            .any(|r| r.name() == "strake_single_partition_optimizer")
    );
    assert!(
        logical_optimizers_on
            .iter()
            .any(|r| r.name() == "correlated_distinct_pushdown_rule")
    );

    Ok(())
}
