// use super::*; // Removed unused import
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::empty::EmptyTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use strake_common::config::{QueryCacheConfig, SourceConfig};
use strake_common::models::SourceName;

// Helper to create a TableScan LogicalPlan using a builder pattern to avoid non-exhaustive struct issues
fn create_table_scan(table_name: TableReference) -> LogicalPlan {
    let table = Arc::new(EmptyTable::new(Arc::new(arrow::datatypes::Schema::empty())));
    let source = Arc::new(DefaultTableSource::new(table));

    // Use LogicalPlanBuilder to avoid non-exhaustive issues
    datafusion::logical_expr::LogicalPlanBuilder::scan(table_name, source, None)
        .unwrap()
        .build()
        .unwrap()
}

proptest! {
    #[test]
    fn test_should_cache_plan_prop(
        enabled_globally in any::<bool>(),
        source_a_enabled in any::<bool>(),
        source_b_enabled in any::<bool>(),
        plan_type in 0..3u8,
    ) {
        let sn_a = SourceName::from("source_a");
        let sn_b = SourceName::from("source_b");

        let mut source_configs = HashMap::new();

        let mut sc_a = SourceConfig::default();
        sc_a.name = sn_a.clone();
        let mut qcc_a = QueryCacheConfig::default();
        qcc_a.enabled = source_a_enabled;
        sc_a.cache = Some(qcc_a);
        source_configs.insert(sn_a.clone(), sc_a);

        let mut sc_b = SourceConfig::default();
        sc_b.name = sn_b.clone();
        let mut qcc_b = QueryCacheConfig::default();
        qcc_b.enabled = source_b_enabled;
        sc_b.cache = Some(qcc_b);
        source_configs.insert(sn_b.clone(), sc_b);

        let plan = match plan_type {
            0 => create_table_scan(TableReference::Bare { table: "source_a".into() }),
            1 => create_table_scan(TableReference::Bare { table: "source_b".into() }),
            2 => create_table_scan(TableReference::Bare { table: "unknown".into() }),
            _ => unreachable!(),
        };

        // Use the function from super module
        let result = crate::federation::should_cache_plan(&plan, enabled_globally, &source_configs);

        if !enabled_globally {
            assert!(!result, "Should never cache if globally disabled");
        } else {
            match plan_type {
                0 => assert_eq!(result, source_a_enabled, "Should respect source_a override"),
                1 => assert_eq!(result, source_b_enabled, "Should respect source_b override"),
                2 => assert!(result, "Should cache by default for unknown sources if globally enabled"),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_should_cache_plan_nested(
        source_a_enabled in any::<bool>(),
        source_b_enabled in any::<bool>(),
    ) {
        let sn_a = SourceName::from("source_a");
        let sn_b = SourceName::from("source_b");

        let mut source_configs = HashMap::new();

        let mut sc_a = SourceConfig::default();
        sc_a.name = sn_a.clone();
        let mut qcc_a = QueryCacheConfig::default();
        qcc_a.enabled = source_a_enabled;
        sc_a.cache = Some(qcc_a);
        source_configs.insert(sn_a.clone(), sc_a);

        let mut sc_b = SourceConfig::default();
        sc_b.name = sn_b.clone();
        let mut qcc_b = QueryCacheConfig::default();
        qcc_b.enabled = source_b_enabled;
        sc_b.cache = Some(qcc_b);
        source_configs.insert(sn_b.clone(), sc_b);

        // Union of source_a and source_b
        let left = create_table_scan(TableReference::Bare { table: "source_a".into() });
        let right = create_table_scan(TableReference::Bare { table: "source_b".into() });

        let plan = LogicalPlan::Union(datafusion::logical_expr::Union {
            inputs: vec![Arc::new(left), Arc::new(right)],
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });

        let result = crate::federation::should_cache_plan(&plan, true, &source_configs);

        // If EITHER source disables caching, the whole plan should be disabled
        let expected = source_a_enabled && source_b_enabled;
        assert_eq!(result, expected, "Nested plan should respect ALL source overrides");
    }
}
