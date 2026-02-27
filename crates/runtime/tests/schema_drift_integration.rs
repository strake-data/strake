use anyhow::Result;
use arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use strake_common::circuit_breaker::{
    AdaptiveCircuitBreaker, CircuitBreakerConfig, CircuitBreakerTableProvider,
};
use strake_common::config::SourceConfig;
use strake_common::config::{Config, ResourceConfig};
use strake_connectors::sources::schema_drift::SchemaDriftTableProvider;
use strake_connectors::sources::SourceProvider;
use strake_runtime::federation::{FederationEngine, FederationEngineOptions};

/// Creates a mock DataFusion TableProvider pretending to be a remote source.
/// It registers an expected schema, but at scan time it actually returns a
/// drifted batch (missing a column, type changed, and extra column).
struct MockDriftedSource {
    expected: Arc<Schema>,
    actual_batch: RecordBatch,
}

impl MockDriftedSource {
    fn new() -> Self {
        let expected = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true), // will be missing
            Field::new("score", DataType::Int64, true), // will be string "100"
        ]));

        let actual_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Utf8, true), // type changed
            Field::new("extra", DataType::Utf8, true), // extra column
        ]));

        let actual_batch = RecordBatch::try_new(
            actual_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(StringArray::from(vec!["100", "200"])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        Self {
            expected,
            actual_batch,
        }
    }
}

pub struct MockDriftedProvider;

#[async_trait]
impl SourceProvider for MockDriftedProvider {
    fn type_name(&self) -> &'static str {
        "mock_drift"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        let mocked = MockDriftedSource::new();
        let expected_schema = mocked.expected.clone();

        let mem_table = MemTable::try_new(
            mocked.actual_batch.schema(),
            vec![vec![mocked.actual_batch]],
        )?;

        // Wrap with our circuit breaker and schema drift, simulating wrappers.rs
        let cb = Arc::new(AdaptiveCircuitBreaker::new(CircuitBreakerConfig::default()));

        // For testing, we create a dummy TableProvider wrapper that pretends its expected schema
        // is the original, but delegates to mem_table (which yields actual drifted).

        #[derive(Debug)]
        struct FakeAtRestProvider {
            inner: Arc<MemTable>,
            schema: Arc<Schema>,
        }

        #[async_trait]
        impl TableProvider for FakeAtRestProvider {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn schema(&self) -> Arc<Schema> {
                self.schema.clone()
            }
            fn table_type(&self) -> datafusion::datasource::TableType {
                datafusion::datasource::TableType::Base
            }
            async fn scan(
                &self,
                state: &dyn datafusion::catalog::Session,
                _projection: Option<&Vec<usize>>,
                filters: &[datafusion::logical_expr::Expr],
                limit: Option<usize>,
            ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>>
            {
                // Here we ignore projection for the mock and just return the full drifted batch plan
                self.inner.scan(state, None, filters, limit).await
            }
            fn supports_filters_pushdown(
                &self,
                filters: &[&datafusion::logical_expr::Expr],
            ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
            {
                Ok(vec![
                        datafusion::logical_expr::TableProviderFilterPushDown::Inexact;
                        filters.len()
                    ])
            }
        }

        let at_rest = Arc::new(FakeAtRestProvider {
            inner: Arc::new(mem_table),
            schema: expected_schema,
        });

        let with_cb = Arc::new(CircuitBreakerTableProvider::new(at_rest, cb));
        let with_drift = Arc::new(SchemaDriftTableProvider::new(with_cb));

        context.register_table(
            datafusion::sql::TableReference::full(catalog_name, "public", config.name.clone()),
            with_drift,
        )?;

        Ok(())
    }
}

#[tokio::test]
async fn test_schema_drift_federation() -> Result<()> {
    let config = Config {
        sources: vec![SourceConfig {
            name: "drifted_src".to_string(),
            source_type: "mock_drift".to_string(),
            url: None,
            default_limit: None,
            cache: None,
            config: serde_json::Value::Null,
            username: None,
            password: None,
            max_concurrent_queries: None,
            tables: vec![],
        }],
        cache: Default::default(),
    };

    let engine = FederationEngine::new(FederationEngineOptions {
        config,
        catalog_name: "strake".to_string(),
        query_limits: strake_common::config::QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![Box::new(MockDriftedProvider)],
        retry: Default::default(),
    })
    .await?;

    let sql = "SELECT * FROM drifted_src";
    let (schema, batches, warnings) = engine.execute_query(sql, None).await?;

    // Schema should match the EXPECTED schema
    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(2).name(), "age");
    assert_eq!(schema.field(3).name(), "score");
    assert_eq!(schema.field(3).data_type(), &DataType::Int64);

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    // Age should be NULLs
    assert_eq!(batch.column(2).null_count(), 2);
    // Score should be coerced to Int64: 100, 200
    let scores = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 100);
    assert_eq!(scores.value(1), 200);

    // Warnings should contain the 3 drift warnings
    let warnings_str = warnings.join("\n");
    assert!(warnings_str.contains("STRAKE-2009"));
    assert!(warnings_str.contains("STRAKE-2010"));
    assert!(warnings_str.contains("STRAKE-2011"));

    Ok(())
}
