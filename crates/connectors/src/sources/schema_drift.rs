//! # Schema Drift Detection
//!
//! Transparent handling of source schema changes during execution.
//!
//! This module implements a [`TableProvider`] wrapper and an [`ExecutionPlan`] node
//! that detect and reconcile differences between the expected catalog schema
//! and the actual source schema at runtime.
//!
//! ## Overview
//!
//! [`SchemaDriftTableProvider`] wraps any existing provider and injects a
//! [`SchemaDriftExec`] node into the physical plan. This node inspects incoming
//! [`RecordBatch`]es and performs:
//! - **Missing Column Handling**: Fills columns present in the catalog but missing
//!   from the source with NULLs (STRAKE-2009).
//! - **Type Coercion**: Attempts to cast source columns to expected types,
//!   NULL-filling if casting fails (STRAKE-2010).
//! - **Extra Column Pruning**: Drops columns present in the source but not
//!   in the catalog (STRAKE-2011).
//!
//! ## Performance Characteristics
//!
//! Reconciliation is a columnar operation that involves slicing and potentially
//! casting arrays. It adds minimal overhead on the happy path.
//!
//! ## Safety
//!
//! This module does not use `unsafe`. It relies on Arrow compute kernels for
//! type-safe transformations.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// Warnings emitted when schema drift is detected and reconciled.
#[derive(Debug, Clone)]
pub enum DriftWarning {
    /// Column defined in catalog but missing from source (reconciled as NULLs)
    MissingColumn {
        /// Name of the missing column.
        name: String,
        /// The expected Arrow data type.
        expected_type: DataType,
    },
    /// Source type differs from catalog but was successfully coerced
    TypeCoerced {
        /// Name of the coerced column.
        name: String,
        /// The expected Arrow data type.
        expected_type: DataType,
        /// The actual Arrow data type found in the source.
        actual_type: DataType,
    },
    /// Column cast failed (all values became NULL)
    CastFailed {
        /// Name of the failed column.
        name: String,
        /// The expected Arrow data type.
        expected_type: DataType,
        /// The actual Arrow data type found in the source.
        actual_type: DataType,
    },
    /// Column partially cast (some values became NULL)
    PartialCast {
        /// Name of the partially cast column.
        name: String,
        /// The expected Arrow data type.
        expected_type: DataType,
        /// The actual Arrow data type found in the source.
        actual_type: DataType,
        /// Number of rows that were successfully cast.
        surviving_rows: usize,
    },
    /// Source contains column not defined in catalog (dropped)
    ExtraColumn {
        /// Name of the extra column.
        name: String,
        /// The actual Arrow data type found in the source.
        actual_type: DataType,
    },
}

impl std::fmt::Display for DriftWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingColumn {
                name,
                expected_type,
            } => write!(
                f,
                "[STRAKE-2009] Missing column '{}' (expected {}), NULL-filled",
                name, expected_type
            ),
            Self::TypeCoerced {
                name,
                expected_type,
                actual_type,
            } => write!(
                f,
                "[STRAKE-2010] Column '{}' type coerced from {} to {}",
                name, actual_type, expected_type
            ),
            Self::CastFailed {
                name,
                expected_type,
                actual_type,
            } => write!(
                f,
                "[STRAKE-2010] Column '{}' cast failed from {} to {} (ALL values nullified)",
                name, actual_type, expected_type
            ),
            Self::PartialCast {
                name,
                expected_type,
                actual_type,
                surviving_rows,
            } => write!(
                f,
                "[STRAKE-2012] Column '{}' partially cast from {} to {} ({} rows survived)",
                name, actual_type, expected_type, surviving_rows
            ),
            Self::ExtraColumn { name, actual_type } => write!(
                f,
                "[STRAKE-2011] Extra column '{}' of type {} dropped",
                name, actual_type
            ),
        }
    }
}

/// Reconciles a [`RecordBatch`] against an expected schema.
///
/// This function performs the following steps:
/// 1. Identifies extra columns in the source and prepares warnings.
/// 2. Iterates through the expected schema and attempts to find matching source columns.
/// 3. If found, it either clones the array (identical type) or attempts coercion.
/// 4. If coercion fails or the column is missing, it fills with NULLs.
///
/// # Errors
/// Returns an error if the reconciled [`RecordBatch`] cannot be constructed.
pub fn reconcile_batch(
    expected: &SchemaRef,
    actual: &RecordBatch,
) -> DataFusionResult<(RecordBatch, Vec<DriftWarning>)> {
    let mut warnings = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(expected.fields().len());

    let actual_schema = actual.schema();
    let expected_names: std::collections::HashSet<_> =
        expected.fields().iter().map(|f| f.name()).collect();

    // 1. Check for extra columns in actual (O(actual_fields))
    for field in actual_schema.fields() {
        if !expected_names.contains(field.name()) {
            warnings.push(DriftWarning::ExtraColumn {
                name: field.name().clone(),
                actual_type: field.data_type().clone(),
            });
        }
    }

    // 2. Build expected columns, coercing/null-filling as needed
    for field in expected.fields() {
        match actual_schema.column_with_name(field.name()) {
            Some((idx, actual_field)) => {
                let array = actual.column(idx);
                if actual_field.data_type() == field.data_type() {
                    columns.push(array.clone());
                } else {
                    // Type differs, attempt to cast
                    match cast(array, field.data_type()) {
                        Ok(casted) => {
                            let array_nulls = array.null_count();
                            let casted_nulls = casted.null_count();
                            let len = casted.len();

                            if casted_nulls <= array_nulls {
                                warnings.push(DriftWarning::TypeCoerced {
                                    name: field.name().clone(),
                                    expected_type: field.data_type().clone(),
                                    actual_type: actual_field.data_type().clone(),
                                });
                            } else if casted_nulls == len {
                                warnings.push(DriftWarning::CastFailed {
                                    name: field.name().clone(),
                                    expected_type: field.data_type().clone(),
                                    actual_type: actual_field.data_type().clone(),
                                });
                            } else {
                                warnings.push(DriftWarning::PartialCast {
                                    name: field.name().clone(),
                                    expected_type: field.data_type().clone(),
                                    actual_type: actual_field.data_type().clone(),
                                    surviving_rows: len - casted_nulls,
                                });
                            }
                            columns.push(casted);
                        }
                        Err(_) => {
                            // Cast failed (incompatible types), NULL-fill
                            warnings.push(DriftWarning::CastFailed {
                                name: field.name().clone(),
                                expected_type: field.data_type().clone(),
                                actual_type: actual_field.data_type().clone(),
                            });
                            let null_arr = datafusion::arrow::array::new_null_array(
                                field.data_type(),
                                actual.num_rows(),
                            );
                            columns.push(null_arr);
                        }
                    }
                }
            }
            None => {
                warnings.push(DriftWarning::MissingColumn {
                    name: field.name().clone(),
                    expected_type: field.data_type().clone(),
                });
                let null_arr =
                    datafusion::arrow::array::new_null_array(field.data_type(), actual.num_rows());
                columns.push(null_arr);
            }
        }
    }

    let batch = RecordBatch::try_new(expected.clone(), columns)?;
    Ok((batch, warnings))
}

/// An [`ExecutionPlan`] node that reconciles schema drift at runtime.
#[derive(Debug)]
pub struct SchemaDriftExec {
    inner: Arc<dyn ExecutionPlan>,
    expected_schema: SchemaRef,
    cache: Arc<PlanProperties>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl SchemaDriftExec {
    /// Creates a new `SchemaDriftExec` node wrapping the given plan.
    pub fn new(inner: Arc<dyn ExecutionPlan>, expected_schema: SchemaRef) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(expected_schema.clone()),
            inner.properties().partitioning.clone(),
            inner.properties().emission_type,
            inner.properties().boundedness,
        ));
        Self {
            inner,
            expected_schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for SchemaDriftExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SchemaDriftExec: expected_columns=[")?;
                for (i, field) in self.expected_schema.fields().iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field.name())?;
                }
                write!(f, "]")
            }
            _ => write!(f, "SchemaDriftExec"),
        }
    }
}

impl ExecutionPlan for SchemaDriftExec {
    fn name(&self) -> &str {
        "SchemaDriftExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.expected_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "SchemaDriftExec wrong number of children".to_string(),
            ));
        }
        Ok(Arc::new(SchemaDriftExec::new(
            children[0].clone(),
            self.expected_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context.clone())?;

        // Capture WarningCollector if present in extensions
        let collector = context
            .session_config()
            .options()
            .extensions
            .get::<crate::extensions::warnings::WarningExtension>()
            .map(|ext| ext.collector.clone());

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_bytes = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .output_bytes(partition);

        // Use stream combinator instead of custom Stream impl
        let expected_schema = self.expected_schema.clone();

        let mapped = inner_stream.map(move |batch_res| match batch_res {
            Ok(batch) => {
                let _timer = baseline_metrics.elapsed_compute().timer();
                match reconcile_batch(&expected_schema, &batch) {
                    Ok((reconciled, warnings)) => {
                        for warning in warnings {
                            let msg = warning.to_string();
                            if let Some(c) = collector.as_ref() {
                                c.add(msg);
                            } else {
                                // Fallback to task-local if collector extension not found (likely in tests)
                                strake_common::warnings::add_warning(msg);
                            }
                        }
                        baseline_metrics.record_output(reconciled.num_rows());
                        output_bytes.add(reconciled.get_array_memory_size());
                        Ok(reconciled)
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => {
                tracing::error!("Inner stream error: {}", e);
                // Propagate error instead of silent data loss
                Err(e)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.expected_schema.clone(),
            mapped,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DataFusionResult<datafusion::physical_plan::Statistics> {
        self.inner.partition_statistics(partition)
    }
}

/// A [`TableProvider`] decorator that enables transparent schema drift detection.
#[derive(Debug)]
pub struct SchemaDriftTableProvider {
    inner: Arc<dyn TableProvider>,
    expected_schema: SchemaRef,
}

impl SchemaDriftTableProvider {
    /// Wraps an existing [`TableProvider`] with schema drift detection.
    pub fn new(inner: Arc<dyn TableProvider>) -> Self {
        let expected_schema = inner.schema();
        Self {
            inner,
            expected_schema,
        }
    }

    /// Returns the underlying [`TableProvider`] wrapped by this schema drift detector.
    pub fn inner(&self) -> Arc<dyn TableProvider> {
        self.inner.clone()
    }
}

#[async_trait]
impl TableProvider for SchemaDriftTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.expected_schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;

        let projected_schema = if let Some(proj) = projection {
            Arc::new(self.expected_schema.project(proj)?)
        } else {
            self.expected_schema.clone()
        };

        Ok(Arc::new(SchemaDriftExec::new(plan, projected_schema)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

impl crate::sources::WrappingTableProvider for SchemaDriftTableProvider {
    fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use std::sync::OnceLock;

    #[test]
    fn test_reconcile_identical() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&schema, &batch).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(reconciled.num_columns(), 1);
        assert_eq!(reconciled.num_rows(), 3);
    }

    #[test]
    fn test_reconcile_missing_column() {
        let expected = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&expected, &batch).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::MissingColumn { .. }));
        assert_eq!(reconciled.num_columns(), 2);
        assert_eq!(reconciled.column(1).null_count(), 3);
    }

    #[test]
    fn test_reconcile_type_changed() {
        let expected = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        // Actual contains strings that CAN be parsed as Int64
        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(StringArray::from(vec!["1", "2", "3"]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&expected, &batch).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::TypeCoerced { .. }));

        let expected_arr = reconciled
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(expected_arr.value(0), 1);
    }

    #[test]
    fn test_reconcile_cast_failed() {
        let expected = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));

        // Actual contains strings that CANNOT be parsed as Int64
        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(StringArray::from(vec!["not_a_number"]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&expected, &batch).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::CastFailed { .. }));
        assert_eq!(reconciled.column(0).null_count(), 1);
    }

    #[test]
    fn test_reconcile_extra_column() {
        let expected = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let actual_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            actual_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&expected, &batch).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::ExtraColumn { .. }));
        assert_eq!(reconciled.num_columns(), 1); // Stripped 'b'
    }

    #[derive(Debug)]
    struct MockExec {
        schema: SchemaRef,
        has_error: bool,
    }

    impl DisplayAs for MockExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            static PROPERTIES: OnceLock<Arc<PlanProperties>> = OnceLock::new();
            PROPERTIES.get_or_init(|| {
                Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(Schema::empty())),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ))
            })
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _: usize,
            _: Arc<datafusion::execution::TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            let schema = self.schema.clone();
            if self.has_error {
                let stream = futures::stream::once(async move {
                    Err(datafusion::error::DataFusionError::Internal(
                        "test error".to_string(),
                    ))
                });
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            } else {
                let stream = futures::stream::empty();
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }

    #[tokio::test]
    async fn test_schema_drift_exec_error_propagation() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

        let inner = Arc::new(MockExec {
            schema: schema.clone(),
            has_error: true,
        });

        let exec = SchemaDriftExec::new(inner, schema.clone());
        let task_context = Arc::new(datafusion::execution::TaskContext::default());

        let mut stream = exec.execute(0, task_context).unwrap();
        let res = stream.next().await;

        assert!(res.is_some());
        assert!(res.unwrap().is_err());
    }

    #[test]
    fn test_reconcile_batch_clean_coercion() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("1"), None]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&schema, &batch).unwrap();
        assert_eq!(reconciled.num_rows(), 2);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::TypeCoerced { .. }));
    }

    #[test]
    fn test_reconcile_batch_partial_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![
                Some("1"),
                Some("not_a_number"),
                Some("3"),
                None,
            ]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&schema, &batch).unwrap();
        assert_eq!(reconciled.num_rows(), 4);
        assert_eq!(warnings.len(), 1);
        if let DriftWarning::PartialCast { surviving_rows, .. } = &warnings[0] {
            assert_eq!(*surviving_rows, 2); // "1" and "3" survived, "not_a_number" became null
        } else {
            panic!("Expected PartialCast, got {:?}", warnings[0]);
        }
    }

    #[test]
    fn test_reconcile_batch_cast_failed() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("bad"), Some("data")]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&schema, &batch).unwrap();
        assert_eq!(reconciled.num_rows(), 2);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::CastFailed { .. }));
    }

    #[test]
    fn test_reconcile_batch_null_reduction() {
        // Unusual case: casting reduces nulls. In Strake this should be clean coercion.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(0), Some(1), None]))],
        )
        .unwrap();

        let (reconciled, warnings) = reconcile_batch(&schema, &batch).unwrap();
        assert_eq!(reconciled.num_rows(), 3);
        // datafusion's cast(Int64(0) as Boolean) -> false, Int64(1) -> true
        assert_eq!(warnings.len(), 1);
        assert!(matches!(warnings[0], DriftWarning::TypeCoerced { .. }));
    }
}
