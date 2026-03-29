//! # Predicate Caching
//!
//! Advanced predicate caching for Parquet and Iceberg data sources.
//!
//! Provides `CachingReaderFactory` and `RecordingExec` to enable
//! row-group level filtering based on previous query results.

use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;
use std::any::Any;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, BooleanArray, Int32RunArray, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
/// Trait for table providers that support dynamic filtering.
pub trait DynamicFilterSource: Any + Send + Sync {
    /// Returns true if this source supports dynamic filtering.
    fn supports_dynamic_filter(&self) -> bool;
}
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{DefaultParquetFileReaderFactory, ParquetFileReaderFactory};
use iceberg::metadata_columns::RESERVED_COL_NAME_FILE;
use strake_common::predicate_cache::{BlockKey, PredicateCache};

/// Cache mode for predicate caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMode {
    /// Recording phase: capture row group results.
    Recording,
    /// Filtering phase: skip row groups based on cached results.
    Filtering,
}

/// Shared state for one file during a recording scan.
#[derive(Debug)]
pub struct FileRecordingState {
    /// Path of the file on disk or object store.
    pub file_path: Arc<str>,
    /// One entry per row group. 0=pending, 1=matched, 2=no_match.
    pub row_group_results: Vec<AtomicU8>,
    /// Cumulative row counts at the entry of each row group.
    pub row_group_offsets: Vec<usize>,
}

impl FileRecordingState {
    fn new(file_path: Arc<str>, metadata: &ParquetMetaData) -> Self {
        let row_groups = metadata.row_groups();
        let mut offsets = Vec::with_capacity(row_groups.len());
        let mut current_offset = 0;
        for rg in row_groups {
            offsets.push(current_offset);
            current_offset += rg.num_rows() as usize;
        }
        Self {
            file_path,
            row_group_results: (0..row_groups.len()).map(|_| AtomicU8::new(0)).collect(),
            row_group_offsets: offsets,
        }
    }
}

/// A factory for creating `CachingAsyncFileReader`s.
#[derive(Debug)]
pub struct CachingReaderFactory {
    /// Inner reader factory.
    pub inner: Arc<dyn ParquetFileReaderFactory>,
    /// Predicate cache.
    pub cache: Arc<PredicateCache>,
    /// Snapshot ID for cache isolation.
    pub snapshot_id: i64,
    /// Recording states for current query.
    pub recording_states: Arc<DashMap<String, Arc<FileRecordingState>>>,
    /// Maps (file_path, partition_index) to the starting row offset for that partition.
    pub partition_row_offsets: Arc<DashMap<(String, usize), usize>>,
    /// Current cache mode.
    pub mode: CacheMode,
}

impl ParquetFileReaderFactory for CachingReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Box<dyn AsyncFileReader + Send>> {
        let file_path: Arc<str> = partitioned_file.object_meta.location.to_string().into();
        let range = partitioned_file
            .range
            .clone()
            .map(|r| (r.start as u64)..(r.end as u64));
        tracing::info!(
            file_path = %file_path,
            mode = ?self.mode,
            "CachingReaderFactory::create_reader invoked"
        );
        let reader = self.inner.create_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            metrics,
        )?;

        Ok(Box::new(CachingAsyncFileReader {
            inner: reader,
            cache: Arc::clone(&self.cache),
            snapshot_id: self.snapshot_id,
            file_path,
            recording_states: Arc::clone(&self.recording_states),
            partition_row_offsets: Arc::clone(&self.partition_row_offsets),
            partition_index,
            mode: self.mode,
            range,
        }))
    }
}

/// A wrapper around `AsyncFileReader` that records or filters row groups.
pub struct CachingAsyncFileReader {
    inner: Box<dyn AsyncFileReader + Send>,
    cache: Arc<PredicateCache>,
    snapshot_id: i64,
    file_path: Arc<str>,
    recording_states: Arc<DashMap<String, Arc<FileRecordingState>>>,
    partition_row_offsets: Arc<DashMap<(String, usize), usize>>,
    partition_index: usize,
    mode: CacheMode,
    range: Option<Range<u64>>,
}

impl CachingAsyncFileReader {
    fn filtered_metadata(&self, metadata: &Arc<ParquetMetaData>) -> Arc<ParquetMetaData> {
        let mut kept = Vec::new();
        let mut skipped_rows = 0_u64;

        for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
            let key = BlockKey::new(
                self.snapshot_id,
                Arc::clone(&self.file_path),
                row_group_index,
            );

            match self.cache.get_block(&key) {
                Some(true) | None => kept.push(row_group.clone()),
                Some(false) => {
                    skipped_rows += row_group.num_rows() as u64;
                }
            }
        }

        if skipped_rows > 0 {
            self.cache.metrics.record_skips(skipped_rows);
        }

        Arc::new(ParquetMetaData::new(metadata.file_metadata().clone(), kept))
    }
}

impl AsyncFileReader for CachingAsyncFileReader {
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = self.inner.get_metadata(options).await?;

            if self.mode == CacheMode::Recording {
                let state = self
                    .recording_states
                    .entry(self.file_path.to_string())
                    .or_insert_with(|| {
                        Arc::new(FileRecordingState::new(
                            Arc::clone(&self.file_path),
                            &metadata,
                        ))
                    });

                // Calculate the start row offset for this partition within the file.
                // This is critical for multi-partition recording.
                let mut start_row = 0;
                if let Some(range) = &self.range {
                    // Find the first row group that overlaps with our byte range
                    for (i, rg) in metadata.row_groups().iter().enumerate() {
                        let rg_offset = rg.file_offset().unwrap_or(0) as u64;
                        if rg_offset >= range.start {
                            start_row = state.row_group_offsets[i];
                            break;
                        }
                    }
                }

                self.partition_row_offsets.insert(
                    (self.file_path.to_string(), self.partition_index),
                    start_row,
                );
            }

            match self.mode {
                CacheMode::Recording => Ok(Arc::clone(&metadata)),
                CacheMode::Filtering => Ok(self.filtered_metadata(&metadata)),
            }
        })
    }

    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }
}

/// An execution plan that records predicate matches for row groups.
#[derive(Debug)]
pub struct RecordingExec {
    inner: Arc<dyn ExecutionPlan>,
    physical_predicate: Arc<dyn PhysicalExpr>,
    recording_states: Arc<DashMap<String, Arc<FileRecordingState>>>,
    partition_row_offsets: Arc<DashMap<(String, usize), usize>>,
    cache: Arc<PredicateCache>,
    snapshot_id: i64,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl RecordingExec {
    /// Create a new `RecordingExec` plan.
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        physical_predicate: Arc<dyn PhysicalExpr>,
        recording_states: Arc<DashMap<String, Arc<FileRecordingState>>>,
        partition_row_offsets: Arc<DashMap<(String, usize), usize>>,
        cache: Arc<PredicateCache>,
        snapshot_id: i64,
    ) -> Self {
        Self {
            properties: inner.properties().clone(),
            inner,
            physical_predicate,
            recording_states,
            partition_row_offsets,
            cache,
            snapshot_id,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DynamicFilterSource for RecordingExec {
    fn supports_dynamic_filter(&self) -> bool {
        true
    }
}

impl DisplayAs for RecordingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingExec")
    }
}

impl ExecutionPlan for RecordingExec {
    fn name(&self) -> &'static str {
        "RecordingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let child = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("RecordingExec requires exactly one child".to_string())
        })?;

        Ok(Arc::new(Self::new(
            child,
            Arc::clone(&self.physical_predicate),
            Arc::clone(&self.recording_states),
            Arc::clone(&self.partition_row_offsets),
            Arc::clone(&self.cache),
            self.snapshot_id,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).counter("output_bytes", partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        Ok(Box::pin(RecordingStream {
            inner: stream,
            physical_predicate: Arc::clone(&self.physical_predicate),
            recording_states: Arc::clone(&self.recording_states),
            partition_row_offsets: Arc::clone(&self.partition_row_offsets),
            partition_index: partition,
            cache: Arc::clone(&self.cache),
            snapshot_id: self.snapshot_id,
            output_rows,
            output_bytes,
            elapsed_compute,
            committed: false,
            rows_processed: 0,
            base_row_offset: 0,
            current_file_path: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A TableProvider that injects predicate caching into the execution plan.
#[derive(Debug)]
pub struct CachingTableProvider {
    inner: Arc<dyn TableProvider>,
    cache: Arc<PredicateCache>,
    snapshot_id: i64,
}

impl CachingTableProvider {
    /// Create a new `CachingTableProvider`.
    pub fn new(
        inner: Arc<dyn TableProvider>,
        cache: Arc<PredicateCache>,
        snapshot_id: i64,
    ) -> Self {
        Self {
            inner,
            cache,
            snapshot_id,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for CachingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
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
        let inner_plan = self.inner.scan(state, projection, filters, limit).await?;

        // Determine cache mode based on whether we are in a recording phase or not.
        // For simplicity, we default to Filtering if items are in cache, Recording otherwise.
        // In a real system, this would be driven by a higher-level policy.
        let mode = if self.cache.has_any_blocks_for_snapshot(self.snapshot_id) {
            CacheMode::Filtering
        } else {
            CacheMode::Recording
        };

        let recording_states = Arc::new(DashMap::new());
        let partition_row_offsets = Arc::new(DashMap::new());

        let new_plan = inject_factory_into_plan(
            inner_plan,
            state.runtime_env().clone(),
            Arc::clone(&self.cache),
            self.snapshot_id,
            Arc::clone(&recording_states),
            Arc::clone(&partition_row_offsets),
            mode,
        )?;

        if mode == CacheMode::Recording && !filters.is_empty() {
            // Find a physical predicate to record
            if let Ok(physical_predicate) = state.create_physical_expr(
                filters[0].clone(),
                &datafusion::common::DFSchema::try_from(self.schema()).unwrap(),
            ) {
                return Ok(Arc::new(RecordingExec::new(
                    new_plan,
                    physical_predicate,
                    recording_states,
                    partition_row_offsets,
                    Arc::clone(&self.cache),
                    self.snapshot_id,
                )));
            }
        }

        Ok(new_plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

impl DynamicFilterSource for CachingTableProvider {
    fn supports_dynamic_filter(&self) -> bool {
        // CachingTableProvider itself supports dynamic filtering signals
        // to coordinate with RecordingExec.
        true
    }
}

/// A stream that records predicate matches during execution.
pub struct RecordingStream {
    inner: SendableRecordBatchStream,
    physical_predicate: Arc<dyn PhysicalExpr>,
    recording_states: Arc<DashMap<String, Arc<FileRecordingState>>>,
    partition_row_offsets: Arc<DashMap<(String, usize), usize>>,
    partition_index: usize,
    cache: Arc<PredicateCache>,
    snapshot_id: i64,
    output_rows: datafusion::physical_plan::metrics::Count,
    output_bytes: datafusion::physical_plan::metrics::Count,
    elapsed_compute: datafusion::physical_plan::metrics::Time,
    committed: bool,
    rows_processed: usize,
    base_row_offset: usize,
    current_file_path: Option<String>,
}

impl RecordingStream {
    fn extract_file_path(&self, batch: &RecordBatch) -> Option<String> {
        let file_col = batch.column_by_name(RESERVED_COL_NAME_FILE);

        if let Some(col) = file_col {
            if let Some(strings) = col.as_any().downcast_ref::<StringArray>() {
                return (strings.len() > 0).then(|| strings.value(0).to_string());
            }

            if let Some(run_array) = col.as_any().downcast_ref::<Int32RunArray>() {
                let values = run_array.values();
                if let Some(strings) = values.as_any().downcast_ref::<StringArray>() {
                    return (strings.len() > 0).then(|| strings.value(0).to_string());
                }
            }
        }

        // Fallback: if there's only one file in the recording states, use it.
        if self.recording_states.len() == 1 {
            return self.recording_states.iter().next().map(|r| r.key().clone());
        }

        None
    }

    fn commit_recordings(&mut self) {
        if self.committed {
            return;
        }
        self.committed = true;

        for state in self.recording_states.iter() {
            for (row_group_index, result) in state.row_group_results.iter().enumerate() {
                match result.load(Ordering::Acquire) {
                    1 => self.cache.insert_block(
                        BlockKey::new(
                            self.snapshot_id,
                            Arc::clone(&state.file_path),
                            row_group_index,
                        ),
                        true,
                    ),
                    2 => self.cache.insert_block(
                        BlockKey::new(
                            self.snapshot_id,
                            Arc::clone(&state.file_path),
                            row_group_index,
                        ),
                        false,
                    ),
                    _ => {}
                }
            }
        }
    }
}

impl RecordBatchStream for RecordingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for RecordingStream {
    type Item = DataFusionResult<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let started = std::time::Instant::now();
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // If batch is empty due to DataSourceExec pushdown, we cannot infer cache status safely.
                if batch.num_rows() == 0 {
                    return Poll::Ready(Some(Ok(batch)));
                }

                if let Some(file_path) = self.extract_file_path(&batch) {
                    // Reset row tracking if we moved to a different file
                    if self.current_file_path.as_deref() != Some(&file_path) {
                        self.current_file_path = Some(file_path.clone());
                        self.rows_processed = 0;
                        // Look up the starting row offset for this partition/file combination
                        self.base_row_offset = self
                            .partition_row_offsets
                            .get(&(file_path.clone(), self.partition_index))
                            .map(|v| *v)
                            .unwrap_or(0);
                    }

                    if let Some(state) = self.recording_states.get(&file_path) {
                        // Map local processed rows to global row_group_index using offsets
                        let global_row = self.base_row_offset + self.rows_processed;
                        let row_group_index =
                            match state.row_group_offsets.binary_search(&global_row) {
                                Ok(idx) => idx,
                                Err(idx) => idx.saturating_sub(1),
                            };

                        let matched = match self.physical_predicate.evaluate(&batch) {
                            Ok(value) => match value.into_array(batch.num_rows()) {
                                Ok(array) => array
                                    .as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .map(|boolean_array| boolean_array.true_count() > 0)
                                    .unwrap_or(true),
                                Err(_) => true,
                            },
                            Err(_) => true,
                        };

                        if row_group_index < state.row_group_results.len() {
                            if matched {
                                state.row_group_results[row_group_index]
                                    .store(1, Ordering::Release);
                            } else if state.row_group_results[row_group_index]
                                .load(Ordering::Acquire)
                                == 0
                            {
                                // Only mark as NO_MATCH (2) if it hasn't matched yet.
                                state.row_group_results[row_group_index]
                                    .store(2, Ordering::Release);
                            }
                        }
                    }
                    self.rows_processed = self
                        .rows_processed
                        .checked_add(batch.num_rows())
                        .expect("row count overflow in predicate cache recording - query exceeds maximum row count");
                }

                self.output_rows.add(batch.num_rows());
                self.output_bytes.add(batch.get_array_memory_size());
                self.elapsed_compute.add_duration(started.elapsed());
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                self.elapsed_compute.add_duration(started.elapsed());
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.commit_recordings();
                let mut no_match_count = 0;
                let mut total_count = 0;
                for state in self.recording_states.iter() {
                    for result in state.row_group_results.iter() {
                        let val = result.load(Ordering::Acquire);
                        if val != 0 {
                            total_count += 1;
                        }
                        if val == 2 {
                            no_match_count += 1;
                        }
                    }
                }
                tracing::info!(
                    snapshot_id = self.snapshot_id,
                    no_match_rgs = no_match_count,
                    total_rgs = total_count,
                    "Recording complete"
                );
                self.elapsed_compute.add_duration(started.elapsed());
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for RecordingStream {
    fn drop(&mut self) {
        self.commit_recordings();
    }
}

/// Injects the `CachingReaderFactory` into a Parquet execution plan.
pub fn inject_factory_into_plan(
    plan: Arc<dyn ExecutionPlan>,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    cache: Arc<PredicateCache>,
    snapshot_id: i64,
    recording_states: Arc<DashMap<String, Arc<FileRecordingState>>>,
    partition_row_offsets: Arc<DashMap<(String, usize), usize>>,
    mode: CacheMode,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        let (base_config, parquet_source) = data_source_exec
            .downcast_to_file_source::<ParquetSource>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "inject_factory_into_plan: expected DataSourceExec with ParquetSource"
                        .to_string(),
                )
            })?;

        let inner_factory: Arc<dyn ParquetFileReaderFactory> =
            match parquet_source.parquet_file_reader_factory() {
                Some(existing) => Arc::clone(existing),
                None => Arc::new(DefaultParquetFileReaderFactory::new(
                    runtime_env.object_store(&base_config.object_store_url)?,
                )),
            };

        let factory = Arc::new(CachingReaderFactory {
            inner: inner_factory,
            cache: Arc::clone(&cache),
            snapshot_id,
            recording_states: Arc::clone(&recording_states),
            partition_row_offsets: Arc::clone(&partition_row_offsets),
            mode,
        });

        let new_source = parquet_source
            .clone()
            .with_parquet_file_reader_factory(factory);
        let new_config = FileScanConfigBuilder::from(base_config.clone())
            .with_source(Arc::new(new_source))
            .build();
        return Ok(DataSourceExec::from_data_source(new_config));
    }

    if let Some(projection_exec) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let child = projection_exec.children()[0].clone();
        let new_child = inject_factory_into_plan(
            Arc::clone(&child),
            runtime_env,
            cache,
            snapshot_id,
            recording_states,
            partition_row_offsets,
            mode,
        )?;
        return plan.with_new_children(vec![new_child]);
    }

    // Handle EmptyExec or other nodes gracefully by just returning the original plan.
    if plan.name() == "EmptyExec" {
        return Ok(plan);
    }

    let children = plan
        .children()
        .into_iter()
        .map(Arc::clone)
        .collect::<Vec<_>>();
    let new_children = children
        .into_iter()
        .map(|child| {
            inject_factory_into_plan(
                child,
                Arc::clone(&runtime_env),
                Arc::clone(&cache),
                snapshot_id,
                Arc::clone(&recording_states),
                Arc::clone(&partition_row_offsets),
                mode,
            )
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    plan.with_new_children(new_children)
}

impl FileRecordingState {
    #[cfg(test)]
    fn new_for_test(file_path: Arc<str>, row_group_count: usize, offsets: Vec<usize>) -> Self {
        Self {
            file_path,
            row_group_results: (0..row_group_count).map(|_| AtomicU8::new(0)).collect(),
            row_group_offsets: offsets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;

    fn make_batch(values: &[i32], file: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new(RESERVED_COL_NAME_FILE, DataType::Utf8, false),
        ]));

        let file_col: ArrayRef = Arc::new(StringArray::from(vec![file; values.len()]));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values.to_vec())), file_col],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_recording_marks_matching_rg() {
        let batch = make_batch(&[1, 10, 11], "file.parquet");
        let schema = batch.schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        let ctx = SessionContext::new();
        let physical_predicate = ctx
            .state()
            .create_physical_expr(
                datafusion::logical_expr::col("value").gt(datafusion::logical_expr::lit(5)),
                &datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap(),
            )
            .unwrap();

        let recording_states = Arc::new(DashMap::new());
        recording_states.insert(
            "file.parquet".to_string(),
            Arc::new(FileRecordingState::new_for_test(
                "file.parquet".into(),
                1,
                vec![0],
            )),
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let mut stream = RecordingStream {
            inner,
            physical_predicate,
            recording_states: Arc::clone(&recording_states),
            partition_row_offsets: Arc::new(DashMap::new()),
            partition_index: 0,
            cache: Arc::new(PredicateCache::new()),
            base_row_offset: 0,
            snapshot_id: 1,
            output_rows: MetricBuilder::new(&metrics).output_rows(0),
            output_bytes: MetricBuilder::new(&metrics).output_bytes(0),
            elapsed_compute: MetricBuilder::new(&metrics).elapsed_compute(0),
            committed: false,
            rows_processed: 0,
            current_file_path: None,
        };

        let _ = stream.next().await.unwrap().unwrap();
        let state = recording_states.get("file.parquet").unwrap();
        assert_eq!(state.row_group_results[0].load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn test_multi_batch_row_group() {
        let batch1 = make_batch(&[1, 2], "file.parquet");
        let batch2 = make_batch(&[10], "file.parquet"); // Matches!
        let schema = batch1.schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch1), Ok(batch2)]),
        ));

        let ctx = SessionContext::new();
        let physical_predicate = ctx
            .state()
            .create_physical_expr(
                datafusion::logical_expr::col("value").gt(datafusion::logical_expr::lit(5)),
                &datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap(),
            )
            .unwrap();

        let recording_states = Arc::new(DashMap::new());
        // Row group 0 starts at row 0.
        recording_states.insert(
            "file.parquet".to_string(),
            Arc::new(FileRecordingState::new_for_test(
                "file.parquet".into(),
                1,
                vec![0],
            )),
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let mut stream = RecordingStream {
            inner,
            physical_predicate,
            recording_states: Arc::clone(&recording_states),
            partition_row_offsets: Arc::new(DashMap::new()),
            partition_index: 0,
            cache: Arc::new(PredicateCache::new()),
            base_row_offset: 0,
            snapshot_id: 1,
            output_rows: MetricBuilder::new(&metrics).output_rows(0),
            output_bytes: MetricBuilder::new(&metrics).output_bytes(0),
            elapsed_compute: MetricBuilder::new(&metrics).elapsed_compute(0),
            committed: false,
            rows_processed: 0,
            current_file_path: None,
        };

        // First batch processed. No match yet.
        let _ = stream.next().await.unwrap().unwrap();
        let state = recording_states.get("file.parquet").unwrap();
        assert_eq!(state.row_group_results[0].load(Ordering::Acquire), 2); // Initial no-match

        // Second batch processed. MATCH!
        let _ = stream.next().await.unwrap().unwrap();
        assert_eq!(state.row_group_results[0].load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn test_empty_row_groups_no_underflow() {
        let batch = make_batch(&[1], "file.parquet");
        let schema = batch.schema();
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        let ctx = SessionContext::new();
        let physical_predicate = ctx
            .state()
            .create_physical_expr(
                datafusion::logical_expr::col("value").gt(datafusion::logical_expr::lit(5)),
                &datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap(),
            )
            .unwrap();

        let recording_states = Arc::new(DashMap::new());
        // Empty row groups!
        recording_states.insert(
            "file.parquet".to_string(),
            Arc::new(FileRecordingState::new_for_test(
                "file.parquet".into(),
                0,
                vec![],
            )),
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let mut stream = RecordingStream {
            inner,
            physical_predicate,
            recording_states: Arc::clone(&recording_states),
            partition_row_offsets: Arc::new(DashMap::new()),
            partition_index: 0,
            cache: Arc::new(PredicateCache::new()),
            base_row_offset: 0,
            snapshot_id: 1,
            output_rows: MetricBuilder::new(&metrics).output_rows(0),
            output_bytes: MetricBuilder::new(&metrics).output_bytes(0),
            elapsed_compute: MetricBuilder::new(&metrics).elapsed_compute(0),
            committed: false,
            rows_processed: 0,
            current_file_path: None,
        };

        // Should not panic or underflow
        let _ = stream.next().await.unwrap().unwrap();
    }
}
