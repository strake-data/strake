//! Policy-driven query execution orchestration.
//!
//! The `ExecutionOrchestrator` manages the high-level query lifecycle, applying
//! pluggable policies for caching, resource budgeting, and timeouts.
//!
//! # Architecture
//!
//! It uses a "policy chain" approach where each `ExecutionPolicy` can:
//! 1. Intercept execution (e.g., return cached results).
//! 2. Wrap the result stream (e.g., record results for future caching).
//!
//! # Usage
//!
//! ```rust
//! // let orchestrator = ExecutionOrchestrator::new(pipeline, vec![cache_policy]);
//! // let (schema, stream) = orchestrator.execute(sql, plan, options, collector).await?;
//! ```
//!
//! # Performance Characteristics
//!
//! The orchestrator adds negligible overhead to the query execution path.
//! Caching policies can significantly improve performance for repeated queries
//! by bypassing the execution pipeline.
//!
//! # Safety
//!
//! This module uses no unsafe code. Resource budgeting is handled via semaphores
//! to ensure system stability under high load.
//!
//! # Errors
//!
//! Returns errors if:
//! - Pre-execution policies fail (e.g., cache corruption).
//! - The underlying pipeline execution fails.
//! - Budget permits cannot be acquired.

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::query::cache::{CacheKey, QueryCache};
use crate::query::pipeline::Pipeline;
use strake_common::warnings::WarningCollector;

/// A trait for pluggable execution policies that drive the query lifecycle.
#[async_trait::async_trait]
pub trait ExecutionPolicy: Send + Sync {
    /// Hook called before query execution. Can return a result early (e.g., cache hit).
    async fn pre_execute(
        &self,
        _request: &ExecutionRequest,
    ) -> Result<Option<(SchemaRef, SendableRecordBatchStream)>> {
        Ok(None)
    }

    /// Hook called after query execution. Can wrap the result stream (e.g., cache recording).
    async fn post_execute(
        &self,
        _request: &ExecutionRequest,
        _schema: SchemaRef,
        stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        Ok(stream)
    }

    /// Hook called to prepare for execution (e.g., acquire budget permits).
    /// Returns a guard that is held until the stream is dropped.
    async fn prepare_execute(
        &self,
        _request: &ExecutionRequest,
    ) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        Ok(None)
    }
}

/// Context for a single query execution request.
pub struct ExecutionRequest {
    /// The SQL query string.
    pub sql: String,
    /// The logical plan for the query.
    pub plan: LogicalPlan,
    /// Execution options.
    pub options: ExecutionOptions,
    /// Collector for query warnings.
    pub collector: WarningCollector,
    /// The unique cache key for this request.
    pub cache_key: CacheKey,
}

#[derive(Debug, Clone, Default)]
/// Options for controlling query execution behavior.
pub struct ExecutionOptions {
    /// Whether the query result should be cached.
    pub should_cache: bool,
    /// The user context for authorization.
    pub user: Option<strake_common::auth::AuthenticatedUser>,
    /// The execution timeout.
    pub timeout: std::time::Duration,
}

/// Orchestrates query execution by applying a chain of policies.
pub struct ExecutionOrchestrator {
    /// The underlying pipeline for physical execution.
    pub pipeline: Arc<dyn Pipeline>,
    /// The list of policies to apply in order.
    pub policies: Vec<Arc<dyn ExecutionPolicy>>,
}

impl ExecutionOrchestrator {
    /// Create a new orchestrator with the given pipeline and policies.
    pub fn new(pipeline: Arc<dyn Pipeline>, policies: Vec<Arc<dyn ExecutionPolicy>>) -> Self {
        Self { pipeline, policies }
    }

    /// Execute a query through the policy chain.
    pub async fn execute(
        &self,
        sql: &str,
        plan: LogicalPlan,
        options: ExecutionOptions,
        collector: WarningCollector,
    ) -> Result<(SchemaRef, SendableRecordBatchStream)> {
        let cache_key = CacheKey::from_plan(&plan, options.user.as_ref());
        let request = ExecutionRequest {
            sql: sql.to_string(),
            plan,
            options,
            collector,
            cache_key,
        };

        // 1. Pre-execution policies (e.g., Cache lookup)
        for policy in &self.policies {
            if let Some(result) = policy.pre_execute(&request).await? {
                return Ok(result);
            }
        }

        // 2. Prepare execution (e.g., acquire permits)
        let mut guards = Vec::new();
        for policy in &self.policies {
            if let Some(guard) = policy.prepare_execute(&request).await? {
                guards.push(guard);
            }
        }

        // 3. Core Execution with Timeout
        //
        // Cancellation Safety: If the timeout expires, the future is dropped.
        // DataFusion physical plans propagate cancellation via the drop glue of
        // the returned stream. Remote connector tasks (e.g. SQL scans) should
        // monitor for stream drop to abort remote requests.
        let timeout_duration = request.options.timeout;
        let result = tokio::time::timeout(
            timeout_duration,
            self.pipeline
                .execute(request.plan.clone(), &request.collector),
        )
        .await
        .map_err(|_| {
            strake_error::StrakeError::new(
                strake_error::ErrorCode::QueryCancelled,
                format!(
                    "Query timed out after {} seconds",
                    timeout_duration.as_secs()
                ),
            )
            .with_hint("Simplify query or increase 'query_timeout_seconds' in config")
        })?;

        let (schema, mut stream) = result.context("Query execution failed")?;

        // 4. Wrap with guards (to ensure they are held throughout streaming)
        if !guards.is_empty() {
            stream = Box::pin(GuardStream {
                input: stream,
                _guards: guards,
            });
        }

        // 5. Post-execution policies (e.g., Cache recording, telemetry)
        for policy in &self.policies {
            stream = policy
                .post_execute(&request, schema.clone(), stream)
                .await?;
        }

        Ok((schema, stream))
    }
}

// --- Built-in Policies ---

/// An execution policy that implements query result caching.
pub struct CachePolicy {
    /// The underlying cache storage.
    pub cache: QueryCache,
}

impl CachePolicy {
    /// Create a new cache policy.
    pub fn new(cache: QueryCache) -> Self {
        Self { cache }
    }
}

#[async_trait::async_trait]
impl ExecutionPolicy for CachePolicy {
    async fn pre_execute(
        &self,
        req: &ExecutionRequest,
    ) -> Result<Option<(SchemaRef, SendableRecordBatchStream)>> {
        if req.options.should_cache
            && let Some(stream) = self.cache.get_stream(&req.cache_key).await
        {
            req.collector.add("x-strake-cache: hit".to_string());
            info!(
                target: "queries",
                user_id = %req.options.user.as_ref().map(|u| u.id.as_ref()).unwrap_or("anonymous"),
                query = req.sql,
                cache_hit = true,
                success = true
            );
            return Ok(Some((stream.schema(), stream)));
        }
        Ok(None)
    }

    async fn post_execute(
        &self,
        req: &ExecutionRequest,
        schema: SchemaRef,
        stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        if req.options.should_cache {
            req.collector.add("x-strake-cache: miss".to_string());

            let (tx, rx) = tokio::sync::mpsc::channel(100);
            let cache = self.cache.clone();
            let key = req.cache_key.clone();
            let schema_clone = schema.clone();
            let key_str = key.to_filename();
            let collector = req.collector.clone();

            let completed = Arc::new(AtomicBool::new(false));
            let completed_clone = Arc::clone(&completed);

            tokio::spawn(async move {
                use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
                let recording_stream = RecordBatchStreamAdapter::new(
                    schema_clone,
                    futures::stream::unfold(rx, |mut rx| async move {
                        rx.recv().await.map(|res| (res, rx))
                    }),
                );
                if let Err(e) = cache
                    .put_stream(key, Box::pin(recording_stream), completed_clone)
                    .await
                {
                    let msg = format!("x-strake-cache-write-error: {}", e);
                    collector.add(msg);
                    warn!(target: "cache", key = %key_str, error = %e, "Failed to background cache query result");
                }
            });

            Ok(Box::pin(TeeStream {
                input: stream,
                tx: Some(tx),
                completed,
                collector: req.collector.clone(),
            }))
        } else {
            Ok(stream)
        }
    }
}

/// An execution policy that limits concurrent query execution.
pub struct BudgetPolicy {
    /// The semaphore used for budgeting.
    pub budget: Arc<Semaphore>,
}

impl BudgetPolicy {
    /// Create a new budget policy.
    pub fn new(budget: Arc<Semaphore>) -> Self {
        Self { budget }
    }
}

#[async_trait::async_trait]
impl ExecutionPolicy for BudgetPolicy {
    async fn prepare_execute(
        &self,
        _req: &ExecutionRequest,
    ) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        let permit = self
            .budget
            .clone()
            .acquire_owned()
            .await
            .context("Failed to acquire connection permit")?;
        Ok(Some(Box::new(permit)))
    }
}

/// A stream that holds opaque guards until it is dropped.
struct GuardStream {
    input: SendableRecordBatchStream,
    _guards: Vec<Box<dyn Any + Send + Sync>>,
}

impl Stream for GuardStream {
    type Item = Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for GuardStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

/// A stream that sends batches to a channel while they are being produced.
///
/// NOTE: This is a pass-through wrapper. If this is ever promoted to a full
/// ExecutionPlan node, it must implement `BaselineMetrics` (including `output_bytes`).
struct TeeStream {
    input: SendableRecordBatchStream,
    tx: Option<
        tokio::sync::mpsc::Sender<
            std::result::Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>,
        >,
    >,
    /// Set to true only if the stream was fully consumed.
    completed: Arc<AtomicBool>,
    collector: WarningCollector,
}

impl Stream for TeeStream {
    type Item = Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Some(tx) = self.tx.as_ref()
                    && tx.try_send(Ok(batch.clone())).is_err()
                {
                    debug!("Cache recording buffer full, aborting recording");
                    self.collector
                        .add("x-strake-cache-recording: aborted (buffer full)".to_string());
                    let _ = tx.try_send(Err(datafusion::error::DataFusionError::External(
                        anyhow::anyhow!("Cache recording buffer full").into(),
                    )));
                    self.tx = None;
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => {
                if let Some(tx) = self.tx.as_ref() {
                    let _ = tx.try_send(Err(datafusion::error::DataFusionError::Execution(
                        e.to_string(),
                    )));
                    self.tx = None;
                }
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.completed.store(true, Ordering::Release);
                self.tx = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TeeStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::time::Duration;

    struct MockPipeline {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        delay: Option<Duration>,
    }

    #[async_trait::async_trait]
    impl Pipeline for MockPipeline {
        async fn execute(
            &self,
            _plan: LogicalPlan,
            _collector: &WarningCollector,
        ) -> Result<(SchemaRef, SendableRecordBatchStream)> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            let stream = RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::iter(self.batches.clone().into_iter().map(Ok)),
            );
            Ok((self.schema.clone(), Box::pin(stream)))
        }
    }

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    #[tokio::test]
    async fn test_orchestrator_cache_hit() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let schema = create_test_schema();
        let cache = QueryCache::new(crate::query::cache::CacheConfig {
            enabled: true,
            directory: temp_dir.path().to_path_buf(),
            ..Default::default()
        })
        .await?;

        let plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        let key = CacheKey::from_plan(&plan, None);

        // Pre-populate cache with a non-empty batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )?;
        cache.put(key, &[batch]).await?;

        let pipeline = Arc::new(MockPipeline {
            schema: schema.clone(),
            batches: vec![],
            delay: None,
        });

        let policies: Vec<Arc<dyn ExecutionPolicy>> = vec![Arc::new(CachePolicy::new(cache))];

        let orchestrator = ExecutionOrchestrator::new(pipeline, policies);

        let options = ExecutionOptions {
            should_cache: true,
            user: None,
            timeout: Duration::from_secs(10),
        };
        let collector = WarningCollector::new();
        let (res_schema, mut stream) = orchestrator
            .execute("SELECT 1", plan, options, collector.clone())
            .await?;

        assert_eq!(res_schema, schema);
        assert!(stream.next().await.is_some());
        assert!(
            collector
                .take_all()
                .contains(&"x-strake-cache: hit".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_orchestrator_budget_policy() -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(1));
        let pipeline = Arc::new(MockPipeline {
            schema: create_test_schema(),
            batches: vec![RecordBatch::new_empty(create_test_schema())],
            delay: None,
        });

        let policies: Vec<Arc<dyn ExecutionPolicy>> =
            vec![Arc::new(BudgetPolicy::new(semaphore.clone()))];
        let orchestrator = ExecutionOrchestrator::new(pipeline, policies);
        let options = ExecutionOptions {
            should_cache: false,
            user: None,
            timeout: Duration::from_secs(10),
        };

        // First execution acquires the only permit
        let (_schema, stream) = orchestrator
            .execute(
                "SELECT 1",
                LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion::common::DFSchema::empty()),
                }),
                options.clone(),
                WarningCollector::new(),
            )
            .await?;

        // Second execution should block (we use try_acquire to check)
        assert_eq!(semaphore.available_permits(), 0);

        // Dropping the stream should release the permit
        drop(stream);
        assert_eq!(semaphore.available_permits(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_tee_stream_buffer_full() -> Result<()> {
        let schema = create_test_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )?;

        let input_stream = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]),
        );

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let collector = WarningCollector::new();

        let mut tee = TeeStream {
            input: Box::pin(input_stream),
            tx: Some(tx),
            collector: collector.clone(),
            completed: Arc::new(AtomicBool::new(false)),
        };

        // First batch should succeed and fill the channel
        let _ = tee.next().await;
        // assert!(_rx.try_recv().is_ok()); // Remove this to leave the channel full

        // Second batch should cause "buffer full" since rx is not drained
        let _ = tee.next().await;

        let warnings = collector.take_all();
        assert!(warnings.iter().any(|w| w.contains("aborted (buffer full)")));

        Ok(())
    }

    #[tokio::test]
    async fn test_budget_policy_stress() -> Result<()> {
        let max_concurrency = 5;
        let semaphore = Arc::new(Semaphore::new(max_concurrency));
        let pipeline = Arc::new(MockPipeline {
            schema: create_test_schema(),
            batches: vec![RecordBatch::new_empty(create_test_schema())],
            delay: Some(Duration::from_millis(10)), // Simulate some work
        });

        let orchestrator = Arc::new(ExecutionOrchestrator::new(
            pipeline,
            vec![Arc::new(BudgetPolicy::new(semaphore.clone()))],
        ));

        let mut handles = vec![];
        for i in 0..20 {
            let orch = orchestrator.clone();
            handles.push(tokio::spawn(async move {
                let options = ExecutionOptions {
                    should_cache: false,
                    user: None,
                    timeout: Duration::from_secs(10),
                };
                let plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion::common::DFSchema::empty()),
                });
                let (_schema, stream) = orch
                    .execute(
                        &format!("SELECT {}", i),
                        plan,
                        options,
                        WarningCollector::new(),
                    )
                    .await
                    .unwrap();

                // Keep the stream alive for a bit to hold the permit
                tokio::time::sleep(Duration::from_millis(20)).await;
                drop(stream);
            }));
        }

        for handle in handles {
            handle.await?;
        }

        assert_eq!(semaphore.available_permits(), max_concurrency);
        Ok(())
    }
}
