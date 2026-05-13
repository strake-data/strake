//! Transparent query result caching.
//!
//! Provides a `QueryCache` that stores query results (Arrow `RecordBatch`es)
//! in Parquet files on disk with a `moka`-based LRU in-memory index.
//!
//! # Safety
//!
//! Cache keys include user context and permissions to ensure results are
//! never leaked across RBAC boundaries (RLS isolation).
//!
//! # Usage
//!
//! ```rust
//! # use std::sync::Arc;
//! # use strake_runtime::query::cache::{QueryCache, CacheConfig, CacheKey};
//! # use datafusion::logical_expr::LogicalPlan;
//! # async fn example() -> anyhow::Result<()> {
//! let config = CacheConfig::default();
//! let cache = QueryCache::new(config).await?;
//! let plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation { produce_one_row: true, schema: Arc::new(datafusion::common::DFSchema::empty()) });
//! let key = CacheKey::from_plan(&plan, None);
//!     
//! if let Some(stream) = cache.get_stream(&key).await {
//!     // Consume stream...
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Performance Characteristics
//!
//! The cache uses a `moka`-based LRU index for fast O(1) lookups. Result data
//! is stored in Parquet format, providing excellent compression and fast
//! scan performance for cached results. Background tasks handle cache writes
//! to avoid blocking the main query execution path.
//!
//! # Safety
//!
//! Cache keys include user context and permissions to ensure results are
//! never leaked across RBAC boundaries (RLS isolation). Filesystem operations
//! are performed asynchronously using `tokio::fs`.
//!
//! # Errors
//!
//! Returns errors if:
//! - The cache directory cannot be created or read.
//! - Hydration from disk fails due to IO issues.
//! - Parquet serialization/deserialization fails.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};

use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use futures::StreamExt;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use strake_common::auth::AuthenticatedUser;
use strake_common::models::ActorName;
use tracing::{debug, info, warn};

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

/// Configuration for the query result cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// The directory where cache files are stored.
    pub directory: PathBuf,
    /// Maximum size of the cache on disk in megabytes.
    pub max_size_mb: u64,
    /// Time-to-live for cache entries in seconds.
    pub ttl_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: PathBuf::from("/tmp/strake-cache"),
            max_size_mb: 10240, // 10GB
            ttl_seconds: 3600,  // 1 hour
        }
    }
}

/// Cache key for uniquely identifying query results
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    /// Hash of the logical plan (captures query semantics)
    plan_hash: String,
    /// User ID (for RBAC isolation)
    user_id: ActorName,
    /// User permissions (part of cache key for RLS)
    permissions_hash: String,
}

impl CacheKey {
    /// Generate a cache key from a logical plan and user context
    pub fn from_plan(plan: &LogicalPlan, user: Option<&AuthenticatedUser>) -> Self {
        // Generate STABLE hash of logical plan
        let plan_str = format!("{}", plan.display_indent());

        let mut hasher = Sha256::new();
        hasher.update(plan_str.as_bytes());
        let plan_hash = format!("{:x}", hasher.finalize());

        // Extract user context
        let (user_id, permissions_hash) = if let Some(u) = user {
            let mut perm_hasher = Sha256::new();
            let mut sorted_perms = u.permissions.raw.clone();
            sorted_perms.sort();
            perm_hasher.update(sorted_perms.join(",").as_bytes());
            (u.id.clone(), format!("{:x}", perm_hasher.finalize()))
        } else {
            (ActorName::from("anonymous"), "none".to_string())
        };

        Self {
            plan_hash,
            user_id,
            permissions_hash,
        }
    }

    /// Convert to filesystem-safe filename
    pub fn to_filename(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.plan_hash.as_bytes());
        hasher.update(b"|"); // Delimiter to prevent boundary shifting collisions
        hasher.update(self.user_id.as_ref().as_bytes());
        hasher.update(b"|");
        hasher.update(self.permissions_hash.as_bytes());
        format!("query_{:x}.parquet", hasher.finalize())
    }
}

/// Metadata for a cached entry
#[derive(Debug, Clone)]
struct CacheEntry {
    file_path: PathBuf,
    size_bytes: u64,
}

/// Production-ready query result cache with moka
#[derive(Clone)]
pub struct QueryCache {
    config: CacheConfig,
    /// Moka cache for efficient concurrent LRU with automatic eviction
    cache: Cache<String, CacheEntry>,
}

impl QueryCache {
    /// Create a new query cache with the given configuration
    pub async fn new(config: CacheConfig) -> Result<Self> {
        tracing::info!(
            "Initializing QueryCache with config: enabled={}, directory={}, ttl={}",
            config.enabled,
            config.directory.display(),
            config.ttl_seconds
        );

        if config.enabled {
            // Ensure cache directory exists (async)
            tokio::fs::create_dir_all(&config.directory)
                .await
                .with_context(|| {
                    format!("Failed to create cache directory: {:?}", config.directory)
                })?;
        }

        // Build moka cache with size and TTL limits
        let cache = Cache::builder()
            .max_capacity(config.max_size_mb.saturating_mul(1024 * 1024)) // Convert MB to bytes
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            .weigher(|_key: &String, entry: &CacheEntry| -> u32 {
                // Weight by file size for accurate size-based eviction
                entry.size_bytes.try_into().unwrap_or(u32::MAX)
            })
            .eviction_listener(|key, entry: CacheEntry, cause| {
                debug!(
                    target: "cache",
                    key = %key,
                    size_bytes = entry.size_bytes,
                    cause = ?cause,
                    "Evicting cache entry"
                );

                let file_path = entry.file_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::remove_file(&file_path).await {
                        warn!(
                            target: "cache",
                            path = ?file_path,
                            error = %e,
                            "Failed to delete evicted cache file"
                        );
                    }
                });
            })
            .build();

        let instance = Self {
            config: config.clone(),
            cache,
        };
        if config.enabled {
            instance.hydrate_from_disk().await?;
        }

        Ok(instance)
    }

    /// Scan cache directory and rebuild metadata from existing files
    async fn hydrate_from_disk(&self) -> Result<()> {
        let mut read_dir = tokio::fs::read_dir(&self.config.directory)
            .await
            .with_context(|| {
                format!(
                    "Failed to read cache directory: {:?}",
                    self.config.directory
                )
            })?;

        let mut hydrated_count = 0;
        let mut total_size = 0u64;

        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }

            if let Ok(metadata) = entry.metadata().await {
                let size_bytes = metadata.len();
                let filename = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                let cache_entry = CacheEntry {
                    file_path: path.clone(),
                    size_bytes,
                };

                self.cache.insert(filename.clone(), cache_entry).await;
                hydrated_count += 1;
                total_size += size_bytes;
            }
        }

        self.cleanup_temp_files().await?;

        info!(
            target: "cache",
            files = hydrated_count,
            total_mb = total_size / 1024 / 1024,
            "Hydrated cache from disk"
        );

        Ok(())
    }

    /// Clean up orphaned .tmp files from crashed writes
    async fn cleanup_temp_files(&self) -> Result<()> {
        let mut read_dir = tokio::fs::read_dir(&self.config.directory).await?;
        let mut cleaned = 0;

        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    warn!(
                        target: "cache",
                        path = ?path,
                        error = %e,
                        "Failed to clean up temp file"
                    );
                } else {
                    cleaned += 1;
                }
            }
        }

        if cleaned > 0 {
            info!(
                target: "cache",
                count = cleaned,
                "Cleaned up orphaned temp files"
            );
        }

        Ok(())
    }

    /// Try to get cached query results as a stream
    pub async fn get_stream(&self, key: &CacheKey) -> Option<SendableRecordBatchStream> {
        if !self.config.enabled {
            return None;
        }

        let filename = key.to_filename();
        let entry = self.cache.get(&filename).await?;

        debug!(
            target: "cache",
            key = %filename,
            size_bytes = entry.size_bytes,
            "Cache hit (streaming)"
        );

        match self.open_parquet_stream(&entry.file_path).await {
            Ok(stream) => Some(stream),
            Err(e) => {
                warn!(
                    target: "cache",
                    key = %filename,
                    error = %e,
                    "Failed to open cache stream, invalidating entry"
                );
                self.cache.invalidate(&filename).await;
                None
            }
        }
    }

    /// Try to get cached query results as a Vec (legacy/small results)
    pub async fn get(&self, key: &CacheKey) -> Option<Vec<RecordBatch>> {
        if !self.config.enabled {
            return None;
        }

        let filename = key.to_filename();
        let entry = self.cache.get(&filename).await?;

        debug!(
            target: "cache",
            key = %filename,
            size_bytes = entry.size_bytes,
            "Cache hit"
        );

        match self.read_parquet(&entry.file_path).await {
            Ok(batches) => Some(batches),
            Err(e) => {
                warn!(
                    target: "cache",
                    key = %filename,
                    error = %e,
                    "Failed to read cache file, invalidating entry"
                );
                self.cache.invalidate(&filename).await;
                None
            }
        }
    }

    /// Store query result stream in cache
    pub async fn put_stream(
        &self,
        key: CacheKey,
        mut stream: SendableRecordBatchStream,
        completed: Arc<AtomicBool>,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let filename = key.to_filename();
        let file_path = self.config.directory.join(&filename);
        let tmp_path = file_path.with_extension("tmp");

        let schema = stream.schema();
        let mut row_count = 0;
        let mut success = true;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(100);

        let tmp_path_buf = tmp_path.clone();
        let schema_clone = schema.clone();

        let write_task = tokio::task::spawn_blocking(move || -> Result<u64, anyhow::Error> {
            let file = std::fs::File::create(&tmp_path_buf)?;
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, schema_clone, Some(props))?;

            while let Some(batch) = rx.blocking_recv() {
                writer.write(&batch)?;
            }

            writer.close()?;
            let metadata = std::fs::metadata(&tmp_path_buf)?;
            Ok::<u64, anyhow::Error>(metadata.len())
        });

        while let Some(batch_res) = stream.next().await {
            match batch_res {
                Ok(batch) => {
                    row_count += batch.num_rows();
                    if tx.try_send(batch).is_err() {
                        warn!(
                            target: "cache",
                            key = %filename,
                            "Cache writer lagging, aborting recording"
                        );
                        success = false;
                        break;
                    }
                }
                Err(e) => {
                    warn!(
                        target: "cache",
                        key = %filename,
                        error = %e,
                        "Source stream error, aborting cache recording"
                    );
                    success = false;
                    break;
                }
            }
        }

        drop(tx);

        let write_result = write_task.await.context("Parquet write task panicked")?;

        if success && row_count > 0 && completed.load(Ordering::Acquire) {
            if let Ok(size_bytes) = write_result {
                if let Err(e) = tokio::fs::rename(&tmp_path, &file_path).await {
                    warn!(target: "cache", error = %e, "Failed to finalize cache file");
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return Ok(());
                }

                let entry = CacheEntry {
                    file_path,
                    size_bytes,
                };
                self.cache.insert(filename.clone(), entry).await;
                debug!(
                    target: "cache",
                    key = %filename,
                    rows = row_count,
                    size_bytes,
                    "Cached query result stream"
                );
            }
        } else {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            if let Err(e) = write_result {
                warn!(target: "cache", error = %e, "Cache write task failed");
            }
        }

        Ok(())
    }

    /// Store query results in cache (legacy/small results)
    pub async fn put(&self, key: CacheKey, batches: &[RecordBatch]) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        if row_count == 0 {
            return Ok(());
        }

        let filename = key.to_filename();
        let file_path = self.config.directory.join(&filename);

        match self.write_parquet(&file_path, batches).await {
            Ok(size_bytes) => {
                let entry = CacheEntry {
                    file_path: file_path.clone(),
                    size_bytes,
                };

                self.cache.insert(filename.clone(), entry).await;

                debug!(
                    target: "cache",
                    key = %filename,
                    rows = row_count,
                    size_bytes,
                    "Cached query result"
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    target: "cache",
                    key = %filename,
                    error = %e,
                    "Failed to write cache file"
                );
                Ok(())
            }
        }
    }

    async fn read_parquet(&self, path: &Path) -> Result<Vec<RecordBatch>> {
        let path_buf = path.to_path_buf();

        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, anyhow::Error> {
            let file = std::fs::File::open(&path_buf)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;

            let mut batches = Vec::new();
            for batch_result in reader {
                batches.push(batch_result?);
            }

            Ok(batches)
        })
        .await
        .context("Parquet read task panicked")?
    }

    async fn write_parquet(&self, path: &Path, batches: &[RecordBatch]) -> Result<u64> {
        if batches.is_empty() {
            return Ok(0);
        }

        let path_buf = path.to_path_buf();
        let tmp_path = path.with_extension("tmp");
        let batches_owned: Vec<RecordBatch> = batches.to_vec();

        let size = tokio::task::spawn_blocking(move || -> Result<u64, anyhow::Error> {
            let file = std::fs::File::create(&tmp_path)?;
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, batches_owned[0].schema(), Some(props))?;

            for batch in &batches_owned {
                writer.write(batch)?;
            }

            writer.close()?;
            let metadata = std::fs::metadata(&tmp_path)?;
            let size = metadata.len();

            std::fs::rename(&tmp_path, &path_buf)?;

            Ok::<u64, anyhow::Error>(size)
        })
        .await
        .context("Parquet write task panicked")??;

        Ok(size)
    }

    async fn open_parquet_stream(&self, path: &Path) -> Result<SendableRecordBatchStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("Failed to open cache file: {:?}", path))?;

        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .context("Failed to create async Parquet reader builder")?;

        let schema = builder.schema().clone();
        let stream = builder
            .build()
            .context("Failed to build async Parquet stream")?;

        let adapter = RecordBatchStreamAdapter::new(
            schema,
            stream.map(
                |res: std::result::Result<RecordBatch, parquet::errors::ParquetError>| {
                    res.map_err(|e| DataFusionError::ArrowError(Box::new(e.into()), None))
                },
            ),
        );

        Ok(Box::pin(adapter))
    }
}

impl QueryCache {
    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            enabled: self.config.enabled,
            entry_count: self.cache.entry_count(),
            weighted_size: self.cache.weighted_size(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Whether the cache is enabled.
    pub enabled: bool,
    /// Number of entries currently in the cache.
    pub entry_count: u64,
    /// Total weighted size of all entries (bytes).
    pub weighted_size: u64,
}
