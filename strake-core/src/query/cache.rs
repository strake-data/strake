use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::auth::AuthenticatedUser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Configuration for the query result cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    pub directory: PathBuf,
    pub max_size_mb: u64,
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
    user_id: String,
    /// User permissions (part of cache key for RLS)
    permissions_hash: String,
}

impl CacheKey {
    /// Generate a cache key from a logical plan and user context
    pub fn from_plan(plan: &LogicalPlan, user: Option<&AuthenticatedUser>) -> Result<Self> {
        // Generate STABLE hash of logical plan
        // Using Display trait provides a stable representation
        // (more stable than Debug which can change across Rust versions)
        let plan_str = format!("{}", plan.display_indent());

        let mut hasher = Sha256::new();
        hasher.update(plan_str.as_bytes());
        let plan_hash = format!("{:x}", hasher.finalize());

        // Extract user context
        let (user_id, permissions_hash) = if let Some(u) = user {
            let mut perm_hasher = Sha256::new();
            let mut sorted_perms = u.permissions.clone();
            sorted_perms.sort();
            perm_hasher.update(sorted_perms.join(",").as_bytes());
            (u.id.clone(), format!("{:x}", perm_hasher.finalize()))
        } else {
            ("anonymous".to_string(), "none".to_string())
        };

        Ok(Self {
            plan_hash,
            user_id,
            permissions_hash,
        })
    }

    /// Convert to filesystem-safe filename
    pub fn to_filename(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.plan_hash.as_bytes());
        hasher.update(self.user_id.as_bytes());
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

            info!(
                target: "cache",
                directory = ?config.directory,
                max_size_mb = config.max_size_mb,
                ttl_seconds = config.ttl_seconds,
                "Initializing query cache"
            );
        }

        // Build moka cache with size and TTL limits
        let cache = Cache::builder()
            .max_capacity(config.max_size_mb * 1024 * 1024) // Convert MB to bytes
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            .weigher(|_key: &String, entry: &CacheEntry| -> u32 {
                // Weight by file size for accurate size-based eviction
                entry.size_bytes.try_into().unwrap_or(u32::MAX)
            })
            .eviction_listener(|key, entry: CacheEntry, cause| {
                // Async cleanup: delete file when evicted
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

        // Hydrate cache from existing files on disk
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

            // Only process .parquet files (skip .tmp files)
            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }

            // Get file metadata
            if let Ok(metadata) = entry.metadata().await {
                let size_bytes = metadata.len();
                let filename = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                // Create cache entry
                let cache_entry = CacheEntry {
                    file_path: path.clone(),
                    size_bytes,
                };

                self.cache.insert(filename.clone(), cache_entry).await;
                hydrated_count += 1;
                total_size += size_bytes;
            }
        }

        // Clean up orphaned .tmp files
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

    /// Try to get cached query results
    pub async fn get(&self, key: &CacheKey) -> Option<Vec<RecordBatch>> {
        if !self.config.enabled {
            return None;
        }

        let filename = key.to_filename();

        // Check moka cache
        let entry = self.cache.get(&filename).await?;

        debug!(
            target: "cache",
            key = %filename,
            size_bytes = entry.size_bytes,
            "Cache hit"
        );

        // Read from disk (defensive: catch all errors)
        match self.read_parquet(&entry.file_path).await {
            Ok(batches) => Some(batches),
            Err(e) => {
                warn!(
                    target: "cache",
                    key = %filename,
                    error = %e,
                    "Failed to read cache file, invalidating entry"
                );
                // Remove corrupted entry
                self.cache.invalidate(&filename).await;
                None
            }
        }
    }

    /// Store query results in cache
    pub async fn put(&self, key: CacheKey, batches: &[RecordBatch]) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Calculate size
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Don't cache empty results
        if row_count == 0 {
            return Ok(());
        }

        let filename = key.to_filename();
        let file_path = self.config.directory.join(&filename);

        // Write to disk (defensive: catch all errors)
        match self.write_parquet(&file_path, batches).await {
            Ok(size_bytes) => {
                // Add to moka cache
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
                Ok(()) // Don't propagate cache write errors
            }
        }
    }

    /// Read RecordBatches from Parquet file (async, non-blocking)
    async fn read_parquet(&self, path: &Path) -> Result<Vec<RecordBatch>> {
        // Clone path for move into blocking task
        let path_buf = path.to_path_buf();

        // Run blocking Parquet I/O in dedicated thread pool
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path_buf)
                .with_context(|| format!("Failed to open cache file: {:?}", path_buf))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .context("Failed to create Parquet reader builder")?;

            let reader = builder.build().context("Failed to build Parquet reader")?;

            let mut batches = Vec::new();
            for batch_result in reader {
                batches.push(batch_result.context("Failed to read record batch")?);
            }

            Ok(batches)
        })
        .await
        .context("Parquet read task panicked")?
    }

    /// Write RecordBatches to Parquet file (async, atomic)
    async fn write_parquet(&self, path: &Path, batches: &[RecordBatch]) -> Result<u64> {
        if batches.is_empty() {
            return Ok(0);
        }

        // Clone data for move into blocking task
        let path_buf = path.to_path_buf();
        let tmp_path = path.with_extension("tmp");
        let batches_owned: Vec<RecordBatch> = batches.to_vec();

        // Run blocking Parquet I/O in dedicated thread pool
        let size = tokio::task::spawn_blocking(move || {
            // Write to temporary file first (atomic write pattern)
            let file = std::fs::File::create(&tmp_path)
                .with_context(|| format!("Failed to create temp cache file: {:?}", tmp_path))?;

            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, batches_owned[0].schema(), Some(props))
                .context("Failed to create Parquet writer")?;

            for batch in &batches_owned {
                writer.write(batch).context("Failed to write batch")?;
            }

            writer.close().context("Failed to close Parquet writer")?;

            // Get file size before rename
            let metadata =
                std::fs::metadata(&tmp_path).context("Failed to get temp file metadata")?;
            let size = metadata.len();

            // Atomic rename (crash-safe)
            std::fs::rename(&tmp_path, &path_buf)
                .with_context(|| format!("Failed to rename {:?} to {:?}", tmp_path, path_buf))?;

            Ok::<u64, anyhow::Error>(size)
        })
        .await
        .context("Parquet write task panicked")??;

        Ok(size)
    }

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
    pub enabled: bool,
    pub entry_count: u64,
    pub weighted_size: u64,
}
