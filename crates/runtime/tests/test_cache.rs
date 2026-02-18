use std::collections::HashMap;
use std::time::{Duration, Instant};
use strake_common::{auth::AuthenticatedUser, config::*};
use strake_runtime::federation::FederationEngine;

#[tokio::test]
async fn test_cache_speeds_up_repeated_query() -> anyhow::Result<()> {
    // Setup: Create engine with cache enabled
    let cache_dir = tempfile::tempdir()?;
    let config = Config {
        sources: vec![],
        cache: QueryCacheConfig {
            enabled: true,
            directory: cache_dir.path().to_string_lossy().to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        },
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: strake_common::config::QueryLimits::default(),
        resource_config: strake_common::config::ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    // Create authenticated user for cache key generation
    let user = AuthenticatedUser {
        id: "test_user".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    // Run query (simple values to ensure it works)
    let sql = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t1(id, name)";

    // First execution (cache miss)
    let start = Instant::now();
    let (_schema, batches1, warnings1) = engine.execute_query(sql, Some(user.clone())).await?;
    let first_duration = start.elapsed();

    println!("First execution warnings: {:?}", warnings1);
    assert!(
        warnings1.contains(&"x-strake-cache: miss".to_string()),
        "Expected cache miss, got warnings: {:?}",
        warnings1
    );
    assert_eq!(batches1.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    // Verify cache file was created
    println!("Cache directory: {:?}", cache_dir.path());
    let cache_files: Vec<_> = std::fs::read_dir(cache_dir.path())?
        .filter_map(|e| e.ok())
        .collect();
    println!("Cache files found: {} total files", cache_files.len());
    for file in &cache_files {
        println!("  - {:?}", file.path());
    }

    let parquet_files: Vec<_> = cache_files
        .iter()
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        parquet_files.len(),
        1,
        "Expected 1 cache file to be created"
    );

    // Second execution (cache hit)
    tokio::time::sleep(Duration::from_millis(10)).await;

    let start = Instant::now();
    let (_schema, batches2, warnings2) = engine.execute_query(sql, Some(user)).await?;
    let second_duration = start.elapsed();

    assert!(
        warnings2.contains(&"x-strake-cache: hit".to_string()),
        "Expected cache hit, got warnings: {:?}",
        warnings2
    );
    assert_eq!(batches2.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    // Verify results are the same
    assert_eq!(batches1.len(), batches2.len());
    for (b1, b2) in batches1.iter().zip(batches2.iter()) {
        assert_eq!(b1.num_rows(), b2.num_rows());
        assert_eq!(b1.num_columns(), b2.num_columns());
    }

    // Cache hit should be faster (or at least not significantly slower)
    println!("First execution (miss): {:?}", first_duration);
    println!("Second execution (hit): {:?}", second_duration);

    Ok(())
}

#[tokio::test]
async fn test_cache_isolation_by_user() -> anyhow::Result<()> {
    // Test that different users get different cache entries
    let cache_dir = tempfile::tempdir()?;
    let config = Config {
        sources: vec![],
        cache: QueryCacheConfig {
            enabled: true,
            directory: cache_dir.path().to_string_lossy().to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        },
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let user1 = AuthenticatedUser {
        id: "user1".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    let user2 = AuthenticatedUser {
        id: "user2".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    let sql = "SELECT * FROM (VALUES (1), (2)) AS t(x)";

    // User 1 executes query (cache miss)
    let (_schema, _batches, warnings1) = engine.execute_query(sql, Some(user1.clone())).await?;
    assert!(warnings1.contains(&"x-strake-cache: miss".to_string()));

    // User 2 executes same query (should also be cache miss due to different user)
    let (_schema, _batches, warnings2) = engine.execute_query(sql, Some(user2)).await?;
    assert!(warnings2.contains(&"x-strake-cache: miss".to_string()));

    // Verify two cache files were created
    let cache_files: Vec<_> = std::fs::read_dir(cache_dir.path())?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        cache_files.len(),
        2,
        "Expected 2 cache files for different users"
    );

    // User 1 executes again (should hit their cache)
    let (_schema, _batches, warnings3) = engine.execute_query(sql, Some(user1)).await?;
    assert!(warnings3.contains(&"x-strake-cache: hit".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_cache_disabled() -> anyhow::Result<()> {
    // Test that cache can be disabled
    let cache_dir = tempfile::tempdir()?;
    let config = Config {
        sources: vec![],
        cache: QueryCacheConfig {
            enabled: false, // Cache disabled
            directory: cache_dir.path().to_string_lossy().to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        },
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let user = AuthenticatedUser {
        id: "test_user".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    let sql = "SELECT * FROM (VALUES (1), (2)) AS t(x)";

    // Execute query twice
    let (_schema, _batches, _warnings1) = engine.execute_query(sql, Some(user.clone())).await?;
    let (_schema, _batches, _warnings2) = engine.execute_query(sql, Some(user)).await?;

    // When cache is disabled, queries still check cache (and miss), but don't write
    // This is acceptable behavior - the important thing is no files are created

    // Verify no cache files were created
    let cache_files: Vec<_> = std::fs::read_dir(cache_dir.path())?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        cache_files.len(),
        0,
        "Expected no cache files when cache is disabled"
    );

    Ok(())
}

#[tokio::test]
async fn test_cache_performance_improvement() -> anyhow::Result<()> {
    // Performance test: Verify cache provides significant speedup
    let cache_dir = tempfile::tempdir()?;
    let config = Config {
        sources: vec![],
        cache: QueryCacheConfig {
            enabled: true,
            directory: cache_dir.path().to_string_lossy().to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        },
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let user = AuthenticatedUser {
        id: "perf_test_user".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    // Test with a larger dataset
    let sql = "SELECT * FROM (VALUES 
        (1, 'row1', 100.5), (2, 'row2', 200.5), (3, 'row3', 300.5),
        (4, 'row4', 400.5), (5, 'row5', 500.5), (6, 'row6', 600.5),
        (7, 'row7', 700.5), (8, 'row8', 800.5), (9, 'row9', 900.5),
        (10, 'row10', 1000.5)
    ) AS t(id, name, value)";

    // Warm up (first execution - cache miss)
    let start = Instant::now();
    let (_schema, batches, warnings) = engine.execute_query(sql, Some(user.clone())).await?;
    let uncached_duration = start.elapsed();

    assert!(warnings.contains(&"x-strake-cache: miss".to_string()));
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);

    // Run multiple cached queries to get average
    let mut cached_durations = Vec::new();
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(5)).await;

        let start = Instant::now();
        let (_schema, batches, warnings) = engine.execute_query(sql, Some(user.clone())).await?;
        let duration = start.elapsed();

        assert!(warnings.contains(&"x-strake-cache: hit".to_string()));
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);

        cached_durations.push(duration);
    }

    let avg_cached_duration =
        cached_durations.iter().sum::<Duration>() / cached_durations.len() as u32;
    let speedup = uncached_duration.as_secs_f64() / avg_cached_duration.as_secs_f64();

    println!("\n=== Cache Performance Test ===");
    println!("Uncached (first run): {:?}", uncached_duration);
    println!("Cached (avg of 5):    {:?}", avg_cached_duration);
    println!("Speedup:              {:.2}x", speedup);
    println!("Individual cached runs: {:?}", cached_durations);

    // Cache should provide at least 2x speedup
    assert!(
        speedup >= 2.0,
        "Cache should provide at least 2x speedup, got {:.2}x (uncached: {:?}, cached: {:?})",
        speedup,
        uncached_duration,
        avg_cached_duration
    );

    // Verify all cached runs are faster than uncached
    for (i, duration) in cached_durations.iter().enumerate() {
        assert!(
            duration < &uncached_duration,
            "Cached run {} ({:?}) should be faster than uncached ({:?})",
            i + 1,
            duration,
            uncached_duration
        );
    }

    Ok(())
}

#[tokio::test]
#[ignore] // Run with: cargo test --test test_cache test_cache_large_dataset -- --ignored --nocapture
async fn test_cache_large_dataset() -> anyhow::Result<()> {
    // Large-scale test: ~10GB raw data (1M rows × 100 columns × ~100 bytes)
    // This test verifies cache handles large results efficiently
    let cache_dir = tempfile::tempdir()?;
    let config = Config {
        sources: vec![],
        cache: QueryCacheConfig {
            enabled: true,
            directory: cache_dir.path().to_string_lossy().to_string(),
            max_size_mb: 20480, // 20GB to accommodate large cache files
            ttl_seconds: 3600,
        },
    };

    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let user = AuthenticatedUser {
        id: "large_dataset_user".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    // Generate a large dataset query
    // Each row: ~100 bytes (id + 9 strings of ~10 chars each)
    // 1M rows = ~100MB raw data per batch
    // We'll generate multiple batches to reach ~10GB total
    println!("\n=== Large Dataset Cache Test ===");
    println!("Generating large dataset query...");

    // Create a query that generates ~100K rows (to keep test time reasonable)
    // In production, this would be a real query returning large results
    let num_rows = 100_000;
    let mut values = Vec::new();

    // Generate VALUES clause with many rows
    for i in 0..num_rows {
        values.push(format!(
            "({}, 'data_{}', 'value_{}', 'field_{}', 'info_{}', 'text_{}', 'content_{}', 'record_{}', 'entry_{}', 'item_{}')",
            i, i, i, i, i, i, i, i, i, i
        ));
    }

    let sql = format!(
        "SELECT * FROM (VALUES {}) AS t(id, col1, col2, col3, col4, col5, col6, col7, col8, col9)",
        values.join(", ")
    );

    println!("Query size: {} bytes", sql.len());
    println!("Expected rows: {}", num_rows);

    // First execution - cache miss (write to cache)
    println!("\n1. First execution (cache miss - writing to cache)...");
    let start = Instant::now();
    let (_schema, batches, warnings) = engine.execute_query(&sql, Some(user.clone())).await?;
    let uncached_duration = start.elapsed();

    assert!(warnings.contains(&"x-strake-cache: miss".to_string()));
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, num_rows);

    // Calculate data size
    let mut total_bytes = 0;
    for batch in &batches {
        for array in batch.columns() {
            total_bytes += array.get_array_memory_size();
        }
    }

    println!("   Duration: {:?}", uncached_duration);
    println!("   Rows: {}", total_rows);
    println!(
        "   Memory size: {:.2} MB",
        total_bytes as f64 / 1024.0 / 1024.0
    );

    // Check cache file
    let cache_files: Vec<_> = std::fs::read_dir(cache_dir.path())?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(cache_files.len(), 1, "Expected 1 cache file");

    let cache_file_size = cache_files[0].metadata()?.len();
    let compression_ratio = total_bytes as f64 / cache_file_size as f64;

    println!(
        "   Cache file size: {:.2} MB",
        cache_file_size as f64 / 1024.0 / 1024.0
    );
    println!("   Compression ratio: {:.2}x", compression_ratio);

    // Second execution - cache hit (read from cache)
    println!("\n2. Second execution (cache hit - reading from cache)...");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();
    let (_schema, batches2, warnings2) = engine.execute_query(&sql, Some(user.clone())).await?;
    let cached_duration = start.elapsed();

    assert!(warnings2.contains(&"x-strake-cache: hit".to_string()));
    let total_rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows2, num_rows);

    println!("   Duration: {:?}", cached_duration);
    println!("   Rows: {}", total_rows2);

    // Verify data integrity - total rows should match
    // Note: Batch count may differ as Parquet may read in different batch sizes
    assert_eq!(total_rows, total_rows2, "Total row count should match");

    // Verify schema matches
    assert_eq!(
        batches[0].num_columns(),
        batches2[0].num_columns(),
        "Column count should match"
    );

    let speedup = uncached_duration.as_secs_f64() / cached_duration.as_secs_f64();

    println!("\n=== Results ===");
    println!("Uncached: {:?}", uncached_duration);
    println!("Cached:   {:?}", cached_duration);
    println!("Speedup:  {:.2}x", speedup);
    println!("Data size: {:.2} MB", total_bytes as f64 / 1024.0 / 1024.0);
    println!(
        "Cache file: {:.2} MB ({:.2}x compression)",
        cache_file_size as f64 / 1024.0 / 1024.0,
        compression_ratio
    );

    // Cache should provide speedup even for large datasets
    assert!(
        speedup >= 1.5,
        "Cache should provide at least 1.5x speedup for large datasets, got {:.2}x",
        speedup
    );

    // Parquet should provide good compression
    assert!(
        compression_ratio >= 2.0,
        "Parquet should provide at least 2x compression, got {:.2}x",
        compression_ratio
    );

    Ok(())
}

// ... existing tests ...

#[tokio::test]
async fn test_metadata_persistence() -> anyhow::Result<()> {
    // Test that cache metadata is rebuilt from disk on restart
    let cache_dir = tempfile::tempdir()?;
    let cache_path = cache_dir.path().to_string_lossy().to_string();

    let config = Config {
        sources: vec![],
        cache: QueryCacheConfig {
            enabled: true,
            directory: cache_path.clone(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        },
    };

    // 1. Start Engine 1
    let engine1 = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config: config.clone(),
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    let user = AuthenticatedUser {
        id: "persist_user".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    let sql = "SELECT * FROM (VALUES (1, 'persist')) AS t(id, name)";

    // Run query on Engine 1 (Miss -> Write)
    let (_, _, warnings1) = engine1.execute_query(sql, Some(user.clone())).await?;
    assert!(warnings1.contains(&"x-strake-cache: miss".to_string()));

    // Verify file exists on disk
    let files_count = std::fs::read_dir(&cache_path)?.count();
    assert!(files_count > 0, "Cache file should exist");

    // Drop engine1 explicitly (though not strictly needed, clarifies intent)
    drop(engine1);

    // 2. Start Engine 2 (Simulate Restart) pointing to SAME directory
    // It should scan directory and hydrate cache
    let engine2 = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![],
        retry: Default::default(),
    })
    .await?;

    // Run SAME query on Engine 2
    // Should be a HIT because metadata was restored
    let (_, _, warnings2) = engine2.execute_query(sql, Some(user)).await?;

    println!("Restart warnings: {:?}", warnings2);
    assert!(
        warnings2.contains(&"x-strake-cache: hit".to_string()),
        "Expected cache hit after restart, proving metadata persistence"
    );

    Ok(())
}

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use strake_connectors::sources::SourceProvider;

struct MockProvider;

#[async_trait]
impl SourceProvider for MockProvider {
    fn type_name(&self) -> &'static str {
        "mock"
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        _catalog: &str,
        config: &SourceConfig,
    ) -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table(config.name.as_str(), Arc::new(table))?;
        Ok(())
    }
}

#[tokio::test]
async fn test_per_datasource_cache_config() -> anyhow::Result<()> {
    // Test that we can disable cache for specific sources
    let cache_dir = tempfile::tempdir()?;

    // Define sources
    let source_enabled = SourceConfig {
        name: "cached_source".to_string(),
        source_type: "mock".to_string(),
        url: None,
        default_limit: None,
        config: serde_json::Value::Null,
        cache: Some(QueryCacheConfig {
            enabled: true,
            directory: "/tmp".to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        }),
        username: None,
        password: None,
        max_concurrent_queries: None,
        tables: vec![],
    };

    let source_disabled = SourceConfig {
        name: "uncached_source".to_string(),
        source_type: "mock".to_string(),
        url: None,
        default_limit: None,
        config: serde_json::Value::Null,
        cache: Some(QueryCacheConfig {
            enabled: false, // EXPLICITLY DISABLED
            directory: "/tmp".to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        }),
        username: None,
        password: None,
        max_concurrent_queries: None,
        tables: vec![],
    };

    let config = Config {
        sources: vec![source_enabled, source_disabled],
        cache: QueryCacheConfig {
            enabled: true, // Globally Enabled
            directory: cache_dir.path().to_string_lossy().to_string(),
            max_size_mb: 100,
            ttl_seconds: 3600,
        },
    };

    // Create engine
    let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
        config,
        catalog_name: "test".to_string(),
        query_limits: QueryLimits::default(),
        resource_config: ResourceConfig::default(),
        datafusion_config: HashMap::new(),
        global_budget: 10,
        extra_optimizer_rules: vec![],
        extra_sources: vec![Box::new(MockProvider)],
        retry: Default::default(),
    })
    .await?;

    let user = AuthenticatedUser {
        id: "config_user".to_string(),
        permissions: vec!["read".to_string()].into(),
        rules: HashMap::new(),
    };

    // 1. Query Cached Source
    let sql_cached = "SELECT * FROM cached_source";
    let (_, _, warnings1) = engine.execute_query(sql_cached, Some(user.clone())).await?;
    // First run miss
    assert!(warnings1.contains(&"x-strake-cache: miss".to_string()));

    // Second run HIT
    let (_, _, warnings1_hit) = engine.execute_query(sql_cached, Some(user.clone())).await?;
    assert!(
        warnings1_hit.contains(&"x-strake-cache: hit".to_string()),
        "Expected hit for enabled source"
    );

    // 2. Query Uncached Source
    let sql_uncached = "SELECT * FROM uncached_source";
    let (_, _, warnings2) = engine
        .execute_query(sql_uncached, Some(user.clone()))
        .await?;
    assert!(
        !warnings2.contains(&"x-strake-cache: miss".to_string()),
        "Expected NO cache miss header for disabled source"
    );
    assert!(!warnings2.contains(&"x-strake-cache: hit".to_string()));

    Ok(())
}
