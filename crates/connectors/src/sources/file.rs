//! # File-based Data Sources
//!
//! Discovery and registration of Parquet, CSV, and JSON tables via
//! DataFusion's `ListingTable` with OpenDAL-backed object storage.
//!
//! ## Overview
//!
//! Supports multiple URI schemes (s3, gs, azblob, http, ftp, sftp) via
//! OpenDAL abstraction. Parquet tables support predicate caching;
//! CSV/JSON do not (row-group metadata unavailable).
//!
//! ## Usage
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//! use strake_connectors::sources::SourceProvider;
//! use strake_connectors::sources::file::FileSourceProvider;
//! use strake_common::predicate_cache::PredicateCache;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//! let provider = FileSourceProvider {
//!     predicate_cache: Arc::new(PredicateCache::new()),
//! };
//! assert_eq!(provider.type_name(), "file");
//! # Ok(())
//! # }
//! ```
//!
//! ## Errors
//!
//! - `SourceError::UnsupportedType` if the source type is not Parquet/CSV/JSON.
//! - `SourceError::InvalidUrl` if the provided path is a malformed URI.
//! - `anyhow::Error` for OpenDAL operator construction failures.
//! - `DataFusionError` for schema inference or table registration failures.
//!
//! ## Performance Characteristics
//!
//! - **Cache Latency**: Predicate caching adds ~1-2ms overhead per query but can
//!   save seconds by skipping Parquet row groups.
//! - **Throughput**: Listing large object storage prefixes is limited by OpenDAL
//!   provider latency and network bandwidth.
//!
//! ## Safety
//!
//! This module does not use `unsafe` code.

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;

use crate::sources::SourceProvider;
use crate::sources::ensure_schema;
use strake_common::config::{ColumnConfig, SourceConfig, TableConfig};
use thiserror::Error;
use url::Url;

/// Errors that can occur when discovering or registering file-based sources.
#[derive(Error, Debug)]
pub enum SourceError {
    /// The specified source type (e.g., Avro) is not supported.
    #[error("Unsupported file source type: {0}")]
    UnsupportedType(String),
    /// The provided path is not a valid URL.
    #[error("Invalid source URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
}

/// Provider for file-based sources (Parquet, CSV, JSON).
///
/// Discovers and registers tables from local or remote object storage
/// (S3, GCS, Azure, etc.) using OpenDAL.
pub struct FileSourceProvider {
    /// Shared predicate cache for Parquet sources.
    pub predicate_cache: Arc<strake_common::predicate_cache::PredicateCache>,
}

#[async_trait]
impl SourceProvider for FileSourceProvider {
    fn type_name(&self) -> &'static str {
        "file"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        use strake_common::models::SourceType;
        let mut source_type = config.source_type.clone();
        if let SourceType::Other(s) = &source_type
            && s == "file"
            && let Some(format_val) = config
                .config
                .get("format")
                .or_else(|| config.config.get("source_type"))
                .and_then(|v| v.as_str())
        {
            use std::str::FromStr;
            let st = SourceType::from_str(format_val).map_err(|e| {
                anyhow::anyhow!(
                    "Cannot infer file source type from format '{}': {}. \
                     Expected one of: parquet, csv, json",
                    format_val,
                    e
                )
            })?;
            source_type = st;
        }

        match &source_type {
            SourceType::Parquet => {
                #[derive(serde::Deserialize)]
                struct ParquetConfig {
                    #[serde(alias = "path")]
                    #[serde(alias = "connection")]
                    url: Option<String>,
                    #[serde(default)]
                    options: Option<HashMap<String, String>>,
                    #[serde(default)]
                    tables: Option<Vec<TableConfig>>,
                }
                let cfg: ParquetConfig = serde_json::from_value(config.config.clone())
                    .context("Failed to parse Parquet source configuration")?;

                let path = config.url.clone()
                    .or_else(|| cfg.url.clone())
                    .context("Parquet source path/URL is required (specify top-level 'url' or nested 'path')")?;

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &path, cfg.options.unwrap_or_default()).await?;
                register_parquet(ParquetRegistration {
                    context,
                    catalog: catalog_name,
                    name: config.name.as_ref(),
                    path: &path,
                    tables_config: &tables,
                    cache: self.predicate_cache.clone(),
                    predicate_cache_enabled: config.predicate_cache,
                    metadata_cache_capacity: config
                        .cache
                        .as_ref()
                        .map(|c| c.metadata_cache_capacity)
                        .unwrap_or_else(strake_common::models::default_metadata_cache_capacity),
                })
                .await
            }
            SourceType::Csv => {
                #[derive(serde::Deserialize)]
                struct CsvConfig {
                    #[serde(alias = "path")]
                    #[serde(alias = "connection")]
                    url: Option<String>,
                    #[serde(default)]
                    options: Option<HashMap<String, String>>,
                    #[serde(default)]
                    has_header: bool,
                    delimiter: Option<char>,
                    #[serde(default)]
                    tables: Option<Vec<TableConfig>>,
                }
                let cfg: CsvConfig = serde_json::from_value(config.config.clone())
                    .context("Failed to parse CSV source configuration")?;

                let path = config.url.clone().or_else(|| cfg.url.clone()).context(
                    "CSV source path/URL is required (specify top-level 'url' or nested 'path')",
                )?;

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &path, cfg.options.unwrap_or_default()).await?;
                register_csv(
                    context,
                    catalog_name,
                    config.name.as_ref(),
                    &path,
                    cfg.has_header,
                    cfg.delimiter,
                    &tables,
                )
                .await
            }
            SourceType::Json => {
                #[derive(serde::Deserialize)]
                struct JsonConfig {
                    #[serde(alias = "path")]
                    #[serde(alias = "connection")]
                    url: Option<String>,
                    #[serde(default)]
                    options: Option<HashMap<String, String>>,
                    #[serde(default)]
                    tables: Option<Vec<TableConfig>>,
                }
                let cfg: JsonConfig = serde_json::from_value(config.config.clone())
                    .context("Failed to parse JSON source configuration")?;

                let path = config.url.clone().or_else(|| cfg.url.clone()).context(
                    "JSON source path/URL is required (specify top-level 'url' or nested 'path')",
                )?;

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &path, cfg.options.unwrap_or_default()).await?;
                register_json(context, catalog_name, config.name.as_ref(), &path, &tables).await
            }
            _ => Err(SourceError::UnsupportedType(source_type.to_string()).into()),
        }
    }
}

/// Registers an OpenDAL-backed object store for the given URI scheme.
///
/// # Errors
/// Returns `Err` if the URL is malformed or if the operator cannot be built
/// from the provided options.
pub async fn register_object_store(
    ctx: &SessionContext,
    path: &str,
    options: HashMap<String, String>,
) -> Result<()> {
    let url = match Url::parse(path) {
        Ok(url) => url,
        Err(e) => {
            // If it's a relative path or doesn't look like a URL, it might be a local file.
            // But we should only ignore it if it's clearly not intended to be a URL.
            if path.contains("://") {
                return Err(e).context(format!("Invalid source URL: {path}"));
            }
            return Ok(());
        }
    };
    let scheme = url.scheme();
    let bucket = url.host_str().unwrap_or_default();

    // OpenDAL supports building via a HashMap map directly.
    let mut map = HashMap::new();
    // Map common options
    map.extend(options);

    let op_scheme = match scheme {
        "s3" => {
            map.insert("bucket".to_string(), bucket.to_string());
            "s3"
        }
        "az" | "azblob" => {
            map.insert("container".to_string(), bucket.to_string());
            "azblob"
        }
        "gs" | "gcs" => {
            map.insert("bucket".to_string(), bucket.to_string());
            "gcs"
        }
        "http" | "https" => {
            map.insert("endpoint".to_string(), path.to_string());
            "http"
        }
        "ftp" | "ftps" => {
            map.insert(
                "endpoint".to_string(),
                format!("{}://{}:{}", scheme, bucket, url.port().unwrap_or(21)),
            );
            if !url.username().is_empty() {
                map.insert("user".to_string(), url.username().to_string());
            }
            if let Some(password) = url.password() {
                map.insert("password".to_string(), password.to_string());
            }
            "ftp"
        }
        "sftp" => {
            #[cfg(not(unix))]
            {
                anyhow::bail!("SFTP is only supported on Unix systems");
            }

            #[cfg(unix)]
            {
                map.insert(
                    "endpoint".to_string(),
                    format!("ssh://{}:{}", bucket, url.port().unwrap_or(22)),
                );
                if !url.username().is_empty() {
                    map.insert("user".to_string(), url.username().to_string());
                }
                if let Some(password) = url.password() {
                    map.insert("password".to_string(), password.to_string());
                }
                "sftp"
            }
        }
        _ => return Ok(()),
    };

    let op = opendal::Operator::via_iter(op_scheme, map)?;

    let store = object_store_opendal::OpendalStore::new(op);

    let mut store_url = url.clone();
    store_url.set_path("");
    store_url.set_query(None);
    store_url.set_fragment(None);

    ctx.register_object_store(&store_url, Arc::new(store));

    Ok(())
}

/// Registration arguments for registering a Parquet source provider.
pub struct ParquetRegistration<'a> {
    /// DataFusion session context
    pub context: &'a SessionContext,
    /// Catalog name target
    pub catalog: &'a str,
    /// Name of the source
    pub name: &'a str,
    /// URI/path pointing to the Parquet file or directory
    pub path: &'a str,
    /// Tables schema and partition projections configuration
    pub tables_config: &'a Option<Vec<TableConfig>>,
    /// Shared predicate cache implementation
    pub cache: Arc<strake_common::predicate_cache::PredicateCache>,
    /// Whether predicate caching is enabled
    pub predicate_cache_enabled: bool,
    /// Limit on the maximum number of items inside metadata cache
    pub metadata_cache_capacity: usize,
}

/// Registers Parquet tables with the DataFusion context.
///
/// Supports predicate caching if enabled.
///
/// # Errors
/// Returns `Err` if the schema cannot be inferred or if table registration fails.
pub async fn register_parquet(reg: ParquetRegistration<'_>) -> Result<()> {
    use crate::sources::predicate_caching::CachingTableProvider;
    use sha2::{Digest, Sha256};

    let mut parquet_options = datafusion::config::TableParquetOptions::default();
    parquet_options.global.pushdown_filters = true;
    parquet_options.global.reorder_filters = true;
    parquet_options.global.pruning = true;
    parquet_options.global.enable_page_index = true;
    parquet_options.global.bloom_filter_on_read = true;

    let file_format = ParquetFormat::default().with_options(parquet_options);
    let listing_options = ListingOptions::new(Arc::new(file_format));

    let start_url = ListingTableUrl::parse(reg.path)?;

    if let Some(tables) = reg.tables_config {
        for table_cfg in tables {
            let table_path = table_cfg.path.as_deref().unwrap_or(reg.path);
            let table_url = ListingTableUrl::parse(table_path)?;
            let resolved_schema = if !table_cfg.column_definitions.is_empty() {
                build_schema_from_config(&table_cfg.column_definitions)?
            } else {
                listing_options
                    .infer_schema(&reg.context.state(), &table_url)
                    .await?
            };

            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options.clone())
                .with_schema(resolved_schema);
            let provider = Arc::new(ListingTable::try_new(config)?);

            let snapshot_id = if let Some(id) = table_cfg.snapshot_id {
                id
            } else {
                let mut hasher = Sha256::new();
                hasher.update(table_path.as_bytes());
                hasher.update(table_cfg.name.as_bytes());
                let result = hasher.finalize();
                // NOTE: Using stable SHA-256 hash truncated to 64-bits as a cache key.
                // This format must remain stable to avoid cache invalidation across restarts.
                // If the key format changes in future versions, a version prefix should be added.
                let bytes: [u8; 8] = result[0..8].try_into().map_err(|_| {
                    anyhow::anyhow!("Failed to truncate SHA-256 hash for snapshot_id")
                })?;
                u64::from_le_bytes(bytes) as i64
            };
            let provider: Arc<dyn TableProvider> = Arc::new(CachingTableProvider::new(
                provider,
                reg.cache.clone(),
                snapshot_id,
                reg.predicate_cache_enabled,
                reg.metadata_cache_capacity,
            ));

            let schema_name = if table_cfg.schema.is_empty() {
                "public"
            } else {
                &table_cfg.schema
            };
            tracing::debug!(
                "Registering table {} in catalog {} schema {}",
                table_cfg.name,
                reg.catalog,
                schema_name
            );
            let schema_provider = ensure_schema(reg.context, reg.catalog, schema_name)?;
            schema_provider.register_table(table_cfg.name.to_string(), provider)?;
            tracing::debug!("Successfully registered table {}", table_cfg.name);
        }
    } else {
        let resolved_schema = listing_options
            .infer_schema(&reg.context.state(), &start_url)
            .await?;
        let config = ListingTableConfig::new(start_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = Arc::new(ListingTable::try_new(config)?);

        let mut hasher = Sha256::new();
        hasher.update(reg.path.as_bytes());
        hasher.update(reg.name.as_bytes());
        let result = hasher.finalize();
        // NOTE: Using stable SHA-256 hash truncated to 64-bits as a cache key.
        // If the key format changes in future versions, a version prefix should be added.
        let bytes: [u8; 8] = result[0..8]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Failed to truncate SHA-256 hash for snapshot_id"))?;
        let snapshot_id = u64::from_le_bytes(bytes) as i64;

        let wrapped = Arc::new(CachingTableProvider::new(
            provider,
            reg.cache,
            snapshot_id,
            reg.predicate_cache_enabled,
            reg.metadata_cache_capacity,
        ));

        let basename = reg
            .path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default();
        let stem = basename.rsplit_once('.').map_or(basename, |(s, _)| s);
        let table_name = if stem.is_empty() || stem.contains('*') || stem.contains('?') {
            reg.name
        } else {
            stem
        };
        let schema_provider = ensure_schema(reg.context, reg.catalog, reg.name)?;
        schema_provider.register_table(table_name.to_string(), wrapped)?;
    }

    Ok(())
}

/// Registers CSV tables with the DataFusion context.
///
/// # Errors
/// Returns `Err` if the schema cannot be inferred or if table registration fails.
pub async fn register_csv(
    context: &SessionContext,
    catalog: &str,
    name: &str,
    path: &str,
    has_header: bool,
    delimiter: Option<char>,
    tables_config: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let mut file_format = CsvFormat::default().with_has_header(has_header);
    if let Some(d) = delimiter {
        file_format = file_format.with_delimiter(d as u8);
    }

    let listing_options = ListingOptions::new(Arc::new(file_format));
    let start_url = ListingTableUrl::parse(path)?;

    if let Some(tables) = tables_config {
        for table_cfg in tables {
            let table_path = table_cfg.path.as_deref().unwrap_or(path);
            let table_url = ListingTableUrl::parse(table_path)?;
            let resolved_schema = if !table_cfg.column_definitions.is_empty() {
                build_schema_from_config(&table_cfg.column_definitions)?
            } else {
                listing_options
                    .infer_schema(&context.state(), &table_url)
                    .await?
            };

            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options.clone())
                .with_schema(resolved_schema);
            let provider = Arc::new(ListingTable::try_new(config)?);

            let schema_name = if table_cfg.schema.is_empty() {
                "public"
            } else {
                &table_cfg.schema
            };
            let schema_provider = ensure_schema(context, catalog, schema_name)?;
            schema_provider.register_table(table_cfg.name.to_string(), provider)?;
        }
    } else {
        let resolved_schema = listing_options
            .infer_schema(&context.state(), &start_url)
            .await?;
        let config = ListingTableConfig::new(start_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = Arc::new(ListingTable::try_new(config)?);

        let basename = path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default();
        let stem = basename.rsplit_once('.').map_or(basename, |(s, _)| s);
        let table_name = if stem.is_empty() || stem.contains('*') || stem.contains('?') {
            name
        } else {
            stem
        };
        let schema_provider = ensure_schema(context, catalog, name)?;
        schema_provider.register_table(table_name.to_string(), provider)?;
    }

    Ok(())
}

/// Registers JSON tables with the DataFusion context.
///
/// # Errors
/// Returns `Err` if the schema cannot be inferred or if table registration fails.
pub async fn register_json(
    context: &SessionContext,
    catalog: &str,
    name: &str,
    path: &str,
    tables_config: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let file_format = JsonFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format));

    let start_url = ListingTableUrl::parse(path)?;

    if let Some(tables) = tables_config {
        for table_cfg in tables {
            let table_path = table_cfg.path.as_deref().unwrap_or(path);
            let table_url = ListingTableUrl::parse(table_path)?;
            let resolved_schema = if !table_cfg.column_definitions.is_empty() {
                build_schema_from_config(&table_cfg.column_definitions)?
            } else {
                listing_options
                    .infer_schema(&context.state(), &table_url)
                    .await?
            };

            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options.clone())
                .with_schema(resolved_schema);
            let provider = Arc::new(ListingTable::try_new(config)?);

            let schema_name = if table_cfg.schema.is_empty() {
                "public"
            } else {
                &table_cfg.schema
            };
            let schema_provider = ensure_schema(context, catalog, schema_name)?;
            schema_provider.register_table(table_cfg.name.to_string(), provider)?;
        }
    } else {
        let resolved_schema = listing_options
            .infer_schema(&context.state(), &start_url)
            .await?;
        let config = ListingTableConfig::new(start_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = Arc::new(ListingTable::try_new(config)?);

        let basename = path
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default();
        let stem = basename.rsplit_once('.').map_or(basename, |(s, _)| s);
        let table_name = if stem.is_empty() || stem.contains('*') || stem.contains('?') {
            name
        } else {
            stem
        };
        let schema_provider = ensure_schema(context, catalog, name)?;
        schema_provider.register_table(table_name.to_string(), provider)?;
    }

    Ok(())
}

/// Builds an Arrow schema from the provided column configurations.
///
/// # Errors
/// Returns `Err` if an unsupported data type is encountered or if decimal
/// precision/scale is invalid.
pub fn build_schema_from_config(
    columns: &[ColumnConfig],
) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|c| {
            let dt = match c.data_type.to_lowercase().as_str() {
                "tinyint" | "int1" => DataType::Int8,
                "smallint" | "int2" => DataType::Int16,
                "int" | "integer" | "int4" => DataType::Int32,
                "bigint" | "int8" => DataType::Int64,
                "float" | "double" | "float8" | "double precision" => DataType::Float64,
                "real" | "float4" => DataType::Float32,
                "string" | "text" | "varchar" | "char" => DataType::Utf8,
                "bool" | "boolean" => DataType::Boolean,
                "date" => DataType::Date32,
                "decimal" => {
                    let precision = c.precision.unwrap_or(15);
                    if precision == 0 {
                        anyhow::bail!(
                            "Decimal precision must be at least 1 for column '{}'",
                            c.name
                        );
                    }
                    let scale = c.scale.unwrap_or(2);
                    if scale > precision {
                        anyhow::bail!(
                            "Invalid decimal spec for column '{}': scale ({}) > precision ({})",
                            c.name,
                            scale,
                            precision
                        );
                    }
                    let scale_i8 = i8::try_from(scale).map_err(|_| {
                        anyhow::anyhow!(
                            "Decimal scale {} exceeds i8 range for column '{}'",
                            scale,
                            c.name
                        )
                    })?;

                    if precision <= 9 {
                        DataType::Decimal32(precision, scale_i8)
                    } else if precision <= 18 {
                        DataType::Decimal64(precision, scale_i8)
                    } else {
                        DataType::Decimal128(precision, scale_i8)
                    }
                }
                other => anyhow::bail!("Unsupported data type: {} for column '{}'", other, c.name),
            };
            let nullable = !c.not_null;

            let mut metadata: HashMap<String, String> = HashMap::new();
            if let Some(len) = c.length {
                metadata.insert("precision".to_string(), len.to_string());
                metadata.insert("characterMaximumLength".to_string(), len.to_string());
            }

            let field = Field::new(&c.name, dt, nullable).with_metadata(metadata);
            Ok(field)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn test_build_schema_decimals() {
        let cols = vec![
            {
                let mut c = ColumnConfig::default();
                c.name = "d32".into();
                c.data_type = "decimal".into();
                c.precision = Some(9);
                c.scale = Some(2);
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "d64".into();
                c.data_type = "decimal".into();
                c.precision = Some(18);
                c.scale = Some(2);
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "d128".into();
                c.data_type = "decimal".into();
                c.precision = Some(38);
                c.scale = Some(2);
                c
            },
        ];

        let schema = build_schema_from_config(&cols).unwrap();
        assert_eq!(
            *schema.field_with_name("d32").unwrap().data_type(),
            DataType::Decimal32(9, 2)
        );
        assert_eq!(
            *schema.field_with_name("d64").unwrap().data_type(),
            DataType::Decimal64(18, 2)
        );
        assert_eq!(
            *schema.field_with_name("d128").unwrap().data_type(),
            DataType::Decimal128(38, 2)
        );
    }

    #[test]
    fn test_build_schema_types() {
        let cols = vec![
            {
                let mut c = ColumnConfig::default();
                c.name = "i".into();
                c.data_type = "int".into();
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "bi".into();
                c.data_type = "bigint".into();
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "s".into();
                c.data_type = "string".into();
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "f".into();
                c.data_type = "float".into();
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "b".into();
                c.data_type = "bool".into();
                c
            },
            {
                let mut c = ColumnConfig::default();
                c.name = "d".into();
                c.data_type = "date".into();
                c
            },
        ];

        let schema = build_schema_from_config(&cols).unwrap();
        assert_eq!(
            *schema.field_with_name("i").unwrap().data_type(),
            DataType::Int32
        );
        assert_eq!(
            *schema.field_with_name("bi").unwrap().data_type(),
            DataType::Int64
        );
        assert_eq!(
            *schema.field_with_name("s").unwrap().data_type(),
            DataType::Utf8
        );
        assert_eq!(
            *schema.field_with_name("f").unwrap().data_type(),
            DataType::Float64
        );
        assert_eq!(
            *schema.field_with_name("b").unwrap().data_type(),
            DataType::Boolean
        );
        assert_eq!(
            *schema.field_with_name("d").unwrap().data_type(),
            DataType::Date32
        );
    }

    #[tokio::test]
    async fn test_register_object_store_url_parsing() {
        let ctx = SessionContext::new();

        // Happy path: Valid HTTP URL
        let res = register_object_store(&ctx, "http://example.com/path", HashMap::new()).await;
        assert!(res.is_ok(), "HTTP registration failed: {:?}", res.err());

        // Happy path: Local file path (should be ignored)
        let res = register_object_store(&ctx, "/tmp/local_file", HashMap::new()).await;
        assert!(
            res.is_ok(),
            "Local path registration failed: {:?}",
            res.err()
        );

        // Bad path: Malformed URL with scheme
        let res = register_object_store(&ctx, "s3://[invalid]", HashMap::new()).await;
        assert!(res.is_err(), "Malformed S3 URL should fail");
        assert!(res.unwrap_err().to_string().contains("Invalid source URL"));

        // Edge case: Unsupported scheme (should be ignored)
        let res = register_object_store(&ctx, "unknown://bucket", HashMap::new()).await;
        assert!(res.is_ok(), "Unsupported scheme should be ignored");

        // Edge case: URL-like path that is malformed
        let res = register_object_store(&ctx, "http://:invalid", HashMap::new()).await;
        assert!(res.is_err(), "Malformed HTTP URL should fail");
    }
}
