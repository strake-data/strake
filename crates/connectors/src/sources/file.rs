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
//! ## Safety
//!
//! No `unsafe` blocks. SFTP support is conditionally compiled for Unix only.
//!
//! ## Errors
//!
//! - `SourceError`: Structured error type for configuration and source failures.
//! - `anyhow::Error`: For non-recoverable internal or wrapper errors.
//! - `DataFusionError`: Schema inference or registration failures.

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

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Unsupported file source type: {0}")]
    UnsupportedType(String),
    #[error("Invalid source URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum DataTypeConfig {
    #[serde(alias = "integer")]
    Int,
    BigInt,
    #[serde(alias = "string", alias = "text", alias = "char")]
    Varchar,
    #[serde(alias = "double")]
    Float,
    #[serde(alias = "bool")]
    Boolean,
    Date,
    Decimal,
}

/// Provider for file-based sources (Parquet, etc.).
pub struct FileSourceProvider {
    /// Shared predicate cache.
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
        match &config.source_type {
            SourceType::Parquet => {
                #[derive(serde::Deserialize)]
                struct ParquetConfig {
                    path: String,
                    #[serde(default)]
                    options: Option<HashMap<String, String>>,
                    #[serde(default)]
                    tables: Option<Vec<TableConfig>>,
                }
                let cfg: ParquetConfig = serde_json::from_value(config.config.clone())
                    .context("Failed to parse Parquet source configuration")?;

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &cfg.path, cfg.options.unwrap_or_default()).await?;
                register_parquet(
                    context,
                    catalog_name,
                    config.name.as_ref(),
                    &cfg.path,
                    &tables,
                    self.predicate_cache.clone(),
                    config.predicate_cache,
                )
                .await
            }
            SourceType::Csv => {
                #[derive(serde::Deserialize)]
                struct CsvConfig {
                    path: String,
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

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &cfg.path, cfg.options.unwrap_or_default()).await?;
                register_csv(
                    context,
                    catalog_name,
                    config.name.as_ref(),
                    &cfg.path,
                    cfg.has_header,
                    cfg.delimiter,
                    &tables,
                )
                .await
            }
            SourceType::Json => {
                #[derive(serde::Deserialize)]
                struct JsonConfig {
                    path: String,
                    #[serde(default)]
                    options: Option<HashMap<String, String>>,
                    #[serde(default)]
                    tables: Option<Vec<TableConfig>>,
                }
                let cfg: JsonConfig = serde_json::from_value(config.config.clone())
                    .context("Failed to parse JSON source configuration")?;

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &cfg.path, cfg.options.unwrap_or_default()).await?;
                register_json(
                    context,
                    catalog_name,
                    config.name.as_ref(),
                    &cfg.path,
                    &tables,
                )
                .await
            }
            _ => Err(SourceError::UnsupportedType(config.source_type.to_string()).into()),
        }
    }
}

/// Helper to register object stores based on URI scheme.
pub async fn register_object_store(
    ctx: &SessionContext,
    path: &str,
    options: HashMap<String, String>,
) -> Result<()> {
    let url = match Url::parse(path) {
        Ok(url) => url,
        Err(_) => return Ok(()), // Likely a local file path
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

/// Register Parquet tables with DataFusion context
pub async fn register_parquet(
    context: &SessionContext,
    catalog: &str,
    name: &str,
    path: &str,
    tables_config: &Option<Vec<TableConfig>>,
    cache: Arc<strake_common::predicate_cache::PredicateCache>,
    predicate_cache_enabled: bool,
) -> Result<()> {
    use crate::sources::predicate_caching::CachingTableProvider;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let file_format = ParquetFormat::default();
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

            let snapshot_id = table_cfg.snapshot_id.unwrap_or_else(|| {
                let mut hasher = DefaultHasher::new();
                table_path.hash(&mut hasher);
                table_cfg.name.hash(&mut hasher);
                hasher.finish() as i64
            });
            let provider: Arc<dyn TableProvider> = if predicate_cache_enabled {
                Arc::new(CachingTableProvider::new(
                    provider,
                    cache.clone(),
                    snapshot_id,
                ))
            } else {
                provider
            };

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

        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        name.hash(&mut hasher);
        let snapshot_id = hasher.finish() as i64;

        let wrapped = Arc::new(CachingTableProvider::new(provider, cache, snapshot_id));

        let schema_provider = ensure_schema(context, catalog, "public")?;
        schema_provider.register_table(name.to_string(), wrapped)?;
    }

    Ok(())
}

/// Register CSV tables with DataFusion context
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

        let schema_provider = ensure_schema(context, catalog, "public")?;
        schema_provider.register_table(name.to_string(), provider)?;
    }

    Ok(())
}

/// Register JSON tables with DataFusion context
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

        let schema_provider = ensure_schema(context, catalog, "public")?;
        schema_provider.register_table(name.to_string(), provider)?;
    }

    Ok(())
}

fn build_schema_from_config(
    columns: &[ColumnConfig],
) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|c| {
            let config_type: DataTypeConfig =
                serde_json::from_str(&format!("\"{}\"", c.data_type.to_lowercase()))
                    .unwrap_or(DataTypeConfig::Varchar);

            let dt = match config_type {
                DataTypeConfig::Int => DataType::Int32,
                DataTypeConfig::BigInt => DataType::Int64,
                DataTypeConfig::Varchar => DataType::Utf8,
                DataTypeConfig::Float => DataType::Float64,
                DataTypeConfig::Boolean => DataType::Boolean,
                DataTypeConfig::Date => DataType::Date32,
                DataTypeConfig::Decimal => {
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
                    if precision <= 9 {
                        DataType::Decimal32(precision, scale as i8)
                    } else if precision <= 18 {
                        DataType::Decimal64(precision, scale as i8)
                    } else {
                        DataType::Decimal128(precision, scale as i8)
                    }
                }
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
}
