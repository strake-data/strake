//! File-based data sources (Parquet, CSV, JSON).
//!
//! Supports reading from local filesystems and remote object stores (S3, GCS, Azure)
//! via DataFusion's `ListingTable`.
use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use std::sync::Arc;

use crate::sources::SourceProvider;
use strake_common::config::{ColumnConfig, SourceConfig, TableConfig};

use std::collections::HashMap;
use url::Url;

pub struct FileSourceProvider;

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
        match config.source_type.as_str() {
            "parquet" => {
                #[derive(serde::Deserialize)]
                struct ParquetConfig {
                    path: String,
                    #[serde(default)]
                    options: Option<HashMap<String, String>>,
                    #[serde(default)]
                    tables: Option<Vec<TableConfig>>,
                }
                // NOTE: config.config is a serde_json::Value because it's stored that way in SourceConfig.
                // Even if the source is YAML, it's converted to JSON Value on load.
                // We use serde_json::from_value to unpack it.
                let cfg: ParquetConfig = serde_json::from_value(config.config.clone())
                    .context("Failed to parse Parquet source configuration")?;

                let tables = if !config.tables.is_empty() {
                    Some(config.tables.clone())
                } else {
                    cfg.tables
                };

                register_object_store(context, &cfg.path, cfg.options).await?;
                register_parquet(context, catalog_name, &config.name, &cfg.path, &tables).await
            }
            "csv" => {
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

                register_object_store(context, &cfg.path, cfg.options).await?;
                register_csv(
                    context,
                    catalog_name,
                    &config.name,
                    &cfg.path,
                    cfg.has_header,
                    cfg.delimiter,
                    &tables,
                )
                .await
            }
            "json" => {
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

                register_object_store(context, &cfg.path, cfg.options).await?;
                register_json(context, catalog_name, &config.name, &cfg.path, &tables).await
            }
            _ => anyhow::bail!(
                "Invalid type for FileSourceProvider: {}",
                config.source_type
            ),
        }
    }
}

async fn register_object_store(
    ctx: &SessionContext,
    path: &str,
    options: Option<HashMap<String, String>>,
) -> Result<()> {
    if let Ok(url) = Url::parse(path) {
        let scheme = url.scheme();
        let bucket = url.host_str().unwrap_or_default();

        // OpenDAL supports building via a HashMap map directly.
        let mut map = HashMap::new();
        // Map common options
        if let Some(opts) = options {
            map.extend(opts);
        }

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
                return Err(anyhow::anyhow!("SFTP is only supported on Unix systems"));

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
    }

    Ok(())
}

pub async fn register_csv(
    context: &SessionContext,
    catalog: &str,
    name: &str,
    path: &str,
    has_header: bool,
    delimiter: Option<char>,
    tables_config: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let file_format = CsvFormat::default().with_has_header(has_header);
    let file_format = if let Some(d) = delimiter {
        file_format.with_delimiter(d as u8)
    } else {
        file_format
    };

    let listing_options = ListingOptions::new(Arc::new(file_format));
    let start_url = ListingTableUrl::parse(path)?;

    if let Some(tables) = tables_config {
        for table_cfg in tables {
            let resolved_schema = if !table_cfg.columns.is_empty() {
                build_schema_from_config(&table_cfg.columns)?
            } else {
                listing_options
                    .infer_schema(&context.state(), &start_url)
                    .await?
            };

            let config = ListingTableConfig::new(start_url.clone())
                .with_listing_options(listing_options.clone())
                .with_schema(resolved_schema);
            let provider = ListingTable::try_new(config)?;

            let schema_name = if table_cfg.schema.is_empty() {
                "public"
            } else {
                &table_cfg.schema
            };
            let schema_provider = ensure_schema(context, catalog, schema_name)?;
            schema_provider.register_table(table_cfg.name.to_string(), Arc::new(provider))?;
        }
    } else {
        let resolved_path = listing_options
            .infer_schema(&context.state(), &start_url)
            .await?;
        let config = ListingTableConfig::new(start_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_path);
        let provider = ListingTable::try_new(config)?;

        let schema_provider = ensure_schema(context, catalog, "public")?;
        schema_provider.register_table(name.to_string(), Arc::new(provider))?;
    }

    Ok(())
}

pub async fn register_parquet(
    context: &SessionContext,
    catalog: &str,
    name: &str,
    path: &str,
    tables_config: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let file_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format));
    let start_url = ListingTableUrl::parse(path)?;

    if let Some(tables) = tables_config {
        for table_cfg in tables {
            let resolved_schema = if !table_cfg.columns.is_empty() {
                build_schema_from_config(&table_cfg.columns)?
            } else {
                listing_options
                    .infer_schema(&context.state(), &start_url)
                    .await?
            };

            let config = ListingTableConfig::new(start_url.clone())
                .with_listing_options(listing_options.clone())
                .with_schema(resolved_schema);
            let provider = ListingTable::try_new(config)?;

            let schema_name = if table_cfg.schema.is_empty() {
                "public"
            } else {
                &table_cfg.schema
            };
            let schema_provider = ensure_schema(context, catalog, schema_name)?;
            schema_provider.register_table(table_cfg.name.to_string(), Arc::new(provider))?;
        }
    } else {
        let resolved_path = listing_options
            .infer_schema(&context.state(), &start_url)
            .await?;
        let config = ListingTableConfig::new(start_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_path);
        let provider = ListingTable::try_new(config)?;

        let schema_provider = ensure_schema(context, catalog, "public")?;
        schema_provider.register_table(name.to_string(), Arc::new(provider))?;
    }

    Ok(())
}

pub async fn register_json(
    context: &SessionContext,
    catalog: &str,
    name: &str,
    path: &str,
    tables_config: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let start_url = ListingTableUrl::parse(path)?;
    let file_format = JsonFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format));

    if let Some(tables) = tables_config {
        for table_cfg in tables {
            let resolved_schema = if !table_cfg.columns.is_empty() {
                build_schema_from_config(&table_cfg.columns)?
            } else {
                listing_options
                    .infer_schema(&context.state(), &start_url)
                    .await?
            };

            let config = ListingTableConfig::new(start_url.clone())
                .with_listing_options(listing_options.clone())
                .with_schema(resolved_schema);
            let provider = ListingTable::try_new(config)?;

            let schema_name = if table_cfg.schema.is_empty() {
                "public"
            } else {
                &table_cfg.schema
            };
            let schema_provider = ensure_schema(context, catalog, schema_name)?;
            schema_provider.register_table(table_cfg.name.to_string(), Arc::new(provider))?;
        }
    } else {
        let resolved_schema = listing_options
            .infer_schema(&context.state(), &start_url)
            .await?;
        let config = ListingTableConfig::new(start_url)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;

        let schema_provider = ensure_schema(context, catalog, "public")?;
        schema_provider.register_table(name.to_string(), Arc::new(provider))?;
    }
    Ok(())
}

fn build_schema_from_config(columns: &[ColumnConfig]) -> Result<arrow::datatypes::SchemaRef> {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            let dt = match c.data_type.to_lowercase().as_str() {
                "int" | "integer" => DataType::Int32,
                "bigint" => DataType::Int64,
                "varchar" | "string" | "text" | "char" => DataType::Utf8,
                "float" | "double" => DataType::Float64,
                "boolean" | "bool" => DataType::Boolean,
                "date" => DataType::Date32,
                "decimal" => DataType::Decimal128(15, 2),
                _ => DataType::Utf8,
            };
            let nullable = !c.not_null;

            let mut metadata: HashMap<String, String> = HashMap::new();
            if let Some(len) = c.length {
                metadata.insert("precision".to_string(), len.to_string());
                metadata.insert("characterMaximumLength".to_string(), len.to_string());
            }

            let field = Field::new(&c.name, dt, nullable);
            field.with_metadata(metadata)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

pub fn ensure_schema(
    ctx: &SessionContext,
    catalog: &str,
    schema: &str,
) -> Result<Arc<dyn SchemaProvider>> {
    let cat = ctx
        .catalog(catalog)
        .ok_or(anyhow::anyhow!("Catalog not found"))?;
    if cat.schema(schema).is_none() {
        cat.register_schema(schema, Arc::new(MemorySchemaProvider::new()))?;
    }
    Ok(cat.schema(schema).unwrap())
}
