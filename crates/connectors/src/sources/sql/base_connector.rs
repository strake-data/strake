//! # Generic SQL Connector
//!
//! Provides a standardized way to register SQL-based data sources into DataFusion,
//! handling table discovery, metadata enrichment, and registration.

use anyhow::Result;
use datafusion::sql::TableReference;
use std::sync::Arc;
use strake_common::retry::retry_async;

use super::common::{SchemaMappingRule, SqlProviderFactory, SqlSourceParams};
use crate::introspect::{IntrospectError, SchemaIntrospector};
use crate::sources::sql::case_insensitive_schema::CaseInsensitiveSchemaProvider;

/// A generic connector for SQL data sources.
///
/// This struct implements the logic for discovering tables via an [`SchemaIntrospector`],
/// creating [`TableProvider`]s via an [`SqlProviderFactory`], and registering them
/// into a DataFusion [`SessionContext`].
#[derive(Clone)]
pub struct GenericSqlConnector {
    /// Strategy for discovering tables and their schemas.
    pub introspector: Arc<dyn SchemaIntrospector>,
    /// Factory for creating dialect-specific table providers.
    pub factory: Arc<dyn SqlProviderFactory>,
    /// Rule for mapping source schemas to DataFusion schemas.
    pub schema_mapping: SchemaMappingRule,
}

impl GenericSqlConnector {
    /// Registers all discovered or explicit tables from the source into DataFusion.
    ///
    /// This method uses retries to handle transient connectivity issues during registration.
    pub async fn register(&self, params: SqlSourceParams) -> Result<()> {
        let name = params.name.clone();
        let retry_settings = params.retry;

        retry_async(
            format!("register_sql_source({})", name),
            retry_settings,
            move || {
                let this = self.clone();
                let params = params.clone();

                async move { this.try_register(params).await }
            },
        )
        .await
    }

    async fn try_register(&self, params: SqlSourceParams) -> Result<()> {
        let tables_to_register = if let Some(config_tables) = params.explicit_tables.as_ref() {
            config_tables
                .iter()
                .map(|t| {
                    let target_schema = self
                        .schema_mapping
                        .map_schema(&t.schema, &params.name)
                        .into_owned();
                    (t.name.clone(), target_schema, t.schema.clone())
                })
                .collect()
        } else {
            // discovery
            let tables = self
                .introspector
                .list_tables(None)
                .await
                .map_err(|e| match e {
                    IntrospectError::Connection(msg) => {
                        anyhow::anyhow!("Connection failed: {}", msg)
                    }
                    IntrospectError::Permission(msg) => {
                        anyhow::anyhow!("Permission denied: {}", msg)
                    }
                    IntrospectError::NotFound(msg) => anyhow::anyhow!("Table not found: {}", msg),
                    other => anyhow::anyhow!("Introspection failed: {}", other),
                })?;

            tables
                .into_iter()
                .map(|t| {
                    let target_schema = self
                        .schema_mapping
                        .map_schema(&t.schema, &params.name)
                        .into_owned();
                    (t.table, target_schema, t.schema)
                })
                .collect::<Vec<_>>()
        };

        let catalog = params
            .context
            .catalog(&params.catalog_name)
            .ok_or_else(|| anyhow::anyhow!("Catalog '{}' not found", params.catalog_name))?;

        for (table_name, target_schema, original_schema) in tables_to_register {
            let config_table = params
                .explicit_tables
                .as_ref()
                .as_ref()
                .and_then(|config_tables| {
                    config_tables
                        .iter()
                        .find(|t| t.name == table_name && t.schema == original_schema)
                });

            let custom_schema = config_table
                .filter(|t| !t.column_definitions.is_empty())
                .map(construct_schema_from_config);

            let table_ref = if self.schema_mapping.is_default_schema(&original_schema) {
                TableReference::bare(table_name.as_str())
            } else {
                TableReference::Partial {
                    schema: original_schema.as_str().into(),
                    table: table_name.as_str().into(),
                }
            };
            tracing::debug!(
                catalog = %params.catalog_name,
                schema = %target_schema,
                original_schema = %original_schema,
                table = %table_name,
                qualified = %table_ref.to_quoted_string(),
                "Registering table with table reference",
            );
            match self
                .factory
                .create_table_provider(table_ref, params.cb.clone(), custom_schema)
                .await
            {
                Ok(provider) => {
                    let target_schema_ref = target_schema.as_str();
                    let table_name_ref = table_name.as_str();

                    // Ensure the schema exists before registration
                    if catalog.schema(target_schema_ref).is_none() {
                        catalog.register_schema(
                            target_schema_ref,
                            Arc::new(CaseInsensitiveSchemaProvider::new()),
                        )?;
                    }

                    let qualified = TableReference::full(
                        params.catalog_name.as_str(),
                        target_schema_ref,
                        table_name_ref,
                    );
                    match params.context.register_table(qualified, provider) {
                        Ok(_) => tracing::info!(
                            "Registered {}.{}.{}",
                            params.catalog_name,
                            target_schema,
                            table_name
                        ),
                        Err(e) => {
                            tracing::warn!(
                                "Failed to register table {}.{}: {}",
                                target_schema,
                                table_name,
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Skipping table {} due to error: {}", table_name, e);
                }
            }
        }

        Ok(())
    }
}

fn map_type_str_to_arrow(type_str: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    let type_str = type_str.to_uppercase();
    if type_str.starts_with("VARCHAR") || type_str == "TEXT" || type_str == "CHARACTER VARYING" {
        DataType::Utf8
    } else if type_str.starts_with("INTEGER") || type_str == "INT" || type_str == "INT4" {
        DataType::Int32
    } else if type_str == "BIGINT" || type_str == "INT8" {
        DataType::Int64
    } else if type_str == "BOOLEAN" || type_str == "BOOL" {
        DataType::Boolean
    } else if type_str.starts_with("NUMERIC") || type_str.starts_with("DECIMAL") {
        let (p, s) = if let (Some(start), Some(end)) = (type_str.find('('), type_str.find(')')) {
            let parts: Vec<&str> = type_str[start + 1..end]
                .split(',')
                .map(|s| s.trim())
                .collect();
            if parts.len() == 2 {
                if let (Ok(p), Ok(s)) = (parts[0].parse::<u8>(), parts[1].parse::<i8>()) {
                    (p, s)
                } else {
                    (38, 10)
                }
            } else if parts.len() == 1 {
                if let Ok(p) = parts[0].parse::<u8>() {
                    (p, 0)
                } else {
                    (38, 10)
                }
            } else {
                (38, 10)
            }
        } else {
            (38, 10)
        };

        if p <= 9 {
            DataType::Decimal32(p, s)
        } else if p <= 18 {
            DataType::Decimal64(p, s)
        } else {
            DataType::Decimal128(p, s)
        }
    } else if type_str == "TIMESTAMP" || type_str.starts_with("TIMESTAMP WITHOUT TIME ZONE") {
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
    } else if type_str == "TIMESTAMPTZ" || type_str.starts_with("TIMESTAMP WITH TIME ZONE") {
        DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some("+00:00".into()),
        )
    } else if type_str == "DOUBLE" || type_str == "DOUBLE PRECISION" || type_str == "FLOAT8" {
        DataType::Float64
    } else if type_str == "FLOAT" || type_str == "REAL" || type_str == "FLOAT4" {
        DataType::Float32
    } else {
        DataType::Utf8
    }
}

fn construct_schema_from_config(
    config: &strake_common::config::TableConfig,
) -> datafusion::arrow::datatypes::SchemaRef {
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::collections::HashMap;

    let mut fields = Vec::new();
    for col in &config.column_definitions {
        let dt = map_type_str_to_arrow(&col.data_type);
        let mut field_metadata = HashMap::new();
        if let Some(desc) = &col.description {
            field_metadata.insert("description".to_string(), desc.clone());
            field_metadata.insert("comment".to_string(), desc.clone());
            field_metadata.insert("remarks".to_string(), desc.clone());
            field_metadata.insert("ARROW:FLIGHT:SQL:REMARKS".to_string(), desc.clone());
        }
        let field = Field::new(&col.name, dt, !col.not_null).with_metadata(field_metadata);
        fields.push(Arc::new(field));
    }

    let mut schema_metadata = HashMap::new();
    if let Some(table_desc) = &config.description {
        schema_metadata.insert("description".to_string(), table_desc.clone());
        schema_metadata.insert("comment".to_string(), table_desc.clone());
        schema_metadata.insert("remarks".to_string(), table_desc.clone());
        schema_metadata.insert("ARROW:FLIGHT:SQL:REMARKS".to_string(), table_desc.clone());
    }

    Arc::new(Schema::new_with_metadata(fields, schema_metadata))
}
