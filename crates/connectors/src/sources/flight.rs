//! # Arrow Flight SQL Data Source
//!
//! Connects to databases supporting the Arrow Flight SQL standard for high-performance,
//! low-overhead data transfer.
//!
//! ## Overview
//!
//! This module provides a `SourceProvider` implementation for Flight SQL, allowing Strake
//! to discover and register tables from remote Flight SQL endpoints (e.g., Snowflake,
//! Dremio, InfluxDB).
//!
//! ## Errors
//!
//! - `anyhow::Error` for connection or protocol-level failures.
//! - `DataFusionError` for table registration failures.
//!
//! ## Performance Characteristics
//!
//! - Uses vectorized metadata discovery to minimize allocations.
//! - Leverages Arrow Flight for zero-copy data transfer where supported.
use anyhow::{Context, Result};
use arrow::array::Array;
use arrow_flight::sql::CommandGetTables;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::flight::FlightTableFactory;
use datafusion_table_providers::flight::sql::FlightSqlDriver;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::sources::SourceProvider;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use strake_common::config::{SourceConfig, TableConfig};

/// A `TableProvider` implementation that always returns a specified error when scanned.
#[derive(Debug)]
pub struct FailingTableProvider {
    error_message: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for FailingTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::External(
            self.error_message.clone().into(),
        ))
    }
}

/// Registers placeholder table providers that return an error on query execution.
pub fn register_failing_tables(
    context: &SessionContext,
    catalog_name: &str,
    schema_name: &str,
    tables: &[TableConfig],
    error_message: &str,
) -> Result<()> {
    // Use the session's actual catalog name rather than the hardcoded "datafusion"
    // name, since Strake configures sessions with a custom default catalog.
    crate::sources::ensure_schema(context, catalog_name, schema_name)
        .context("Failed to ensure schema for failing table placeholders")?;

    let dummy_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

    for t in tables {
        let provider = FailingTableProvider {
            error_message: error_message.to_string(),
            schema: dummy_schema.clone(),
        };
        let qualified = TableReference::partial(schema_name, t.name.as_str());
        context.register_table(qualified, Arc::new(provider) as Arc<dyn TableProvider>)?;
        tracing::info!(
            "Registered failing Flight SQL placeholder table: {}.{}",
            schema_name,
            t.name
        );
    }

    Ok(())
}

/// A provider for Arrow Flight SQL data sources.
pub struct FlightSqlSourceProvider;

#[async_trait]
impl SourceProvider for FlightSqlSourceProvider {
    fn type_name(&self) -> &'static str {
        "flight_sql"
    }

    async fn register(
        &self,
        context: &SessionContext,
        catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        #[derive(serde::Deserialize)]
        struct FlightSqlConfig {
            url: Option<String>,
            connection: Option<String>,
        }
        let cfg: FlightSqlConfig =
            serde_json::from_value(config.config.clone()).unwrap_or(FlightSqlConfig {
                url: None,
                connection: None,
            });

        let url_str = config
            .url
            .as_ref()
            .or(cfg.url.as_ref())
            .or(cfg.connection.as_ref())
            .context("Flight SQL source requires a 'url' or 'connection' configuration")?;

        if let Err(e) =
            register_flight_sql_source(context, catalog_name, config.name.as_ref(), url_str).await
        {
            tracing::warn!(
                "Flight SQL source '{}' failed to register: {:#}. Registering failing placeholders.",
                config.name,
                e
            );
            if !config.tables.is_empty() {
                register_failing_tables(
                    context,
                    catalog_name,
                    config.name.as_ref(),
                    &config.tables,
                    &e.to_string(),
                )?;
            } else {
                let mut dummy_table = TableConfig::default();
                dummy_table.name = "some_table".to_string();
                register_failing_tables(
                    context,
                    catalog_name,
                    config.name.as_ref(),
                    &[dummy_table],
                    &e.to_string(),
                )?;
            }
        }
        Ok(())
    }
}

/// Registers a Flight SQL source with DataFusion.
///
/// This allows Strake to federate queries to any Flight SQL compatible endpoint
/// (Snowflake, Dremio, InfluxDB, or another Strake instance).
pub async fn register_flight_sql_source(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    url: &str,
) -> Result<()> {
    tracing::info!("Connecting to Flight SQL source: {} at {}", name, url);

    let endpoint = Channel::from_shared(url.to_string()).context("Invalid Flight SQL URL")?;

    let channel = endpoint
        .connect()
        .await
        .context("Failed to connect to Flight SQL endpoint")?;

    let mut client = FlightSqlServiceClient::new(channel);

    // 1. Fetch tables
    let query = CommandGetTables::default();
    let info = client
        .get_tables(query)
        .await
        .context("Failed to get tables from Flight SQL source")?;

    let mut discovered_tables: Vec<(Option<String>, String)> = Vec::new();

    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.context("Missing ticket in endpoint")?;
        let mut stream = client
            .do_get(ticket)
            .await
            .context("Failed to execute do_get for table metadata")?;

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res.context("Error in metadata stream")?;

            // The schema for GetTables is defined by Flight SQL spec
            // catalog_name, db_schema_name, table_name, table_type, ...
            let table_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .context("Failed to downcast table_name column")?;

            let schema_names = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .context("Failed to downcast db_schema_name column")?;

            // Optimization: Iterate directly over the arrays to avoid intermediate indices and take allocations
            for i in 0..batch.num_rows() {
                if table_names.is_valid(i) {
                    let t_name = table_names.value(i).to_string();
                    let s_name = if schema_names.is_valid(i) && !schema_names.value(i).is_empty() {
                        Some(schema_names.value(i).to_string())
                    } else {
                        None
                    };
                    discovered_tables.push((s_name, t_name));
                }
            }
        }
    }

    // 2. Register tables
    let driver = Arc::new(FlightSqlDriver::new());
    let factory = FlightTableFactory::new(driver);

    // Ensure schema exists using the session's actual default catalog.
    crate::sources::ensure_schema(context, catalog_name, name)
        .context("Failed to ensure schema for Flight SQL source")?;

    for (_s_name, t_name) in discovered_tables {
        let mut options = HashMap::new();
        // The FlightSqlDriver in datafusion-table-providers 0.9.0 expects the query in this key
        options.insert(
            "flight.sql.query".to_string(),
            // Fix [Safety]: Quote table name to prevent SQL injection
            format!("SELECT * FROM \"{}\"", t_name.replace('"', "\"\"")),
        );

        match factory.open_table(url, options).await {
            Ok(provider) => {
                let qualified = TableReference::partial(name, t_name.as_str());
                context.register_table(qualified, Arc::new(provider) as Arc<dyn TableProvider>)?;
                tracing::info!("Registered Flight SQL table: {}.{}", name, t_name);
            }
            Err(e) => {
                tracing::warn!("Failed to open Flight SQL table {}.{}: {}", name, t_name, e);
            }
        }
    }

    Ok(())
}
