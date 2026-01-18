//! Arrow Flight SQL data source.
//!
//! Connects to databases supporting the Arrow Flight SQL standard for high-performance,
//! low-overhead data transfer.
use anyhow::{Context, Result};
use arrow::array::Array;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetTables;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::flight::sql::FlightSqlDriver;
use datafusion_table_providers::flight::FlightTableFactory;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::sources::SourceProvider;
use async_trait::async_trait;
use strake_common::config::SourceConfig;

pub struct FlightSqlSourceProvider;

#[async_trait]
impl SourceProvider for FlightSqlSourceProvider {
    fn type_name(&self) -> &'static str {
        "flight_sql"
    }

    async fn register(
        &self,
        context: &SessionContext,
        _catalog_name: &str,
        config: &SourceConfig,
    ) -> Result<()> {
        #[derive(serde::Deserialize)]
        struct FlightSqlConfig {
            url: String,
        }
        let cfg: FlightSqlConfig = serde_yaml::from_value(config.config.clone())
            .context("Failed to parse Flight SQL source configuration")?;

        register_flight_sql_source(context, &config.name, &cfg.url).await
    }
}

/// Registers a Flight SQL source with DataFusion.
///
/// This allows Strake to federate queries to any Flight SQL compatible endpoint
/// (Snowflake, Dremio, InfluxDB, or another Strake instance).
pub async fn register_flight_sql_source(
    context: &SessionContext,
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

    // Ensure schema exists
    use datafusion::catalog::MemorySchemaProvider;
    if context
        .catalog("datafusion")
        .unwrap()
        .schema(name)
        .is_none()
    {
        context
            .catalog("datafusion")
            .unwrap()
            .register_schema(name, Arc::new(MemorySchemaProvider::new()))?;
    }

    for (_s_name, t_name) in discovered_tables {
        let mut options = HashMap::new();
        // The FlightSqlDriver in datafusion-table-providers 0.9.0 expects the query in this key
        options.insert(
            "flight.sql.query".to_string(),
            format!("SELECT * FROM {}", t_name),
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
