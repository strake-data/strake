use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::datasource::{TableProvider, TableType, MemTable};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::Expr;
use datafusion::arrow::record_batch::RecordBatch;
use url::Url;

use crate::config::{TableConfig, RetrySettings};
use super::common::{SqlMetadataFetcher, FetchedMetadata, SqlProviderFactory, next_retry_delay};
use super::wrappers::register_tables;

pub struct ClickHouseMetadataFetcher;

#[async_trait]
impl SqlMetadataFetcher for ClickHouseMetadataFetcher {
    async fn fetch_metadata(&self, _schema: &str, _table: &str) -> Result<FetchedMetadata> {
        Ok(FetchedMetadata::default())
    }
}

pub struct ClickHouseTableFactory {
    pub connection_string: String,
}

#[async_trait]
impl SqlProviderFactory for ClickHouseTableFactory {
    async fn create_table_provider(&self, table_ref: TableReference) -> Result<Arc<dyn TableProvider>> {
        let table_name = table_ref.table().to_string();
        let schema = infer_clickhouse_schema(&self.connection_string, &table_name).await?;
        Ok(Arc::new(ClickHouseTableProvider {
            connection_string: self.connection_string.clone(),
            table_name,
            schema: Arc::new(schema),
        }))
    }
}

#[derive(Debug)]
pub struct ClickHouseTableProvider {
    pub connection_string: String,
    pub table_name: String,
    pub schema: SchemaRef,
}

#[async_trait]
impl TableProvider for ClickHouseTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let batches = self.execute_query(projection).await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        
        let mem_table = MemTable::try_new(self.schema(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

impl ClickHouseTableProvider {
    async fn execute_query(&self, projection: Option<&Vec<usize>>) -> Result<Vec<RecordBatch>> {
        let columns = if let Some(p) = projection {
            p.iter()
                .map(|i| self.schema.field(*i).name().clone())
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "*".to_string()
        };

        let sql = format!("SELECT {} FROM {} FORMAT Arrow", columns, self.table_name);
        query_clickhouse_arrow(&self.connection_string, &sql).await
    }
}

pub async fn register_clickhouse(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<crate::query::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
    retry: RetrySettings,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        match try_register_clickhouse(context, catalog_name, name, connection_string, cb.clone(), explicit_tables).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                attempt += 1;
                if attempt >= retry.max_attempts {
                    return Err(e);
                }
                let delay = next_retry_delay(attempt, retry.base_delay_ms, retry.max_delay_ms);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn try_register_clickhouse(
    context: &SessionContext,
    catalog_name: &str,
    name: &str,
    connection_string: &str,
    cb: Arc<crate::query::circuit_breaker::AdaptiveCircuitBreaker>,
    explicit_tables: &Option<Vec<TableConfig>>,
) -> Result<()> {
    let factory = ClickHouseTableFactory {
        connection_string: connection_string.to_string(),
    };

    let tables_to_register: Vec<(String, String)> = if let Some(config_tables) = explicit_tables {
        config_tables.iter().map(|t| {
             let target_schema = t.schema.clone().unwrap_or_else(|| name.to_string());
             (t.name.clone(), target_schema)
        }).collect()
    } else {
        introspect_clickhouse_tables(connection_string).await?
            .into_iter()
            .map(|t| (t, name.to_string()))
            .collect()
    };

    let fetcher: Option<Box<dyn SqlMetadataFetcher>> = Some(Box::new(ClickHouseMetadataFetcher));

    register_tables(context, catalog_name, name, fetcher, &factory, cb, tables_to_register).await?;
    Ok(())
}

async fn introspect_clickhouse_tables(connection_string: &str) -> Result<Vec<String>> {
    let url = Url::parse(connection_string)?;
    let db = url.path().trim_start_matches('/');
    let db_filter = if db.is_empty() { "default" } else { db };

    let sql = format!("SELECT name FROM system.tables WHERE database = '{}'", db_filter);
    let client = reqwest::Client::new();
    let resp = client.post(connection_string)
        .body(sql)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    
    Ok(resp.lines().map(|s| s.to_string()).collect())
}

async fn infer_clickhouse_schema(connection_string: &str, table_name: &str) -> Result<datafusion::arrow::datatypes::Schema> {
    let sql = format!("SELECT * FROM {} WHERE 0 FORMAT Arrow", table_name);
    let batches = query_clickhouse_arrow(connection_string, &sql).await?;
    if batches.is_empty() {
        anyhow::bail!("Failed to infer schema for table {}: no data returned", table_name);
    }
    Ok((*batches[0].schema()).clone())
}

async fn query_clickhouse_arrow(connection_string: &str, sql: &str) -> Result<Vec<RecordBatch>> {
    let client = reqwest::Client::new();
    let resp = client.post(connection_string)
        .body(sql.to_string())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    if resp.is_empty() {
        return Ok(vec![]);
    }

    let cursor = std::io::Cursor::new(resp);
    let reader = datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, body_string};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use datafusion::arrow::ipc::writer::StreamWriter;

    #[tokio::test]
    async fn test_introspect_clickhouse_tables() -> Result<()> {
        let server = MockServer::start().await;
        
        Mock::given(method("POST"))
            .and(body_string("SELECT name FROM system.tables WHERE database = 'default'"))
            .respond_with(ResponseTemplate::new(200).set_body_string("table1\ntable2"))
            .mount(&server)
            .await;

        let tables = introspect_clickhouse_tables(&server.uri()).await?;
        assert_eq!(tables, vec!["table1", "table2"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_query_clickhouse_arrow() -> Result<()> {
        let server = MockServer::start().await;
        
        // Create a mock Arrow batch
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", datafusion::arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )?;

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(buffer))
            .mount(&server)
            .await;

        let batches = query_clickhouse_arrow(&server.uri(), "SELECT * FROM test").await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        Ok(())
    }
}
