use thiserror::Error;

use super::TableVersionSpec;

/// Structured errors for Iceberg connector operations
#[derive(Error, Debug)]
pub enum IcebergConnectorError {
    #[error("Catalog API error: {operation} failed after {retries} retries")]
    CatalogError {
        operation: String,
        retries: u32,
        #[source]
        source: anyhow::Error,
    },

    #[error("Table '{table}' not found in namespace '{namespace}'")]
    TableNotFound { table: String, namespace: String },

    #[error("Time travel unavailable: {version:?} not found (showing 10 of {} snapshots: {:?})",
        available_snapshots.len(),
        available_snapshots.iter().take(10).collect::<Vec<_>>())]
    TimeTravelUnavailable {
        version: TableVersionSpec,
        available_snapshots: Vec<i64>,
    },

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Table loading failed: {0}")]
    TableLoadError(#[from] anyhow::Error),
}

// Implement conversion to DataFusionError
impl From<IcebergConnectorError> for datafusion::error::DataFusionError {
    fn from(err: IcebergConnectorError) -> Self {
        match &err {
            IcebergConnectorError::TableNotFound { .. } => {
                datafusion::error::DataFusionError::Plan(err.to_string())
            }
            IcebergConnectorError::InvalidConfiguration(_) => {
                datafusion::error::DataFusionError::Plan(err.to_string())
            }
            _ => datafusion::error::DataFusionError::Execution(err.to_string()),
        }
    }
}
