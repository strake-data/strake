use thiserror::Error;

use super::TableVersionSpec;

/// Structured errors for Iceberg connector operations
#[derive(Error, Debug)]
pub enum IcebergConnectorError {
    /// Catalog API operation failed after retrying.
    #[error("Catalog API error: {operation} failed after {retries} retries")]
    CatalogError {
        /// Name of the failed operation.
        operation: String,
        /// Number of retries attempted.
        retries: u32,
        /// Underlying error source.
        #[source]
        source: anyhow::Error,
    },

    /// The requested table could not be found.
    #[error("Table '{table}' not found in namespace '{namespace}'")]
    TableNotFound {
        /// The name of the table.
        table: String,
        /// The namespace of the table.
        namespace: String,
    },

    /// Time travel query requested a version/snapshot that is unavailable.
    #[error("Time travel unavailable: {version:?} not found (showing 10 of {} snapshots: {:?})",
        available_snapshots.len(),
        available_snapshots.iter().take(10).collect::<Vec<_>>())]
    TimeTravelUnavailable {
        /// The requested version specification.
        version: TableVersionSpec,
        /// Available snapshot IDs in the table.
        available_snapshots: Vec<i64>,
    },

    /// Invalid configuration provided to the connector.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Loading the table metadata failed.
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
