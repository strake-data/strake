//! # Iceberg Connector Errors
//!
//! ## Overview
//! Structured error types for Iceberg catalog and table operations, with
//! conversions to `DataFusionError` for propagation through the
//! DataFusion execution pipeline. This module ensures structured error codes survive
//! the DataFusion execution boundary instead of being flattened to a string.
//!
//! ## Usage
//! ```rust
//! # use anyhow::anyhow;
//! let err = strake_connectors::sources::iceberg::error::IcebergConnectorError::from(anyhow!("boom"));
//! let df_err: datafusion::error::DataFusionError = err.into();
//! ```
//!
//! ## Performance Characteristics
//! Conversions are lightweight and allocate only when wrapping dynamic anyhow::Error
//! chains or formatting messages during display generation.
//!
//! ## Errors
//! - `TableNotFound` → `DataFusionError::External(StrakeError{code: TableNotFound, ..})`
//! - `InvalidConfiguration` → `DataFusionError::Plan`
//! - `TableLoadError` → `DataFusionError::External` wrapping the inner `anyhow::Error`
//! - `CatalogError` / `TimeTravelUnavailable` → `DataFusionError::External(Box<Self>)`

use thiserror::Error;

use super::TableVersionSpec;

/// Structured errors for Iceberg connector operations
#[derive(Error, Debug)]
#[non_exhaustive]
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

/// Converts `IcebergConnectorError` to `DataFusionError`.
///
/// This implementation preserves structured error types by boxing specific errors
/// (e.g. `TableNotFound` mapped to a structured `StrakeError`, and `TableLoadError` wrapping the
/// underlying `anyhow::Error` cause) into `DataFusionError::External`.
impl From<IcebergConnectorError> for datafusion::error::DataFusionError {
    fn from(err: IcebergConnectorError) -> Self {
        match err {
            IcebergConnectorError::TableNotFound { .. } => {
                let display = err.to_string();
                datafusion::error::DataFusionError::External(Box::new(
                    strake_error::StrakeError::new(strake_error::ErrorCode::TableNotFound, display),
                ))
            }
            IcebergConnectorError::InvalidConfiguration(_) => {
                let display = err.to_string();
                datafusion::error::DataFusionError::Plan(display)
            }
            IcebergConnectorError::TableLoadError(e) => {
                datafusion::error::DataFusionError::External(e.into())
            }
            _ => datafusion::error::DataFusionError::External(Box::new(err)),
        }
    }
}
