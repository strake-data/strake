//! # Configuration Errors
//!
//! error types for the Strake configuration subsystem.
//!
//! ## Overview
//!
//! Defines the [`ConfigError`] enum which wraps errors from the `config` crate,
//! validation failures from `validator`, and custom Strake configuration logic.
//!
//! ## Usage
//!
//! ```rust
//! # use strake_common::config::error::ConfigError;
//! let err = ConfigError::FileNotFound("config.yaml".to_string());
//! println!("{}", err);
//! ```
//!
//! ## Performance Characteristics
//!
//! Error types are designed for one-time startup failures. They use `thiserror`
//! for efficient display and error chain management.
//!
//! ## Safety
//!
//! This module contains no unsafe code.
//!
//! ## Errors
//!
//! This module defines error types and does not trigger them itself.
//!

use thiserror::Error;

/// Error type for configuration loading and validation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration file was not found.
    #[error("Configuration file not found: {0}")]
    FileNotFound(String),

    /// Failed to parse the configuration file via the `config` crate.
    #[error("Failed to parse configuration: {0}")]
    ParseError(#[from] config::ConfigError),

    /// Validation of a configuration field failed.
    #[error("Validation failed: {0}")]
    Validation(String),

    /// Authentication is enabled but the API key is missing.
    #[error("Authentication enabled but API key is not set")]
    MissingApiKey,

    /// A generic error with a custom message.
    #[error("{0}")]
    Custom(String),
}

/// Type alias for results in the configuration module.
pub type Result<T, E = ConfigError> = std::result::Result<T, E>;
