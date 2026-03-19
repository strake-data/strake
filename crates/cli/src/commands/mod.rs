//! # CLI Commands
//!
//! CLI command implementations, split into logical modules for maintainability.
//!
//! ## Overview
//!
//! Central export hub for all Strake CLI subcommands.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::commands::{ApplyOptions, apply};
//! use crate::config::CliConfig;
//! use crate::output::OutputFormat;
//! use crate::secrets::ResolverContext;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = CliConfig::default();
//! let ctx = ResolverContext { system_env: Default::default(), dotenv: Default::default(), offline: false };
//! let store = crate::metadata::init_store(&config).await?;
//! let options = ApplyOptions {
//!     file_path: "sources.yaml".to_string(),
//!     force: false,
//!     dry_run: false,
//!     expected_version: None,
//!     format: OutputFormat::Human,
//!     notify_url: None,
//! };
//! apply(&*store, options, &config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Characteristics
//!
//! Re-exports modules. No runtime overhead.
//!
//! ## Safety
//!
//! Standard safe Rust.
//!
//! ## References
//!
//! - [Strake CLI Commands Reference](https://docs.strake.io/cli/commands)

mod apply;
mod apply_models;
mod describe;
mod diff;
mod discovery;
mod domain;
mod helpers;
mod init;
mod remove;
mod secrets;
mod status;
mod validate;

// Re-export public command functions
pub use apply::{ApplyOptions, apply};
pub use describe::{describe, test_connection};
pub use diff::diff;
pub use discovery::{add, search};
pub use domain::{list_domains, rollback, show_domain_history};

#[cfg(test)]
mod tests;

pub use init::init;
pub use remove::remove;
pub use secrets::validate_secrets;
pub use status::status;
pub use validate::validate;
