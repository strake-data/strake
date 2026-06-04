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
//! use crate::commands::{ValidateOptions, validate};
//! use crate::config::CliConfig;
//! use crate::output::OutputFormat;
//! use crate::secrets::ResolverContext;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = CliConfig::default();
//! let ctx = ResolverContext { system_env: Default::default(), dotenv: Default::default(), offline: false };
//! let opts = ValidateOptions {
//!     file: "sources.yaml".to_string(),
//!     offline: false,
//!     fail_on_warnings: false,
//!     dry_run: false,
//!     notify_url: None,
//!     format: OutputFormat::Human,
//! };
//! validate(opts, &config, &ctx).await?;
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

pub mod ai;
pub mod apikey;
pub mod db;
mod describe;
mod diff;
mod discovery;
mod helpers;
mod init;
mod remove;
mod secrets;
mod status;
mod sync;
mod validate;

// Re-export public command functions
pub use describe::{describe, test_connection};
pub use diff::{DiffOptions, diff};
pub use discovery::{AddOptions, add, search};
pub use helpers::DiffChange;

#[cfg(test)]
mod tests;

pub use init::init;
pub use remove::remove;
pub use secrets::validate_secrets;
pub use status::status;
pub use sync::{SyncOptions, sync};
pub use validate::{ValidateOptions, validate};
