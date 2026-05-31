//! # Database Management Commands
//!
//! CLI commands for initializing and migrating the metadata database schema.
//!
//! ## Overview
//!
//! Provides the primary operational boundaries for setting up the local SQLite or
//! remote Postgres database schemas. It ensures that metadata schemas are kept
//! in synchronization with the server expectations.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let code = db::init(&store).await?;
//! let code = db::migrate(&store).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! These commands are extremely lightweight, applying SQL migration scripts sequentially.
//! Operation is highly network and database-bound.
//!
//! ## Errors
//!
//! Returns `Err` if the target metadata database is unreachable or if a migration script
//! fails to execute.

use crate::exit_codes;
use crate::metadata::MetadataStore;
use anyhow::Result;

/// Initializes a fresh metadata database by running migrations.
///
/// # Errors
///
/// Returns an error if the connection or schema creation queries fail.
pub async fn init(store: &dyn MetadataStore) -> Result<i32> {
    println!("Initializing metadata database...");
    store.init().await?;
    println!("Metadata database initialized successfully.");
    Ok(exit_codes::EXIT_OK)
}

/// Runs outstanding migrations to bring the database schema up to date.
///
/// # Errors
///
/// Returns an error if applying pending database migrations fails.
pub async fn migrate(store: &dyn MetadataStore) -> Result<i32> {
    println!("Running outstanding database migrations...");
    store.migrate().await?;
    println!("Database migrations completed successfully.");
    Ok(exit_codes::EXIT_OK)
}
