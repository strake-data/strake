//! # AI Descriptions Module
//!
//! Provides the ability to enrich table metadata with AI-generated column descriptions.
//! Supports multiple providers (OpenAI, Gemini) with a modular architecture.
//!
//! ## Overview
//!
//! This module coordinates the interaction with AI models to automatically introspect,
//! describe, and document database columns.
//!
//! ## Usage
//!
//! ```ignore
//! // let provider = registry.get("openai").unwrap();
//! // provider.enrich_descriptions(&mut table).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! Network requests are bounded by 30-second timeouts and execute with exponential backoff
//! retry policies to avoid blocking the main runtime thread pool indefinitely.
//!
//! ## Safety
//!
//! Validates prompts against payload sizes to prevent stack overflows and runs in standard safe Rust.
//!
//! ## Errors
//!
//! Propagates request timeout errors, connection errors, and authentication context failures.
//!
//! ## References
//!
//! - AI Enrichment Specs.

pub mod registry;
pub use registry::AiProviderRegistry;

mod adapter;
mod gemini;
mod prompt;
// mod anthropic; // TODO(#123): Implement Anthropic provider
mod openai;

use anyhow::Result;
use async_trait::async_trait;
use strake_common::schema::IntrospectedTable;

/// Public contract for all AI description providers.
#[async_trait]
pub trait AiDescriptionProvider: Send + Sync {
    /// Enriches the `table` inline with AI generated descriptions.
    ///
    /// # Errors
    /// Returns an error if the API request fails, times out, or receives an invalid response format.
    async fn enrich_descriptions(&self, table: &mut IntrospectedTable) -> Result<()>;
}
