//! # AI Descriptions Module
//!
//! Provides the ability to enrich table metadata with AI-generated column descriptions.
//! Supports multiple providers (OpenAI, Gemini) with a modular architecture.

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
    async fn enrich_descriptions(&self, table: &mut IntrospectedTable) -> Result<()>;
}
