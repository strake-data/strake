//! # AI Provider Registry
//!
//! Manages the lifecycle and resolution of AI description providers.
//! Handlers are initialized from environment variables and configuration.

use super::{AiDescriptionProvider, adapter::GenericChatProvider};
use super::{gemini, openai};
use crate::config::AiConfig;
use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, header::HeaderValue};
use std::collections::HashMap;

/// Registry of available AI providers.
///
/// This handles provider resolution from environment variables and configuration,
/// enforcing strict model selection.
#[derive(Default)]
pub struct AiProviderRegistry {
    /// Map of provider names to their implementations.
    ///
    /// NOTE: Iteration order is not deterministic as we use a standard HashMap.
    /// This is acceptable as we currently resolve providers by name.
    providers: HashMap<String, Box<dyn AiDescriptionProvider>>,
}

impl AiProviderRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register providers based on detected environment variables and CLI configuration.
    ///
    /// Only the `selected` provider is registered to avoid unnecessary errors
    /// (e.g., from missing models) for unused providers with active environment variables.
    pub fn register_from_env(mut self, config: &AiConfig, selected: &str) -> Result<Self> {
        let client = Client::new(); // one pool, shared by all providers

        match selected {
            "gemini" => {
                if let Ok(key) = std::env::var("GOOGLE_API_KEY") {
                    let model = resolve_model("gemini", config)?;
                    let api_key_value = HeaderValue::from_str(&key)
                        .context("GOOGLE_API_KEY contains characters invalid for an HTTP header")?;

                    self.providers.insert(
                        "gemini".to_string(),
                        Box::new(GenericChatProvider {
                            client,
                            adapter: gemini::GeminiAdapter {
                                api_key_value,
                                model,
                                temperature: config.temperature.unwrap_or(0.1),
                                url: config.url.clone(),
                            },
                        }),
                    );
                }
            }
            "openai" => {
                if let Ok(key) = std::env::var("OPENAI_API_KEY") {
                    let model = resolve_model("openai", config)?;
                    let api_key_value = HeaderValue::from_str(&format!("Bearer {}", key))
                        .context("OPENAI_API_KEY contains characters invalid for an HTTP header")?;

                    self.providers.insert(
                        "openai".to_string(),
                        Box::new(GenericChatProvider {
                            client,
                            adapter: openai::OpenAiAdapter {
                                api_key_value,
                                model,
                                temperature: config.temperature.unwrap_or(0.1),
                                url: config.url.clone(),
                            },
                        }),
                    );
                }
            }
            other => {
                bail!("Unknown AI provider '{}'. Supported: gemini, openai", other);
            }
        }

        Ok(self)
    }

    /// Resolve a specific provider by name.
    ///
    /// # Errors
    /// Returns an error if the provider name is not found in the registry,
    /// with a sorted list of available providers to guide the user.
    pub fn resolve(&self, name: &str) -> Result<&dyn AiDescriptionProvider> {
        self.providers.get(name).map(|p| p.as_ref()).ok_or_else(|| {
            let mut available: Vec<_> = self.providers.keys().cloned().collect();
            available.sort();
            let available_str = available.join(", ");

            anyhow!(
                "AI provider '{}' not found or not configured. \
                     Set the corresponding API key env var. \
                     Available: [{}]",
                name,
                if available_str.is_empty() {
                    "none".to_string()
                } else {
                    available_str
                }
            )
        })
    }
}

/// Robustly resolve a model name for a provider, prioritizing provider-specific
/// environment variables, then generic AI model environment variables, then
/// the configuration file.
fn resolve_model(provider: &str, config: &AiConfig) -> Result<String> {
    // 1. Provider-specific env var (e.g. STRAKE_GEMINI_MODEL)
    let env_prefix = format!("STRAKE_{}_MODEL", provider.to_uppercase());
    if let Ok(m) = std::env::var(&env_prefix) {
        return Ok(m);
    }

    // 2. Generic AI model env var
    if let Ok(m) = std::env::var("STRAKE_AI_MODEL") {
        return Ok(m);
    }

    // 3. Configuration file
    config.model.clone().ok_or_else(|| {
        anyhow!(
            "AI model not configured for provider '{}'. \
             Set {} or 'ai.model' in strake.yaml",
            provider,
            env_prefix
        )
    })
}
