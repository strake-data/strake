//! # Chat API Adapter
//!
//! Private abstraction layer between the shared HTTP executor
//! and per-provider wire formats.

use anyhow::Result;
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use serde_json::Value;
use strake_common::schema::IntrospectedTable;

use crate::commands::ai::prompt;

pub(super) const DEFAULT_RETRY_ATTEMPTS: u32 = 3;
pub(super) const DEFAULT_RETRY_BASE_SECS: u64 = 2;

/// Encodes only the wire-format differences between provider REST APIs.
/// Implementors must not perform HTTP calls directly; that is the
/// responsibility of [`GenericChatProvider`].
pub(super) trait ChatApiAdapter: Send + Sync {
    /// Returns the name of the provider (e.g., "gemini", "openai").
    fn name(&self) -> &'static str;
    /// Returns the full REST endpoint URL.
    fn endpoint(&self) -> String;
    /// Returns the headers required for the API call.
    fn headers(&self) -> HeaderMap;
    /// Builds the JSON request body for the given prompts.
    fn body(&self, system_prompt: &str, user_prompt: &str) -> Value;
    /// Extracts the generated text from the provider's JSON response.
    fn extract_text<'a>(&self, response: &'a Value) -> Option<&'a str>;
}

/// Generic HTTP executor. Written once; shared by every provider.
///
/// Handles the details of HTTP execution, including retries and response parsing,
/// using a provider-specific [`ChatApiAdapter`].
pub(super) struct GenericChatProvider<A: ChatApiAdapter> {
    /// Shared connection pool — constructed once per provider instance.
    pub(super) client: reqwest::Client,
    /// The provider-specific adapter.
    pub(super) adapter: A,
}

#[async_trait]
impl<A: ChatApiAdapter> super::AiDescriptionProvider for GenericChatProvider<A> {
    async fn enrich_descriptions(&self, table: &mut IntrospectedTable) -> Result<()> {
        let system_prompt = prompt::SYSTEM_PROMPT;
        let user_prompt = prompt::build_user_prompt(table);
        let body = self.adapter.body(system_prompt, &user_prompt);
        let url = self.adapter.endpoint();
        let headers = self.adapter.headers();

        let mut last_err = anyhow::anyhow!(
            "{}: all {} attempts failed",
            self.adapter.name(),
            DEFAULT_RETRY_ATTEMPTS
        );

        for attempt in 1..=DEFAULT_RETRY_ATTEMPTS {
            // We clone headers per attempt as reqwest::RequestBuilder::headers()
            // takes ownership. HeaderMap implements cheap cloning via internal Arc.
            let res = self
                .client
                .post(&url)
                .headers(headers.clone())
                .json(&body)
                .send()
                .await;

            match res {
                Ok(resp) if resp.status().is_success() => match resp.json::<Value>().await {
                    Ok(json) => match self.adapter.extract_text(&json) {
                        Some(text) => return prompt::apply_descriptions(table, text),
                        None => {
                            let raw = serde_json::to_string_pretty(&json)
                                .unwrap_or_else(|e| format!("<serialization error: {e}>"));
                            last_err = anyhow::anyhow!(
                                "{}: response contained no extractable text\nRaw: {}",
                                self.adapter.name(),
                                raw
                            );
                        }
                    },
                    Err(e) => {
                        last_err = anyhow::anyhow!(
                            "{}: failed to deserialise response body: {}",
                            self.adapter.name(),
                            e
                        );
                    }
                },
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    last_err =
                        anyhow::anyhow!("{} API error {}: {}", self.adapter.name(), status, text);
                }
                Err(e) => {
                    last_err = anyhow::anyhow!("{} connection error: {}", self.adapter.name(), e);
                }
            }

            if attempt < DEFAULT_RETRY_ATTEMPTS {
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    DEFAULT_RETRY_BASE_SECS * attempt as u64,
                ))
                .await;
            }
        }

        Err(last_err)
    }
}
