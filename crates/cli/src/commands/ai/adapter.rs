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

use futures::future::BoxFuture;
use std::future::Future;

/// Configures retry execution strategy.
///
/// # Object Safety
/// Not object-safe by design because of generic parameter `F`. Use `DefaultRetryPolicy` directly
/// or wrap in a concrete type-erased adapter if dynamic dispatch is required.
#[must_use = "retry results must be handled"]
pub(super) trait RetryPolicy: Send + Sync {
    /// Execute an operation under a retry strategy.
    #[must_use = "retry results must be handled"]
    fn execute<'a, F, Fut, T, E>(&'a self, operation: F) -> BoxFuture<'a, Result<T, E>>
    where
        F: Fn() -> Fut + Send + Sync + 'a,
        Fut: Future<Output = Result<T, E>> + Send + 'a,
        T: Send + 'a,
        E: Send + 'a;
}

pub(super) const DEFAULT_RETRY_ATTEMPTS: u32 = 3;
pub(super) const DEFAULT_RETRY_BASE_SECS: u64 = 2;

/// Default exponential backoff policy.
pub(super) struct DefaultRetryPolicy;

impl DefaultRetryPolicy {
    /// Runs the provided operation with retries.
    #[must_use = "retrying an operation does nothing unless the result is used"]
    pub async fn run<F, Fut, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut last_err = match operation().await {
            Ok(val) => return Ok(val),
            Err(e) => e,
        };

        for attempt in 1..=DEFAULT_RETRY_ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_secs(
                DEFAULT_RETRY_BASE_SECS * attempt as u64,
            ))
            .await;
            match operation().await {
                Ok(val) => return Ok(val),
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }
}

impl RetryPolicy for DefaultRetryPolicy {
    fn execute<'a, F, Fut, T, E>(&'a self, operation: F) -> BoxFuture<'a, Result<T, E>>
    where
        F: Fn() -> Fut + Send + Sync + 'a,
        Fut: Future<Output = Result<T, E>> + Send + 'a,
        T: Send + 'a,
        E: Send + 'a,
    {
        Box::pin(async move { self.run(operation).await })
    }
}

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

        let make_request = || async {
            let res_future = self
                .client
                .post(&url)
                .headers(headers.clone())
                .json(&body)
                .send();

            let resp = tokio::time::timeout(std::time::Duration::from_secs(30), res_future)
                .await
                .map_err(|_| anyhow::anyhow!("{}: request timed out", self.adapter.name()))?
                .map_err(|e| anyhow::anyhow!("{} connection error: {}", self.adapter.name(), e))?;

            if resp.status().is_success() {
                let json_future = resp.json::<Value>();
                let json = tokio::time::timeout(std::time::Duration::from_secs(30), json_future)
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("{}: response body read timed out", self.adapter.name())
                    })?
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "{}: failed to deserialise response body: {}",
                            self.adapter.name(),
                            e
                        )
                    })?;

                match self.adapter.extract_text(&json) {
                    Some(text) => Ok(text.to_string()),
                    None => {
                        let raw = serde_json::to_string_pretty(&json)
                            .unwrap_or_else(|e| format!("<serialization error: {e}>"));
                        Err(anyhow::anyhow!(
                            "{}: response contained no extractable text\nRaw: {}",
                            self.adapter.name(),
                            raw
                        ))
                    }
                }
            } else {
                let status = resp.status();
                let text_future = resp.text();
                let text = tokio::time::timeout(std::time::Duration::from_secs(30), text_future)
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("{}: error body read timed out", self.adapter.name())
                    })?
                    .unwrap_or_default();
                Err(anyhow::anyhow!(
                    "{} API error {}: {}",
                    self.adapter.name(),
                    status,
                    text
                ))
            }
        };

        let retry_policy = DefaultRetryPolicy;
        let text_result = retry_policy.execute(make_request).await?;
        prompt::apply_descriptions(table, &text_result)
    }
}
