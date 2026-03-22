//! # OpenAI Adapter
//!
//! Implementation of the OpenAI Chat Completion API.

use super::adapter::ChatApiAdapter;
use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::{Value, json};

/// Adapter for the OpenAI Chat Completion API.
pub(super) struct OpenAiAdapter {
    /// Pre-validated header value containing "Bearer <api_key>".
    pub(super) api_key_value: HeaderValue,
    /// The model name to use (e.g., "gpt-4o").
    pub(super) model: String,
    /// Sampling temperature.
    pub(super) temperature: f32,
    /// Optional base URL override.
    pub(super) url: Option<String>,
}

#[async_trait]
impl ChatApiAdapter for OpenAiAdapter {
    fn name(&self) -> &'static str {
        "openai"
    }

    fn endpoint(&self) -> String {
        self.url
            .clone()
            .unwrap_or_else(|| "https://api.openai.com/v1/chat/completions".to_string())
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(AUTHORIZATION, self.api_key_value.clone());
        headers
    }

    fn body(&self, system_prompt: &str, user_prompt: &str) -> Value {
        json!({
            "model": self.model,
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user", "content": user_prompt }
            ],
            "temperature": self.temperature,
            "response_format": { "type": "json_object" }
        })
    }

    fn extract_text<'a>(&self, r: &'a Value) -> Option<&'a str> {
        r.get("choices")?
            .as_array()?
            .first()?
            .get("message")?
            .get("content")?
            .as_str()
    }
}
