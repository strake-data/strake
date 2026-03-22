//! # Gemini Adapter
//!
//! Implementation of the Google Gemini generative language API.

use super::adapter::ChatApiAdapter;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};

/// Adapter for the Google Gemini API.
pub(super) struct GeminiAdapter {
    /// Pre-validated header value containing the API key.
    pub(super) api_key_value: HeaderValue,
    /// The model name to use (e.g., "gemini-1.5-pro-latest").
    pub(super) model: String,
    /// Sampling temperature.
    pub(super) temperature: f32,
    /// Optional base URL override.
    pub(super) url: Option<String>,
}

#[async_trait]
impl ChatApiAdapter for GeminiAdapter {
    fn name(&self) -> &'static str {
        "gemini"
    }

    fn endpoint(&self) -> String {
        self.url.clone().unwrap_or_else(|| {
            format!(
                "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent",
                self.model
            )
        })
    }

    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        // "x-goog-api-key" is the correct header for Gemini API key auth.
        // Authorization: Bearer is for OAuth2 tokens only.
        h.insert("x-goog-api-key", self.api_key_value.clone());
        h
    }

    fn body(&self, system_prompt: &str, user_prompt: &str) -> Value {
        json!({
            "system_instruction": {
                "parts": [{ "text": system_prompt }]
            },
            "contents": [{
                "role": "user",
                "parts": [{ "text": user_prompt }]
            }],
            "generationConfig": {
                "temperature": self.temperature,
            }
        })
    }

    fn extract_text<'a>(&self, r: &'a Value) -> Option<&'a str> {
        r.get("candidates")?
            .as_array()?
            .first()?
            .get("content")?
            .get("parts")?
            .as_array()?
            .first()?
            .get("text")?
            .as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_gemini_body_format() {
        let adapter = GeminiAdapter {
            api_key_value: HeaderValue::from_static("test-key"),
            model: "gemini-1.5-pro".to_string(),
            temperature: 0.5,
            url: None,
        };

        let body = adapter.body("sys prompt", "user prompt");

        let expected = json!({
            "system_instruction": {
                "parts": [{ "text": "sys prompt" }]
            },
            "contents": [{
                "role": "user",
                "parts": [{ "text": "user prompt" }]
            }],
            "generationConfig": {
                "temperature": 0.5,
            }
        });

        assert_eq!(body, expected);
    }
}
