extern crate async_trait;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use std::collections::HashMap;
use strake_common::schema::IntrospectedTable;

#[async_trait]
pub trait AiDescriptionProvider: Send + Sync {
    /// Enrich the table and column descriptions using AI.
    async fn enrich_descriptions(&self, table: &mut IntrospectedTable) -> Result<()>;

    /// Returns the provider name (e.g., "gemini", "anthropic").
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

pub struct AiProviderRegistry {
    providers: HashMap<String, Box<dyn AiDescriptionProvider>>,
}

impl AiProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, provider: Box<dyn AiDescriptionProvider>) {
        self.providers.insert(name.to_string(), provider);
    }

    pub fn get(&self, name: &str) -> Option<&dyn AiDescriptionProvider> {
        self.providers.get(name).map(|p| p.as_ref())
    }

    pub fn resolve_from_env(&self) -> Result<&dyn AiDescriptionProvider> {
        let provider_name =
            std::env::var("STRAKE_AI_PROVIDER").unwrap_or_else(|_| "gemini".to_string());
        self.get(&provider_name)
            .ok_or_else(|| anyhow!("AI provider '{}' not found", provider_name))
    }
}

// ----------------------------------------------------------------------------
// Gemini Implementation
// ----------------------------------------------------------------------------

pub struct GeminiProvider {
    pub api_key: String,
    pub model: String,
}

#[async_trait]
impl AiDescriptionProvider for GeminiProvider {
    async fn enrich_descriptions(&self, table: &mut IntrospectedTable) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            self.model, self.api_key
        );

        let prompt = self.build_prompt(table);

        let payload = serde_json::json!({
            "contents": [{
                "parts": [{
                    "text": prompt
                }]
            }],
            "generationConfig": {
                "temperature": 0.1,
                "topP": 0.95,
                "topK": 40,
                "maxOutputTokens": 1024,
                "responseMimeType": "application/json"
            }
        });

        let mut last_err = None;
        for attempt in 1..=3 {
            match client.post(&url).json(&payload).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let json: serde_json::Value = resp.json().await?;
                        if let Some(text) =
                            json["candidates"][0]["content"]["parts"][0]["text"].as_str()
                        {
                            self.parse_and_apply_descriptions(table, text)?;
                            return Ok(());
                        }
                    } else {
                        let err_text = resp.text().await?;
                        last_err = Some(anyhow!("Gemini API error: {}", err_text));
                    }
                }
                Err(e) => {
                    last_err = Some(anyhow!("Gemini connection error: {}", e));
                }
            }
            if attempt < 3 {
                tokio::time::sleep(tokio::time::Duration::from_secs(attempt * 2)).await;
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("Gemini enrichment failed after 3 attempts")))
    }

    fn name(&self) -> &'static str {
        "gemini"
    }
}

impl GeminiProvider {
    fn build_prompt(&self, table: &IntrospectedTable) -> String {
        let mut cols = Vec::new();
        for col in &table.columns {
            cols.push(format!(
                "- {}: {} ({})",
                col.name,
                col.type_str,
                if col.nullable { "nullable" } else { "not null" }
            ));
        }

        format!(
            "Generate concise, one-sentence business descriptions for columns in the table '{}.{}'.\n\
            Columns:\n{}\n\n\
            Return exactly a JSON object with a 'columns' key mapping column names to descriptions.",
            table.schema,
            table.name,
            cols.join("\n")
        )
    }

    fn parse_and_apply_descriptions(
        &self,
        table: &mut IntrospectedTable,
        json_text: &str,
    ) -> Result<()> {
        let parsed: serde_json::Value =
            serde_json::from_str(json_text).context("Failed to parse Gemini JSON response")?;

        let cols: &serde_json::Map<String, serde_json::Value> = parsed["columns"]
            .as_object()
            .context("Expected 'columns' field to be an object in Gemini response")?;

        for col in &mut table.columns {
            if let Some(desc) = cols.get(&col.name).and_then(|v| v.as_str()) {
                col.ai_description = Some(desc.to_string());
            }
        }
        Ok(())
    }
}

// ----------------------------------------------------------------------------
// Anthropic Implementation
// ----------------------------------------------------------------------------

pub struct AnthropicProvider {
    pub _api_key: String,
    pub _model: String,
}

#[async_trait]
impl AiDescriptionProvider for AnthropicProvider {
    async fn enrich_descriptions(&self, table: &mut IntrospectedTable) -> Result<()> {
        // Implement Anthropic Messages API call here.
        for col in &mut table.columns {
            col.ai_description = Some(format!("Anthropic-generated description for {}", col.name));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "anthropic"
    }
}
