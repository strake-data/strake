//! Authentication helpers for REST datasources.
//!
//! Supports OAuth 2.0 Client Credentials flow with token caching,
//! and self-signed JWT assertions for service account authentication.

use anyhow::{Context, Result};
use moka::future::Cache;
use serde::Deserialize;
use std::time::{Duration, Instant};

/// OAuth 2.0 token response from the authorization server.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub scope: Option<String>,
}

/// Cached token with expiry tracking.
#[derive(Clone, Debug)]
pub struct CachedToken {
    pub access_token: String,
    pub expires_at: Instant,
}

impl CachedToken {
    /// Check if token is expired (with 60s buffer).
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at - Duration::from_secs(60)
    }
}

/// OAuth Client Credentials configuration.
#[derive(Debug, Clone)]
pub struct OAuthClientCredentialsConfig {
    pub client_id: String,
    pub client_secret: String,
    pub token_url: String,
    pub scopes: Vec<String>,
}

/// JWT Assertion configuration for self-signed JWTs.
#[derive(Debug, Deserialize, Clone)]
pub struct JwtAssertionConfig {
    pub issuer: String,
    pub audience: String,
    pub private_key_pem: String, // PEM-encoded private key
    #[serde(default = "default_algorithm")]
    pub algorithm: String, // RS256, ES256
    #[serde(default = "default_expiry_secs")]
    pub expiry_secs: u64,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub claims: std::collections::HashMap<String, serde_json::Value>,
}

fn default_algorithm() -> String {
    "RS256".to_string()
}

fn default_expiry_secs() -> u64 {
    3600
}

/// Thread-safe OAuth token cache.
pub struct OAuthTokenCache {
    /// Cache keyed by token_url + client_id
    cache: Cache<String, CachedToken>,
    client: reqwest::Client,
}

impl OAuthTokenCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(3600))
                .max_capacity(100)
                .build(),
            client: reqwest::Client::new(),
        }
    }

    /// Get a valid token, fetching/refreshing if needed.
    pub async fn get_token(&self, config: &OAuthClientCredentialsConfig) -> Result<String> {
        let cache_key = format!("{}:{}", config.token_url, config.client_id);
        let config_clone = config.clone();

        // Use moka's try_get_with for atomic check-and-set (prevents thundering herd/TOCTOU)
        // Fix [Concurrency]: TOCTOU Race
        let config_for_cache = config_clone.clone();
        let mut cached = self
            .cache
            .try_get_with(cache_key.clone(), async move {
                tracing::info!(
                    "Fetching new OAuth token from {}",
                    config_for_cache.token_url
                );
                let response = self.fetch_token(&config_for_cache).await?;

                let expires_in = response.expires_in.unwrap_or(3600);
                Ok(CachedToken {
                    access_token: response.access_token,
                    expires_at: Instant::now() + Duration::from_secs(expires_in),
                }) as Result<CachedToken>
            })
            .await
            .map_err(|e| anyhow::anyhow!("Token fetch failed: {}", e))?;

        if cached.is_expired() {
            tracing::info!("Token expired (buffer check), refreshing...");
            self.cache.invalidate(&cache_key).await;

            // Retry fetch
            let config_for_retry = config_clone.clone();
            cached = self
                .cache
                .try_get_with(cache_key, async move {
                    tracing::info!(
                        "Fetching new OAuth token from {} (retry)",
                        config_for_retry.token_url
                    );
                    let response = self.fetch_token(&config_for_retry).await?;

                    let expires_in = response.expires_in.unwrap_or(3600);
                    Ok(CachedToken {
                        access_token: response.access_token,
                        expires_at: Instant::now() + Duration::from_secs(expires_in),
                    }) as Result<CachedToken>
                })
                .await
                .map_err(|e| anyhow::anyhow!("Token fetch failed: {}", e))?;
        }

        Ok(cached.access_token)
    }

    /// Fetch a new token from the OAuth server.
    async fn fetch_token(&self, config: &OAuthClientCredentialsConfig) -> Result<TokenResponse> {
        let scope = config.scopes.join(" ");

        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", &config.client_id),
            ("client_secret", &config.client_secret),
        ];

        if !scope.is_empty() {
            form.push(("scope", &scope));
        }

        let resp = self
            .client
            .post(&config.token_url)
            .form(&form)
            .send()
            .await
            .context("Failed to send OAuth token request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("OAuth token request failed: {} - {}", status, body);
        }

        resp.json::<TokenResponse>()
            .await
            .context("Failed to parse OAuth token response")
    }
}

impl Default for OAuthTokenCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Global token cache singleton.
static OAUTH_TOKEN_CACHE: std::sync::OnceLock<OAuthTokenCache> = std::sync::OnceLock::new();

/// Get the global OAuth token cache.
pub fn global_token_cache() -> &'static OAuthTokenCache {
    OAUTH_TOKEN_CACHE.get_or_init(OAuthTokenCache::new)
}

/// Fetch an OAuth access token using client credentials flow.
pub async fn fetch_oauth_token(
    client_id: &str,
    client_secret: &str,
    token_url: &str,
    scopes: &[String],
) -> Result<String> {
    let config = OAuthClientCredentialsConfig {
        client_id: client_id.to_string(),
        client_secret: client_secret.to_string(),
        token_url: token_url.to_string(),
        scopes: scopes.to_vec(),
    };

    global_token_cache().get_token(&config).await
}

/// Generate a self-signed JWT assertion.
pub fn generate_jwt_assertion(config: &JwtAssertionConfig) -> Result<String> {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    let algorithm = match config.algorithm.as_str() {
        "RS256" => Algorithm::RS256,
        "RS384" => Algorithm::RS384,
        "RS512" => Algorithm::RS512,
        "ES256" => Algorithm::ES256,
        "ES384" => Algorithm::ES384,
        other => anyhow::bail!("Unsupported JWT algorithm: {}", other),
    };

    let mut header = Header::new(algorithm);
    header.typ = Some("JWT".to_string());

    // Build claims
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut claims = serde_json::Map::new();
    claims.insert(
        "iss".to_string(),
        serde_json::Value::String(config.issuer.clone()),
    );
    claims.insert(
        "aud".to_string(),
        serde_json::Value::String(config.audience.clone()),
    );
    claims.insert("iat".to_string(), serde_json::json!(now));
    claims.insert(
        "exp".to_string(),
        serde_json::json!(now + config.expiry_secs),
    );

    if let Some(sub) = &config.subject {
        claims.insert("sub".to_string(), serde_json::Value::String(sub.clone()));
    }

    // Merge custom claims
    for (k, v) in &config.claims {
        claims.insert(k.clone(), v.clone());
    }

    let encoding_key = if config.algorithm.starts_with("RS") {
        EncodingKey::from_rsa_pem(config.private_key_pem.as_bytes())
            .context("Invalid RSA private key PEM")?
    } else {
        EncodingKey::from_ec_pem(config.private_key_pem.as_bytes())
            .context("Invalid EC private key PEM")?
    };

    encode(&header, &claims, &encoding_key).context("Failed to encode JWT assertion")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_fetch_oauth_token_success() {
        let mock_server: MockServer = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("client_id=test_client"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "mock_token_12345",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .mount(&mock_server)
            .await;

        let cache = OAuthTokenCache::new();
        let config = OAuthClientCredentialsConfig {
            client_id: "test_client".to_string(),
            client_secret: "test_secret".to_string(),
            token_url: format!("{}/oauth/token", mock_server.uri()),
            scopes: vec!["read".to_string()],
        };

        let token = cache.get_token(&config).await.expect("Token fetch failed");
        assert_eq!(token, "mock_token_12345");
    }

    #[tokio::test]
    async fn test_token_caching() {
        let mock_server: MockServer = MockServer::start().await;

        // Set up mock to track call count
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "cached_token",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .expect(1) // Should only be called once due to caching
            .mount(&mock_server)
            .await;

        let cache = OAuthTokenCache::new();
        let config = OAuthClientCredentialsConfig {
            client_id: "cache_test".to_string(),
            client_secret: "secret".to_string(),
            token_url: format!("{}/oauth/token", mock_server.uri()),
            scopes: vec![],
        };

        // First call - fetches from server
        let token1 = cache.get_token(&config).await.unwrap();
        // Second call - should use cache
        let token2 = cache.get_token(&config).await.unwrap();

        assert_eq!(token1, token2);
        assert_eq!(token1, "cached_token");
    }

    #[tokio::test]
    async fn test_oauth_token_failure() {
        let mock_server: MockServer = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_string("invalid_client: Client authentication failed"),
            )
            .mount(&mock_server)
            .await;

        let cache = OAuthTokenCache::new();
        let config = OAuthClientCredentialsConfig {
            client_id: "bad_client".to_string(),
            client_secret: "bad_secret".to_string(),
            token_url: format!("{}/oauth/token", mock_server.uri()),
            scopes: vec![],
        };

        let result = cache.get_token(&config).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("401") || err.contains("failed"));
    }

    #[tokio::test]
    async fn test_token_refresh_after_expiry() {
        let mock_server: MockServer = MockServer::start().await;

        // First response - short expiry
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token1",
                "token_type": "Bearer",
                "expires_in": 1 // Very short
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second response - new token
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token2",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .mount(&mock_server)
            .await;

        let cache = OAuthTokenCache::new();
        let config = OAuthClientCredentialsConfig {
            client_id: "refresh_test".to_string(),
            client_secret: "secret".to_string(),
            token_url: format!("{}/oauth/token", mock_server.uri()),
            scopes: vec![],
        };

        // First fetch
        // Token1 expires in 1s. Our buffer is 60s. So it is considered expired immediately.
        // The cache logic should retry and fetch token2.
        let t1 = cache.get_token(&config).await.unwrap();
        assert_eq!(t1, "token2");

        // Second fetch should return the cached token2 (valid for 3600s)
        let t2 = cache.get_token(&config).await.unwrap();
        assert_eq!(t2, "token2");
    }

    #[tokio::test]
    async fn test_oauth_invalid_json() {
        let mock_server: MockServer = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_string("corrupt{json"))
            .mount(&mock_server)
            .await;

        let cache = OAuthTokenCache::new();
        let config = OAuthClientCredentialsConfig {
            client_id: "json_test".to_string(),
            client_secret: "secret".to_string(),
            token_url: format!("{}/oauth/token", mock_server.uri()),
            scopes: vec![],
        };

        let result = cache.get_token(&config).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("parse"));
    }

    #[test]
    fn test_oauth_config_parsing() {
        let yaml = r#"
            client_id: "my_client"
            client_secret: "my_secret"
            token_url: "https://auth.example.com/oauth/token"
            scopes:
              - "read:data"
              - "write:data"
        "#;

        #[derive(Debug, Deserialize)]
        #[allow(dead_code)] // Test struct - fields verified via deserialization
        struct TestConfig {
            client_id: String,
            client_secret: String,
            token_url: String,
            #[serde(default)]
            scopes: Vec<String>,
        }

        let config: TestConfig = serde_yaml::from_str(yaml).expect("Failed to parse");
        assert_eq!(config.client_id, "my_client");
        assert_eq!(config.client_secret, "my_secret");
        assert_eq!(config.token_url, "https://auth.example.com/oauth/token");
        assert_eq!(config.scopes.len(), 2);
    }

    #[test]
    fn test_jwt_assertion_config_parsing() {
        let yaml = r#"
            issuer: "service@project.iam.example.com"
            audience: "https://api.example.com"
            private_key_pem: "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
            algorithm: "RS256"
            expiry_secs: 1800
            claims:
              custom: "value"
        "#;

        let config: JwtAssertionConfig = serde_yaml::from_str(yaml).expect("Failed to parse");
        assert_eq!(config.issuer, "service@project.iam.example.com");
        assert_eq!(config.audience, "https://api.example.com");
        assert_eq!(config.algorithm, "RS256");
        assert_eq!(config.expiry_secs, 1800);
        assert!(config.claims.contains_key("custom"));
    }
}
