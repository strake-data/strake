use crate::sources::rest_auth::fetch_oauth_token;
use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use secrecy::{ExposeSecret, SecretString};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: SecretString,
    pub session_token: Option<SecretString>,
}

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"***REDACTED***")
            .field("session_token", &"***REDACTED***")
            .finish()
    }
}

impl S3Credentials {
    pub fn from_sdk(creds: &aws_credential_types::Credentials) -> Self {
        Self {
            access_key_id: creds.access_key_id().to_string(),
            secret_access_key: SecretString::new(creds.secret_access_key().to_string().into()),
            session_token: creds
                .session_token()
                .map(|t| SecretString::new(t.to_string().into())),
        }
    }
}

/// Pluggable authentication provider for Iceberg catalog
#[async_trait]
pub trait IcebergAuthProvider: Send + Sync + std::fmt::Debug {
    /// Get token for REST catalog requests
    async fn get_token(&self) -> Result<Option<String>>;

    /// Get credentials for S3/FileIO access
    async fn s3_credentials(&self) -> Result<Option<S3Credentials>>;
}

#[derive(Debug)]
pub struct OAuthIcebergAuth {
    client_id: String,
    client_secret: SecretString,
    token_url: String,
    scopes: Vec<String>,
}

impl OAuthIcebergAuth {
    pub fn new(
        client_id: String,
        client_secret: SecretString,
        token_url: String,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            token_url,
            scopes,
        }
    }
}

#[async_trait]
impl IcebergAuthProvider for OAuthIcebergAuth {
    async fn get_token(&self) -> Result<Option<String>> {
        let token = fetch_oauth_token(
            &self.client_id,
            self.client_secret.expose_secret(),
            &self.token_url,
            &self.scopes,
        )
        .await
        .context("Failed to fetch OAuth token")?;
        Ok(Some(token))
    }

    async fn s3_credentials(&self) -> Result<Option<S3Credentials>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct StaticTokenAuth {
    token: SecretString,
}

impl StaticTokenAuth {
    pub fn new(token: SecretString) -> Self {
        Self { token }
    }
}

#[async_trait]
impl IcebergAuthProvider for StaticTokenAuth {
    async fn get_token(&self) -> Result<Option<String>> {
        Ok(Some(self.token.expose_secret().to_string()))
    }

    async fn s3_credentials(&self) -> Result<Option<S3Credentials>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct AwsIrsaAuth {
    cache: RwLock<Option<(S3Credentials, SystemTime)>>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl Default for AwsIrsaAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsIrsaAuth {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait]
impl IcebergAuthProvider for AwsIrsaAuth {
    async fn get_token(&self) -> Result<Option<String>> {
        Ok(None)
    }

    async fn s3_credentials(&self) -> Result<Option<S3Credentials>> {
        // Fast path: check cache first (5 min buffer before expiry)
        {
            let read = self.cache.read().await;
            if let Some((creds, expiry)) = read.as_ref() {
                if let Some(valid_until) = expiry.checked_sub(Duration::from_secs(300)) {
                    if SystemTime::now() < valid_until {
                        return Ok(Some(creds.clone()));
                    }
                }
            }
        }

        // Coordinated refresh: only one task performs the refresh
        let _guard = self.refresh_lock.lock().await;

        // Double-check after acquiring lock (another task may have refreshed)
        {
            let read = self.cache.read().await;
            if let Some((creds, expiry)) = read.as_ref() {
                if let Some(valid_until) = expiry.checked_sub(Duration::from_secs(300)) {
                    if SystemTime::now() < valid_until {
                        tracing::debug!("AWS credentials refreshed by another task");
                        return Ok(Some(creds.clone()));
                    }
                }
            }
        }

        // Perform the actual refresh (only one task reaches here)
        tracing::debug!("Refreshing AWS credentials");
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let provider = config
            .credentials_provider()
            .ok_or_else(|| anyhow::anyhow!("No AWS credentials provider found"))?;

        let creds = provider
            .provide_credentials()
            .await
            .context("Failed to load AWS credentials")?;

        let s3_creds = S3Credentials::from_sdk(&creds);

        let mut write = self.cache.write().await;
        let expiry_time = match creds.expiry() {
            Some(expiry) => {
                // If creds expire within buffer, fallback to force refresh soon
                if let Ok(duration) = expiry.duration_since(SystemTime::now()) {
                    if duration < Duration::from_secs(300) {
                        tracing::warn!(
                            "AWS credentials expiring in {}s, refreshing soon",
                            duration.as_secs()
                        );
                        // Just use the expiry, checked_sub will handle invalidity next check
                        expiry
                    } else {
                        expiry
                    }
                } else {
                    // Already expired
                    return Err(anyhow::anyhow!(
                        "AWS credentials already expired at {:?} (now: {:?})",
                        expiry,
                        SystemTime::now()
                    ));
                }
            }
            None => SystemTime::now() + Duration::from_secs(3600),
        };

        *write = Some((s3_creds.clone(), expiry_time));

        tracing::debug!("AWS credentials refreshed successfully");
        Ok(Some(s3_creds))
    }
}

#[derive(Debug)]
pub struct CompositeAuth {
    rest_auth: Option<Box<dyn IcebergAuthProvider>>,
    s3_auth: Box<dyn IcebergAuthProvider>,
}

impl CompositeAuth {
    pub fn new(
        rest_auth: Option<Box<dyn IcebergAuthProvider>>,
        s3_auth: Box<dyn IcebergAuthProvider>,
    ) -> Self {
        Self { rest_auth, s3_auth }
    }
}

#[async_trait]
impl IcebergAuthProvider for CompositeAuth {
    async fn get_token(&self) -> Result<Option<String>> {
        if let Some(auth) = &self.rest_auth {
            auth.get_token().await
        } else {
            Ok(None)
        }
    }

    async fn s3_credentials(&self) -> Result<Option<S3Credentials>> {
        self.s3_auth.s3_credentials().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aws_irsa_auth_expiry_arithmetic() {
        let auth = AwsIrsaAuth::new();

        // Populate cache with an expiry very close to now (or in past)
        // to ensure checked_sub doesn't panic
        let creds = S3Credentials {
            access_key_id: "test".into(),
            secret_access_key: SecretString::new("secret".into()),
            session_token: None,
        };

        let now = SystemTime::now();
        // Case 1: Expiry in past (already expired)
        // expiry - 300s would underflow if using unchecked sub on Instant/SystemTime if not careful
        // SystemTime subtraction returns duration or error, but here we used:
        // if let Some(valid_until) = expiry.checked_sub(Duration::from_secs(300))

        let past_expiry = now - Duration::from_secs(1000);
        {
            let mut write = auth.cache.write().await;
            *write = Some((creds.clone(), past_expiry));
        }

        // Should not panic, should try to refresh (and fail network in test, or return error)
        let result = auth.s3_credentials().await;
        // It will try to refresh because cache check fails
        // We expect it to fail physically fetching creds in unit test environment without AWS setup
        assert!(result.is_err());

        // Case 2: Expiry in future but < 300s
        let near_future_expiry = now + Duration::from_secs(100);
        {
            let mut write = auth.cache.write().await;
            *write = Some((creds.clone(), near_future_expiry));
        }

        // valid_until = now + 100 - 300 -> Underflow?
        // SystemTime::checked_sub(duration) returns Some(time) or None.
        // if near_future_expiry < 300s from epoch? No.
        // near_future_expiry is big.
        // checked_sub(300) returns a time 300s before expiry.
        // valid_until = now - 200s.
        // check: now < valid_until?
        // now < now - 200s? False.
        // So it should try refresh.
        // This confirms logic is safe and correct for "expires soon".

        let result = auth.s3_credentials().await;
        assert!(result.is_err()); // Refresh initiated
    }
}
