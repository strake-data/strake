use argon2::{Argon2, PasswordHash, PasswordVerifier};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::{metadata::MetadataMap, Status};
use tower::{Layer, Service, ServiceExt};

use axum::{
    body::Body as AxumBody,
    http::{Request as AxumRequest, Response as AxumResponse},
    middleware::Next,
};
use moka::future::Cache;
use std::time::Duration;
use strake_common::auth::{AuthenticatedUser, PermissionSet};

pub const API_KEY_PREFIX_LENGTH: usize = 8;

#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<AuthenticatedUser, Status>;
}

// OSS Implementation: Database-backed API Keys with Argon2
pub struct ApiKeyAuthenticator {
    pool: Pool,
    cache: Cache<String, AuthenticatedUser>,
}

impl ApiKeyAuthenticator {
    pub fn new(pool: Pool, ttl_secs: u64, capacity: u64) -> Self {
        let cache = Cache::builder()
            .time_to_live(Duration::from_secs(ttl_secs))
            .max_capacity(capacity)
            .build();
        Self { pool, cache }
    }
}

#[async_trait]
impl Authenticator for ApiKeyAuthenticator {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<AuthenticatedUser, Status> {
        let token = match metadata.get("authorization") {
            Some(t) => t
                .to_str()
                .map_err(|_| Status::unauthenticated("Invalid auth header"))?,
            None => return Err(Status::unauthenticated("Missing authorization header")),
        };

        let token_str = token.strip_prefix("Bearer ").unwrap_or(token);

        // 1. Check Cache
        if let Some(user) = self.cache.get(token_str).await {
            return Ok(user);
        }

        // 2. Verify Credentials
        let (user_id, _key_id, permissions) =
            verify_api_key_credentials(&self.pool, token_str).await?;

        let user = AuthenticatedUser {
            id: user_id,
            permissions: PermissionSet::from(permissions),
            ..Default::default()
        };

        // 3. Populate Cache
        self.cache.insert(token_str.to_string(), user.clone()).await;

        Ok(user)
    }
}

/// Verifies an API key against the database.
///
/// This function:
/// 1. Validates the key format and length.
/// 2. Fetches candidate keys from the database using the prefix.
/// 3. Verifies the full key using Argon2 hashing.
/// 5. Updates the `last_used_at` timestamp on success.
///
/// Returns `(user_id, key_id, permissions)` on success.
pub async fn verify_api_key_credentials(
    pool: &Pool,
    token_str: &str,
) -> Result<(String, uuid::Uuid, Vec<String>), Status> {
    // We need at least the prefix length
    if token_str.len() < API_KEY_PREFIX_LENGTH {
        return Err(Status::unauthenticated("Invalid API Key format"));
    }

    let prefix = &token_str[..API_KEY_PREFIX_LENGTH];

    let client = pool.get().await.map_err(|e| {
        tracing::error!("DB Pool error: {}", e);
        Status::internal("Authentication service unavailable")
    })?;

    // Fetch candidate keys by prefix
    let rows = client
        .query(
            "SELECT id, key_hash, user_id, permissions FROM api_keys WHERE key_prefix = $1 AND revoked_at IS NULL",
            &[&prefix],
        )
        .await
        .map_err(|e| {
            tracing::error!("DB Query error: {}", e);
            Status::internal("Authentication service error")
        })?;

    let argon2 = Argon2::default();

    for row in rows {
        let hash_str: String = row.get("key_hash");
        let user_id: String = row.get("user_id");
        let permissions: Vec<String> = row.get("permissions");

        let parsed_hash = PasswordHash::new(&hash_str)
            .map_err(|_| Status::internal("Invalid key hash in database"))?;

        if argon2
            .verify_password(token_str.as_bytes(), &parsed_hash)
            .is_ok()
        {
            // Update last_used_at (best effort)
            let key_id: uuid::Uuid = row.get("id");
            if let Err(e) = client
                .execute(
                    "UPDATE api_keys SET last_used_at = NOW() WHERE id = $1",
                    &[&key_id],
                )
                .await
            {
                tracing::error!(
                    key_id = %key_id,
                    error = %e,
                    "Failed to update API key last_used_at timestamp"
                );
            }

            return Ok((user_id, key_id, permissions));
        }
    }

    Err(Status::unauthenticated("Invalid API Key"))
}

// Tower Middleware for Async Auth (Unchanged from previous successful version)
#[derive(Clone)]
pub struct AuthLayer {
    authenticator: Arc<dyn Authenticator>,
}

impl AuthLayer {
    pub fn new(authenticator: Arc<dyn Authenticator>) -> Self {
        Self { authenticator }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            authenticator: self.authenticator.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    authenticator: Arc<dyn Authenticator>,
}

impl<S, ReqBody> Service<Request<ReqBody>> for AuthService<S>
where
    S: Service<Request<ReqBody>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        let inner = self.inner.clone();
        let authenticator = self.authenticator.clone();

        Box::pin(async move {
            let metadata = MetadataMap::from_headers(req.headers().clone());

            match authenticator.authenticate(&metadata).await {
                Ok(user) => {
                    req.extensions_mut().insert(user);
                    // Use oneshot to ensure readiness (resolves 'send_item called without first calling poll_reserve')
                    inner.oneshot(req).await
                }
                Err(status) => {
                    let (parts, body) = status.into_http().into_parts();
                    Ok(Response::from_parts(parts, body))
                }
            }
        })
    }
}

pub async fn axum_auth_middleware(
    req: AxumRequest<AxumBody>,
    next: Next,
    authenticator: Arc<dyn Authenticator>,
) -> AxumResponse<AxumBody> {
    let metadata = MetadataMap::from_headers(req.headers().clone());
    tracing::info!(
        "Auth middleware: Checking request with headers: {:?}",
        req.headers()
    );

    match authenticator.authenticate(&metadata).await {
        Ok(user) => {
            tracing::info!("Auth middleware: Success for user {}", user.id);
            let mut req = req;
            req.extensions_mut().insert(user);
            next.run(req).await
        }
        Err(status) => {
            tracing::warn!("Auth middleware: Failed. Status: {:?}", status);
            // Return proper HTTP 401 for REST API
            let json_err = serde_json::json!({
                "status": "error",
                "message": status.message(),
                "code": "UNAUTHENTICATED"
            });

            let body = AxumBody::from(serde_json::to_vec(&json_err).unwrap_or_default());

            AxumResponse::builder()
                .status(axum::http::StatusCode::UNAUTHORIZED)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(body)
                .unwrap_or_else(|_| AxumResponse::new(AxumBody::empty()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::server::NamedService;
    use tower::{ServiceBuilder, ServiceExt};

    #[tokio::test]
    async fn test_verify_api_key_credentials_format() {
        let mut cfg = deadpool_postgres::Config::new();
        cfg.host = Some("localhost".to_string());
        cfg.dbname = Some("strake".to_string());
        let pool = cfg.create_pool(None, tokio_postgres::NoTls).unwrap();

        let res = verify_api_key_credentials(&pool, "short").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().message(), "Invalid API Key format");
    }

    #[derive(Clone)]
    struct MockAuthenticator;

    #[async_trait]
    impl Authenticator for MockAuthenticator {
        async fn authenticate(&self, _metadata: &MetadataMap) -> Result<AuthenticatedUser, Status> {
            Ok(AuthenticatedUser::default())
        }
    }

    #[derive(Clone)]
    struct MockService;

    impl Service<Request<Body>> for MockService {
        type Response = Response<Body>;
        type Error = Status;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            Box::pin(async move { Ok(Response::new(Body::empty())) })
        }
    }

    impl NamedService for MockService {
        const NAME: &'static str = "mock";
    }

    #[tokio::test]
    async fn test_auth_layer_with_buffer() {
        let authenticator = Arc::new(MockAuthenticator);
        let layer = AuthLayer::new(authenticator);
        let service = MockService;

        // Wrap the service in a Buffer, then apply AuthLayer.
        // This reproduces the structure where AuthLayer holds a Buffer.
        // Use a small buffer size to ensure we exercise the logic.
        let buffered = ServiceBuilder::new()
            .layer(layer)
            .buffer(1)
            .service(service);

        // Use the buffered service (which involves AuthLayer cloning the inner service)
        let mut client = buffered;

        // This should not panic
        let req = Request::new(Body::empty());
        let _ = client.ready().await.unwrap().call(req).await.unwrap();
    }
}
