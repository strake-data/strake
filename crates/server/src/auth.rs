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

use moka::future::Cache;
use std::time::Duration;
use strake_core::auth::AuthenticatedUser;

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

        // We need at least the prefix length (e.g., 8)
        if token_str.len() < 8 {
            return Err(Status::unauthenticated("Invalid API Key format"));
        }

        let prefix = &token_str[..8];

        let client = self.pool.get().await.map_err(|e| {
            tracing::error!("DB Pool error: {}", e);
            Status::internal("Authentication service unavailable")
        })?;

        // 2. Fetch candidate keys by prefix
        let rows = client.query(
            "SELECT id, key_hash, user_id, permissions FROM api_keys WHERE key_prefix = $1 AND revoked_at IS NULL",
            &[&prefix],
        ).await.map_err(|e| {
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
                // Update last_used_at (fire and forget or best effort)
                let key_id: uuid::Uuid = row.get("id");
                let _ = client
                    .execute(
                        "UPDATE api_keys SET last_used_at = NOW() WHERE id = $1",
                        &[&key_id],
                    )
                    .await;

                let user = AuthenticatedUser {
                    id: user_id,
                    permissions,
                    ..Default::default()
                };

                // 3. Populate Cache
                self.cache.insert(token_str.to_string(), user.clone()).await;

                return Ok(user);
            }
        }

        Err(Status::unauthenticated("Invalid API Key"))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::server::NamedService;
    use tower::{ServiceBuilder, ServiceExt};

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
