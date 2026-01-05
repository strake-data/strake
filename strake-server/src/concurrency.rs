use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::Status;
use tower::{Layer, Service, ServiceExt};

/// Abstraction for connection slot management
#[async_trait]
pub trait ConnectionSlotManager: Send + Sync {
    /// Acquire a slot, waiting up to `timeout` if none available
    async fn acquire(&self, timeout: Duration) -> Result<ConnectionSlot, SlotError>;

    /// Current active connections
    fn active_count(&self) -> usize;

    /// Maximum allowed connections
    fn max_connections(&self) -> usize;
}

pub struct ConnectionSlot {
    /// Opaque guard that holds the license slot.
    /// Access to the underlying resource is abstracted.
    /// When this struct is dropped, the slot is released (via the inner guard's Drop impl).
    pub guard: Box<dyn SlotGuard>,
}

pub trait SlotGuard: Send + Sync {}
// Implement for OwnedSemaphorePermit? No, it's external.
// Wrapper struct `LocalPermit(OwnedSemaphorePermit)` implements SlotGuard.

impl ConnectionSlot {
    pub fn new(guard: Box<dyn SlotGuard>) -> Self {
        Self { guard }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SlotError {
    #[error("Connection queue timeout waiting for slot")]
    QueueTimeout,
    #[error("License connection limit exceeded")]
    LicenseExceeded,
    #[error("Internal concurrency error: {0}")]
    Internal(String),
}

const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ConcurrencyLayer {
    slot_manager: Option<Arc<dyn ConnectionSlotManager>>,
    queue_timeout: Duration,
}

impl ConcurrencyLayer {
    pub fn new(slot_manager: Option<Arc<dyn ConnectionSlotManager>>) -> Self {
        Self {
            slot_manager,
            queue_timeout: DEFAULT_QUEUE_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.queue_timeout = timeout;
        self
    }
}

impl<S> Layer<S> for ConcurrencyLayer {
    type Service = ConcurrencyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyService {
            inner,
            slot_manager: self.slot_manager.clone(),
            queue_timeout: self.queue_timeout,
        }
    }
}

#[derive(Clone)]
pub struct ConcurrencyService<S> {
    inner: S,
    slot_manager: Option<Arc<dyn ConnectionSlotManager>>,
    queue_timeout: Duration,
}

impl<S, Request> Service<Request> for ConcurrencyService<S>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send + 'static,
    Request: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send, // inner error must be boxable
{
    type Response = S::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>; // WE return this
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let inner = self.inner.clone();
        let slot_manager = self.slot_manager.clone();
        let timeout = self.queue_timeout;

        Box::pin(async move {
            let _slot = if let Some(mgr) = slot_manager {
                match mgr.acquire(timeout).await {
                    Ok(slot) => Some(slot),
                    Err(e) => {
                        let status = match e {
                            SlotError::QueueTimeout => {
                                Status::unavailable("Service Unavailable: Connection queue full")
                            }
                            SlotError::LicenseExceeded => {
                                Status::resource_exhausted("License connection limit exceeded")
                            }
                            SlotError::Internal(msg) => Status::internal(msg),
                        };
                        return Err(Box::new(status) as Box<dyn std::error::Error + Send + Sync>);
                    }
                }
            } else {
                None
            };

            // Map inner error to our Box error.
            // slot is dropped here at the end of the scope, releasing the license.
            // Use oneshot to ensure readiness (resolves 'send_item called without first calling poll_reserve')
            inner.oneshot(req).await.map_err(|e| e.into())
        })
    }
}
