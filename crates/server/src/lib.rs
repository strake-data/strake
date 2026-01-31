//! Strake Server: The HTTP and gRPC API layer.
//!
//! Exposes the Federation Engine via:
//! - **Flight SQL (50051)**: High-performance Arrow data access.
//! - **REST (8080)**: Management API and JSON query endpoint.
//! - **Observability**: Prometheus metrics and OpenTelemetry tracing.
use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{response::IntoResponse, routing::get, Json, Router};
use once_cell::sync::Lazy;
use prometheus::{Encoder, IntCounter, IntGauge, Opts, Registry, TextEncoder};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use strake_common::config::Config;
use strake_runtime::federation::FederationEngine;
use tonic::transport::Server;
use tonic::transport::{Identity, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

// Global metrics registry
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub const METRICS_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

// Example metrics
pub static QUERY_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    let opts = Opts::new("strake_queries_total", "Total number of queries executed");
    let counter = IntCounter::with_opts(opts).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static ACTIVE_QUERIES: Lazy<IntGauge> = Lazy::new(|| {
    let opts = Opts::new(
        "strake_active_queries",
        "Number of currently active queries",
    );
    let gauge = IntGauge::with_opts(opts).unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});

// Re-export modules so they are available
pub mod api;
pub mod auth;
pub mod concurrency;
pub mod flight_sql;

pub use auth::{ApiKeyAuthenticator, AuthLayer, Authenticator};
pub use concurrency::{ConcurrencyLayer, ConnectionSlotManager};
use flight_sql::StrakeFlightSqlService;
pub use strake_common::auth::AuthenticatedUser;

pub struct StrakeServer {
    config_path: String,
    app_config_path: String,
    authenticator: Option<Arc<dyn Authenticator>>,
    concurrency_manager: Option<Arc<dyn ConnectionSlotManager>>,
    audit_logging_enabled: bool,
    observability_enabled: bool,
    extra_sources: Vec<Box<dyn strake_connectors::sources::SourceProvider>>,
    extra_optimizer_rules:
        Vec<Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>>,
    api_router: Router,
}

impl Default for StrakeServer {
    fn default() -> Self {
        Self {
            config_path: "config/sources.yaml".to_string(),
            app_config_path: "config/strake.yaml".to_string(),
            authenticator: None,
            concurrency_manager: None,
            audit_logging_enabled: false,
            observability_enabled: false,
            extra_sources: vec![],
            extra_optimizer_rules: vec![],
            api_router: Router::new(),
        }
    }
}

impl StrakeServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(mut self, config_path: &str) -> Self {
        self.config_path = config_path.to_string();
        self
    }

    pub fn with_app_config(mut self, app_config_path: &str) -> Self {
        self.app_config_path = app_config_path.to_string();
        self
    }

    pub fn with_authenticator(mut self, auth: Arc<dyn Authenticator>) -> Self {
        self.authenticator = Some(auth);
        self
    }

    pub fn with_concurrency_manager(mut self, manager: Arc<dyn ConnectionSlotManager>) -> Self {
        self.concurrency_manager = Some(manager);
        self
    }

    pub fn with_observability(mut self, enabled: bool) -> Self {
        self.observability_enabled = enabled;
        self
    }

    pub fn with_audit_logging(mut self, enabled: bool) -> Self {
        self.audit_logging_enabled = enabled;
        self
    }

    pub fn with_source_provider(
        mut self,
        provider: Box<dyn strake_connectors::sources::SourceProvider>,
    ) -> Self {
        self.extra_sources.push(provider);
        self
    }

    pub fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn datafusion::optimizer::optimizer::OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.extra_optimizer_rules.extend(rules);
        self
    }

    pub fn with_api_router(mut self, router: Router) -> Self {
        self.api_router = router;
        self
    }

    pub fn with_config_path(mut self, path: impl Into<String>) -> Self {
        self.config_path = path.into();
        self
    }

    pub async fn run(self) -> anyhow::Result<()> {
        // Critical Security Check: Governance Rules vs Authentication
        // If governance rules (RLS/Contracts) are present, we MUST have an Enterprise-capable authenticator.
        // The default OSS ApiKeyAuthenticator does not support RLS rules, leading to a fail-open scenario.
        if self.authenticator.is_none() {
            let has_enterprise_rules = self.extra_optimizer_rules.iter().any(|r| {
                let name = r.name();
                name == "policy_rewriter" || name == "contract_optimizer"
            });

            if has_enterprise_rules {
                panic!(
                    "Enterprise governance rules detected but no enterprise authenticator provided. \
                     The OSS ApiKeyAuthenticator doesn't support RLS/Masking rules. \
                     Ensure you're running via strake-enterprise main.rs which calls with_authenticator() \
                     with EnterpriseApiKeyAuthenticator or OidcAuthenticator."
                );
            }
        }

        // 0. Initialize Telemetry (OpenTelemetry) if enabled
        use strake_common::config::AppConfig;
        let app_config = AppConfig::from_file(&self.app_config_path)?;
        let config = Config::from_file(&self.config_path)?;

        let otel_layer = if self.observability_enabled {
            strake_common::telemetry::init_telemetry(
                &app_config.telemetry.service_name,
                &app_config.telemetry.endpoint,
            )?
        } else {
            Box::new(tracing_subscriber::layer::Identity::new())
        };

        // Ensure logs directory exists
        std::fs::create_dir_all("logs").ok();

        // Appenders
        let metrics_appender = tracing_appender::rolling::daily("logs", "metrics.jsonl");
        let errors_appender = tracing_appender::rolling::daily("logs", "errors.jsonl");

        let metrics_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(metrics_appender)
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.target() == "metrics"
            }));

        let errors_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(errors_appender)
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.target() == "errors"
            }));

        // Stdout layer
        let stdout_layer =
            tracing_subscriber::fmt::layer().with_filter(EnvFilter::from_default_env());

        let registry = tracing_subscriber::registry()
            .with(stdout_layer)
            .with(otel_layer)
            .with(metrics_layer)
            .with(errors_layer);

        // Conditional Audit Logging (Enterprise Only)
        if self.audit_logging_enabled {
            let queries_appender = tracing_appender::rolling::daily("logs", "queries.jsonl");
            let queries_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(queries_appender)
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.target() == "queries"
                }));

            let audit_appender = tracing_appender::rolling::daily("logs", "audit.jsonl");
            let audit_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(audit_appender)
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.target() == "audit"
                }));

            registry
                .with(queries_layer)
                .with(audit_layer)
                .try_init()
                .ok();
        } else {
            registry.try_init().ok();
        }
        let engine = Arc::new(
            FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
                config,
                catalog_name: app_config.server.catalog.clone(),
                query_limits: app_config.query_limits.clone(),
                resource_config: app_config.resources.clone(),
                datafusion_config: app_config.server.datafusion_config.clone(),
                global_budget: app_config.server.global_connection_budget,
                extra_optimizer_rules: self.extra_optimizer_rules,
                extra_sources: self.extra_sources,
            })
            .await?,
        );

        // 1. Determine Authenticator
        let final_authenticator: Option<Arc<dyn Authenticator>> = if self.authenticator.is_some() {
            self.authenticator
        } else if app_config.server.auth.enabled {
            let db_url = app_config.server.database_url.clone();

            let mut cfg = deadpool_postgres::Config::new();
            cfg.url = Some(db_url);
            cfg.manager = Some(deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            });
            let pool = cfg
                .create_pool(None, tokio_postgres::NoTls)
                .context("Failed to create database pool")?;

            Some(Arc::new(ApiKeyAuthenticator::new(
                pool,
                app_config.server.auth.cache_ttl_secs,
                app_config.server.auth.cache_max_capacity,
            )))
        } else {
            None
        };

        // 2. Spawn Health & API Server
        let mut health_app = Router::new()
            .route("/health", get(health_handler))
            .route("/ready", get(ready_handler));

        if self.observability_enabled {
            health_app = health_app.route("/metrics", get(metrics_handler));
        }

        let base_api_router = api::create_api_router(engine.clone());
        let mut final_v1 = base_api_router.merge(self.api_router);

        if let Some(auth) = &final_authenticator {
            let auth_clone = auth.clone();
            final_v1 = final_v1.layer(axum::middleware::from_fn(move |req, next| {
                auth::axum_auth_middleware(req, next, auth_clone.clone())
            }));
        }

        let final_app = health_app.nest("/api/v1", final_v1);

        let health_addr: SocketAddr = app_config.server.health_addr.parse()?;
        info!("Management & API server listening on {}", health_addr);

        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(health_addr).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, final_app).await {
                        tracing::error!("REST API server error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to bind to health address {}: {}", health_addr, e);
                }
            }
        });

        // 3. Spawn Metrics Task
        if self.observability_enabled {
            let engine_clone = engine.clone();
            let start_time = std::time::Instant::now();
            tokio::spawn(async move {
                use sysinfo::{Pid, ProcessesToUpdate, System};
                let mut sys = System::new();
                let pid = Pid::from_u32(std::process::id());

                loop {
                    tokio::time::sleep(METRICS_REFRESH_INTERVAL).await;
                    sys.refresh_memory();
                    sys.refresh_processes(ProcessesToUpdate::All, true);

                    let active = engine_clone.active_queries();
                    ACTIVE_QUERIES.set(active as i64);

                    let uptime = start_time.elapsed().as_secs();
                    let available_memory_mb = sys.available_memory() / 1024 / 1024;

                    let (proc_memory_mb, proc_cpu, thread_count, open_fds) =
                        if let Some(proc) = sys.process(pid) {
                            let mem = proc.memory() / 1024 / 1024;
                            let cpu = proc.cpu_usage();
                            let fds = std::fs::read_dir("/proc/self/fd")
                                .map(|d| d.count())
                                .unwrap_or(0);
                            let threads = std::fs::read_dir("/proc/self/task")
                                .map(|d| d.count())
                                .unwrap_or(0);
                            (mem, cpu, threads, fds)
                        } else {
                            (0, 0.0, 0, 0)
                        };

                    info!(
                        target: "metrics",
                        active_queries = active,
                        uptime_seconds = uptime,
                        process_memory_mb = proc_memory_mb,
                        process_cpu_percent = proc_cpu,
                        thread_count = thread_count,
                        open_fds = open_fds,
                        available_memory_mb = available_memory_mb,
                    );
                }
            });
        }

        let service = StrakeFlightSqlService {
            engine,
            server_name: app_config.server.name.clone(),
        };
        let addr = app_config.server.listen_addr.parse()?;

        // Load TLS Config
        let tls_config = if app_config.server.tls.enabled {
            let cert = std::fs::read_to_string(&app_config.server.tls.cert).context(format!(
                "Failed to read cert file: {}",
                app_config.server.tls.cert
            ))?;
            let key = std::fs::read_to_string(&app_config.server.tls.key).context(format!(
                "Failed to read key file: {}",
                app_config.server.tls.key
            ))?;
            let identity = Identity::from_pem(cert, key);
            Some(ServerTlsConfig::new().identity(identity))
        } else {
            None
        };

        info!(
            "Flight SQL server listening on {} (TLS={}, Auth={}, ConcurrencyLimit={})",
            addr,
            app_config.server.tls.enabled,
            final_authenticator.is_some(),
            self.concurrency_manager.is_some()
        );

        let mut builder = Server::builder();

        if let Some(tls) = tls_config {
            builder = builder.tls_config(tls)?;
        }
        let mut builder = builder.layer(ConcurrencyLayer::new(self.concurrency_manager));

        if let Some(auth) = final_authenticator {
            let auth_layer = AuthLayer::new(auth);
            builder
                .layer(auth_layer)
                .add_service(FlightServiceServer::new(service))
                .serve(addr)
                .await?;
        } else {
            builder
                .add_service(FlightServiceServer::new(service))
                .serve(addr)
                .await?;
        }

        strake_common::telemetry::shutdown_telemetry();
        Ok(())
    }
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn ready_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ready" }))
}

async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, encoder.format_type())
        .body(axum::body::Body::from(buffer))
        .unwrap()
}

#[cfg(test)]
mod metrics_tests;
