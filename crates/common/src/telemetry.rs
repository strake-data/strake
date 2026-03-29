//! # Telemetry and Observability
//!
//! Provides infrastructure for tracing, metrics, and logs using OpenTelemetry.
//!
//! ## Overview
//!
//! This module integrates Strake with the OpenTelemetry ecosystem. It sets up a
//! global `tracing` subscriber that exports spans and metrics via OTLP/gRPC.
//!
//! ## Usage
//!
//! ```rust
//! use strake_common::telemetry::init_telemetry;
//! use tracing_subscriber::Registry;
//! use tracing_subscriber::layer::SubscriberExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let layer = init_telemetry("my-service", "http://localhost:4317").unwrap();
//!     let subscriber = Registry::default().with(layer);
//!     tracing::subscriber::set_global_default(subscriber).unwrap();
//! }
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Batch Exporting**: Spans and metrics are buffered and exported in background
//!   batches to minimize impact on the request hot path.
//! - **No-op Overhead**: When the `telemetry` feature is disabled, all calls
//!   compile to no-ops with zero runtime cost.
//!
//! ## Safety
//!
//! This module contains no unsafe code. Resource management (dropping exporters)
//! is handled automatically by the OpenTelemetry SDK.
//!
//! ## Errors
//!
//! - [`anyhow::Error`]: Returned from `init_telemetry` if the OTLP exporter fails
//!   to initialize or the service name is invalid.
//!

use anyhow::Result;

#[cfg(feature = "telemetry")]
use {
    opentelemetry::KeyValue,
    opentelemetry::trace::TracerProvider,
    opentelemetry_otlp::WithExportConfig,
    opentelemetry_sdk::Resource,
    opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider},
    opentelemetry_sdk::trace::SdkTracerProvider,
    tracing_opentelemetry::OpenTelemetryLayer,
};

use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

#[cfg(feature = "telemetry")]
static TRACER_PROVIDER: once_cell::sync::Lazy<std::sync::Mutex<Option<SdkTracerProvider>>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(None));

#[cfg(feature = "telemetry")]
static METER_PROVIDER: once_cell::sync::Lazy<std::sync::Mutex<Option<SdkMeterProvider>>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(None));

/// Initializes the global telemetry layer for tracing and metrics.
///
/// Returns a `tracing_subscriber::Layer` that can be added to a registry.
pub fn init_telemetry<S>(
    service_name: &str,
    endpoint: &str,
) -> Result<Box<dyn Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    #[cfg(feature = "telemetry")]
    {
        // 1. Initialize Tracing
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;

        let resource = Resource::builder()
            .with_attributes(vec![KeyValue::new(
                "service.name",
                service_name.to_string(),
            )])
            .build();

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        let tracer = provider.tracer(service_name.to_string());

        // Set the global tracer provider and store it for shutdown
        opentelemetry::global::set_tracer_provider(provider.clone());
        *TRACER_PROVIDER.lock().unwrap() = Some(provider);

        // 2. Initialize Metrics
        init_metrics(service_name, endpoint)?;

        Ok(Box::new(OpenTelemetryLayer::new(tracer)))
    }
    #[cfg(not(feature = "telemetry"))]
    {
        let _ = service_name;
        let _ = endpoint;
        Ok(Box::new(tracing_subscriber::layer::Identity::new()))
    }
}

#[cfg(feature = "telemetry")]
fn init_metrics(service_name: &str, endpoint: &str) -> Result<()> {
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let reader = PeriodicReader::builder(exporter).build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            Resource::builder()
                .with_attributes(vec![KeyValue::new(
                    "service.name",
                    service_name.to_string(),
                )])
                .build(),
        )
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());
    *METER_PROVIDER.lock().unwrap() = Some(provider);
    Ok(())
}

/// Shuts down the telemetry system and flushes any pending spans/metrics.
pub fn shutdown_telemetry() {
    #[cfg(feature = "telemetry")]
    {
        if let Some(provider) = TRACER_PROVIDER.lock().unwrap().take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = METER_PROVIDER.lock().unwrap().take() {
            let _ = provider.shutdown();
        }
    }
}
