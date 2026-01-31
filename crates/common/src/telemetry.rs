//! Telemetry and observability initialization for the Strake engine.
//!
//! This module provides a unified interface for initializing OpenTelemetry (OTLP)
//! tracing using the `tracing` crate. When the `telemetry` feature is enabled, it sets up
//! OTLP/gRPC exports to a specified collector.

use anyhow::Result;

#[cfg(feature = "telemetry")]
use {
    opentelemetry::trace::TracerProvider, opentelemetry::KeyValue,
    opentelemetry_otlp::WithExportConfig,
    opentelemetry_sdk::metrics::PeriodicReader,
    opentelemetry_sdk::trace::SdkTracerProvider, opentelemetry_sdk::Resource,
    tracing_opentelemetry::OpenTelemetryLayer,
};

use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

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
            .with_attributes(vec![KeyValue::new("service.name", service_name.to_string())])
            .build();

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        let tracer = provider.tracer(service_name.to_string());

        // Set the global tracer provider
        opentelemetry::global::set_tracer_provider(provider);

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
                .with_attributes(vec![KeyValue::new("service.name", service_name.to_string())])
                .build(),
        )
        .build();

    opentelemetry::global::set_meter_provider(provider);
    Ok(())
}

pub fn shutdown_telemetry() {
    #[cfg(feature = "telemetry")]
    {
        // Global shutdown moved to explicit provider drops or flush in 0.31
    }
}
