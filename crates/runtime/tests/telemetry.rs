#![cfg(feature = "telemetry")]

use strake_common::telemetry::{init_telemetry, shutdown_telemetry};
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_telemetry_emission() -> anyhow::Result<()> {
    // Start a simple HTTP server to act as OTLP collector
    use axum::{body::Bytes, routing::post, Router};
    use std::sync::{Arc, Mutex};

    let received_data = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received_data.clone();

    let app = Router::new().route(
        "/v1/traces",
        post(move |body: Bytes| {
            let data = received_clone.clone();
            async move {
                data.lock().unwrap().push(body.to_vec());
                axum::http::StatusCode::OK
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let endpoint = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Initialize telemetry pointing to the mock server
    let otel_layer = init_telemetry("test-service", &endpoint)?;

    let subscriber = tracing_subscriber::registry().with(otel_layer);

    let _guard = tracing::subscriber::set_default(subscriber);

    // Generate some spans
    {
        let _span = tracing::info_span!("test_operation").entered();
        tracing::info!("This is a test event inside a span");
    }

    // Shutdown to flush spans
    shutdown_telemetry();

    // Wait for async flush
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Verify we received data
    let data = received_data.lock().unwrap();
    assert!(!data.is_empty(), "Expected to receive OTLP trace data");

    Ok(())
}
