#[cfg(test)]
mod tests {
    use super::super::{QUERY_COUNT, ACTIVE_QUERIES, metrics_handler};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn test_metrics_registration() {
        // Increment count
        QUERY_COUNT.inc();
        assert_eq!(QUERY_COUNT.get(), 1);
    }

    #[tokio::test]
    async fn test_metrics_endpoint_format() {
        // Touch metrics to ensure they are registered
        let _ = QUERY_COUNT.get();
        let _ = ACTIVE_QUERIES.get();

        let response = metrics_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        
        // Extract body
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        
        // Check for Prometheus format
        assert!(body_str.contains("strake_queries_total"), "Body: {}", body_str);
        assert!(body_str.contains("strake_active_queries"), "Body: {}", body_str);
    }
}
