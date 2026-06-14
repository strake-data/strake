#[cfg(test)]
mod tests {
    use crate::api::{ReloadState, create_api_router};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Write;
    use std::sync::Arc;
    use strake_common::config::{Config, QueryLimits, ResourceConfig};
    use strake_runtime::federation::{FederationEngine, FederationEngineOptions};
    use tempfile::tempdir;
    use tower::ServiceExt;

    // Helper to create a test FederationEngine
    async fn make_test_engine(config: Config) -> Arc<FederationEngine> {
        Arc::new(
            FederationEngine::new(FederationEngineOptions {
                config,
                catalog_name: "strake_test".to_string(),
                query_limits: QueryLimits::default(),
                resource_config: ResourceConfig::default(),
                datafusion_config: HashMap::new(),
                global_budget: 10,
                extra_optimizer_rules: vec![],
                extra_sources: vec![],
                retry: Default::default(),
            })
            .await
            .unwrap(),
        )
    }

    fn make_auth_user() -> strake_common::auth::AuthenticatedUser {
        let mut user = strake_common::auth::AuthenticatedUser::default();
        user.id = strake_common::models::ActorName::from("test_user");
        user
    }

    #[tokio::test]
    async fn test_reload_unauthenticated() {
        let engine = make_test_engine(Config::default()).await;
        let license_cache = Arc::new(crate::license::LicenseCache::new());
        let reload_state = Arc::new(ReloadState::new(engine.clone(), None, true));
        let app = create_api_router(engine, license_cache, reload_state);

        // Make request without auth extension
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_reload_disabled_by_default() {
        let engine = make_test_engine(Config::default()).await;
        let license_cache = Arc::new(crate::license::LicenseCache::new());
        let reload_state = Arc::new(ReloadState::new(engine.clone(), None, false));
        let app = create_api_router(engine, license_cache, reload_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body_json["status"], "error");
        assert!(body_json["message"].as_str().unwrap().contains("disabled"));
    }

    #[tokio::test]
    async fn test_reload_no_config_path() {
        let engine = make_test_engine(Config::default()).await;
        let license_cache = Arc::new(crate::license::LicenseCache::new());
        let reload_state = Arc::new(ReloadState::new(engine.clone(), None, true));
        let app = create_api_router(engine, license_cache, reload_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body_json["status"], "error");
        assert!(
            body_json["message"]
                .as_str()
                .unwrap()
                .contains("No sources configuration path")
        );
    }

    #[tokio::test]
    async fn test_reload_invalid_yaml() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("sources.yaml");
        let mut file = File::create(&file_path).unwrap();
        // Write invalid yaml
        writeln!(file, "sources:\n  - name: invalid\n  [unclosed list").unwrap();

        let engine = make_test_engine(Config::default()).await;
        let license_cache = Arc::new(crate::license::LicenseCache::new());
        let reload_state = Arc::new(ReloadState::new(engine.clone(), Some(file_path), true));
        let app = create_api_router(engine, license_cache, reload_state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body_json["status"], "error");
        assert!(
            body_json["message"]
                .as_str()
                .unwrap()
                .contains("parsing error")
        );
    }

    #[tokio::test]
    async fn test_reload_success() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("sources.yaml");

        // Write initial empty config
        {
            let mut file = File::create(&file_path).unwrap();
            writeln!(file, "sources: []").unwrap();
        }

        let engine = make_test_engine(Config::default()).await;

        assert_eq!(engine.list_sources().len(), 0);

        let license_cache = Arc::new(crate::license::LicenseCache::new());
        let reload_state = Arc::new(ReloadState::new(
            engine.clone(),
            Some(file_path.clone()),
            true,
        ));
        let app = create_api_router(engine.clone(), license_cache, reload_state);

        let db_path = dir.path().join("test.db");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute("CREATE TABLE my_table (id INTEGER);", [])
                .unwrap();
        }

        // Update the file to include a simple mock source
        {
            let mut file = File::create(&file_path).unwrap();
            writeln!(file, "sources:").unwrap();
            writeln!(file, "  - name: my_source").unwrap();
            writeln!(file, "    type: sqlite").unwrap();
            writeln!(file, "    url: \"{}\"", db_path.to_str().unwrap()).unwrap();
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(status, StatusCode::OK, "Body: {:?}", body_json);
        assert_eq!(body_json["status"], "success");

        // Verify that the engine now has 1 source configured
        assert_eq!(engine.list_sources().len(), 1);
        assert_eq!(engine.list_sources()[0].name.name, "my_source");

        // Verify that the schema is present in the DataFusion catalog
        let catalog = engine.context().catalog("strake_test").unwrap();
        assert!(catalog.schema("my_source").is_some());

        // Verify that the custom query planner is preserved after successful reload
        let debug_str = format!("{:?}", engine.context().state().query_planner());
        assert!(
            debug_str.contains("QueryPlanner"),
            "Expected custom QueryPlanner, got {:?}",
            debug_str
        );

        // Now reload with an empty configuration to verify old source removal
        {
            let mut file = File::create(&file_path).unwrap();
            writeln!(file, "sources: []").unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(engine.list_sources().len(), 0);

        // Verify that the schema is no longer present in the DataFusion catalog
        let catalog = engine.context().catalog("strake_test").unwrap();
        assert!(catalog.schema("my_source").is_none());

        // Verify that the custom query planner is still preserved
        let debug_str = format!("{:?}", engine.context().state().query_planner());
        assert!(
            debug_str.contains("QueryPlanner"),
            "Expected custom QueryPlanner, got {:?}",
            debug_str
        );
    }

    #[tokio::test]
    async fn test_reload_strict_failure() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("sources.yaml");
        let db_path = dir.path().join("original_test.db");

        // Create a table in the database so that it registers the schema
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute("CREATE TABLE my_table (id INTEGER);", [])
                .unwrap();
        }

        // Write initial valid config with one source
        {
            let mut file = File::create(&file_path).unwrap();
            writeln!(file, "sources:").unwrap();
            writeln!(file, "  - name: original_source").unwrap();
            writeln!(file, "    type: sqlite").unwrap();
            writeln!(file, "    url: \"{}\"", db_path.to_str().unwrap()).unwrap();
        }

        let engine = make_test_engine(Config::default()).await;
        let license_cache = Arc::new(crate::license::LicenseCache::new());
        let reload_state = Arc::new(ReloadState::new(
            engine.clone(),
            Some(file_path.clone()),
            true,
        ));
        let app = create_api_router(engine.clone(), license_cache, reload_state);

        // Load the initial config successfully first
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(engine.list_sources().len(), 1);
        assert_eq!(engine.list_sources()[0].name.name, "original_source");

        // Now update the file to include a failing source configuration
        {
            let mut file = File::create(&file_path).unwrap();
            writeln!(file, "sources:").unwrap();
            writeln!(file, "  - name: failing_source").unwrap();
            writeln!(file, "    type: sqlite").unwrap();
            // A non-existent directory path will fail connection creation/database opening.
            writeln!(file, "    url: \"/non_existent_directory_abc_xyz/test.db\"").unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sources/reload")
                    .extension(make_auth_user())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // The reload should return an error (Bad Request or Internal Server Error depending on implementation, but NOT success)
        assert!(response.status() != StatusCode::OK);

        // The catalog and source configs should remain unchanged (atomic rollback)
        assert_eq!(engine.list_sources().len(), 1);
        assert_eq!(engine.list_sources()[0].name.name, "original_source");

        // Verify that the catalog provider remains intact and queryable after a failed reload
        let catalog = engine.context().catalog("strake_test").unwrap();
        assert!(catalog.schema("original_source").is_some());

        // Verify that the custom query planner is preserved after a failed reload
        let debug_str = format!("{:?}", engine.context().state().query_planner());
        assert!(
            debug_str.contains("QueryPlanner"),
            "Expected custom QueryPlanner, got {:?}",
            debug_str
        );
    }
}
