//! # Common SQL Connector Types
//!
//! Shared traits and types used across all SQL-based data connectors,
//! including provider factories and metadata fetchers.

use anyhow::Result;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_federation::FederatedTableProviderAdaptor;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use strake_common::circuit_breaker::AdaptiveCircuitBreaker;

/// Merges optional top-level username/password overrides into a SQL URL.
///
/// Credentials already embedded in the URL take precedence. Unparseable
/// connection strings are returned unchanged so dialect-specific parsing can
/// surface the original error.
pub fn merge_credentials_into_url(
    connection_string: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    let Ok(mut parsed) = url::Url::parse(connection_string) else {
        return connection_string.to_string();
    };

    let try_set =
        |field: &str, value: Option<&str>, setter: &mut dyn FnMut(&str) -> Result<(), ()>| {
            let Some(value) = value.filter(|s| !s.is_empty()) else {
                return;
            };
            if let Err(()) = setter(value) {
                tracing::warn!("Failed to inject {field} into SQL URL");
            }
        };

    if parsed.username().is_empty() {
        try_set("username", username, &mut |v| parsed.set_username(v));
    }
    if parsed.password().is_none() {
        try_set("password", password, &mut |v| parsed.set_password(Some(v)));
    }

    parsed.to_string()
}

/// Enriched metadata for a table or column, typically sourced from database comments.
#[derive(Debug, Default, Clone)]
pub struct FetchedMetadata {
    /// Optional human-readable description of the table.
    pub table_description: Option<String>,
    /// Mapping of column names to their human-readable descriptions.
    pub columns: HashMap<String, String>,
}

/// Factory for creating dialect-specific [`TableProvider`] instances.
#[async_trait]
pub trait SqlProviderFactory: Send + Sync {
    /// Creates a new [`TableProvider`] for the given table reference.
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        cb: Arc<AdaptiveCircuitBreaker>,
        custom_schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// Lower-level factory for dialect-specific table providers without generic wrapping.
#[async_trait]
pub trait TableFactory: Send + Sync {
    /// Creates a raw dialect-specific [`TableProvider`].
    async fn table_provider(&self, table_ref: TableReference) -> Result<Arc<dyn TableProvider>>;
}

/// A generic implementation of [`SqlProviderFactory`] that adds federation support.
///
/// Wraps an inner [`TableFactory`] with a [`SQLFederationProvider`] to enable
/// cross-table join pushdown within the same source.
pub struct GenericFederatedTableFactory<F> {
    /// The inner factory creating the dialect-specific provider.
    pub inner_factory: F,
    /// The federation provider shared across all tables in the source.
    pub federation_provider: Arc<super::strake_federation::StrakeFederationProvider>,
    /// Whether to enable schema drift detection.
    pub schema_drift: bool,
    /// Maximum concurrent queries for the provider.
    pub max_concurrent_queries: usize,
}

#[async_trait]
impl<F: TableFactory + Send + Sync> SqlProviderFactory for GenericFederatedTableFactory<F> {
    async fn create_table_provider(
        &self,
        table_ref: TableReference,
        cb: Arc<AdaptiveCircuitBreaker>,
        custom_schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner = self
            .inner_factory
            .table_provider(table_ref.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let schema_adapted = if let Some(custom_schema) = custom_schema {
            Arc::new(super::wrappers::SchemaAdaptingTableProvider::new(
                inner,
                custom_schema,
            ))
        } else {
            inner
        };

        // Wrapper order: the adapted (custom-schema) provider sits INSIDE the
        // circuit breaker and schema-drift wrapper, so drift detection compares
        // batches against the custom schema (which the adaptor always projects
        // onto). For custom-schema tables drift detection is therefore a
        // structural no-op — the user declared the schema explicitly. If drift
        // detection against the raw source schema is ever required, the
        // adaptation would need to wrap `wrapped` instead of `inner`.
        let wrapped = super::wrappers::wrap_provider(schema_adapted, cb, self.schema_drift);
        let limited = super::wrappers::wrap_concurrent(wrapped, self.max_concurrent_queries);

        let sql_source = super::strake_federation::StrakeTableSource::new(
            self.federation_provider.clone(),
            table_ref,
            limited.schema(),
        );
        let adaptor =
            FederatedTableProviderAdaptor::new_with_provider(Arc::new(sql_source), limited);

        Ok(Arc::new(adaptor))
    }
}

/// Rules for mapping database schema names to Strake source names.
///
/// This struct is constructed from a [`strake_common::config::SchemaMappingConfig`] and
/// determines whether a given source schema should be omitted from pushdown SQL (i.e. treated
/// as the "default" schema) or explicitly qualified (e.g. `"GWS_DWH"."GWS_WAFERS"`).
#[derive(Debug, Clone)]
pub struct SchemaMappingRule {
    default_schemas: Vec<String>,
}

impl SchemaMappingRule {
    /// Creates a rule for standard SQL dialects (Postgres, MySQL, ClickHouse, Oracle).
    ///
    /// Uses the `default_schemas` list from the provided config verbatim.
    pub fn standard(config: &strake_common::config::SchemaMappingConfig) -> Self {
        Self {
            default_schemas: config.default_schemas.clone(),
        }
    }

    /// Creates a rule for SQLite and DuckDB, ensuring `"main"` is always treated as default.
    ///
    /// Appends `"main"` to the configured list if not already present.
    pub fn sqlite(config: &strake_common::config::SchemaMappingConfig) -> Self {
        let mut schemas = config.default_schemas.clone();
        if !schemas.iter().any(|s| s.eq_ignore_ascii_case("main")) {
            schemas.push("main".to_string());
        }
        Self {
            default_schemas: schemas,
        }
    }

    /// Maps a database schema name to a Strake catalog schema name.
    ///
    /// If `schema` is a default schema, returns the `source_name` (the Strake catalog schema).
    /// Otherwise returns the original `schema` unchanged.
    pub fn map_schema<'a>(&self, schema: &'a str, source_name: &str) -> std::borrow::Cow<'a, str> {
        if self.is_default_schema(schema) {
            std::borrow::Cow::Owned(source_name.to_string())
        } else {
            std::borrow::Cow::Borrowed(schema)
        }
    }

    /// Returns `true` if `schema` is treated as the dialect's default and should
    /// **not** be explicitly qualified in pushdown SQL.
    ///
    /// Empty schemas are always considered default. Comparison is case-insensitive.
    pub fn is_default_schema(&self, schema: &str) -> bool {
        if schema.is_empty() {
            return true;
        }
        self.default_schemas
            .iter()
            .any(|s| s.eq_ignore_ascii_case(schema))
    }
}

/// Supported SQL dialects for data sources.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    /// PostgreSQL dialect.
    Postgres,
    /// MySQL dialect.
    MySql,
    /// SQLite dialect.
    Sqlite,
    /// ClickHouse dialect.
    Clickhouse,
    /// DuckDB dialect.
    #[serde(alias = "duckdb")]
    DuckDB,
    /// Oracle dialect.
    Oracle,
}

impl SqlDialect {
    /// Returns the lowercase string representation of the dialect.
    pub fn as_str(&self) -> &'static str {
        match self {
            SqlDialect::Postgres => "postgres",
            SqlDialect::MySql => "mysql",
            SqlDialect::Sqlite => "sqlite",
            SqlDialect::Clickhouse => "clickhouse",
            SqlDialect::DuckDB => "duckdb",
            SqlDialect::Oracle => "oracle",
        }
    }
}

impl std::str::FromStr for SqlDialect {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" => Ok(SqlDialect::Postgres),
            "mysql" => Ok(SqlDialect::MySql),
            "sqlite" => Ok(SqlDialect::Sqlite),
            "clickhouse" => Ok(SqlDialect::Clickhouse),
            "duckdb" => Ok(SqlDialect::DuckDB),
            "oracle" => Ok(SqlDialect::Oracle),
            _ => Err(anyhow::anyhow!("Unknown SQL dialect: '{s}'")),
        }
    }
}

impl TryFrom<&strake_common::models::SourceType> for SqlDialect {
    type Error = anyhow::Error;

    fn try_from(source_type: &strake_common::models::SourceType) -> Result<Self, Self::Error> {
        use strake_common::models::SourceType;
        match source_type {
            SourceType::Postgres => Ok(SqlDialect::Postgres),
            SourceType::Mysql => Ok(SqlDialect::MySql),
            SourceType::Sqlite => Ok(SqlDialect::Sqlite),
            SourceType::Clickhouse => Ok(SqlDialect::Clickhouse),
            SourceType::Duckdb => Ok(SqlDialect::DuckDB),
            SourceType::Oracle => Ok(SqlDialect::Oracle),
            SourceType::Other(s) => s.parse::<SqlDialect>().map_err(|_| {
                anyhow::anyhow!(
                    "SQL dialect must be explicitly configured or inferred from source type ('{s}')"
                )
            }),
            other => anyhow::bail!("Cannot infer SQL dialect for non-SQL source type: {other:?}"),
        }
    }
}

/// Parameters for creating and registering a SQL-based data source.
#[derive(Clone)]
pub struct SqlSourceParams {
    /// The session context to register the source in.
    pub context: Arc<SessionContext>,
    /// The name of the catalog.
    pub catalog_name: String,
    /// The unique name of the source.
    pub name: String,
    /// The connection string to the database.
    pub connection_string: secrecy::SecretString,
    /// The maximum size of the connection pool.
    pub pool_size: usize,
    /// The circuit breaker for the source.
    pub cb: Arc<strake_common::circuit_breaker::AdaptiveCircuitBreaker>,
    /// Explicitly listed tables to register (if any).
    pub explicit_tables: Arc<Option<Vec<strake_common::config::TableConfig>>>,
    /// Retry settings for the source.
    pub retry: strake_common::config::RetrySettings,
    /// Maximum number of concurrent queries allowed for this source.
    pub max_concurrent_queries: usize,
    /// Schema mapping configuration for this source.
    pub schema_mapping: strake_common::config::SchemaMappingConfig,
}

/// Options for registering a SQL source.
pub struct SqlRegistrationOptions {
    /// The session context to register the source in.
    pub context: Arc<SessionContext>,
    /// The name of the catalog.
    pub catalog_name: String,
    /// The unique name of the source.
    pub name: String,
    /// The dialect of the SQL source.
    pub dialect: SqlDialect,
    /// The connection string to the database.
    pub connection_string: String,
    /// The maximum size of the connection pool.
    pub pool_size: usize,
    /// Explicitly listed tables to register (if any).
    pub explicit_tables: Arc<Option<Vec<strake_common::config::TableConfig>>>,
    /// Retry settings for the source.
    pub retry: strake_common::config::RetrySettings,
    /// Maximum number of concurrent queries allowed for this source.
    pub max_concurrent_queries: usize,
    /// Schema mapping configuration for this source.
    pub schema_mapping: strake_common::config::SchemaMappingConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_credentials_into_url_injects_missing_credentials() {
        let merged = merge_credentials_into_url(
            "postgres://localhost:5432/app",
            Some("strake"),
            Some("password123"),
        );
        let parsed = url::Url::parse(&merged).unwrap();

        assert_eq!(parsed.username(), "strake");
        assert_eq!(parsed.password(), Some("password123"));
    }

    #[test]
    fn merge_credentials_into_url_preserves_embedded_credentials() {
        let merged = merge_credentials_into_url(
            "mysql://existing:secret@localhost:3306/app",
            Some("other"),
            Some("other"),
        );
        let parsed = url::Url::parse(&merged).unwrap();

        assert_eq!(parsed.username(), "existing");
        assert_eq!(parsed.password(), Some("secret"));
    }

    #[test]
    fn merge_credentials_into_url_leaves_unparseable_strings_unchanged() {
        let connection_string = "relative/path/to.db";

        assert_eq!(
            merge_credentials_into_url(connection_string, Some("user"), Some("secret")),
            connection_string
        );
    }

    // --- SchemaMappingRule tests ---

    #[test]
    fn schema_mapping_standard_defaults_are_default() {
        let config = strake_common::config::SchemaMappingConfig::default();
        let rule = SchemaMappingRule::standard(&config);

        assert!(
            rule.is_default_schema("public"),
            "'public' should be default"
        );
        assert!(
            rule.is_default_schema("default"),
            "'default' should be default (ClickHouse)"
        );
        assert!(
            rule.is_default_schema("PUBLIC"),
            "comparison should be case-insensitive"
        );
        assert!(rule.is_default_schema(""), "empty schema is always default");
    }

    #[test]
    fn schema_mapping_standard_non_default_schemas_are_qualified() {
        let config = strake_common::config::SchemaMappingConfig::default();
        let rule = SchemaMappingRule::standard(&config);

        // Core bug scenario: Oracle non-default schemas must NOT be treated as default
        assert!(
            !rule.is_default_schema("GWS_DWH"),
            "'GWS_DWH' must not be default"
        );
        assert!(
            !rule.is_default_schema("oracle_schema"),
            "arbitrary schema must not be default"
        );
        assert!(
            !rule.is_default_schema("main"),
            "'main' is not in standard defaults"
        );
    }

    #[test]
    fn schema_mapping_sqlite_includes_main() {
        let config = strake_common::config::SchemaMappingConfig::default();
        let rule = SchemaMappingRule::sqlite(&config);

        assert!(
            rule.is_default_schema("main"),
            "'main' must be default for SQLite/DuckDB"
        );
        assert!(
            rule.is_default_schema("MAIN"),
            "case-insensitive comparison"
        );
        assert!(
            rule.is_default_schema("public"),
            "'public' also default for SQLite"
        );
    }

    #[test]
    fn schema_mapping_sqlite_does_not_duplicate_main() {
        // If "main" is already in default_schemas, sqlite() must not add a second copy
        let config = strake_common::config::SchemaMappingConfig::with_schemas(vec![
            "main".to_string(),
            "public".to_string(),
        ]);
        let rule = SchemaMappingRule::sqlite(&config);
        let main_count = rule
            .default_schemas
            .iter()
            .filter(|s| s.eq_ignore_ascii_case("main"))
            .count();
        assert_eq!(main_count, 1, "'main' should appear exactly once");
    }

    #[test]
    fn schema_mapping_custom_default_schemas() {
        let config = strake_common::config::SchemaMappingConfig::with_schemas(vec![
            "app_schema".to_string(),
        ]);
        let rule = SchemaMappingRule::standard(&config);

        assert!(
            rule.is_default_schema("app_schema"),
            "custom schema should be default"
        );
        assert!(
            !rule.is_default_schema("public"),
            "'public' not in custom list"
        );
    }

    #[test]
    fn schema_mapping_empty_default_schemas_always_qualifies() {
        // Oracle use-case: no schema is default, everything is qualified
        let config = strake_common::config::SchemaMappingConfig::with_schemas(vec![]);
        let rule = SchemaMappingRule::standard(&config);

        assert!(
            !rule.is_default_schema("GWS_DWH"),
            "no schema should be default"
        );
        assert!(
            !rule.is_default_schema("public"),
            "even 'public' is not default"
        );
        assert!(
            rule.is_default_schema(""),
            "empty string is still always default"
        );
    }

    #[test]
    fn schema_mapping_map_schema_returns_source_name_for_default() {
        let config = strake_common::config::SchemaMappingConfig::default();
        let rule = SchemaMappingRule::standard(&config);

        let result = rule.map_schema("public", "my_source");
        assert_eq!(result.as_ref(), "my_source");
    }

    #[test]
    fn schema_mapping_map_schema_returns_schema_for_non_default() {
        let config = strake_common::config::SchemaMappingConfig::default();
        let rule = SchemaMappingRule::standard(&config);

        let result = rule.map_schema("GWS_DWH", "my_source");
        assert_eq!(result.as_ref(), "GWS_DWH");
    }

    #[test]
    fn test_sql_dialect_from_source_type() {
        use std::str::FromStr;
        use strake_common::models::SourceType;

        assert_eq!(
            SqlDialect::try_from(&SourceType::Postgres).unwrap(),
            SqlDialect::Postgres
        );
        assert_eq!(
            SqlDialect::try_from(&SourceType::Mysql).unwrap(),
            SqlDialect::MySql
        );
        assert_eq!(
            SqlDialect::try_from(&SourceType::Sqlite).unwrap(),
            SqlDialect::Sqlite
        );
        assert_eq!(
            SqlDialect::try_from(&SourceType::Clickhouse).unwrap(),
            SqlDialect::Clickhouse
        );
        assert_eq!(
            SqlDialect::try_from(&SourceType::Duckdb).unwrap(),
            SqlDialect::DuckDB
        );
        assert_eq!(
            SqlDialect::try_from(&SourceType::Oracle).unwrap(),
            SqlDialect::Oracle
        );

        // Test fallback matching via Other
        assert_eq!(
            SqlDialect::try_from(&SourceType::Other("oracle".into())).unwrap(),
            SqlDialect::Oracle
        );
        assert_eq!(
            SqlDialect::try_from(&SourceType::Other("PostgreSQL".into())).unwrap(),
            SqlDialect::Postgres
        );

        assert!(SqlDialect::try_from(&SourceType::Csv).is_err());
        assert!(SqlDialect::try_from(&SourceType::Other("unknown_db".into())).is_err());

        // Test FromStr directly
        assert_eq!(SqlDialect::from_str("oracle").unwrap(), SqlDialect::Oracle);
        assert_eq!(
            SqlDialect::from_str("postgres").unwrap(),
            SqlDialect::Postgres
        );
    }
}
