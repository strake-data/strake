# Changelog 

## [v0.2.5]

### Added
- **Core: DuckDB-style execution profiling & tracing** — Introduced `explain_tree()` for physical plan ASCII tree inspection and `trace()` for live operator-level CPU/I/O profiling with warning metrics.
- **Core: Advanced predicate and metadata caching** — Added a metadata and predicate caching layer (`CachingTableProvider`) using `moka` LRU-caching and SHA-256 stable keys for Parquet/Iceberg sources.
- **Core: High-performance join & optimization controls** — Added configurable broadcast join, push-down filter, single-node aggregation, single-partition, and correlated distinct pushdown optimizations.
- **Core: Oracle support and federation refactor** — Integrated Oracle as a first-class data source and refactored the federation engine for improved dialect extensibility.
- **Core: Zensical migration** — Migrated documentation from MkDocs to Zensical for a more streamlined developer experience.
- **Enterprise: Excel connector update** — Improved schema detection and performance for large Excel workbooks.

### Fixed
- **Core: Enhanced plan tree formatting & execution traces** — Fixed physical plan tree formatting to use correct column widths and added `file_groups` path condensation (e.g. `2 files, e.g. gs://.../file.parquet`) in execution traces to prevent layout breaking in verbose contexts.
- **Core: Postgres connection string parsing** — Fixed PG connection string parsing to correctly map user credentials, host, database name (`db`/`dbname`), pool size, and SSL options.
- **Core: Registry config parsing** — Fixed embedded backend initialization to correctly load user limit, resource, and retry settings from config files.
- **Dialect refactoring and fixes** — Resolved several SQL generation and schema mapping issues across multiple dialects.
- **Enterprise: License fallback** — Fixed fallback logic for local license keys when remote validation is unavailable.

### Changed
- **Build: GLIBC 2.31 compatibility** — Switched CLI release pipelines to Ubuntu 20.04 and pinned the Protoc version to 25.2 to ensure stable, highly compatible GLIBC 2.31 builds.
- **Build: Statically-linked musl support** — Added a `--musl` installation flag to the CLI download script to force installation of fully static binaries on legacy Linux distributions.
- **DataFusion 53.1.0** — Updated core engine to DataFusion 53.1.0 across the entire workspace.
- **OpenSSL version bump** — Updated `openssl` to 0.10.79 for security and compatibility.

### Docs
- **Docs: Query Profiling & Tuning Guide** — Created comprehensive documentation on performance profiling, troubleshooting execution bottlenecks, and plan tuning.
- **Docs: Connector examples** — Massive reference expansion with custom setup examples for ClickHouse, DuckDB, Files, Flight SQL, gRPC, Iceberg, MySQL, Oracle, Postgres, REST, and SQLite.
- **Docs: Configuration reference** — Documented all configuration parameters including resource limits, partition controls, and executor parameters.
- **Docs: Architecture update** — Updated conceptual diagrams, schema mappings, and code examples.
- **CBO comments** — Enhanced documentation for the Cost-Based Optimizer.
- **Auth guide** — Added a comprehensive guide for authentication configuration.

## [v0.2.4]

### Added
- **CI: Domain isolation verification** — Added CI step to verify `strake-common` does not depend on `datafusion` or `arrow`, enforcing clean domain/type isolation.
- **New crates/modules:**
  - `connectors/src/extensions/warnings.rs` — DataFusion session extension for `WarningCollector`.
  - `connectors/src/resilience/circuit_breaker.rs` — Execution-layer adaptive circuit breaker with DataFusion metrics.
  - `connectors/src/sources/sql/base_connector.rs` — Generic SQL connector unifying table discovery and registration.
  - `connectors/src/sources/sql/sqlite_introspect.rs` — SQLite introspector using PRAGMA queries.
  - `runtime/src/extensions/auth_config.rs` — DataFusion session extension for `AuthenticatedUser`.
- **Schema drift detection granularity** — New `PartialCast` drift warning category that reports how many rows survived a partial type cast (e.g. "2 of 4 rows survived"), distinguishing between clean coercion, partial cast, and total cast failure.
- **Schema mapping rules** — `SchemaMappingRule` enum (`Standard` / `SQLite`) for dialect-aware schema namespace mapping.
- **Concurrency limiting metrics** — `ConcurrencyLimitedExec` now exposes DataFusion `output_rows`, `output_bytes`, and `elapsed_compute` metrics.
- **Pre-commit hooks (`prek.toml`)** — Added `cargo fmt` and `cargo clippy` pre-commit hooks.
- **Cargo build config** — `.cargo/config.toml` with `lld` linker flag for faster Linux builds.
- **Documentation & examples** — Extensive rustdoc examples added to `retry`, `telemetry`, `scrubber`, `schema`, `predicate_cache`, `auth`, and `config` modules.

### Changed
- **Rust edition 2021 → 2024** — Entire workspace migrated to Rust 2024 edition.
- **Major config & type modernization** (`af4a29c`) — 127 files refactored:
  - Replaced manual struct instantiation with `Default` + field mutation pattern across CLI commands.
  - `DomainName`, `ActorName` are now strongly typed instead of raw `String`s.
  - `SourceType` deserialization rejects known types inside `Other` variant.
  - `apply.rs` diff/comparison logic simplified using derived `PartialEq` instead of custom equality functions.
- **Connector architecture refactored** (`0645b4b`) — All SQL connectors (Postgres, MySQL, SQLite, DuckDB, ClickHouse) now use the new `GenericSqlConnector` with `SchemaIntrospector` + `SqlProviderFactory` + `SchemaMappingRule`, eliminating ~500 lines of duplicated registration boilerplate per dialect.
- **Circuit breaker made async** (`1512a9f`) — `state()`, `record_success()`, `record_failure()`, `should_trip()` are now `async` using `tokio::sync::{Mutex, RwLock}` instead of `std::sync` primitives. Failure/success recording inside DataFusion plans is now fire-and-forget via `tokio::spawn`.
- **Circuit breaker metrics** — Switched from generic `counter("output_bytes")` to `MetricBuilder::output_bytes()`.
- **Domain/DataFusion decoupling** (`0db969d`) — 36 files changed:
  - `strake-common` no longer depends on `datafusion` or `arrow` by default.
  - `AuthenticatedUser` and `WarningCollector` no longer implement `ExtensionOptions`/`ConfigExtension` directly; thin wrappers (`AuthExtension`, `WarningExtension`) live in `runtime` and `connectors` crates respectively.
  - `CircuitBreakerTableProvider` and `CircuitBreakerExec` moved from `strake-common` to `connectors::resilience`.
  - `PredicateKey` now takes a stable `expr_display: &str` instead of a `datafusion::logical_expr::Expr`, decoupling cache keys from DataFusion internals.
  - `strake-error` crate now has an optional `datafusion` feature.
- **API key handling hardened** — `api_key` in `AuthSettings` is now `Option<SecretString>` with custom serde, and missing-key validation is more robust (requires `ExposeSecret`).
- **Retry jitter** — Jitter range changed from fixed `0..1000` ms to `0..=delay` ms, scaling with backoff.
- **Retry bypass** — `retry_async` now short-circuits when `max_attempts == 0`.
- **Scrubber optimization** — `scrub()` now returns `Cow<'_, str>` and only allocates when a regex actually matches.
- **DuckDB** — Upgraded to `1.10501.0` with `vtab-arrow` and `r2d2` features; added `r2d2` dependency.
- **Telemetry globals** — Replaced `once_cell::sync::Lazy` with `std::sync::LazyLock`; added poisoned-lock warnings.
- **Config loading** — In production, missing config file now returns `ConfigError::FileNotFound` instead of silently proceeding.

### Fixed
- **CLI metadata store robustness** (`199b7c4`):
  - `get_domain_version` no longer auto-creates missing domains; returns a proper error.
  - `increment_domain_version` uses UPSERT (`INSERT … ON CONFLICT … RETURNING`) for atomicity.
  - `diff_logic` gracefully falls back to default config when domain is missing.
  - Postgres metadata store: explicit `Option` casting for `INTEGER`/`BOOLEAN` columns to avoid `tokio-postgres` type-mismatch panics.
- **DuckDB federation** — Fixed off-by-one in batch schema compatibility check (`<=` instead of `<`).
- **DuckDB scan** — Fixed `BaselineMetrics` partition index (was hardcoded `0`, now uses `_partition`).
- **ClickHouse introspection** — Switched from string-interpolated SQL to parameterized `{db:String}` queries with `param_db` for safety.
- **Postgres metadata SQL** — Fixed indentation/whitespace in UPSERT and SELECT statements.
- **SourceType `Display`** — Now delegates to `as_str()` instead of a large manual match.
- **Schema normalization** — `normalize_type_str` no longer double-lowercases input; preserves original casing for parameterized types.

### Deprecated / Removed
- **Apache Iceberg integration temporarily disabled** (`fb9fb9c`) — Due to DataFusion 53 compatibility issues, the `iceberg` feature is now a no-op (`experimental-iceberg = []`). All iceberg dependencies are commented out in `Cargo.toml` and related tests are skipped.
- **Removed `register_tables` helper** — Superseded by `GenericSqlConnector::register`.
- **Removed per-dialect `try_register_*` functions** — Replaced by the generic connector pipeline.

### Security
- **Secret hardening** — Connection strings and API keys now use `secrecy::SecretString` with `ExposeSecret` in ClickHouse, MySQL, Postgres, SQLite, and DuckDB introspectors.
- **Unsafe code forbidden** — `#![deny(unsafe_code)]` added to `strake-common`.

