## [v0.2.5-rc2]

### Added
- **Core: DuckDB-style execution profiling & tracing** — Introduced `explain_tree()` for physical plan ASCII tree inspection and `trace()` for live operator-level CPU/I/O profiling with warning metrics.
- **Core: High-performance join & optimization controls** — Added configurable broadcast join, push-down filter, single-node aggregation, single-partition, and correlated distinct pushdown optimizations.

### Fixed
- **Core: Postgres connection string parsing** — Fixed PG connection string parsing to correctly map user credentials, host, database name (`db`/`dbname`), pool size, and SSL options.
- **Core: Registry config parsing** — Fixed embedded backend initialization to correctly load user limit, resource, and retry settings from config files.

### Changed
- **Build: GLIBC 2.31 compatibility** — Switched CLI release pipelines to Ubuntu 20.04 and pinned the Protoc version to 25.2 to ensure stable, highly compatible GLIBC 2.31 builds.
- **Build: Statically-linked musl support** — Added a `--musl` installation flag to the CLI download script to force installation of fully static binaries on legacy Linux distributions.

### Docs
- **Docs: Query Profiling & Tuning Guide** — Created comprehensive documentation on performance profiling, troubleshooting execution bottlenecks, and plan tuning.
- **Docs: Connector examples** — Massive reference expansion with custom setup examples for ClickHouse, DuckDB, Files, Flight SQL, gRPC, Iceberg, MySQL, Oracle, Postgres, REST, and SQLite.
- **Docs: Configuration reference** — Documented all configuration parameters including resource limits, partition controls, and executor parameters.
- **Docs: Architecture update** — Updated conceptual diagrams, schema mappings, and code examples.

## [v0.2.5-rc1]

### Added
- **Core: Oracle support and federation refactor** — Integrated Oracle as a first-class data source and refactored the federation engine for improved dialect extensibility.
- **Core: Zensical migration** — Migrated documentation from MkDocs to Zensical for a more streamlined developer experience.
- **Enterprise: Excel connector update** — Improved schema detection and performance for large Excel workbooks.

### Changed
- **DataFusion 53.1.0** — Updated core engine to DataFusion 53.1.0 across the entire workspace.
- **OpenSSL version bump** — Updated `openssl` to 0.10.79 for security and compatibility.

### Fixed
- **Dialect refactoring and fixes** — Resolved several SQL generation and schema mapping issues across multiple dialects.
- **Enterprise: License fallback** — Fixed fallback logic for local license keys when remote validation is unavailable.

### Docs
- **CBO comments** — Enhanced documentation for the Cost-Based Optimizer.
- **Auth guide** — Added a comprehensive guide for authentication configuration.

