## [v0.2.6-rc1]

### Added
- **Core: Firecracker Sandbox integration** — Added support for a Firecracker VM pool, userfaultfd (UFFD) memory demand paging, and secure stdin configuration.
- **Core: Python Sandbox `strptime` import** — Added `strptime` to sandbox imports to support datetime parsing.
- **CLI: Metadata DB & Authentication key commands** — Added `init db` command to initialize metadata database and `auth keys` command for key management.
- **CLI: Streamlined CLI and metadata sync** — Refactored and streamlined CLI options and consolidated metadata database schemas.

### Fixed
- **Core: Connection and pooling issues** — Resolved connection and pooling bugs across various database/session engines.
- **Core: Flight table metadata** — Fixed table metadata display when querying via Flight.

### Changed
- **Chore: Dependency updates** — Updated dependencies to resolve security alerts.
- **Refactor: CLI command validation** — Removed the `apply` command and migrated its configuration validation logic into `validate`.
- **Refactor: Firecracker modularization** — Modularized the Firecracker sandbox internals and references.

### Docs
- **Docs: Sandbox documentation** — Created comprehensive guides for sandbox usage and configuration.
- **Docs: Firecracker parameters** — Documented all Firecracker VM configurations and parameters.
- **Docs: CLI sync command** — Added reference documentation for the CLI synchronization command.

## [v0.2.5]

### Added
- **Core: DuckDB-style execution profiling & tracing** — Introduced `explain_tree()` for physical plan ASCII tree inspection and `trace()` for live operator-level CPU/I/O profiling with warning metrics.
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

