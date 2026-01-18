<div align="center">
  <img src="docs/assets/logo.png" width="200" height="auto" alt="Strake Logo">
  <h1>Strake</h1>
  <p>
    <strong>High-Performance Federated SQL Engine</strong>
  </p>
  <p>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue.svg" alt="License"></a>
    <a href="CONTRIBUTING.md"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome"></a>
  </p>
</div>

<br>

**Strake** is a high-performance federated SQL engine built on [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion). It enables users to query across disparate data sources—including PostgreSQL, Parquet, and JSON—using a single SQL interface without the need for data movement or ETL.

> 📚 **Full Documentation**: Check out the [complete documentation](https://strake-data.github.io/strake/) for installation, architecture, and API references.

---

##  Overview

Strake acts as an "Intelligent Pipe," sitting between your data sources and your analysis tools. It focuses on operational stability, ensuring that federated queries are executed efficiently and safely through aggressive pushdown optimization and memory-limit enforcement.

### Key Features

- **GitOps Native**: Manage your data mesh configuration as code. Version control your sources, policies, and metrics.
- **Developer First**: Built for engineers. Type-safe configuration, rich CLI tooling, and local development workflows.
- **High Performance**: Sub-second latency for federated joins using Apache Arrow.
- **Pluggable Sources**: Postgres, S3, Local Files, REST, gRPC, and more.
- **Enterprise Governance**: Row-Level Security (RLS), Column Masking, and OIDC Authentication (Enterprise Edition).
- **Python Native**: Zero-copy integration with Pandas and Polars via PyO3.
- **Observability**: Built-in OpenTelemetry tracing and Prometheus metrics.
- **Enterprise Features**: OIDC, Row-Level Security, and Data Contracts (see [Enterprise Edition](https://strake-data.github.io/strake/enterprise/)).

## Quick Start

### 1. Installation

#### Quick Install (Linux/macOS)
```bash
curl -sSfL https://strakedata.com/install.sh | sh
```

#### Install via Cargo (Rust)
```bash
cargo install --path crates/cli
cargo install --path crates/server
```

#### Python Client
```bash
pip install strake
```

### 2. Configuration (GitOps)

Initialize and apply your data source configuration:

```bash
# Initialize a new config
strake-cli init

# Validate configuration
strake-cli validate sources.yaml

# Apply to the metadata store (Sync)
strake-cli apply sources.yaml --force
```

### 3. Usage (Python)

Strake provides a seamless interface for data scientists and engineers:

```python
import strake
import polars as pl

# Connect using a local configuration file (Embedded Mode)
LOCAL_CONFIG = "config/strake.yaml" 
conn = strake.StrakeConnection(LOCAL_CONFIG)

print("\n--- List Tables ---")
print(conn.describe())

print("\n--- Describe Table ---")
print(conn.describe("measurements"))

print("\n--- Query PyArrow Table ---")
data = conn.sql("SELECT * FROM measurements LIMIT 5")
# Convert to Polars DataFrame
print(pl.from_arrow(data))
```

## Project Structure

| Component | Description |
|-----------|-------------|
| [**strake-runtime**](crates/runtime) | Orchestration layer (Federation Engine, Sidecar). |
| [**strake-connectors**](crates/connectors) | Data source implementations (Postgres, S3, REST, etc). |
| [**strake-sql**](crates/sql) | SQL Dialects, Query Optimization, and Substrait generation. |
| [**strake-common**](crates/common) | Shared types, configuration, and error handling. |
| [**strake-server**](crates/server) | Arrow Flight SQL server implementation. |
| [**strake-cli**](crates/cli) | GitOps CLI for managing data mesh configurations. |
| [**strake-python**](python) | Python bindings for high-performance data access. |


## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on how to get started.

## License

Strake is licensed under the [Apache 2.0](LICENSE) license.
