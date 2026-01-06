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

> 📚 **Full Documentation**: Check out the [complete documentation](docs/index.md) for installation, architecture, and API references.

---

##  Overview

Strake acts as an "Intelligent Pipe," sitting between your data sources and your analysis tools. It focuses on operational stability, ensuring that federated queries are executed efficiently and safely through aggressive pushdown optimization and memory-limit enforcement.

### Key Features

- **Zero-Copy Federation**: Query Postgres, MySQL, SQLite, Parquet, JSON, and more without moving data.
- **Enterprise Governance**: License-keyed concurrency control, RLS, column masking, and OIDC/SSO integration.
- **GitOps-Driven**: Manage your data mesh configuration as code with `strake-cli`.
- **High-Performance Python**: Direct PyO3 bindings for zero-copy conversion to Pandas/Polars.
- **Production Observability**: Integrated OpenTelemetry and Prometheus support.
- **Flight SQL**: Standard-compliant Arrow Flight SQL interface.

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
pip install strakepy
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
import strakepy
import polars as pl

# Connect using a local configuration file (Embedded Mode)
LOCAL_CONFIG = "config/strake.yaml" 
conn = strakepy.StrakeConnection(LOCAL_CONFIG)

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
| [**strake-core**](crates/core) | Central engine for configuration validation and source registration. |
| [**strake-server**](crates/server) | Arrow Flight SQL server implementation. |
| [**strake-cli**](crates/cli) | GitOps CLI for managing data mesh configurations. |
| [**strake-python**](python) | Python bindings for high-performance data access. |
| [**strake-enterprise**](strake-enterprise) | Enterprise features (SSO, Governance). |

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on how to get started.

## License

Strake is licensed under the [Apache 2.0](LICENSE) license.
