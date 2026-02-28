<div align="center">
  <img src="docs/assets/logo.png" width="200" height="auto" alt="Strake Logo">
  <h1>Strake</h1>
  <p>
    <strong>The AI Data Layer</strong>
  </p>
  <p>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue.svg" alt="License"></a>
    <a href="CONTRIBUTING.md"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome"></a>
    <a href="https://strake-data.github.io/strake/"><img src="https://img.shields.io/badge/docs-latest-blue.svg" alt="Docs"></a>
  </p>
</div>

<br>

**Strake** is the AI Data Layer. Not just a query tool, and not a RAG pipeline. It's the sandboxed execution environment where agents meet your data and return answers, not rows.

Built on [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion), Strake enables AI agents to discover, query, and process data across your entire stack (PostgreSQL, Snowflake, S3, and more) without the need for data movement or ETL.

> 📚 **Full Documentation**: Check out the [complete documentation](https://strake-data.github.io/strake/) for installation, architecture, and API references.

---

##  Key Features

- **Developer First**: Built for engineers. Type-safe configuration, rich CLI tooling, and local development workflows.
- **Secure Execution Layer**: Run untrusted Python code safely using Firecracker MicroVMs or Native OS Sandboxing (Landlock, Seccomp, Namespaces).
- **High Performance**: Sub-second latency for federated joins using Apache Arrow.
- **Pluggable Sources**: Postgres, S3, Local Files, REST, gRPC, and more.
- **MCP-Native Discovery**: Built for the Model Context Protocol. Your agents discover your entire data catalog and schemas instantly.
- **Python Native**: Zero-copy integration with Pandas and Polars via PyO3.
- **Enterprise Governance**: Row-Level Security (RLS), Column Masking, and OIDC Authentication (Enterprise Edition).
- **Observability**: Built-in OpenTelemetry tracing and Prometheus metrics.
- **GitOps Native**: Manage your data mesh configuration as code. Version control your sources, policies, and metrics.
- **Enterprise Features**: OIDC, Row-Level Security, and Data Contracts (see [Enterprise Edition](https://strake-data.github.io/strake/enterprise/)).

## Code Mode: Don't Compute in Context

Most agents fail by swallowing thousands of raw SQL rows. Strake's **Code Mode** lets them process data in Python inside a secure sandbox, sending only the parsed results to the LLM.

```python
import strake
from strake.mcp import run_python

# Query 10M rows instantly via DataFusion
# Aggregate in Python to prevent context bloat
script = """
df = strake.sql("SELECT * FROM user_events")
summary = df.groupby('feature_flag')['latency'].median()
print(summary.to_json())
"""

# Runs isolated with OS Sandboxing or Firecracker VMs
result = await run_python(script)
```

## Quick Start (5-Minute Setup)

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

Initialize a new config and validate your sources:

```bash
# Initialize a new config
strake-cli init

# Validate configuration
strake-cli validate sources.yaml

# Apply to the metadata store (Sync)
strake-cli apply sources.yaml --force
```

### 3. Query with Python

First, define your data sources in a `sources.yaml` file:

```yaml
sources:
  - name: local_files
    type: csv
    path: "data/*.csv"
    has_header: true
    tables:
      - name: measurements
```

Then, query using the Strake Python client:

```python
import strake
import polars as pl

# Connect using your source configuration
conn = strake.connect(sources_config="sources.yaml")

# Query across sources using standard SQL
query = "SELECT * FROM measurements LIMIT 5"
data = conn.sql(query)

# Zero-copy integration with Polars/Pandas
df = pl.from_arrow(data)
print(df)
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
