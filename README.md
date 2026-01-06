# Strake

Strake is a high-performance federated SQL engine built on Apache Arrow DataFusion. It enables users to query across disparate data sources—including PostgreSQL, Parquet, and JSON—using a single SQL interface without the need for data movement or ETL.

> 📚 **Full Documentation**: Check out the [complete documentation](docs/index.md) for installation, architecture, and API references.

## Overview

Strake acts as an "Intelligent Pipe," sitting between your data sources and your analysis tools. It focuses on operational stability, ensuring that federated queries are executed efficiently and safely through aggressive pushdown optimization and memory-limit enforcement.

**Developer-First, Enterprise-Ready**:
* **Local Development**: The OSS library is perfect for local developer setups, providing a high-performance federated engine at zero cost.
* **Production Scale**: Strake can be deployed as a standalone server for horizontal scaling and multi-user access.
* **Enterprise Features**: Unlock advanced governance (RLS & Masking), OIDC/SSO, and license-managed features with the Enterprise Edition.

## Key Features

* **GitOps-Driven Configuration**: Manage your entire data mesh configuration as code using `strake-cli`. Supports secret expansion (`${VAR}`), offline validation, and automated synchronization (pruning).
* **License-Keyed Concurrency Control**: Enterprise enforcement of query concurrency slots via Tokio semaphores and Tower middleware. Verified with zero-limit denial security.
* **Pluggable Source Registry**: Modularly query **Postgres, MySQL, SQLite** and:
  - **Enterprise**: Snowflake, BigQuery, Dremio (via Flight SQL), and **Excel** (Verified).
  - **Flexible Config**: Providers are decoupled from the core, allowing for custom connector development.
  - **Files**: Parquet, CSV, JSON (with verified glob-based multi-file support).
* **Secure Identity & Governance**: 
  - **OSS**: Asynchronous authentication API supporting **Argon2** hashed API keys with database persistence.
  - **Enterprise**: Single Sign-On (OIDC/SSO), **SQL-based RLS**, **Column Masking**, OIDC Group mapping, and asymmetric JWT-based license enforcement.
* **Hardened Resource Management**: Prevents cascading failures via **Connection Pooling**, **Global Budgets**, and **Adaptive Circuit Breakers**.
* **Production Caching**: **Moka**-backed in-memory cache with **Parquet** disk spillover and restart persistence for instant query results.
* **Query Safety & Validation**: Built-in QueryValidator prevents "queries of death" by enforcing row-count pushdowns and memory safety.
* **High-Performance Python Bindings**: Direct PyO3 bindings allow zero-copy conversion of results into PyArrow Tables, Pandas DataFrames, or Polars DataFrames.
* **Flight SQL Server**: Standard-compliant Arrow Flight SQL interface with **full prepared statement support**.
* **Production Observability**: Integrated **OpenTelemetry** tracing and **Prometheus** metrics for real-time monitoring.

## Quick Start

### 1. Configuration & GitOps

Manage your configuration using the **Strake CLI**.

1. **Initialize** a new configuration:
   ```bash
   strake-cli init
   ```
2. **Edit** `sources.yaml`. You can use environment variables for secrets:
   ```yaml
   sources:
     - name: pg
       type: sql
       dialect: postgres
       connection: "postgres://postgres:${POSTGRES_PASSWORD}@172.19.0.2:5432/postgres"
   ```
3. **Validate** your configuration:
   ```bash
   # Standard validation (checks structure + source connectivity)
   strake-cli validate sources.yaml
   
   # Offline validation (CI/CD friendly, structure only)
   strake-cli validate sources.yaml --offline
   ```
4. **Apply** changes to the metadata store (Sync):
   ```bash
   # Updates Postgres metadata. Prunes (deletes) any sources not in the file.
   strake-cli apply sources.yaml --force
   ```
   *Note: `--force` is required if applying an empty config to prevent accidental data loss.*
### 2. Python Usage

Strake provides a seamless interface for data scientists and engineers:

```python
import strakepy

# Initialize the connection (Flight SQL Client)
dsn = "grpc://localhost:50053"
conn = strakepy.StrakeConnection(dsn)

# Execute a federated join across Postgres and JSON
query = """
    SELECT 
        m.id, 
        meta.first_name, 
        meta.last_name
    FROM strake.public.measurements m
    JOIN strake.public.metadata meta ON m.hash_16 = meta.hash_16
    LIMIT 10
"""

# Fetch as a Pandas DataFrame
df = conn.sql(query)
print(df)

# Trace the query (works for Remote and Embedded!)
conn.trace(query)
```


### 3. Running the Server

Strake can be run as a standalone server to provide remote access via Flight SQL.

#### Open Source Edition (OSS)
To run the standard OSS server:
```bash
cargo run --package strake-server
```

#### Enterprise Edition
To run the Enterprise Edition (includes OIDC/SSO and advanced governance):

**1. Configure OIDC (strake.yaml)**
```yaml
server:
  oidc:
    issuer_url: "https://<your-domain>.us.auth0.com/"
    audience: ["https://strake.io"]
```

**2. Run the Server**
```bash
# Set your license key (Required)
export STRAKE_LICENSE_KEY="your_license_key_here"

# For local development/testing, you can use the bypass key:
# export STRAKE_LICENSE_KEY="sk_ent_dev_bypass"

cargo run --package strake-enterprise
```

> [!NOTE]
> The server listens on `0.0.0.0:50051`. You can connect using any Arrow Flight SQL JDBC/ODBC driver.

## Project Structure

* [strake-core](crates/core): The central engine responsible for configuration, source registration, and query validation.
* [strake-server](crates/server): The Arrow Flight SQL server implementation.
* [strake-cli](crates/cli): GitOps CLI for managing data mesh configurations.
* [strake-python](python): PyO3-based Python bindings for high-performance data access.

