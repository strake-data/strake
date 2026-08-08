# AGENTS.md — Strake Python Library Agent Instructions

## Overview
Strake is Python's AI Data Layer. Agents use `strake` to query, join, and analyze data across federated sources (Postgres, Oracle, Snowflake, S3, CSV, Parquet) in a secure, sandboxed environment.

This guide provides instructions and best practices for AI agents writing Python code with `strake`.

---

## 1. Connecting to Data Sources

### Option A: Embedded Execution (Local `sources.yaml`)
```python
import strake

# Connect using local configuration file
conn = strake.connect(sources_config="sources.yaml")
```

### Option B: Remote Execution (Strake Server over gRPC)
```python
import os
import strake

# Connect to remote Strake Flight SQL server
conn = strake.connect(
    dsn_or_config=os.getenv("STRAKE_SERVER_URL", "grpc://localhost:50051"),
    api_key=os.getenv("STRAKE_API_KEY"),
    mode="remote"
)
```

---

## 2. Querying & Data Processing

### SQL Query Execution
`conn.sql(...)` executes federated SQL queries with pushdown optimization:
```python
# Returns an Apache Arrow Table
arrow_table = conn.sql("""
    SELECT user_id, COUNT(*) as event_count, AVG(latency_ms) as avg_latency
    FROM user_events
    WHERE event_date >= '2026-01-01'
    GROUP BY user_id
    HAVING COUNT(*) > 10
    LIMIT 100
""")
```

### Zero-Copy Conversion to Polars / Pandas
Always process data using vectorized operations rather than iterating over rows:
```python
import polars as pl

# Convert Arrow table to Polars DataFrame (Zero-copy)
df = pl.from_arrow(arrow_table)

# Perform aggregations or transformations
summary = df.group_by("user_id").agg(pl.col("event_count").sum())
```

For Pandas:
```python
# Convert directly to Pandas DataFrame
df_pandas = arrow_table.to_pandas()
```

---

## 3. Code Mode (Preventing LLM Prompt Bloat)

When handling large datasets (thousands/millions of rows), run Python processing inside the sandbox so only summarized results reach the LLM context:

```python
import strake
from strake.mcp import run_python

# Code Mode script executed inside sandbox
script = """
import strake
import polars as pl

conn = strake.connect(sources_config="sources.yaml")
raw_data = conn.sql("SELECT * FROM large_log_table")
df = pl.from_arrow(raw_data)

# Compute lightweight summary
metrics = {
    "total_rows": len(df),
    "error_rate": df.filter(pl.col("status") == "ERROR").height / len(df)
}
print(metrics)
"""

# Execute sandboxed script and return output
result = await run_python(script)
```

---

## 4. Configuration Reference (`sources.yaml` & `strake.yaml`)

### `sources.yaml` (Data Sources Definition)
Defines upstream databases, files, and lakehouse connections that Strake federates:

```yaml
sources:
  # Relational Database (PostgreSQL / JDBC)
  - name: production_pg
    type: JDBC
    url: "jdbc:postgresql://${PG_HOST:localhost}:5432/${PG_DB:postgres}"
    tables:
      - name: user_events
        schema: public

  # Local Files (Parquet / CSV)
  - name: local_logs
    type: parquet
    config:
      path: "data/logs/*.parquet"
    predicate_cache: false

  # Object Store (S3)
  - name: s3_analytics
    type: s3
    config:
      bucket: "company-analytics"
      region: "us-east-1"
      file_format: "parquet"
```

### `strake.yaml` (Engine & Server Runtime Settings)
Configures partition concurrency, optimizer flags, and server ports:

```yaml
server:
  listen_addr: "0.0.0.0:50051"

resources:
  target_partitions: 4
  enable_broadcast_join: true
  enable_push_down_filter: true
  enable_single_node_aggregation: false
```

---

## 5. Key Agent Guidelines & Safety

1. **Prevent Prompt Context Overflow**:
   - Never print or dump thousands of raw data rows to stdout or return values.
   - Aggregate, sample, or convert data to high-level summaries/JSON before outputting.

2. **Read-Only Safety**:
   - Strake sources are **read-only** by default. Do not attempt `INSERT`, `UPDATE`, `DELETE`, or DDL statements.

3. **Secrets & Authentication**:
   - Load API keys, database credentials, and server URLs via `os.getenv(...)` or env variable interpolation (`${ENV_VAR}`) in YAML configs. Never hardcode secrets.

4. **Error Handling**:
   - Wrap connection and query attempts in explicit error handling:
     ```python
     try:
         conn = strake.connect(sources_config="sources.yaml")
         data = conn.sql("SELECT * FROM my_table LIMIT 10")
     except strake.ConfigError as e:
         print(f"Configuration error: {e}")
     except Exception as e:
         print(f"Query execution failed: {e}")
     ```
