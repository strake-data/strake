# Strake AI Context

This document provides context for AI agents (like Cursor, Copilot) to understand the Strake project, its configuration patterns, and architecture.

## Core Concept
Strake is a high-performance **federated SQL engine** built on **Apache Arrow DataFusion**. It acts as an "Intelligent Pipe," enabling zero-copy federation across disparate data sources via a single **Arrow Flight SQL** interface. It does **not** copy data; it pushes queries down to the source.

## Configuration: `sources.yaml`
Strake uses a GitOps-driven configuration file `sources.yaml` to define data sources.

### SQL Source Example
```yaml
- name: pg_production
  type: sql
  dialect: postgres
  connection: postgresql://user:pass@db:5432/dbname
  tables:
  - name: users
    schema: public
    columns:
    - name: id
      type: integer
      primary_key: true
```

### Parquet/S3 Example
```yaml
- name: data_lake
  type: parquet
  path: s3://my-bucket/data/sales_*.parquet
  tables:
  - name: sales
    schema: public
    columns: [] # Auto-inferred
```

### REST Example
```yaml
- name: api_source
  type: rest
  base_url: https://api.example.com/v1
  pagination:
    type: header
    header_name: Link
```

## Architecture
- **`crates/runtime`**: The query engine (DataFusion wrapper).
- **`crates/connectors`**: Source adapters (Postgres, Oracle, DuckDB, REST, etc.).
- **`crates/server`**: Flight SQL server implementation.
- **`crates/error`**: Unified error handling (`StrakeError`).
- **`strakepy`**: Python bindings via PyO3.

## Coding Standards
- **Error Handling**: Use `strake_error::StrakeError`. Do not use `anyhow` in core library code.
- **Async**: Use `tokio` for runtime.
- **Python**: All Python code in `strakepy` must use proper type hints.
