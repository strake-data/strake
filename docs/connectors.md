# Connectors

Strake connects to your entire data landscape. Move compute, not data, by querying distributed sources as a single logical warehouse.

---

## SQL Databases

| Connector | Protocol | Notes |
|-----------|----------|-------|
| **PostgreSQL** | Postgres Wire | Full predicate pushdown & SSL support |
| **MySQL / MariaDB** | MySQL Wire | Optimized for transactional consistency |
| **SQLite** | VFS / Local | High-performance local caching & testing |
| **ClickHouse** | Native / HTTP | SQL pushdown for heavy OLAP workloads |
| **Snowflake** | Flight SQL | Secure, high-throughput cloud warehousing |
| **Dremio** | Flight SQL | Unified data lakehouse federation |

---

## Modern File Formats

| Format | Protocol | Notes |
|--------|-------------|-------|
| **Apache Parquet** | Columnar (Native) | Zero-copy reading with filter pushdown |
| **CSV / TSV** | Text / Delimited | Tunable parallelism & schema inference |
| **JSON / NDJSON** | Semi-structured | Handles nested objects & dynamic schemas |
| **Microsoft Excel** | Calamite / XML | Native XLSX parsing (*Enterprise Edition*) |
| **Apache Avro** | Binary / Row | Robust schema evolution & fast decoding |

---

## Object Storage & Cloud

| Provider | Protocol | Notes |
|----------|----------|-------|
| **AWS S3** | Apache OpenDAL | S3-compatible endpoints (MinIO, R2, etc.) |
| **Google Cloud Storage** | Apache OpenDAL | High-performance GCS bucket integration |
| **Azure Blob Storage** | Apache OpenDAL | Optimized for Azure Data Lake Gen2 |

---

## REST APIs & SaaS

| Connector | Protocol | Notes |
|-----------|----------|-------|
| **Generic REST** | HTTP / JSON | Declarative endpoint-to-table mapping |
| **GitHub API** | REST / GraphQL | Pre-built analytics for repo and issue data |
| **gRPC Services** | gRPC (HTTP/2) | High-performance typed microservice joins |

---

## Adding New Sources

Strake uses a declarative `sources.yaml` to register these connectors. See the [Quickstart Guide](quickstart.md) for step-by-step instructions on configuring your first data sources.
