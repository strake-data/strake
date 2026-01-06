# Welcome to Strake

**Strake** is a high-performance **federated query engine** built on [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion). It acts as an "Intelligent Pipe," enabling you to query and join data across disparate sources—PostgreSQL, S3 (Parquet/JSON/CSV), REST APIs, and more—using a single standard SQL interface.

---

## Why Strake?

Modern data stacks are fragmented. You have transactional data in Postgres, historical logs in S3 Parquet files, and SaaS data in REST APIs. operationalizing analysis across these often requires fragile ETL pipelines or expensive data warehousing.

Strake solves this by **bringing the compute to the data**:

*   **Zero-ETL**: Query data where it lives. No need to load it into a central store first.
*   **High Performance**: Built on Rust and Arrow, Strake pushes down filters, projections, and limits to the source whenever possible, minimizing data transfer.
*   **Unified Interface**: One SQL dialect (Postgres-compatible) for all your data.
*   **Embeddable or Server**: Run it locally inside your Python script for development, or deploy it as a Flight SQL server for production.

## Key Features

*   **High Performance**: Sub-second latency for federated joins using Apache Arrow.
*   **Pluggable Sources**: Postgres, S3, Local Files, REST, gRPC, and more.
*   **Enterprise Governance**: Row-Level Security (RLS), Column Masking, and OIDC Authentication (Enterprise Edition).
*   **Python Native**: Zero-copy integration with Pandas and Polars via PyO3.
*   **Observability**: Built-in OpenTelemetry tracing and Prometheus metrics.
*   **Enterprise Features**: OIDC, Row-Level Security, and Data Contracts (see [Enterprise Edition](./enterprise.md)).

## Next Steps

*   [**Installation**](./install.md): Get Strake running on your machine.
*   [**Quickstart**](./quickstart.md): Run your first federated query in 5 minutes.
*   [**Core Concepts**](./concepts.md): Understand how Strake optimizes your queries.
*   [**Python API**](./python-api.md): Detailed reference for the Python client.
