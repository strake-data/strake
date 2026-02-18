# Core Concepts

Strake is designed to bridge the gap between operational databases, data lakes, and analytical tools.

## Federated Querying

Federation allows you to execute SQL queries across multiple data sources as if they were tables in a single database.

Strake does **not** copy data into a central repository. Instead, it queries data *in place*.
*   **Postgres**: Strake speaks the Postgres wire protocol or native generic SQL.
*   **S3**: Strake reads Parquet, CSV, and JSON files directly from object storage using the S3 API.
*   **REST/gRPC**: Strake can map API responses to tables.

## Pushdown Optimization

The key to high performance in a federated system is **Pushdown Optimization**.

Strake analyzes your SQL query and "pushes down" as much work as possible to the source system.

Example:
```sql
SELECT * FROM users WHERE id > 1000
```
*   **Without Pushdown**: Strake fetches *all* users, then filters them in memory. (Slow, High Network I/O)
*   **With Pushdown**: Strake sends `SELECT * FROM users WHERE id > 1000` to the Postgres database. Only the relevant rows are transferred. (Fast, Low Network I/O)

Strake currently supports pushing down:
*   Projections (selecting specific columns)
*   Filters (`WHERE` clauses)
*   Limits (`LIMIT n`)

## The Source Registry

The **Source Registry** is the heart of Strake's extensibility. It manages the connections to all your data sources.

When you define a source in `sources.yaml`, the Registry:
1.  Validates the configuration.
2.  Establishes a connection (pool).
3.  Fetches schemas/metadata (for schema inference).
4.  Registers the source as a "Table Provider" in the Apache Arrow DataFusion context.

## Governance (Enterprise)

Strake Enterprise adds a layer of security and policy management on top of your data.

1.  **Row-Level Security (RLS)**: Define SQL-based policies (e.g., `org_id = current_setting('user.org_id')`) that automatically filter rows based on the authenticated user.
2.  **Column Masking**: Automatically mask sensitive data (PII) for unauthorized roles (e.g., `SELECT mask(email) ...`).
3.  **Role-Based Access Control (RBAC)**: Assign permissions to Roles and map Users to Roles via OIDC claims.

## Cache Consistency Model

Strake implements a **TTL-based, User-Isolated** caching strategy to ensure performance while maintaining security.

*   **User Isolation**: Cache keys include the User ID (or Roles) to prevent cross-user data leakage. A query run by User A will not return cached results for User B unless they share the exact same context permissioning (depending on configuration).
*   **TTL-Based Expiry**: Cached entries are valid for a configured duration (e.g., 60 seconds). Strake does not actively invalidate cache on source changes; it relies on eventual consistency driven by the TTL.
*   **Defensive Caching**: If the cache backend (e.g., Redis) fails, Strake fails open and executes the query directly against the source, logging a warning.

## Resource Governance

To protect both the Strake engine and your upstream data sources, Strake enforces multi-layer resource limits:

1.  **Global Query Timeout**: A hard limit on the total execution time of any query (e.g., 30s). If exceeded, the query is cancelled to free up resources.
2.  **Per-Source Concurrency Limits**: Each data source (e.g., "Production DB") can be configured with a maximum number of concurrent queries (e.g., `max_concurrent_queries: 5`). This acts as a bulk-head pattern, preventing a flood of analytical queries from overwhelming a transactional database. Queries exceeding this limit are queued or rejected.
3.  **Defensive Rows Limit**: (Planned) Strake can enforce a maximum number of rows returned to prevent OOM errors.
