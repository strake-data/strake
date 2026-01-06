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
