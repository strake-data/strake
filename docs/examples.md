# Examples

## 1. Cross-Source Join (Postgres + Parquet)

This is the "Hello World" of federation. Join hot transactional data (Postgres) with cold historical logs (S3 Parquet).

### Scenario
You have:
*   `users` table in **PostgreSQL** (User metadata).
*   `logs` directory of Parquet files in **S3** (Clickstream events).

### Configuration (`sources.yaml`)

```yaml
sources:
  - name: pg
    type: sql
    dialect: postgres
    connection: "postgres://user:pass@localhost:5432/app_db"

  - name: s3_logs
    type: s3
    bucket: "my-datalake"
    paths: ["logs/*.parquet"]
    # ... creds ...
```

### Query

Join the `s3_logs` (implicitly registered as tables based on filename or folder) with `pg.public.users`.

```python
query = """
    SELECT 
        u.email, 
        count(l.event_id) as event_count
    FROM strake.pg.public.users u
    JOIN strake.s3_logs.logs l ON u.id = l.user_id
    WHERE l.event_date > '2023-01-01'
    GROUP BY u.email
    ORDER BY event_count DESC
    LIMIT 10
"""

df = conn.sql(query)
```

## 2. Performance Tuning: `explain_tree()` vs. `trace()`

Strake provides two tools to inspect how your queries are executed. For performance tuning, we recommend using `explain_tree()` in **Embedded Mode**.

### `explain_tree()` (Recommended)
Returns a detailed ASCII tree visualization of the **Physical Plan**, including pushdown indicators and execution metrics.

> [!NOTE]
> `explain_tree()` requires direct access to the engine's physical plan and is only available in **Embedded Mode**. For remote connections, it falls back to the logical `trace()` output.

```python
# Verify if filtering is happening at the source (e.g., S3/Parquet)
print(conn.explain_tree("SELECT * FROM strake.s3_logs.logs WHERE event_date > '2023-01-01'"))
```

#### Analysis: Good vs. Bad Plans

*   **✅ Good (Pushdown Active)**: Look for the `[PUSHED]` marker.
    ```text
    └─ DataSource (source: parquet) [PUSHED]
       filter: event_date > '2023-01-01' [PUSHED]
    ```
    *Impact: The filter is executed by the source (S3/Postgres). Only matching rows are transferred over the network.*

*   **❌ Bad (Local Execution)**: Look for the `[NOT PUSHED]` marker.
    ```text
    └─ Filter: event_date > '2023-01-01' [NOT PUSHED - Executed Locally]
       └─ DataSource (source: parquet) [PUSHED]
    ```
    *Impact: Strake fetches **every** row from the source and filters them in memory. This is a common performance bottleneck.*

### `trace()`
Returns the **Logical Plan** of the query as a pretty-printed table. This is useful for understanding the high-level query structure and works across both Embedded and Remote modes.

```python
print(conn.trace("SELECT * FROM strake.pg.public.users WHERE id > 1000"))
```
