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

## 2. Using `trace()` for Performance Tuning

If a query feels slow, use `trace()` to see if pushdown is working.

```python
conn.trace("SELECT * FROM strake.pg.public.users WHERE id > 1000")
```

**Output Analysis**:

Look for `TableScan`.

*   **Good (Pushdown)**:
    ```
    TableScan: users projection=[id, email], filters=[id > 1000]
    ```
    The filter is part of the scan! It's happening inside Postgres.

*   **Bad (No Pushdown)**:
    ```
    Filter: id > 1000
      TableScan: users
    ```
    The filter is separate. Strake is scanning *everything* and filtering locally.
