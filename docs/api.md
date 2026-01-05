# API Reference

## Python Client

The Python client is the primary way to interact with Strake.

### `strake_client` Module

#### class `StrakeConnection`

Establish a connection to a Strake engine.

```python
class StrakeConnection:
    def __init__(self, dsn: str, api_key: Optional[str] = None):
        ...
```

**Arguments**:
*   `dsn` (str):
    *   **Remote**: A URL starting with `grpc://` or `http://` pointing to a running Strake Server (e.g., `grpc://localhost:50051`).
    *   **Embedded**: A file path to a `sources.yaml` configuration file (e.g., `./config/sources.yaml`).
*   `api_key` (str, optional): Authentication token/key.

---

#### method `sql`

Execute a SQL query and return the result.

```python
def sql(self, query: str) -> pd.DataFrame:
    ...
```

**Arguments**:
*   `query` (str): The SQL query to execute.

**Returns**:
*   `pandas.DataFrame`: The result set.

---

#### method `trace`

Execute a query and print a detailed execution trace for performance debugging.

```python
def trace(self, query: str) -> None:
    ...
```

## Configuration Schema (`sources.yaml`)

The `sources.yaml` file defines the data sources available to the engine.

### Top-Level Structure

```yaml
sources:
  - name: string      # Unique identifier for the source (used in SQL hierarchy)
    type: string      # Source type (sql, s3, file, rest)
    dialect: string   # (Optional) SQL dialect (postgres, mysql, sqlite)
    connection: string # Connection string or URL
    ...
```

### Source Types

#### `sql` (PostgreSQL, MySQL, SQLite)
Connect to a relational database.

*   `dialect`: `postgres`, `mysql`, `sqlite`
*   `connection`: JDBC-style connection string (e.g., `postgres://user:pass@host:port/db`)

#### `s3` (AWS S3, MinIO)
Read files from S3-compatible storage.

*   `access_key_id`: AWS Access Key
*   `secret_access_key`: AWS Secret Key
*   `endpoint`: (Optional) Custom endpoint URL
*   `region`: (Optional) AWS Region
*   `bucket`: Bucket name
*   `paths`: List of file patterns (globs supported)

```yaml
  - name: my_lake
    type: s3
    access_key_id: "..."
    bucket: "analytics-data"
    paths:
      - "events/2023/*.parquet"
```
