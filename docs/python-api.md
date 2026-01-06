# Python API Reference

The Strake Python client provides a high-performance, zero-copy interface to the Strake engine, powered by Rust and Apache Arrow.

## class `StrakeConnection`

The primary interface for interacting with Strake. It can operate in **Remote** mode (connecting to a running server) or **Embedded** mode (running the engine locally).

---

### `__init__`

<div class="api-signature">
<code>__init__(dsn_or_config, sources_config=None, api_key=None)</code>
<span class="type">constructor</span>
</div>

Initializes a new connection to the Strake engine.

**Parameters:**

`dsn_or_config` : `str`
:   **Remote mode**: A gRPC URL (e.g., `grpc://localhost:50051`).  
    **Embedded mode**: A path to a `strake.yaml` or `sources.yaml` file.

`sources_config` : `str`, *optional*
:   **Embedded mode only**: Specific path to a `sources.yaml` file. If omitted, the engine looks in the same directory as `dsn_or_config`.

`api_key` : `str`, *optional*
:   Your Strake API key. Required for remote Enterprise servers.

---

### `sql`

<div class="api-signature">
<code>sql(query, params=None)</code>
<span class="type">method</span>
</div>

Executes a SQL query and returns the results.

**Parameters:**

`query` : `str`
:   The SQL query string to execute.

`params` : `dict`, *optional*
:   Query parameters for prepared statements (Experimental).

**Returns:**

`pyarrow.Table`
:   A PyArrow Table containing the query results. Use `.to_pandas()` or `.to_polars()` for downstream analysis.

**Example:**

```python
import strake

# 1. Connect (Embedded Mode)
conn = strake.StrakeConnection("./config/strake.yaml")

# 2. Query
table = conn.sql("SELECT * FROM demo_pg.public.users LIMIT 10")

# 3. Analyze
df = table.to_pandas()
print(df.head())
```

---

### `describe`

<div class="api-signature">
<code>describe(table_name=None)</code>
<span class="type">method</span>
</div>

Introspects the available sources or a specific table schema.

**Parameters:**

`table_name` : `str`, *optional*
:   The full name of the table to describe (e.g., `source.schema.table`). If omitted, returns a list of all available tables across all sources.

**Returns:**

`str`
:   A pretty-printed table of metadata.

---

### `trace`

<div class="api-signature">
<code>trace(query)</code>
<span class="type">method</span>
</div>

Returns the logical execution plan for a query without actually executing it.

**Parameters:**

`query` : `str`
:   The SQL query string to profile.

**Returns:**

`str`
:   The pretty-printed execution plan showing optimizer transformations and source pushdowns.

---

## Data Interchange

Strake is built on **Apache Arrow**. When you call `conn.sql()`, data is streamed into Python as Arrow record batches. This ensures:

1.  **Zero-Copy**: Near-zero overhead when converting to Pandas or Polars.
2.  **Type Safety**: Typed data remains typed from source to dataframe.
3.  **Memory Efficiency**: Massive datasets can be processed without inflating memory usage.
