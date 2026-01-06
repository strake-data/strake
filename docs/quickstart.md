# Quickstart

This guide will get you up and running with Strake in under 5 minutes. We will configure a simple Postgres source and run a query using the Python client.

## 1. Initialize Configuration

Strake uses a declarative configuration file (`sources.yaml`) to define your data mesh.

Create a directory for your config:
```bash
mkdir -p strake-demo/config
cd strake-demo
```

Initialize a new configuration:
```bash
strake-cli init
```

This creates a `sources.yaml`. Let's edit it to add a public PostgreSQL demo source (or use your own).

```yaml
# config/sources.yaml
sources:
  - name: demo_pg
    type: sql
    dialect: postgres
    # Using a standard local postgres for demonstration
    connection: "postgres://postgres:postgres@localhost:5432/postgres"
```

> [!TIP]
> You can also use environment variables in your config like `${POSTGRES_PASSWORD}` for security.

## 2. Start the Server within Python (Embedded Mode)

You don't need to run a separate server process to get started. The Python client can run the engine embedded.

Create a file `main.py`:

```python
import strake_client
import pandas as pd

# Point to your configuration file
# In embedded mode, we pass the config path directly
conn = strake_client.StrakeConnection("./config/sources.yaml")

print("🔌 Connected to Strake Embedded Engine")

# Run a query!
# Note the namespace: strake.<source_name>.<schema>.<table>
query = """
    SELECT * 
    FROM strake.demo_pg.public.users 
    LIMIT 5
"""

print(f"Running: {query}")
df = conn.sql(query)

print("\n📊 Results:")
print(df)
```

## 3. Run It

```bash
python3 main.py
```

## 4. Next Steps

Now that you have a basic query running:
*   Add an **S3 Source** to join against Parquet files.
*   Check out the [Concepts](./concepts.md) page to understand how federation works.
*   View the [API Reference](./api.md) for more advanced usage.
