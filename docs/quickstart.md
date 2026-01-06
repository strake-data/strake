# Quickstart

This guide will get you up and running with Strake in under 5 minutes. We will configure a project, add a Postgres source, and run a query using the Python client.

## 1. Setup Project with `uv`

The recommended way to use Strake is with [uv](https://github.com/astral-sh/uv).

```bash
# Create a new project directory
mkdir strake-demo && cd strake-demo

# Initialize with uv
uv init

# Add the Strake Python client
uv add strake
```

## 2. Initialize Strake Configuration

Strake uses a declarative configuration file (`sources.yaml`) to define your data mesh.

Initialize a new configuration using the CLI:

```bash
# If you don't have the CLI, install it first:
# curl -sSfL https://strakedata.com/install.sh | sh

strake-cli init
```

By default, this creates a `sources.yaml`. Let's ensure it has a PostgreSQL source:

```yaml
# sources.yaml
sources:
  - name: demo_pg
    type: sql
    dialect: postgres
    # Example connection string
    connection: "postgres://postgres:postgres@localhost:5432/postgres"
```

## 3. Query Data (Embedded Mode)

You don't need a standalone server to get started. The Python client can run the Strake engine directly.

Create a file `main.py`:

```python
import strake
import pandas as pd

# 1. Connect (Embedded Mode)
# We pass the path to our config directly
conn = strake.StrakeConnection("./sources.yaml")

print("Connected to Strake Embedded Engine")

# 2. Run a query
# Tables are accessible via: strake.<source_name>.<schema>.<table>
query = """
    SELECT * 
    FROM strake.demo_pg.public.users 
    LIMIT 5
"""

print(f"Running: {query}")
table = conn.sql(query)

# 3. Analyze results as a DataFrame
df = table.to_pandas()
print("\nResults:")
print(df)
```

## 4. Run the Script

Use `uv` to run your script with all dependencies managed automatically:

```bash
uv run main.py
```

## 5. Next Steps

Now that you have your first query running:

* Add an **S3 Source** to join S3 Parquet files against your Postgres DB.
* Explore the [Connectors](connectors.md) page for a full list of supported sources.
* Check out the [Python API](python-api.md) for more advanced usage.
