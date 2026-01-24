# Strake Workspace

This workspace was initialized with `strake-cli init`.

## Quick Start

### 1. Review Your Configuration

- **`sources.yaml`**: Defines your data sources (databases, APIs, files)
- **`strake.yaml`**: Runtime configuration (metadata DB, server settings, caching)

### 2. Validate Your Setup

```bash
strake-cli validate
```

### 3. Apply Configuration

Apply your sources to the metadata store:

```bash
strake-cli apply
```

### 4. Start Querying

Start the Strake server:

```bash
strake-server
```

Or use the Python SDK (Remote Mode):

```python
import strake

# Connect to the running server
conn = strake.connect("grpc://localhost:50053")
df = conn.execute("SELECT * FROM my_table LIMIT 10").to_pandas()
print(df)
```

### 5. Running in Embedded Mode

Strake can run in embedded mode without a standalone server, perfect for CLI tools or local data processing:

```python
import strake

# Automatically loads local strake.yaml and sources.yaml
conn = strake.connect(mode="embedded")
df = conn.execute("SELECT * FROM my_table").to_pandas()
print(df)
```

## Common Commands

- `strake-cli domain list` — List all domains
- `strake-cli search <source>` — Discover tables in a source
- `strake-cli add <source> <table>` — Add a table to sources.yaml
- `strake-cli diff` — Preview configuration changes

## Learn More

- [Documentation](https://docs.strake.dev)
- [GitHub](https://github.com/strake-project/strake)
