# Oracle Database Connector

Strake supports high-performance querying of Oracle databases. It connects using the `rust_oracle` client driver wrapped in an asynchronous `bb8-oracle` connection pool.

---

## 1. Connection Syntax

Strake parses all database connection strings using URL schemas. For Oracle databases, the connection string is defined under the `url` (or `connection`) field and **must** be a valid URL prefixed with the `oracle://` scheme. You can set credentials either by embedding them in the URI or separating them into top-level parameters:

### Method 1: Embedded in the URI URL
```yaml
url: "oracle://system:OraclePassword123@oracle-free:1521/FREEPDB1"
```

### Method 2: Separated Top-Level Parameters (Safe for Environment Variables)
```yaml
url: "oracle://oracle-free:1521/FREEPDB1"
username: "system"
password: "${env:ORACLE_PASSWORD}"
```

---

## 2. Oracle Client Driver Requirements

Unlike pure-Rust SQL engines (like Postgres or SQLite), the underlying `rust_oracle` crate links against Oracle's **ODPI-C** dynamic wrapper, which **requires the Oracle Instant Client shared library (`libclntsh.so`) to be loaded by the OS dynamic linker at runtime**.

If the dynamic library is missing or cannot be located in the library path, Strake will fail during startup with a **`bb8 PoolCreationError`** (indicating library loading failure). If the client loading succeeds but network connections fail (due to incorrect credentials, closed ports, or firewall isolation), a **Connection Timeout** will occur instead.

### Step-by-Step Linux Installation

Follow these steps to configure the Instant Client on your Linux host or container environment:

#### Step 1: Download the Instant Client
1. Visit the [Oracle Instant Client Downloads for Linux](https://www.oracle.com/database/technologies/instant-client/downloads.html).
2. Download the **Instant Client Basic** (or **Basic Light**) ZIP package (e.g. version 21.x or 19.x).

#### Step 2: Extract to `/opt/oracle`
```bash
sudo mkdir -p /opt/oracle
sudo unzip instantclient-basic-linux.x64-21.13.0.0.0dbru.zip -d /opt/oracle
```

#### Step 3: Configure the System Dynamic Linker
Tell your operating system's loader where to locate the shared object library files:

```bash
# Option A: Expose via environment variable in your shell session (.bashrc)
export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_13:$LD_LIBRARY_PATH

# Option B: Persistent System-wide setting (Recommended)
echo "/opt/oracle/instantclient_21_13" | sudo tee /etc/ld.so.conf.d/oracle-instantclient.conf
sudo ldconfig
```

#### Step 4: Verify Generic Symlinks
The database driver looks specifically for the unversioned `libclntsh.so` file. Verify that a symlink exists pointing to the versioned file:
```bash
cd /opt/oracle/instantclient_21_13
# If libclntsh.so does not exist, link it manually:
ln -s libclntsh.so.21.1 libclntsh.so
```

---

## 3. Configuration Snippets

### Embedded Credentials
```yaml
sources:
  - name: legacy_oracle
    type: oracle
    # Strict URL format containing the oracle:// scheme
    url: "oracle://system:OraclePassword123@oracle-free:1521/FREEPDB1"
    pool_size: 10
    tables:
      - name: orders
        schema: sales
      - name: invoices
        schema: billing
```

### Separated Credentials (Environment Variables)
```yaml
sources:
  - name: legacy_oracle_secure
    type: oracle
    url: "oracle://oracle-free:1521/FREEPDB1"
    username: "system"
    password: "${env:ORACLE_PASSWORD}"
    pool_size: 10
    tables:
      - name: orders
        schema: sales
```

### Non-Default Schemas

If your tables live in a schema other than a standard default, Strake will automatically qualify the generated SQL as `"SCHEMA"."TABLE"`. No extra configuration is required for this to work correctly.

To make every schema always qualified (i.e. never treat any schema as a "default"), set `schema_mapping.default_schemas` to an empty list:

```yaml
sources:
  - name: oracle_prod
    type: oracle
    url: "oracle://system:OraclePassword123@oracle-host:1521/ORCL"
    pool_size: 10
    schema_mapping:
      default_schemas: []   # Always qualify — no schema is treated as default
    tables:
      - name: ORDERS
        schema: SALES     # Must match Oracle's case (typically uppercase)
      - name: INVOICES
        schema: BILLING
```

---

## 4. Schema Qualification

Oracle folds unquoted identifiers to **uppercase**. Strake enforces this during table discovery: schema names returned from `ALL_TABLES` are always uppercased internally.

When Strake generates pushdown SQL for an Oracle table, it decides whether to qualify the table reference based on the `schema_mapping.default_schemas` list:

- **Schemas in `default_schemas`** → generated as bare table names: `"ORDERS"`
- **Schemas NOT in `default_schemas`** → generated as fully qualified: `"SCHEMA"."TABLE"`

The built-in defaults for Oracle are `["public", "default"]`. Since Oracle schemas are typically uppercase (e.g. `SALES`, `BILLING`), they will almost always be qualified automatically — no extra configuration needed.

> [!TIP]
> If you are connecting to a schema that happens to be named `public` or `default` and you want it qualified, override the defaults:
> ```yaml
> schema_mapping:
>   default_schemas: []
> ```
