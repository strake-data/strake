# Configuration Reference

Strake is configured using a layered model: a primary configuration file (`strake.yaml`) sets base values, and environment variables can override these at runtime.

---

## Configuration Layering

Strake loads its settings in a specific sequence:
1. **Default values** compiled into the binary.
2. **`strake.yaml` configuration file** (or a file specified via the `--config` flag).
3. **Environment variables** overriding any file-based configurations.

### Environment Variable Overrides
All configuration parameters can be overridden using environment variables prefixed with `STRAKE__` and separated by double underscores (`__`) to denote nesting.

*Example:*
To override `server.listen_addr` and `resources.enable_broadcast_join`:
```bash
export STRAKE__SERVER__LISTEN_ADDR="0.0.0.0:60061"
export STRAKE__RESOURCES__ENABLE_BROADCAST_JOIN="true"
```

---

## Complete `strake.yaml` Example

Here is a fully annotated example of a production-ready `strake.yaml` file:

```yaml
# Deployment environment (development | production)
environment: production

# Core server networking and database settings
server:
  name: "Strake Production Cluster"
  listen_addr: "0.0.0.0:50051"
  health_addr: "0.0.0.0:8080"
  catalog: "strake"
  api_url: "https://api.strake.internal/api/v1"
  global_connection_budget: 150
  database_url: "postgres://user:pass@db-host:5432/strake_metadata"
  
  # TLS configuration
  tls:
    enabled: true
    cert: "/etc/strake/certs/tls.crt"
    key: "/etc/strake/certs/tls.key"
    
  # Authentication settings
  auth:
    enabled: true
    api_key: "secure-api-key-here"
    cache_ttl_secs: 600
    cache_max_capacity: 20000

  # Audit logging (Enterprise)
  audit:
    enabled: true
    failure_mode: alert

# Query optimization & resource controls
resources:
  memory_limit_mb: 8192         # Limits query RAM usage; spills to disk when exceeded
  spill_dir: "/mnt/fast-ssd/spill" # Scratch space for spill-to-disk
  target_partitions: 8          # Partitions for query execution
  
  # High-Performance Optimizer Switches
  enable_broadcast_join: true   # High-performance physical broadcast join
  enable_push_down_filter: true # Aggressive query pushdown
  enable_single_node_aggregation: true
  enable_single_partition_optimizer: true
  enable_correlated_distinct_pushdown: true

# Global query limits to prevent resource exhaustion
query_limits:
  max_output_rows: 500000
  max_scan_bytes: 107374182400  # 100 GB
  default_limit: 5000
  query_timeout_seconds: 60

# Query Result Caching
cache:
  enabled: true
  directory: "/var/cache/strake"
  max_size_mb: 20480            # 20 GB max cache size
  ttl_seconds: 7200             # 2 hours
  metadata_cache_capacity: 5000

# Security and Agent Guard settings
security:
  agent_guard_mode: enforce

# Resilience and retry policies
retry:
  max_attempts: 5
  base_delay_ms: 1000
  max_delay_ms: 30000

# Model Context Protocol (MCP) sidecar configuration
mcp:
  enabled: false
  port: 8001
  use_firecracker: true

# Telemetry and observability exporter
telemetry:
  enabled: true
  endpoint: "http://otel-collector.internal:4317"
```

---

## Parameter Reference

### Root Parameters

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `environment` | `string` | `development` | Deployment environment for Strake. Set to `production` to enforce strict verification, file checks, and secure defaults. |

---

### `server` Settings

Configures the primary gRPC API, health checks, catalog identification, and storage.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.name` | `string` | `Strake Server` | Human-readable name for this server instance. |
| `server.listen_addr` | `string` | `0.0.0.0:50051` | Network address to listen on for the gRPC API. |
| `server.health_addr` | `string` | `0.0.0.0:8080` | Address for health check endpoints and Prometheus metrics. |
| `server.catalog` | `string` | `strake` | Default catalog name registered in DataFusion. |
| `server.api_url` | `string` | `http://localhost:8080/api/v1` | Public API URL of this Strake instance. |
| `server.global_connection_budget` | `integer` | `100` | Maximum number of concurrent connections allowed across all federated sources. |
| `server.database_url` | `string` | `""` | Connection URL for the PostgreSQL metadata database (required for GitOps & multi-node deployments). |
| `server.datafusion_config` | `map[string]string` | `{}` | Key-value pairs to pass raw configuration overrides directly to the underlying Apache DataFusion SessionContext. |

#### `server.tls` Settings
| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.tls.enabled` | `boolean` | `false` | Enable TLS encryption on the gRPC listener. |
| `server.tls.cert` | `string` | `""` | Path to the TLS certificate chain file (PEM format). |
| `server.tls.key` | `string` | `""` | Path to the TLS private key file. |

#### `server.auth` Settings
| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.auth.enabled` | `boolean` | `false` | Enable API Key authentication. If `true`, a non-empty API key is strictly required. |
| `server.auth.api_key` | `string` | `""` | The secret API key required to access the server. Hardened via memory redaction. |
| `server.auth.cache_ttl_secs` | `integer` | `300` | Duration (in seconds) that authenticated keys/tokens are cached. |
| `server.auth.cache_max_capacity` | `integer` | `10000` | Maximum number of records in the auth cache. |

#### `server.oidc` Settings
| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.oidc.issuer_url` | `string` | `""` | Issuer URL of the OpenID Connect identity provider (e.g. Okta, Keycloak). |
| `server.oidc.audience` | `list[string]` | `[]` | Allowed audience values checked against incoming JWTs. |

#### `server.audit` Settings
| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.audit.enabled` | `boolean` | `false` | Enable structured audit logging for administrative and database queries. |
| `server.audit.failure_mode` | `string` | `alert` | Behavior when audit logs fail to write. Set to `alert` to log the failure, or `shutdown` to immediately stop the server for strict compliance. |

---

### `resources` Settings

Controls CPU, memory limits, and the high-performance query optimization engine.

> [!TIP]
> Customizing these settings is the single most effective way to maximize execution speed and control node resource footprint under high analytical workloads.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `resources.memory_limit_mb` | `integer` | `None` | The maximum RAM (in MB) the engine is allowed to consume for analytical queries. Spills memory-intensive operations to disk when exceeded. |
| `resources.spill_dir` | `string` | `None` | Path to a high-speed disk directory used for spilling intermediate query results (e.g., hash joins or sort blocks). |
| `resources.target_partitions` | `integer` | `None` | The partition count for query execution. If omitted, this dynamically scales to match the number of available CPU cores. |
| **`resources.enable_broadcast_join`** | `boolean` | `false` | **(Latest Feature)** Enables high-performance physical broadcast join optimization. Broad-casts small datasets to all worker partitions to eliminate expensive partition shuffling. |
| `resources.enable_push_down_filter` | `boolean` | `false` | Enables filter pushdown to remote sources, executing filtering closer to the source databases to reduce network latency. |
| `resources.enable_single_node_aggregation`| `boolean` | `false` | Optimizes aggregations by running them directly on the feeding partition node where possible. |
| `resources.enable_single_partition_optimizer`| `boolean` | `false` | Optimizes plans by skipping distribution steps for queries processed entirely within a single data partition. |
| `resources.enable_correlated_distinct_pushdown`| `boolean` | `false` | Automatically pushes down `DISTINCT` filters inside correlated subqueries to remote sources. |

---

### `query_limits` Settings

Enforces protective guardrails on incoming queries to prevent Denial of Service (DoS) and out-of-memory errors.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `query_limits.max_output_rows` | `integer` | `None` | Rejects any query estimated to return more than this number of rows. |
| `query_limits.max_scan_bytes` | `integer` | `None` | Maximum bytes allowed to be scanned from external data sources per query. |
| `query_limits.default_limit` | `integer` | `1000` | Fallback limit appended to SQL queries if no explicit `LIMIT` clause is specified. |
| `query_limits.query_timeout_seconds` | `integer` | `None` | Hard timeout (in seconds) after which query execution is cancelled. |

---

### `security` Settings

Controls query execution safety verification layers.


| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `security.agent_guard_mode` | `string` | `dry_run` | Prompt injection guard mode. Scans for injection keywords. Options:<br>• `disabled`: No guard checks.<br>• `dry_run`: Log warnings on detection but allow the query to complete.<br>• `enforce`: Abort execution and return a `PromptInjectionDetected` error if prompt-injection keywords are found. |

---

### `retry` Settings

Governs retry policies when communicating with external databases or APIs.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `retry.max_attempts` | `integer` | `5` | Maximum number of retry attempts before giving up. |
| `retry.base_delay_ms` | `integer` | `1000` | Initial exponential backoff delay in milliseconds. |
| `retry.max_delay_ms` | `integer` | `60000` | Upper limit on backoff delays in milliseconds. |

---

### `cache` Settings

Manages the global analytical result cache and metadata cache capacity.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `cache.enabled` | `boolean` | `false` | Enable global query result caching. |
| `cache.directory` | `string` | `/tmp/strake-cache` | Local filesystem directory for storing cached Parquet execution batches. |
| `cache.max_size_mb` | `integer` | `10240` | Maximum size of the cache folder on disk in MB (Default is 10 GB). |
| `cache.ttl_seconds` | `integer` | `3600` | Lifetime (TTL) of cached results in seconds (Default is 1 hour). |
| `cache.metadata_cache_capacity` | `integer` | `1000` | Max count of Parquet and database schema metadata objects cached in memory. |

---

### `mcp` Settings

Manages the Model Context Protocol (MCP) sidecar process configuration.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `mcp.environment` | `string` | `development` | Environment mode for the MCP sidecar (`development` or `production`). |
| `mcp.enabled` | `boolean` | `false` | Enable spawning of the MCP sidecar process. |
| `mcp.port` | `integer` | `8001` | TCP port for the MCP sidecar listener. |
| `mcp.max_retries` | `integer` | `5` | Retry attempts for sidecar IPC connectivity. |
| `mcp.retry_delay_ms` | `integer` | `1000` | Retry delay between connection attempts. |
| `mcp.startup_delay_ms` | `integer` | `500` | Milliseconds allowed for the sidecar process to initialize. |
| `mcp.shutdown_timeout_ms` | `integer` | `5000` | Grace period in milliseconds for clean sidecar shutdown. |
| `mcp.python_bin` | `string` | `None` | Path to python executable used to invoke the MCP server. |
| `mcp.health_check_interval_ms`| `integer` | `10000` | Milliseconds between automated sidecar health checks. |
| `mcp.max_output_rows` | `integer` | `1000` | Maximum rows returned in a single MCP interaction block. |
| `mcp.cooldown_secs` | `integer` | `30` | Cool-down period in seconds after sidecar process crash before attempting reboot. |
| `mcp.health_check_url` | `string` | `None` | Endpoint for sidecar HTTP ping checks. |
| `mcp.use_firecracker` | `boolean` | `false` | Enable Firecracker microVM execution for sidecar sandboxing (Enterprise). |

---

### `telemetry` Settings

Controls the exported observability footprint.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `telemetry.enabled` | `boolean` | `false` | Enable telemetry tracing and metrics reporting. |
| `telemetry.endpoint` | `string` | `http://localhost:4317` | OTLP collector address for traces/metrics. |
| `telemetry.service_name` | `string` | `Strake Server` | Service identifier reported to OpenTelemetry. |
