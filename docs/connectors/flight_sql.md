# Arrow Flight SQL Connector

Strake supports high-performance querying over **Arrow Flight SQL** endpoints, allowing low-overhead, zero-copy vectorized data transfers from remote warehouses (like Snowflake or Dremio) or another Strake instance.

---

## 1. Connection Syntax

The Arrow Flight SQL connection is registered with `type: flight_sql`. The `url` field specifies the remote gRPC/Flight server address:

```yaml
url: "grpc://<host>:<port>"
```

### Connection Example
```yaml
url: "grpc://localhost:32010"
```

---

## 2. Configuration Parameters

The Arrow Flight SQL configuration is simple and concise:

| Parameter | Type | Required | Description |
|:---|:---|:---|:---|
| `url` | string | **Yes** | Flight SQL server endpoint (e.g. `grpc://localhost:32010`). |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a Flight SQL source:

```yaml
sources:
  - name: warehouse_flight
    type: flight_sql
    url: "grpc://localhost:32010"
```
