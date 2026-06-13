# gRPC Connector

Strake supports querying typed microservices directly over gRPC. It dynamically encodes JSON request payloads into binary Protobuf payloads, sends the request over dynamic HTTP/2 channels, and decodes the stream directly into Arrow `RecordBatch` formats.

---

## 1. Schema Discovery Modes

To map binary Protobuf data to Arrow records, Strake requires schema definitions. It supports two modes:

### Active gRPC Reflection
Strake queries the active server reflection endpoint to discover service types and structure.

### Serialized FileDescriptorSet
To avoid runtime reflection overhead or support offline catalog building, you can provide a path to a precompiled Protobuf descriptor binary file (`descriptor.bin`):

```bash
# Generate descriptor using protoc
protoc --include_imports --descriptor_set_out=user_service_desc.bin user_service.proto
```

---

## 2. Configuration Parameters

The gRPC connector is registered with `type: grpc`:

| Parameter | Type | Required | Description |
|:---|:---|:---|:---|
| `url` | string | **Yes** | Active gRPC server endpoint (e.g. `http://localhost:50051`). |
| `service` | string | **Yes** | Fully qualified service name (e.g. `package.Service`). |
| `method` | string | **Yes** | RPC method to invoke. |
| `descriptor_set` | string | No | Path to the local precompiled FileDescriptorSet binary file. |
| `request_body` | string | No | JSON string body to serve as the default RPC request payload. |

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a gRPC microservice connector:

```yaml
sources:
  - name: user_service_grpc
    type: grpc
    url: "http://user-service.internal:50051"
    service: "my.company.UserService"
    method: "GetActiveUsers"
    descriptor_set: "/workspaces/rust-postgres/data/user_service_desc.bin"
    request_body: '{"status": "ACTIVE"}'
    columns:
      - name: "user_id"
        type: "Int64"
      - name: "email"
        type: "Utf8"
```
