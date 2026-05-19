# Apache Iceberg Connector

> [!WARNING]
> **Experimental Connector:**
> Apache Iceberg rest catalog integration in Strake is currently **experimental** and subject to configuration changes in upcoming minor releases.

Strake supports reading Apache Iceberg tables by integrating with an active Iceberg REST Catalog (such as Nessie, AWS Glue, Polaris, or Tabular). It supports partition pruning and snapshot-isolated time travel queries.

---

## 1. Credentials & Configuration Parameters

The Iceberg database is registered with `type: iceberg_rest`. It supports static API tokens or dynamic OAuth 2.0 client credential flows:

| Parameter | Type | Required | Description |
|:---|:---|:---|:---|
| `catalog_uri` | string | **Yes** | REST catalog endpoint (e.g. `http://localhost:8181/v1`). |
| `warehouse` | string | **Yes** | Iceberg warehouse path (e.g. `s3://bucket/warehouse`). |
| `region` | string | **Yes** | Cloud region (e.g. `us-east-1`). |
| `token` | string | No | Static API Token for authentication. |
| `oauth_client_id` | string | No | OAuth2 Client ID for dynamic token refresh. |
| `oauth_client_secret` | string | No | OAuth2 Client Secret for dynamic token refresh. |
| `oauth_token_url` | string | No | OAuth2 Token refresh URL. |

---

## 2. Configuration Snippet

Add the following block to your `sources.yaml` to register an experimental Iceberg REST catalog:

```yaml
sources:
  - name: analytics_iceberg
    type: iceberg_rest
    config:
      catalog_uri: "http://polaris:8181/api/catalog"
      warehouse: "s3://my-iceberg-lakehouse/"
      region: "us-east-1"
      oauth_client_id: "strake-client"
      oauth_client_secret: "super_secret_oauth_token"
      oauth_token_url: "http://polaris:8181/api/catalog/v1/oauth/tokens"
      s3_endpoint: "http://minio:9000"
```
