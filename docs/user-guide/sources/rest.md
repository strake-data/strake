# REST APIs & SaaS Connector

Strake allows you to map any HTTP/JSON REST endpoint directly into a logical virtual table using declarative configuration. It supports authentication, various pagination schemes, and query filter pushdown.

---

## 1. Authentication Configuration

Under the `auth` block of your REST config, you can specify one of four authentication protocols:

### HTTP Basic Auth
```yaml
auth:
  type: basic
  username: "api_key"
  password: "optional_password"
```

### HTTP Bearer Token
```yaml
auth:
  type: bearer
  token: "sk_test_..."
```

### OAuth 2.0 Client Credentials Flow
Dynamically refreshes tokens against an authorization server:
```yaml
auth:
  type: oauth_client_credentials
  client_id: "my-client-id"
  client_secret: "my-client-secret"
  token_url: "https://auth.example.com/oauth2/token"
```

### JWT Self-Signed Assertion
For enterprise SaaS accounts (Google, GitHub Apps):
```yaml
auth:
  type: jwt_assertion
  issuer: "service-account@iam.gserviceaccount.com"
  audience: "https://oauth2.googleapis.com/token"
  private_key_pem: "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADAN..."
```

---

## 2. Pagination Models

Strake automatically iterates through API pages using the following `pagination` configs:

### Link Header (`type: header`)
Reads standard `Link` header or custom response headers.
```yaml
pagination:
  type: header
  header_name: "Link"
```

### JSON-Pointer URL (`type: bodyurl`)
Reads next page URL from response JSON body path.
```yaml
pagination:
  type: bodyurl
  path: "meta.next_url"
```

### Continuation Token (`type: token`)
Extracts a token and injects it into query parameters of the next page request.
```yaml
pagination:
  type: token
  token_path: "next_page_token"
  param_name: "starting_after"
```

### Index Offset / Limit (`type: indices`)
Calculates offsets mathematically page by page.
```yaml
pagination:
  type: indices
  param_offset: "offset"
  param_limit: "limit"
  limit: 100
```

---

## 3. Query Filter Pushdown

Map standard SQL filters (e.g. `customer_id = 'cus_123'`) to REST API query arguments:

```yaml
pushdown:
  - column: "customer_id"
    operator: "="
    param: "customer"
```

---

## 3. Configuration Snippet

Add the following block to your `sources.yaml` to register a REST API connector:

```yaml
sources:
  - name: stripe_api
    type: rest
    url: "https://api.stripe.com/v1/charges"
    method: "GET"
    headers:
      Accept: "application/json"
    auth:
      type: bearer
      token: "sk_test_51Nz..."
    pagination:
      type: token
      token_path: "next_page_token"
      param_name: "starting_after"
    pushdown:
      - column: "customer_id"
        operator: "="
        param: "customer"
```
