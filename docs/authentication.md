# Authentication

Strake provides built-in authentication mechanisms to secure your data federation server. You can choose between basic API Key authentication (OSS) or advanced Single Sign-On via OIDC (Enterprise).

## API Key Authentication (OSS)

The Open Source version of Strake uses database-backed API keys hashed with Argon2. This allows you to manage multiple users and system actors with fine-grained permissions.

### 1. Enable Authentication

In your `strake.yaml` or via environment variables, enable the authentication layer and provide a connection to a metadata database (PostgreSQL):

```yaml
server:
  auth:
    enabled: true
    cache_ttl_secs: 3600    # Cache duration for verified tokens
    cache_max_capacity: 1000
  database_url: "postgres://user:password@localhost:5432/strake_metadata"
```

### 2. Setup the Database

Strake expects an `api_keys` table in the metadata database. Execute the following SQL to create the required schema:

```sql
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_prefix VARCHAR(8) NOT NULL,
    key_hash VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    permissions TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used_at TIMESTAMP WITH TIME ZONE,
    revoked_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix) WHERE revoked_at IS NULL;
```

### 3. Generate and Manage Keys

Keys are not stored in plaintext. To create a new key:
1. Generate a secure random string (at least 32 characters).
2. Take the first 8 characters as the `key_prefix`.
3. Generate an Argon2 hash of the full key string.
4. Insert the prefix, hash, and desired permissions into the `api_keys` table.

### 4. Client Usage

Clients must provide the key in the `Authorization` header using the `Bearer` scheme.

**Using curl:**
```bash
curl -H "Authorization: Bearer <YOUR_API_KEY>" http://localhost:8080/api/v1/health
```

**Using Strake CLI:**
```bash
export STRAKE_API_KEY="<YOUR_API_KEY>"
strake query "SELECT * FROM sales_data"
```

---

## OIDC / SSO (Enterprise)

Strake Enterprise supports OpenID Connect (OIDC) for integration with identity providers like Okta, Auth0, or Azure AD.

### Configuration

Add the OIDC configuration to your `strake.yaml`:

```yaml
server:
  oidc:
    issuer_url: "https://your-provider.com/"
    audience: ["strake-api"]
```

Enterprise features like **Row-Level Security (RLS)** and **Column Masking** are automatically applied based on the claims (roles/groups) present in the OIDC token.

For more details, see the [Enterprise Documentation](./enterprise.md).
