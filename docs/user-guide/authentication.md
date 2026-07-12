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

Use the `strake-cli` command to initialize the metadata database and create the required tables:

```bash
strake-cli db init
```

### 3. Generate and Manage Keys

Keys are not stored in plaintext. To generate a new API key:

```bash
strake-cli apikey create --name "my-app-key" --user "user-123" --permissions "read"
```

This will generate a cryptographically secure key, compute its Argon2 hash, and store it in the database. The raw key (starting with `strk_`) will be displayed exactly once.

To list or revoke keys:

```bash
# List key prefixes and active status
strake-cli apikey list

# Revoke a key using its 8-character prefix
strake-cli apikey revoke <prefix>
```

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

For more details, see the [Enterprise Documentation](../advanced/enterprise.md).
