# Enterprise Edition

Strake Enterprise provides the security, governance, and performance features required for production-scale data meshes in highly regulated environments.

---

## Security & Identity

### Single Sign-On (SSO)

Strake Enterprise integrates with your existing identity provider via **OpenID Connect (OIDC)**. Support includes:

*   **Azure AD (Entra ID)**
*   **Okta**
*   **Auth0**
*   **Keycloak**

### Enterprise API Keys

Advanced API key management with lifecycle controls, rotation policies, and granular scoping for shared service accounts.

---

## Data Governance

### Row-Level Security (RLS)

Define fine-grained access policies directly in your `sources.yaml`. Strake Enterprise uses a **high-performance policy rewriter** to automatically inject filters into your queries based on the authenticated user's context.

> [!NOTE]
> Policies are applied at the plan level, ensuring zero performance overhead for simple filters.

### Column-Level Masking

Dynamically mask sensitive data (PII, PHI, PCI) without altering the source data. Masking rules can be conditioned on user roles or group membership.

### Data Contracts

Enforce data quality and schema consistency across your data mesh.

*   **Contract Validation**: Ensure upstream sources adhere to agreed-upon schemas (types, nullability).
*   **Semantic Constraints**: Inject native filters (`BETWEEN`, `REGEX`, `IN`) at the plan level to enforce business logic and data quality.
*   **Strict Mode**: Optionally block any queries that attempt to access columns not explicitly defined in the contract.

---

## Advanced Connectivity

### Microsoft Excel (XLSX)

The Enterprise Edition includes a high-performance **Calamite-based Excel connector**, allowing you to query spreadsheets in S3 or local storage as if they were SQL tables.

---

## Performance & Scaling

### Concurrency Control

Advanced queue management to prevent noisy neighbors from saturating the engine. 

*   **Connection Slots**: Limit the number of concurrent queries per user or domain.
*   **Query Buffering**: Intelligently queue requests during peak loads to maintain sub-second latency for priority workloads.

### Background Materialization

Schedule and manage data materialization from external sources into high-performance Parquet caches automatically.

---

## Request a Demo

Interested in Strake Enterprise? [Contact our team](mailto:hello@strakedata.com) for a trial or a technical deep dive.
