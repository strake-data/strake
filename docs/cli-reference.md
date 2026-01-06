# CLI Reference

The `strake-cli` is the primary tool for managing your Strake configuration, validating metadata, and performing GitOps-style deployments.

## Project Lifecycle

---

### `init`

<div class="api-signature">
<code>strake-cli init</code>
<span class="type">command</span>
</div>

Initialize a new Strake project with a template configuration.

**Options:**

`--template` : `sql | rest | file | grpc`, *optional*
:   The type of project template to generate as a starting point.

---

## GitOps & Deployment

---

### `validate`

<div class="api-signature">
<code>strake-cli validate [file]</code>
<span class="type">command</span>
</div>

Check your configuration for syntax and connectivity errors without applying changes.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to validate.

`--offline` : `bool`, *default: false*
:   Skip network connectivity checks and perform only local schema validation.

---

### `diff`

<div class="api-signature">
<code>strake-cli diff [file]</code>
<span class="type">command</span>
</div>

Preview the differences between your local configuration and the live metadata store.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to compare.

---

### `apply`

<div class="api-signature">
<code>strake-cli apply [file]</code>
<span class="type">command</span>
</div>

Deploy your local configuration to the metadata store.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to deploy.

`--force` : `bool`, *default: false*
:   Required for potentially destructive actions (e.g., mass-deleting sources).

`--dry-run` : `bool`, *default: false*
:   Preview changes in the registry without actually persisting them.

`--expected-version` : `int`, *optional*
:   Enforces optimistic locking. The operation will fail if the current version in the metadata store does not match this value.

---

## Discovery & Inspection

---

### `search`

<div class="api-signature">
<code>strake-cli search &lt;source&gt; [file]</code>
<span class="type">command</span>
</div>

Search for tables and schemas in an upstream source.

**Options:**

`source` : `str`
:   The name of the source to search.

`--domain` : `str`, *optional*
:   Narrow the search scope to a specific domain.

---

### `add`

<div class="api-signature">
<code>strake-cli add &lt;source&gt; &lt;table&gt; [file]</code>
<span class="type">command</span>
</div>

Automatically adds a discovered table into your `sources.yaml`.

**Options:**

`source` : `str`
:   The name of the source.

`table` : `str`
:   The full name of the table to add (e.g., `schema.table`).

---

### `introspect`

<div class="api-signature">
<code>strake-cli introspect &lt;source&gt; [file]</code>
<span class="type">command</span>
</div>

Describe the physical schema of a specific upstream source.

**Options:**

`--registered` : `bool`, *default: false*
:   If true, introspect a source that is already registered in the metadata store instead of the local config.

---

## Domain Management

---

### `domain list`

<div class="api-signature">
<code>strake-cli domain list</code>
<span class="type">command</span>
</div>

List all registered domains currently tracked in the metadata store.

---

### `domain history`

<div class="api-signature">
<code>strake-cli domain history</code>
<span class="type">command</span>
</div>

Show the audit trail of deployment events for a domain.

**Options:**

`--name` : `str`, *default: default*
:   The name of the domain to inspect.

---

### `domain rollback`

<div class="api-signature">
<code>strake-cli domain rollback --to-version &lt;n&gt;</code>
<span class="type">command</span>
</div>

Revert a domain to a previous known-good version.

**Options:**

`--to-version` : `int`
:   **Required**. The specific target version to revert to.
