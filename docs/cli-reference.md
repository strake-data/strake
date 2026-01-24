# CLI Reference

The `strake-cli` is the primary tool for managing your Strake configuration, validating metadata, and performing GitOps-style deployments.

## Global Options

These options can be used with any command.

- `--output`, `-o`: `human | json | yaml`. *Default: human*.
  Sets the output format. Machine-readable formats (`json`, `yaml`) are suitable for CI/CD and automation.
- `--token`: `str`. *Default: STRAKE_TOKEN environment variable*.
  API token for authentication with the Strake server.
- `--profile`: `str`. *Default: STRAKE_PROFILE environment variable*.
  Specifies the configuration profile to use from `strake.yaml`.

---

## Project Lifecycle

### `init`

<div class="api-signature">
<code>strake-cli init [file]</code>
<span class="type">command</span>
</div>

Initialize a new Strake project with a template configuration.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path where the new configuration file should be created.

`--template` : `sql | rest | file | grpc`, *optional*
:   The type of project template to generate as a starting point.

`--sources-only` : `bool`, *default: false*
:   Only create `sources.yaml`, skipping `strake.yaml` and `README.md`. Also skips metadata database initialization.

---

## GitOps & Deployment

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
:   Required for potentially destructive actions (e.g., deleting a source).

`--dry-run` : `bool`, *default: false*
:   Runs validation and previews changes (via `diff`) without actually persisting them.

`--expected-version` : `int`, *optional*
:   Enforces optimistic locking. The operation will fail if the current version in the metadata store does not match this value.

---

## Discovery & Inspection

### `search`

<div class="api-signature">
<code>strake-cli search &lt;source&gt; [file]</code>
<span class="type">command</span>
</div>

Search for tables and schemas in an upstream source.

**Options:**

`source` : `str`
:   The name of the source to search.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file (used to find source connection details).

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

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to update.

`--domain` : `str`, *optional*
:   Specify the domain context for the search.

---

### `introspect`

<div class="api-signature">
<code>strake-cli introspect &lt;source&gt; [file]</code>
<span class="type">command</span>
</div>

Legacy alias for `search`.

---

### `test-connection`

<div class="api-signature">
<code>strake-cli test-connection [file]</code>
<span class="type">command</span>
</div>

Read the configuration and attempt to connect to every defined source to verify credentials and network reachability.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to test.

---

### `describe`

<div class="api-signature">
<code>strake-cli describe [file]</code>
<span class="type">command</span>
</div>

Shows the current configuration and metadata stored in the metadata database for a domain.

**Options:**

`file` : `str`, *default: sources.yaml*
:   Path to the local configuration file (used to resolve domain).

`--domain` : `str`, *optional*
:   Explicitly specify the domain to describe.

---

## Domain Management

### `domain list`

<div class="api-signature">
<code>strake-cli domain list</code>
<span class="type">command</span>
</div>

List all registered domains currently tracked in the metadata store.

---

### `domain history`

<div class="api-signature">
<code>strake-cli domain history [name]</code>
<span class="type">command</span>
</div>

Show the audit trail of deployment events for a domain.

**Options:**

`name` : `str`, *default: default*
:   The name of the domain to inspect.

---

### `domain rollback`

<div class="api-signature">
<code>strake-cli domain rollback [name] --to-version &lt;n&gt;</code>
<span class="type">command</span>
</div>

Revert a domain to a previous known-good version.

**Options:**

`name` : `str`, *default: default*
:   The name of the domain to rollback.

`--to-version` : `int`
:   **Required**. The specific target version to revert to.
