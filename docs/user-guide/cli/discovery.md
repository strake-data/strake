# CLI Discovery & Inspection Commands

These commands allow you to inspect upstream sources, add/remove tables dynamically, and describe active schemas.

## `search`

<div class="api-signature">
<code>strake-cli search &lt;source&gt; [file]</code>
<span class="type">command</span>
</div>

Search for tables and schemas in an upstream source. If the source is configured in the local configuration file (e.g., Postgres, MySQL, ClickHouse, SQLite, DuckDB, or Oracle), `search` connects directly to the database using the local introspector. Otherwise, it queries the remote Strake API.

**Options:**

`source` : `str`
:   The name of the source to search.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file (used to find source connection details).

`--domain` : `str`, *optional*
:   Narrow the search scope to a specific domain.

---

## `add`

<div class="api-signature">
<code>strake-cli add &lt;source&gt; [table] [file]</code>
<span class="type">command</span>
</div>

Automatically adds one or more discovered tables into your `sources.yaml`.

**Options:**

`source` : `str`
:   The name of the source.

`table` : `str`, *optional*
:   The full name of the table to add (e.g., `schema.table`). Required unless `--all`, `--pattern`, or `--stdin` is used.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to update.

`--full` : `bool`, *default: false*
:   Perform deep introspection of the source (e.g., fetching constraints and native types).

`--ai-descriptions` : `bool`, *default: false*
:   Connect to the configured AI provider to generate natural language descriptions for the table and its columns. See the [AI-Powered Metadata Enrichment Guide](../metadata-enrichment.md) for details.

`--merge` : `bool`, *default: true*
:   Fill in missing fields from introspection but preserve any existing manual edits in `sources.yaml`.

`--overwrite` : `bool`, *default: false*
:   Completely replace the existing table entry with fresh introspection results. If both `--merge` and `--overwrite` are provided, the last one wins.

`--pattern` : `str`, *optional*
:   Bulk add: Add all tables matching a glob pattern (e.g., `public.*`).

`--all` : `bool`, *default: false*
:   Bulk add: Add every table discoverable from the source.

`--stdin` : `bool`, *default: false*
:   Bulk add: Read a list of schema-qualified table names from standard input.

`--yes` : `bool`, *default: false*
:   Skip confirmation prompts for destructive changes or large bulk operations.

`--dry-run` : `bool`, *default: false*
:   Show the changes that would be made without persisting them.

`--to-contracts` : `bool`, *default: false*
:   Automatically promote the introspected schema to `contracts.yaml`.

---

## `remove`

<div class="api-signature">
<code>strake-cli remove &lt;source&gt; [table] [file]</code>
<span class="type">command</span>
</div>

Safely remove a table or source entry from `sources.yaml`. Checks for orphaned contract or policy references.

**Options:**

`source` : `str`
:   The name of the source.

`table` : `str`, *optional*
:   The name of the table to remove. Required unless `--source-only` is used.

`file` : `str`, *default: sources.yaml*
:   Path to the configuration file to update.

`--dry-run` : `bool`, *default: false*
:   Perform checks without writing changes to the file.

`--force` : `bool`, *default: false*
:   Proceed with removal even if orphaned contract or policy references are detected.

`--source-only` : `bool`, *default: false*
:   Remove the entire source entry (currently a stub).

---

## `describe`

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
