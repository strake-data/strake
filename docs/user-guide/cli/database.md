# CLI Database Management Commands

These commands manage the lifecycle and schema migrations of the metadata store for database-backed authentication and metadata management.

## `db init`

<div class="api-signature">
<code>strake-cli db init</code>
<span class="type">command</span>
</div>

Initialize a fresh metadata database schema. This creates all necessary system tables and runs initial setup.

---

## `db migrate`

<div class="api-signature">
<code>strake-cli db migrate</code>
<span class="type">command</span>
</div>

Run outstanding database schema migrations to upgrade the metadata store to the latest version.
