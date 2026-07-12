# CLI API Key Management Commands

These commands manage database-backed access tokens (API keys) for securing the Strake server.

## `apikey create`

<div class="api-signature">
<code>strake-cli apikey create --name &lt;name&gt; --user &lt;user&gt;</code>
<span class="type">command</span>
</div>

Generate a new cryptographically secure API key, hash it with Argon2id, and register it in the metadata store. The full secret key is output exactly once.

**Options:**

`--name` : `str`
:   **Required**. A human-readable name to easily identify the key later.

`--user` : `str`
:   **Required**. The user ID or actor name associated with the key.

`--description` : `str`, *optional*
:   An optional description of the key's purpose.

`--permissions` : `str`, *default: read*
:   Comma-separated list of permissions (e.g., `read,write`).

---

## `apikey list`

<div class="api-signature">
<code>strake-cli apikey list</code>
<span class="type">command</span>
</div>

List all registered API keys in the store, ordered by creation time descending. Truncates long fields for alignment in human-readable mode.

---

## `apikey revoke`

<div class="api-signature">
<code>strake-cli apikey revoke &lt;prefix&gt;</code>
<span class="type">command</span>
</div>

Revoke an active API key using its unique 8-character prefix. In JSON/YAML mode, returns a structured object indicating revocation success.

**Options:**

`prefix` : `str`
:   **Required**. The 8-character prefix of the API key to revoke.
