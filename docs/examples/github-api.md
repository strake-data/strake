# Example: Querying the GitHub REST API

This example demonstrates how to configure a REST API data source in Strake using the **GitHub API**. It maps active HTTP endpoints to virtual database tables and executes standard SQL queries against them.

The configuration for this example is stored in [github_source.yaml](github_source.yaml).

---

## 1. REST Source Configuration

Here is the declarative YAML configuration to define the GitHub REST API source. Since GitHub requires a `User-Agent` header, we declare it globally along with Accept headers and a Link-header pagination handler.

```yaml
# github_source.yaml
sources:
- name: github
  type: rest
  config:
    base_url: "https://api.github.com"
    headers:
      User-Agent: "strake-data" # GitHub API requires a user-agent header
      Accept: "application/vnd.github.v3+json"
    pagination:
      type: header
      header_name: "link" # GitHub uses the standard Link header for pagination

  # Map specific REST API endpoints to SQL Tables
  tables:
    - name: repos
      path: "/orgs/rust-lang/repos" # Maps to https://api.github.com/orgs/rust-lang/repos
      columns:
        - name: id
          data_type: int
          not_null: true
        - name: name
          data_type: string
        - name: full_name
          data_type: string
        - name: description
          data_type: string
        - name: stargazers_count
          data_type: int
        - name: language
          data_type: string

    - name: issues
      path: "/repos/rust-lang/rust/issues" # Maps to https://api.github.com/repos/rust-lang/rust/issues
      columns:
        - name: id
          data_type: int
        - name: number
          data_type: int
        - name: title
          data_type: string
        - name: state
          data_type: string
```

---

## 2. Python Script to Query the API

Save the following script as `github_query.py` to connect in embedded library mode and run SQL queries across the virtual GitHub tables:

```python
import strake
import pandas as pd

# 1. Initialize Strake connection referencing the github_source.yaml config
conn = strake.StrakeConnection("github_source.yaml")
print("Connected to Strake Embedded Engine.")

# 2. Run standard SQL queries against the remote REST API endpoint
query_repos = """
    SELECT name, stargazers_count, language
    FROM strake.github.public.repos
    WHERE language = 'Rust'
    ORDER BY stargazers_count DESC
    LIMIT 5
"""

print(f"\nExecuting Query:\n{query_repos}")
repos_table = conn.sql(query_repos)

# Convert Arrow RecordBatch stream directly to a Pandas DataFrame
df_repos = repos_table.to_pandas()
print("\nTop Rust Language Repositories on Rust-Lang Org:")
print(df_repos.to_string(index=False))

# 3. Query issues
query_issues = """
    SELECT number, title, state
    FROM strake.github.public.issues
    LIMIT 5
"""

print(f"\nExecuting Query:\n{query_issues}")
issues_table = conn.sql(query_issues)
df_issues = issues_table.to_pandas()
print("\nRecent Open Issues:")
print(df_issues.to_string(index=False))
```

---

## 3. How to Run the Query

Install dependencies and run the script using `uv` (recommended):

```bash
# Add strake to your project
uv add strake

# Run the python script
uv run github_query.py
```
