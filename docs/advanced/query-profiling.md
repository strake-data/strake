# Query Profiling & Tuning

Strake provides powerful performance introspection tools to help you verify query pushdown, debug network bottlenecks, and inspect physical plans. By using execution tracing and structural analysis, you can ensure that filters and projections are being offloaded directly to your underlying datasources.

---

## 1. Trace vs. Explain

Strake provides two main tools for query analysis: static physical plan inspection (`explain_tree()`) and live execution profiling (`trace()`).

| Feature | `explain_tree()` | `trace()` |
| :--- | :--- | :--- |
| **Action** | Static Structural Analysis | **Live Execution Profiling** |
| **Mode** | Embedded Only | Embedded & Remote |
| **Output** | ASCII Tree (Physical Plan) | Detailed Report (Logical/Physical) |
| **Cost** | Instant (No query cost) | Full execution cost |
| **Best For** | **Pushdown & Join Verification** | **Performance & I/O Debugging** |

---

## 2. Static Analysis with `explain_tree()`

The `explain_tree()` method parses and compiles the query, returning a readable ASCII tree representing the **Physical Plan**. It is highly recommended for validating predicate pushdown before running heavy queries.

> [!NOTE]
> `explain_tree()` requires direct access to the query engine's optimizer and is only available in **Embedded Mode**. For remote client connections, it falls back to the logical `trace()` output.

```python
# Verify if filtering is happening at the source (e.g. S3/Parquet)
print(conn.explain_tree("SELECT * FROM strake.s3_logs.logs WHERE event_date > '2023-01-01'"))
```

### Analysis: Good vs. Bad Plans

When examining the output of `explain_tree()`, look for pushdown indicators:

*   **✅ Good Plan (Pushdown Active)**: The filter is successfully offloaded to the source connector. Only matching rows are transferred over the network.
    ```text
    └─ DataSource (source: parquet) [PUSHED]
       filter: event_date > '2023-01-01' [PUSHED]
    ```

*   **❌ Bad Plan (Local Execution)**: The filter could not be pushed down and must be evaluated in-memory by Strake's execution engine. The engine has to fetch *every single row* over the network first, creating an I/O bottleneck.
    ```text
    └─ Filter: event_date > '2023-01-01' [NOT PUSHED - Executed Locally]
       └─ DataSource (source: parquet) [PUSHED]
    ```

---

## 3. Live Execution Profiling with `trace()`

For deep performance debugging and identifying runtime execution bottlenecks, use the `trace()` method. This runs the query to completion and gathers exact timings for each physical operator.

### Python Example

```python
import strake

conn = strake.StrakeConnection("sources.yaml")

# Run a trace to diagnose query latency
report = conn.trace("""
    SELECT c_name, sum(l_extendedprice) 
    FROM customer 
    JOIN lineitem ON c_custkey = l_orderkey
    WHERE l_shipdate > '1998-01-01'
    GROUP BY c_name
""")

print(report)
```

### Understanding the Trace Report

A trace report contains three distinct sections:

1.  **Execution Plan Analysis:** Uses live `EXPLAIN ANALYZE` metrics to display operator-level timings.
    *   `elapsed_compute`: CPU/execution time spent by Strake on this operator node.
    *   `output_rows`: Exact number of Arrow records emitted to the parent node.
    *   `[PUSHED]` / `[LOCAL-PUSHDOWN]`: Success indicators for filter and projection offloading.
    *   `⚠ Executed Locally`: Alert indicating that a filter failed to push down and is executing in Strake's memory space.
2.  **Query Execution Preview:** Displays a zero-copy preview of the first 10 rows of the output.
3.  **Performance Summary:**
    *   **Execution Time**: Total wall-clock time for the query.
    *   **Total Output Rows**: The final count of records returned to the caller.
    *   **Total RecordBatches**: Total number of Arrow RecordBatches parsed.

---

## 4. Troubleshooting Performance Bottlenecks

### Missing Pushdown
If you notice a `FilterExec` node high up in the physical plan with a high `output_rows` count and a `⚠ Executed Locally` warning, it means Strake is pulling excessive data.
*   **Resolution:** Check that database columns have matching data types inside your `WHERE` conditions (e.g. comparing string fields to string literals) and that the source connector supports the SQL function you are using.

### I/O vs. Compute Latency
If the overall **Execution Time** is significantly higher than the sum of the operator **`elapsed_compute`** values, your query is I/O bound.
*   **Resolution:** Optimize networking, enable **predicate caching** on file connectors, or verify that your relational database is using indexes for pushdown predicates.

---

## 5. Best Practices

*   **Apply LIMIT in Traces:** Because `trace()` executes the entire query, always include a `LIMIT` clause on large datasets to avoid unnecessary resource consumption during testing.
*   **Verify Schema Changes:** Run `explain_tree()` or `trace()` after modifying database schemas or updating connector configurations to ensure pushdowns continue to execute correctly.
