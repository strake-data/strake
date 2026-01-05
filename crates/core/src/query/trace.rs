use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use std::time::Instant;

/// Executes a SQL query and returns a detailed report string including the execution plan,
/// pushdown details, and performance metrics.
pub async fn execute_and_report(context: &SessionContext, sql: &str) -> Result<String> {
    use std::fmt::Write;
    let mut report = String::new();

    writeln!(report, "\n{}", "=".repeat(80))?;
    writeln!(report, "STRAKE QUERY REPORT")?;
    writeln!(report, "{}", "=".repeat(80))?;
    writeln!(report, "SQL Query:\n{}", sql)?;

    // 1. Get the Execution Plan
    writeln!(report, "\n[1/3] Execution Plan Analysis")?;
    writeln!(report, "{}", "-".repeat(30))?;

    let explain_query = format!("EXPLAIN ANALYZE {}", sql);
    let df_explain = context
        .sql(&explain_query)
        .await
        .context("Failed to create explain plan")?;
    let explain_results = df_explain
        .collect()
        .await
        .context("Failed to collect explain results")?;

    let pretty_explain = datafusion::arrow::util::pretty::pretty_format_batches(&explain_results)
        .context("Failed to format explain results")?
        .to_string();
    writeln!(report, "{}", pretty_explain)?;

    // 2. Execute and measure
    writeln!(report, "\n[2/3] Query Execution (Showing first 10 results)")?;
    writeln!(report, "{}", "-".repeat(30))?;
    let start_time = Instant::now();
    let df = context.sql(sql).await.context("Failed to execute query")?;
    let results = df.collect().await.context("Failed to collect results")?;
    let duration = start_time.elapsed();

    // Create a limited version of results for display
    let display_df = context
        .sql(sql)
        .await
        .context("Failed to create display query")?
        .limit(0, Some(10))
        .context("Failed to apply limit")?;
    let display_results = display_df
        .collect()
        .await
        .context("Failed to collect display results")?;

    let pretty_results = datafusion::arrow::util::pretty::pretty_format_batches(&display_results)
        .context("Failed to format display results")?
        .to_string();
    writeln!(report, "{}", pretty_results)?;

    // 3. Performance Summary
    writeln!(report, "\n[3/3] Performance Summary")?;
    writeln!(report, "{}", "-".repeat(30))?;
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    let total_batches = results.len();

    writeln!(report, "Execution Time:      {:?}", duration)?;
    writeln!(report, "Total Output Rows:   {}", total_rows)?;
    writeln!(report, "Total RecordBatches: {}", total_batches)?;

    writeln!(report, "{}\n", "=".repeat(80))?;

    Ok(report)
}
