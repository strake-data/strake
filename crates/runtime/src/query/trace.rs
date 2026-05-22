use anyhow::{Context, Result};
use datafusion::arrow::array::StringArray;
use datafusion::prelude::SessionContext;
use std::time::Instant;

/// Condenses verbose file_groups entries in physical plans to prevent line wrapping/truncation issues.
fn clean_file_groups(s: &str) -> String {
    let mut result = String::new();
    let mut last_idx = 0;

    while let Some(start_idx) = s[last_idx..].find("file_groups={") {
        let abs_start = last_idx + start_idx;
        result.push_str(&s[last_idx..abs_start]);

        // Find the matching closing "}"
        let mut depth = 0;
        let mut end_idx = None;
        for (i, c) in s[abs_start..].char_indices() {
            if c == '{' {
                depth += 1;
            } else if c == '}' {
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(abs_start + i);
                    break;
                }
            }
        }

        if let Some(abs_end) = end_idx {
            let inner = &s[abs_start + "file_groups={".len()..abs_end];

            // Scan the inner part for file paths.
            let mut files = Vec::new();
            let mut current_file = String::new();

            for c in inner.chars() {
                if c == '[' || c == ']' || c == ',' || c == ' ' || c == '{' || c == '}' {
                    if !current_file.is_empty() {
                        if current_file.contains('.') || current_file.contains('/') {
                            files.push(current_file.clone());
                        }
                        current_file.clear();
                    }
                } else {
                    current_file.push(c);
                }
            }
            if !current_file.is_empty()
                && (current_file.contains('.') || current_file.contains('/'))
            {
                files.push(current_file.clone());
            }

            if !files.is_empty() {
                let file_count = files.len();
                let first = &files[0];
                if file_count > 1 {
                    let display_path = if first.starts_with("gs://") || first.starts_with("s3://") {
                        if let Some(last_slash) = first.rfind('/') {
                            let proto_len = first.find("://").unwrap_or(0) + 3;
                            format!("{}.../{}", &first[..proto_len], &first[last_slash + 1..])
                        } else {
                            first.clone()
                        }
                    } else if let Some(last_slash) = first.rfind('/') {
                        first[last_slash + 1..].to_string()
                    } else {
                        first.clone()
                    };
                    result.push_str(&format!(
                        "file_groups={{{} files, e.g. {}}}",
                        file_count, display_path
                    ));
                } else {
                    result.push_str(&format!("file_groups={{{}}}", first));
                }
            } else {
                result.push_str(&format!("file_groups={{{}}}", inner));
            }

            last_idx = abs_end + 1;
        } else {
            result.push_str("file_groups={");
            last_idx = abs_start + "file_groups={".len();
        }
    }
    result.push_str(&s[last_idx..]);
    result
}

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

    for batch in &explain_results {
        let plan_type_col = batch
            .column_by_name("plan_type")
            .context("plan_type column not found")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("plan_type is not a StringArray")?;
        let plan_col = batch
            .column_by_name("plan")
            .context("plan column not found")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("plan is not a StringArray")?;

        for i in 0..batch.num_rows() {
            let plan_type = plan_type_col.value(i);
            let plan = plan_col.value(i);
            writeln!(report, "Plan [{}]:", plan_type)?;
            let cleaned_plan = clean_file_groups(plan);
            writeln!(report, "{}", cleaned_plan)?;
        }
    }

    // 2. Execute and measure
    writeln!(report, "\n[2/3] Query Execution (Showing first 10 results)")?;
    writeln!(report, "{}", "-".repeat(30))?;
    let start_time = Instant::now();
    let df = context.sql(sql).await.context("Failed to execute query")?;
    let results = df.collect().await.context("Failed to collect results")?;
    let duration = start_time.elapsed();

    // Create a limited version of results for display without re-executing
    let display_results = if results.is_empty() {
        vec![]
    } else {
        // Zero-copy slice of the first batch up to 10 rows
        // If results has multiple batches, we just take the first one and slice it
        let batch = &results[0];
        let take_rows = batch.num_rows().min(10);
        vec![batch.slice(0, take_rows)]
    };

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_file_groups_single() {
        let input = "DataSourceExec: file_groups={1 group: [[tpch/sf50/region.parquet]]}, projection=[r_regionkey]";
        let expected =
            "DataSourceExec: file_groups={tpch/sf50/region.parquet}, projection=[r_regionkey]";
        assert_eq!(clean_file_groups(input), expected);
    }

    #[test]
    fn test_clean_file_groups_multiple_local() {
        let input = "DataSourceExec: file_groups={2 groups: [[a.parquet], [b.parquet]]}, projection=[r_regionkey]";
        let expected =
            "DataSourceExec: file_groups={2 files, e.g. a.parquet}, projection=[r_regionkey]";
        assert_eq!(clean_file_groups(input), expected);
    }

    #[test]
    fn test_clean_file_groups_multiple_remote() {
        let input = "DataSourceExec: file_groups={8 groups: [[gs://bucket/dir/part-0.parquet], [gs://bucket/dir/part-1.parquet]]}, projection=[r_regionkey]";
        let expected = "DataSourceExec: file_groups={2 files, e.g. gs://.../part-0.parquet}, projection=[r_regionkey]";
        assert_eq!(clean_file_groups(input), expected);
    }

    #[test]
    fn test_clean_file_groups_no_match() {
        let input = "SortExec: expr=[revenue@1 DESC]";
        assert_eq!(clean_file_groups(input), input);
    }
}
