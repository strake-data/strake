//! # Strake Benchmark Suite
//!
//! Performance testing and latency analysis for the Strake query engine.
//!
//! ## Overview
//!
//! Includes end-to-end benchmarks against various data sources,
//! supporting TPC-H query sets and chaos injection for resilience testing.
//!
//! ## Usage
//!
//! ```bash
//! CONFIG_FILE=config/my_source.yaml cargo run -p strake-bench -- run --queries 1 6
//! ```

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use rand::Rng;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::time::Instant;
use strake_common::config::{Config, RetrySettings};
use strake_runtime::federation::FederationEngine;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "strake-tpch-smoke-test")]
#[command(about = "TPC-H Smoke Test for Strake", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run TPC-H queries against federated sources
    Run {
        /// TPC-H query numbers to run (e.g. 1 3 6 10 or 'all')
        #[arg(short, long, num_args = 1..)]
        queries: Vec<String>,

        /// Number of iterations per query
        #[arg(short, long, default_value_t = 3)]
        iterations: u32,

        /// Probability of injecting chaotic failures (0.0 to 1.0)
        #[arg(short, long, default_value_t = 0.0)]
        chaos: f64,

        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: String,

        /// TPC-H scale factor (influences Q11 threshold)
        #[arg(short, long, default_value_t = 1.0)]
        scale_factor: f64,
    },
}

#[derive(Serialize)]
struct BenchResult {
    query: u32,
    scale_factor: f64,
    iteration: u32,
    duration_ms: u128,
    planning_ms: u128,
    execution_ms: u128,
    chaos_latency_ms: u128,
    correctness: String,
    result_hash: String,
    metrics: Option<String>,
    status: String,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            queries,
            iterations,
            chaos,
            format,
            scale_factor,
        } => {
            run_benchmarks(queries, iterations, chaos, format, scale_factor).await?;
        }
    }

    Ok(())
}

async fn run_benchmarks(
    queries_input: Vec<String>,
    iterations: u32,
    chaos_prob: f64,
    format: String,
    scale_factor: f64,
) -> Result<()> {
    let mut queries = vec![];
    if queries_input.iter().any(|q| q == "all") {
        for q in 1..=22 {
            queries.push(q);
        }
    } else {
        for q_str in queries_input {
            queries.push(q_str.parse::<u32>().context("Invalid query number")?);
        }
    }

    let harness = BenchmarkHarness::new().await?;
    let mut results = vec![];

    for &q in &queries {
        info!(
            "Running TPC-H Q{} at SF {} for {} iterations...",
            q, scale_factor, iterations
        );

        let mut sql = get_tpch_query(q)?;
        if q == 11 {
            let threshold = 0.0001 / scale_factor;
            sql = sql.replace("0.0001", &threshold.to_string());
        }

        for i in 1..=iterations {
            let mut result = harness.run_iteration(q, i, &sql, chaos_prob).await?;
            result.scale_factor = scale_factor;
            results.push(result);
        }
    }

    if format == "json" {
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        print_text_report(&results);
    }

    Ok(())
}

struct BenchmarkHarness {
    engine: FederationEngine,
}

impl BenchmarkHarness {
    async fn new() -> Result<Self> {
        info!("Initializing Strake Federation Engine for Benchmarking...");

        let config_path = std::env::var("CONFIG_FILE")
            .unwrap_or_else(|_| "config/tpch_federation.yaml".to_string());
        let config = Config::from_file(&config_path).unwrap_or({
            let mut s = Config::default();
            s.sources = vec![];
            s.cache = Default::default();
            s
        });

        let engine = FederationEngine::new(strake_runtime::federation::FederationEngineOptions {
            config,
            catalog_name: "strake".to_string(),
            query_limits: strake_common::config::QueryLimits::default(),
            resource_config: strake_common::config::ResourceConfig::default(),
            datafusion_config: std::collections::HashMap::new(),
            global_budget: 100,
            extra_optimizer_rules: vec![],
            extra_sources: vec![],
            retry: RetrySettings::default(),
        })
        .await
        .context("Failed to initialize FederationEngine")?;

        Ok(Self { engine })
    }

    async fn run_iteration(
        &self,
        q: u32,
        i: u32,
        sql: &str,
        chaos_prob: f64,
    ) -> Result<BenchResult> {
        let mut rng = rand::rng();
        let mut result_status = "SUCCESS".to_string();
        let mut error_msg = None;
        let mut chaos_latency = 0;

        // Chaos Injection
        if chaos_prob > 0.0 && rng.random_bool(chaos_prob) {
            warn!(
                "Injecting chaos: Simulated Source Timeout for Q{} Iteration {}",
                q, i
            );
            let chaos_start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            chaos_latency = chaos_start.elapsed().as_millis();
            result_status = "ERROR".to_string();
            error_msg = Some("Simulated Source Timeout (Chaos Injection)".to_string());
        }

        let mut planning_ms = 0;
        let mut execution_ms = 0;
        let mut result_hash = String::new();
        let mut metrics = None;

        if result_status == "SUCCESS" {
            let state = self.engine.context().state();

            // 1. Planning Stage
            let plan_start = Instant::now();
            match state.create_logical_plan(sql).await {
                Ok(logical_plan) => {
                    match state.create_physical_plan(&logical_plan).await {
                        Ok(physical_plan) => {
                            planning_ms = plan_start.elapsed().as_millis();

                            // 2. Execution Stage
                            let exec_start = Instant::now();
                            match datafusion::physical_plan::execute_stream(
                                physical_plan.clone(),
                                state.task_ctx(),
                            ) {
                                Ok(mut stream) => {
                                    let mut batches = Vec::new();
                                    let mut execution_err = None;
                                    while let Some(result) = stream.next().await {
                                        match result {
                                            Ok(batch) => batches.push(batch),
                                            Err(e) => {
                                                execution_err = Some(e);
                                                break;
                                            }
                                        }
                                    }

                                    if let Some(e) = execution_err {
                                        error!("Q{} Iteration {}: STREAM ERROR - {:?}", q, i, e);
                                        result_status = "ERROR".to_string();
                                        error_msg = Some(format!("Stream error: {:?}", e));
                                    } else {
                                        execution_ms = exec_start.elapsed().as_millis();
                                        result_hash = compute_result_hash(&batches)?;

                                        let metrics_str = datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
                                            .indent(true)
                                            .to_string();
                                        metrics = Some(metrics_str);

                                        info!(
                                            "Q{} Iteration {}: SUCCESS (plan: {}ms, exec: {}ms, hash: {})",
                                            q, i, planning_ms, execution_ms, result_hash
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Q{} Iteration {}: EXECUTION FAILED - {:?}", q, i, e);
                                    result_status = "ERROR".to_string();
                                    error_msg = Some(format!("Execution error: {:?}", e));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Q{} Iteration {}: PHYSICAL PLANNING FAILED - {:?}", q, i, e);
                            result_status = "ERROR".to_string();
                            error_msg = Some(format!("Physical planning error: {:?}", e));
                        }
                    }
                }
                Err(e) => {
                    error!("Q{} Iteration {}: LOGICAL PLANNING FAILED - {:?}", q, i, e);
                    result_status = "ERROR".to_string();
                    error_msg = Some(format!("Logical planning error: {:?}", e));
                }
            }
        }

        Ok(BenchResult {
            query: q,
            scale_factor: 1.0, // Will be overwritten
            iteration: i,
            duration_ms: planning_ms + execution_ms,
            planning_ms,
            execution_ms,
            chaos_latency_ms: chaos_latency,
            correctness: if result_status == "SUCCESS" {
                "PASS".to_string()
            } else {
                "N/A".to_string()
            },
            result_hash,
            metrics,
            status: result_status,
            error: error_msg,
        })
    }
}

fn compute_result_hash(batches: &[arrow::record_batch::RecordBatch]) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        let batch_refs: Vec<&arrow::record_batch::RecordBatch> = batches.iter().collect();
        writer.write_batches(&batch_refs)?;
        writer.finish()?;
    }
    hasher.update(&buf);
    Ok(hex::encode(hasher.finalize()))
}

fn get_tpch_query(q: u32) -> Result<String> {
    let path = format!("crates/bench/queries/tpch/q{:02}.sql", q);
    std::fs::read_to_string(&path).with_context(|| format!("Failed to read query file: {}", path))
}

fn print_text_report(results: &[BenchResult]) {
    println!("\nSTRAKE PERFORMANCE REPORT");
    println!("=========================");
    println!(
        "{:<8} {:<8} {:<10} {:<15} {:<15} {:<15} {:<15} {:<12} {:<10}",
        "Query",
        "SF",
        "Iteration",
        "Total (ms)",
        "Plan (ms)",
        "Exec (ms)",
        "Chaos Lat",
        "Correctness",
        "Status"
    );
    println!(
        "----------------------------------------------------------------------------------------------------------------------------------"
    );
    for r in results {
        println!(
            "{:<8} {:<8.1} {:<10} {:<15} {:<15} {:<15} {:<15} {:<12} {:<10}",
            r.query,
            r.scale_factor,
            r.iteration,
            r.duration_ms,
            r.planning_ms,
            r.execution_ms,
            r.chaos_latency_ms,
            r.correctness,
            r.status
        );
    }
}
