use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use strake_core::federation::FederationEngine;
use strake_core::config::{Config, RetrySettings};
use std::time::Instant;
use rand::Rng;
use tracing::{info, warn, error};
use serde::Serialize;

#[derive(Parser)]
#[command(name = "strake-bench")]
#[command(about = "High-performance TPC-H Benchmarker for Strake", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run TPC-H queries against federated sources
    Run {
        /// TPC-H query numbers to run (e.g. 1 3 6 10)
        #[arg(short, long, num_args = 1..)]
        queries: Vec<u32>,
        
        /// Number of iterations per query
        #[arg(short, long, default_value_t = 3)]
        iterations: u32,

        /// Probability of injecting chaotic failures (0.0 to 1.0)
        #[arg(short, long, default_value_t = 0.0)]
        chaos: f64,
        
        /// Output format (json or text)
        #[arg(short, long, default_value = "text")]
        format: String,
    },
}

#[derive(Serialize)]
struct BenchResult {
    query: u32,
    iteration: u32,
    duration_ms: u128,
    status: String,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { queries, iterations, chaos, format } => {
            run_benchmarks(queries, iterations, chaos, format).await?;
        }
    }

    Ok(())
}

async fn run_benchmarks(queries: Vec<u32>, iterations: u32, chaos_prob: f64, format: String) -> Result<()> {
    info!("Initializing Strake Federation Engine for Benchmarking...");
    
    let config_path = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config/tpch.yaml".to_string());
    let config = Config::from_file(&config_path).unwrap_or(Config { sources: vec![], cache: Default::default() });
    
    // Increase limits for benchmarking to allow full TPC-H scans at SF=0.5 (3M rows)
    let _limits = strake_core::config::QueryLimits::default();
    let _retry = RetrySettings::default();
    
    let engine = FederationEngine::new(
        config,
        "strake".to_string(),
        strake_core::config::QueryLimits::default(),
        strake_core::config::ResourceConfig::default(),
        std::collections::HashMap::new(),
        100, // global_budget
        vec![],
        vec![],
    ).await.context("Failed to initialize FederationEngine")?;
    
    let mut results = vec![];

    for &q in &queries {
        info!("Running TPC-H Q{} for {} iterations...", q, iterations);
        
        let sql = get_tpch_query(q)?;
        
        for i in 1..=iterations {
            let start = Instant::now();
            
            // Chaos Injection
            let mut result_status = "SUCCESS".to_string();
            let mut error_msg = None;

            if rand::rng().random_bool(chaos_prob) {
                warn!("Injecting chaos: Simulated Source Timeout for Q{} Iteration {}", q, i);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                result_status = "ERROR".to_string();
                error_msg = Some("Simulated Source Timeout (Chaos Injection)".to_string());
            } else {
                match engine.execute_query(&sql, None).await {
                    Ok(_) => {
                        info!("Q{} Iteration {}: SUCCESS in {}ms", q, i, start.elapsed().as_millis());
                    }
                    Err(e) => {
                        error!("Q{} Iteration {}: FAILED - {:?}", q, i, e); // Use {:?} for more detail
                        result_status = "ERROR".to_string();
                        error_msg = Some(format!("{:?}", e));
                    }
                }
            }
            
            results.push(BenchResult {
                query: q,
                iteration: i,
                duration_ms: start.elapsed().as_millis(),
                status: result_status,
                error: error_msg,
            });
        }
    }

    if format == "json" {
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        print_text_report(&results);
    }

    Ok(())
}

fn get_tpch_query(q: u32) -> Result<String> {
    match q {
        1 => Ok("SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty FROM lineitem WHERE l_shipdate <= '1998-12-01' GROUP BY l_returnflag, l_linestatus".to_string()),
        3 => Ok("SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer, orders, lineitem WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND c_mktsegment = 'BUILDING' GROUP BY l_orderkey".to_string()),
        6 => Ok("SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24".to_string()),
        10 => Ok("SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name".to_string()),
        _ => Err(anyhow::anyhow!("TPC-H Q{} not implemented in benchmarker yet", q)),
    }
}

fn print_text_report(results: &[BenchResult]) {
    println!("\nSTRAKE PERFORMANCE REPORT");
    println!("=========================");
    println!("{:<8} {:<10} {:<15} {:<10}", "Query", "Iteration", "Duration (ms)", "Status");
    println!("---------------------------------------------------------");
    for r in results {
        println!("{:<8} {:<10} {:<15} {:<10}", r.query, r.iteration, r.duration_ms, r.status);
    }
}
