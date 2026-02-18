use tracing::info;

pub struct IcebergTelemetry;

impl IcebergTelemetry {
    #[inline]
    pub fn table_registered(catalog: &str, schema: &str, table: &str) {
        info!(
            target: "strake_iceberg",
            catalog = %catalog,
            schema = %schema,
            table = %table,
            "iceberg_table_registered"
        );
    }

    /// Log scan completion with detailed metrics
    #[inline]
    pub fn scan_planning_completed(table: &str, total_duration_ms: u64) {
        info!(
            target: "strake_iceberg",
            table = %table,
            total_duration_ms = total_duration_ms,
            "iceberg_scan_planning_completed"
        );
    }

    /// Log scan initiation
    #[inline]
    pub fn scan_started(table: &str, filters_count: usize, projection_count: Option<usize>) {
        info!(
            target: "strake_iceberg",
            table = %table,
            filters_count = filters_count,
            projection_count = projection_count,
            "iceberg_scan_started"
        );
    }

    /// Log catalog API latency
    #[inline]
    pub fn catalog_api_call(operation: &str, latency_ms: u64, success: bool) {
        info!(
            target: "strake_iceberg",
            operation = %operation,
            latency_ms = latency_ms,
            success = success,
            "iceberg_catalog_api_call"
        );
    }
}
