macro_rules! with_generator {
    ($gen:ident, $body:block) => {
        let dialect_path = strake_sql::dialect_router::route_dialect("postgres");
        let (dialect_arc, mapper) = match dialect_path {
            strake_sql::dialect_router::DialectPath::Native(d) => (d, None),
            strake_sql::dialect_router::DialectPath::Custom(d, m) => (d, m),
            _ => panic!("Expected native or custom dialect"),
        };
        let generator_dialect = strake_sql::sql_generator::dialect::GeneratorDialect::new(
            dialect_arc.as_ref(),
            mapper.as_ref(),
            "postgres",
        );
        let mut $gen = strake_sql::sql_generator::SqlGenerator::new(generator_dialect);
        $body
    };
}
