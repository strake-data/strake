macro_rules! with_generator {
    ($gen:ident, $body:block) => {
        let dialect_path = strake_sql::dialect_router::route_dialect("postgres");
        let (dialect_arc, capabilities, type_mapper, mapper) = match dialect_path {
            strake_sql::dialect_router::DialectPath::Native(d, c, t) => (d, c, t, None),
            strake_sql::dialect_router::DialectPath::Custom(d, c, t, m) => (d, c, t, m),
            _ => panic!("Expected native or custom dialect"),
        };
        let generator_dialect = strake_sql::sql_generator::dialect::GeneratorDialect::new(
            dialect_arc.as_ref(),
            mapper.as_ref(),
            capabilities,
            type_mapper,
            "postgres",
        );
        #[allow(unused_mut)]
        let mut $gen = strake_sql::sql_generator::SqlGenerator::new(generator_dialect);
        $body
    };
}
