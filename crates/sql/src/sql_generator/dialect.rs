use crate::dialects::FunctionMapper;
use datafusion::sql::unparser::dialect::Dialect;

/// dialect configuration for the generator
pub struct GeneratorDialect<'a> {
    pub unparser_dialect: &'a dyn Dialect,
    pub function_mapper: Option<&'a FunctionMapper>,
    pub dialect_name: &'a str,
}

impl<'a> GeneratorDialect<'a> {
    pub fn new(
        unparser_dialect: &'a dyn Dialect,
        function_mapper: Option<&'a FunctionMapper>,
        dialect_name: &'a str,
    ) -> Self {
        Self {
            unparser_dialect,
            function_mapper,
            dialect_name,
        }
    }
}
