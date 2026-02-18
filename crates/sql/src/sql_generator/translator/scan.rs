use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, LimitClause, ObjectName, ObjectNamePart, SelectItem,
    TableAlias, TableFactor, TableWithJoins,
};

pub(crate) fn handle_table_scan(
    gen: &mut SqlGenerator,
    scan: &datafusion::logical_expr::TableScan,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let alias = gen.context.next_alias();

    let table_name = scan.table_name.table().to_string();
    let columns = scan
        .projected_schema
        .fields()
        .iter()
        .map(|f| ColumnEntry {
            name: std::sync::Arc::from(f.name().as_ref()),
            data_type: f.data_type().clone(),
            source_alias: std::sync::Arc::from(alias.as_str()),
            provenance: vec![table_name.clone(), alias.clone()],
            unique_id: gen.context.next_column_id(),
        })
        .collect::<Vec<_>>()
        .into();

    gen.context
        .enter_scope(alias.clone(), columns, vec![table_name.clone()])
        .commit();

    let relation = TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(safe_ident(&table_name)?)]),
        alias: Some(TableAlias {
            name: safe_ident(&alias)?,
            columns: vec![],
        }),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    };

    let projection = scan
        .projected_schema
        .fields()
        .iter()
        .map(|f| {
            Ok(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                safe_ident(alias.as_ref())?,
                safe_ident(f.name().as_ref())?,
            ])))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut select = gen.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation,
        joins: vec![],
    }];
    select.projection = projection;

    // Handle pushed-down filters
    if !scan.filters.is_empty() {
        let mut translator = ExprTranslator::new(&mut gen.context, &gen.dialect);
        let mut selection: Option<SqlExpr> = None;

        for f in &scan.filters {
            let f_sql = translator.expr_to_sql(f)?;
            selection = match selection {
                Some(e) => Some(SqlExpr::BinaryOp {
                    left: Box::new(e),
                    op: BinaryOperator::And,
                    right: Box::new(f_sql),
                }),
                None => Some(f_sql),
            };
        }
        select.selection = selection;
    }

    let mut query = gen.create_skeleton_query();
    query.body = Box::new(sqlparser::ast::SetExpr::Select(Box::new(select)));

    // Handle pushed-down fetch (limit)
    if let Some(fetch) = scan.fetch {
        let limit_expr =
            SqlExpr::Value(sqlparser::ast::Value::Number(fetch.to_string(), false).into());
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit: Some(limit_expr),
            offset: None,
            limit_by: vec![],
        });
    }

    Ok(query)
}
