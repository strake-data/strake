//! # Table Scan Translator
//!
//! Translates DataFusion `TableScan` logical plan nodes into SQL `FROM` clauses.
//!
//! ## Usage
//! Handled internally by [`SqlGenerator`](crate::sql_generator::translator::SqlGenerator); not intended for direct use.
//!
//! ## Performance Characteristics
//! - **Complexity:** Validates target table and maps projections.
//! - **Allocation:** Allocates initial base scopes for parent nodes to reference.
//!
//! ## Errors
//! - [`SqlGenError::UnsupportedPlan`]: If table projection contains unrecognized identifiers.

use super::SqlGenerator;
use crate::sql_generator::context::ColumnEntry;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::expr::ExprTranslator;
use crate::sql_generator::sanitize::safe_ident;
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, LimitClause, ObjectName, ObjectNamePart, TableAlias,
    TableFactor, TableWithJoins,
};
use std::sync::Arc;

pub(crate) fn handle_table_scan(
    generator: &mut SqlGenerator,
    scan: &datafusion::logical_expr::TableScan,
) -> Result<sqlparser::ast::Query, SqlGenError> {
    let alias = generator.context.next_alias();
    let table_ref = &scan.table_name;
    let table_name = table_ref.table().to_string();
    let full_table_name = table_ref.to_string();

    // Use the full table schema for scope to allow resolving filters on non-projected columns.
    let table_schema = scan.source.schema();

    let mut qualifiers = vec![table_name.clone()];
    if full_table_name != table_name {
        qualifiers.push(full_table_name.clone());
        let unquoted = table_ref.to_string();
        let parts: Vec<&str> = unquoted.split('.').map(|s| s.trim_matches('"')).collect();
        if parts.len() > 1 {
            qualifiers.push(parts.join("."));
        }
    }

    let alias_arc: Arc<str> = alias.clone().into();
    let columns_vec: Vec<ColumnEntry> = table_schema
        .fields()
        .iter()
        .map(|f| {
            // Arrow fields don't have qualifiers, so we use the table's qualifiers.
            let provenance = qualifiers.clone();

            // Normalize and lowercase for registration
            let name_str = f.name();
            let stable_name = super::derive_bare_name(name_str);
            let stable_name_lower = Arc::from(stable_name.to_lowercase().as_str());

            ColumnEntry {
                name: stable_name,
                name_lower: stable_name_lower,
                data_type: f.data_type().clone(),
                source_alias: alias_arc.clone(),
                provenance,
                unique_id: generator.context.next_column_id(),
            }
        })
        .collect();
    let columns = Arc::<[ColumnEntry]>::from(columns_vec.into_boxed_slice());

    generator
        .context
        .enter_scope(alias.clone(), columns.clone(), qualifiers)
        .commit();

    let mut parts = Vec::new();
    if let Some(catalog) = table_ref.catalog()
        && !generator.dialect.capabilities.strip_catalog_qualifier()
    {
        parts.push(ObjectNamePart::Identifier(safe_ident(catalog)?));
    }
    if let Some(schema_name) = table_ref.schema()
        && !generator.dialect.capabilities.strip_schema_qualifier()
    {
        parts.push(ObjectNamePart::Identifier(safe_ident(schema_name)?));
    }
    parts.push(ObjectNamePart::Identifier(safe_ident(table_ref.table())?));

    let relation = TableFactor::Table {
        name: ObjectName(parts),
        alias: Some(TableAlias {
            name: safe_ident(&alias)?,
            columns: vec![],
            explicit: true,
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

    // KEY FIX: Project ALL columns from the full table schema, using derive_bare_name
    // for the column identifiers. This ensures:
    // 1. All columns are available for parent nodes (fixes Binder Error)
    // 2. Column names in SQL match the scope registration (fixes ScopeViolation)
    let projection = columns
        .iter()
        .map(|c| {
            Ok(sqlparser::ast::SelectItem::UnnamedExpr(
                SqlExpr::CompoundIdentifier(vec![
                    safe_ident(alias.as_ref())?,
                    safe_ident(&c.name)?,
                ]),
            ))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;

    let mut select = generator.create_skeleton_select();
    select.from = vec![TableWithJoins {
        relation,
        joins: vec![],
    }];
    select.projection = projection;

    if !scan.filters.is_empty() {
        let (ctx, dial) = (&mut generator.context, &generator.dialect);
        let mut translator = ExprTranslator::new(ctx, dial);
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

    let mut query = generator.create_skeleton_query();
    query.body = Box::new(sqlparser::ast::SetExpr::Select(Box::new(select)));

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
