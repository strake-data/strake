#[tokio::test]
    async fn test_three_table_join_alias_leakage() -> Result<()> {
        // Reproduce the failing plan structure:
        // Join(u.id = o.user_id)
        //   Left: Projection(u.id, d.name) -> Input: Join(u.dept = d.id) of (SubqueryAlias u, SubqueryAlias d)
        //   Right: SubqueryAlias o
        
        let ctx = SessionContext::new();
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("department_id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("user_id", arrow::datatypes::DataType::Int32, false),
        ]);
        let table = std::sync::Arc::new(datafusion::datasource::empty::EmptyTable::new(std::sync::Arc::new(schema)));
        ctx.register_table("users", table.clone())?;
        ctx.register_table("depts", table.clone())?;
        ctx.register_table("orders", table.clone())?;

        let u = ctx.table("users").await?.alias("u")?;
        let d = ctx.table("depts").await?.alias("d")?;
        let o = ctx.table("orders").await?.alias("o")?;

        // Inner Join: u.department_id = d.id
        let inner_join = u.join(d, JoinType::Inner, (vec!["department_id"], vec!["id"]), None)?;
        
        // Inner Projection: u.id, d.name
        // Note: we project `u.id` specifically
        let inner_proj = inner_join.select(vec![col("u.id"), col("d.name")])?;

        // Outer Join: u.id = o.user_id
        // CRITICAL: We join on `u.id`. If `u` leaks, this resolves. If `u` is hidden, it should fail or resolve to unqualified `id`.
        // However, standard DF builder might fail if we ask for `u.id` and `u` is hidden.
        // But `PlanRemapper` takes an EXISTING valid plan (where `u` IS visible) and transforms it for SQL gen.
        // So we construct the "bad" plan first.
        let outer_join = inner_proj.join(o, JoinType::Inner, (vec!["u.id"], vec!["user_id"]), None)?;
        
        let plan = outer_join.into_optimized_plan()?;
        println!("Original Plan:\n{}", plan.display_indent());

        let sql = get_sql_for_plan(&plan, "postgres")?.expect("sql generated");
        println!("Generated SQL: {}", sql);

        // Assertions
        assert!(!sql.contains("u.id ="), "SQL should not use leaky alias u.id in outer join condition");
        assert!(sql.contains("id =") || sql.contains("derived_id ="), "SQL should use unqualified or derived alias");

        Ok(())
    }
