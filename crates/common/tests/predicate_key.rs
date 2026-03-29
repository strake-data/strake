//! # Predicate Key Tests
//!
//! Property-based and smoke tests for `PredicateKey` stability and normalization.

use datafusion::logical_expr::{Expr, col, lit};
use proptest::{prop_assert_eq, prop_assert_ne, proptest};
use strake_common::predicate_cache::PredicateKey;

proptest! {
    #[test]
    fn test_predicate_key_identity(
        table in "[a-z]{1,10}",
        partition in 0..100usize,
    ) {
        let e = col("a").eq(lit(1));
        let key1 = PredicateKey::new(&table, &e, partition);
        let key2 = PredicateKey::new(&table, &e, partition);
        prop_assert_eq!(key1, key2);
    }

    #[test]
    fn test_predicate_key_different_partitions(
        partition1 in 0..50usize,
        partition2 in 51..100usize,
    ) {
        let e = col("a").eq(lit(1));
        let key1 = PredicateKey::new("t", &e, partition1);
        let key2 = PredicateKey::new("t", &e, partition2);
        prop_assert_ne!(key1, key2);
    }
}

#[test]
fn test_predicate_key_normalization_stability() {
    // Manually test the sorting logic used in PredicateCachingTableProvider
    let f1 = col("a").eq(lit(1));
    let f2 = col("b").gt(lit(10));

    let combine = |filters: Vec<Expr>| {
        let mut sorted = filters;
        sorted.sort_by_key(|f| format!("{:?}", f));
        sorted.into_iter().reduce(|acc, f| acc.and(f)).unwrap()
    };

    let res1 = combine(vec![f1.clone(), f2.clone()]);
    let res2 = combine(vec![f2, f1]);

    let key1 = PredicateKey::new("t", &res1, 0);
    let key2 = PredicateKey::new("t", &res2, 0);

    assert_eq!(key1, key2);
    assert_eq!(key1.expr_display, key2.expr_display);
}
