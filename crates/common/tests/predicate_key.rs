//! # Predicate Key Tests
//!
//! Property-based and smoke tests for `PredicateKey` stability and normalization.

use proptest::{prop_assert_eq, prop_assert_ne, proptest};
use strake_common::predicate_cache::PredicateKey;

proptest! {
    #[test]
    fn test_predicate_key_identity(
        table in "[a-z]{1,10}",
        partition in 0..100usize,
    ) {
        let e = "id = 1";
        let key1 = PredicateKey::new(&table, e, partition);
        let key2 = PredicateKey::new(&table, e, partition);
        prop_assert_eq!(key1, key2);
    }

    #[test]
    fn test_predicate_key_different_partitions(
        partition1 in 0..50usize,
        partition2 in 51..100usize,
    ) {
        let e = "id = 1";
        let key1 = PredicateKey::new("t", e, partition1);
        let key2 = PredicateKey::new("t", e, partition2);
        prop_assert_ne!(key1, key2);
    }
}

#[test]
fn test_predicate_key_normalization_stability() {
    // We test that PredicateKey itself is stable.
    // Normalization logic belongs to the caller (e.g. datafusion-table-providers),
    // but PredicateKey must preserve whatever it is given.
    let res1 = "a = 1 AND b > 10";
    let res2 = "a = 1 AND b > 10";

    let key1 = PredicateKey::new("t", res1, 0);
    let key2 = PredicateKey::new("t", res2, 0);

    assert_eq!(key1, key2);
    assert_eq!(key1.expr_display, res1);
    assert_eq!(key1.expr_display, key2.expr_display);
}
