#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // This is a unit-level proptest for type conversions if we had manual ones.
    // Since connector-x handles it, we can test our wrapping logic or 
    // mock the database data for roundtrip testing.
    
    proptest! {
        #[test]
        fn test_integer_roundtrip_logic(val in any::<i32>()) {
            // Simulated roundtrip through Arrow
            let schema = Arc::new(Schema::new(vec![
                Field::new("val", DataType::Int32, false),
            ]));
            let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![val])),
            ];
            let batch = RecordBatch::try_new(schema, columns).unwrap();
            
            let retrieved = batch.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .value(0);
                
            prop_assert_eq!(retrieved, val);
        }

        #[test]
        fn test_string_roundtrip_logic(val in "[a-zA-Z0-9]{1,100}") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("val", DataType::Utf8, false),
            ]));
            let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![
                Arc::new(datafusion::arrow::array::StringArray::from(vec![val.as_str()])),
            ];
            let batch = RecordBatch::try_new(schema, columns).unwrap();
            
            let retrieved = batch.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap()
                .value(0);
                
            prop_assert_eq!(retrieved, val);
        }
    }
}
