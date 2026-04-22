use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion::datasource::TableProvider;
use std::sync::Arc;

fn main() {
    // This is just to check what methods are available on FederatedTableProviderAdaptor
    // We can't actually run this easily because we need a mock provider,
    // but we can try to compile it or use it to see if it has table_provider() or inner()
}

#[cfg(test)]
mod tests {
     use super::*;
     #[test]
     fn test_methods() {
         // FederatedTableProviderAdaptor::new_with_provider(...)
     }
}
