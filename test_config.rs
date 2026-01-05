
use strake_core::config::AppConfig;

fn main() {
    let path = "config/strake.yaml";
    match AppConfig::from_file(path) {
        Ok(config) => println!("Success: {:?}", config),
        Err(e) => {
            println!("Error: {:?}", e);
            println!("Context: {}", e.root_cause());
        }
    }
}
