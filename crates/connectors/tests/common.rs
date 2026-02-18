use std::sync::LazyLock;
use std::sync::Mutex;

static ENV_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// RAII guard for setting environment variables that automatically cleans up on drop.
/// Uses a global mutex to ensure thread-safety across concurrent tests.
pub struct EnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    keys: Vec<String>,
}

impl EnvGuard {
    pub fn new(vars: Vec<(&str, &str)>) -> Self {
        let lock = ENV_MUTEX.lock().unwrap();
        let keys: Vec<String> = vars
            .iter()
            .map(|(k, v)| {
                std::env::set_var(k, v);
                k.to_string()
            })
            .collect();
        Self { _lock: lock, keys }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for key in &self.keys {
            std::env::remove_var(key);
        }
    }
}
