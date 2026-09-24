use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RotationConfig {
    pub log_file_path: String,
    pub max_size_bytes: u64,
    pub max_backups: usize,
    pub compression: bool,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            log_file_path: "app.log".to_string(),
            max_size_bytes: 10 * 1024 * 1024, // 10MB
            max_backups: 5,
            compression: false,
        }
    }
}