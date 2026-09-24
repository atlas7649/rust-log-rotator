use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use anyhow::{Context, Result};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RotationConfig {
    pub log_file_path: String,
    pub max_size_bytes: u64,
    pub max_backups: usize,
    pub compression: bool,
    pub dry_run: bool,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            log_file_path: "app.log".to_string(),
            max_size_bytes: 10 * 1024 * 1024, // 10MB
            max_backups: 5,
            compression: false,
            dry_run: false,
        }
    }
}

impl RotationConfig {
    pub async fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path).await
            .context("Failed to read configuration file")?;
        let config = serde_json::from_str(&content)
            .context("Failed to parse configuration JSON")?;
        Ok(config)
    }
}