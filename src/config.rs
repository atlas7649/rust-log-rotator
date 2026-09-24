use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use anyhow::{Context, Result};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_default_config() {
        let cfg = RotationConfig::default();
        assert_eq!(cfg.log_file_path, "app.log");
        assert_eq!(cfg.max_backups, 5);
    }

    #[tokio::test]
    async fn test_load_config() -> Result<()> {
        let mut tmp_file = NamedTempFile::new()?;
        let json = r#"{"log_file_path": "test.log", "max_size_bytes": 100, "max_backups": 2, "compression": true, "dry_run": true}"#;
        tmp_file.write_all(json.as_bytes())?;

        let config = RotationConfig::load_from_file(tmp_file.path()).await?;
        assert_eq!(config.log_file_path, "test.log");
        assert_eq!(config.max_size_bytes, 100);
        assert_eq!(config.max_backups, 2);
        assert!(config.compression);
        assert!(config.dry_run);
        Ok(())
    }
}