use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use anyhow::{Context, Result};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum RotationStrategy {
    Size,
    Daily,
    Age,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RotationConfig {
    pub log_file_path: String,
    pub max_size_bytes: u64,
    pub max_backups: usize,
    pub compression: bool,
    pub dry_run: bool,
    pub strategy: RotationStrategy,
    pub check_interval_secs: u64,
    pub backup_pattern: Option<String>,
    pub max_age_days: u64,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            log_file_path: "app.log".to_string(),
            max_size_bytes: 10 * 1024 * 1024, // 10MB
            max_backups: 5,
            compression: false,
            dry_run: false,
            strategy: RotationStrategy::Size,
            check_interval_secs: 60,
            backup_pattern: None,
            max_age_days: 7,
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
        assert_eq!(cfg.strategy, RotationStrategy::Size);
        assert_eq!(cfg.check_interval_secs, 60);
        assert_eq!(cfg.backup_pattern, None);
        assert_eq!(cfg.max_age_days, 7);
    }

    #[tokio::test]
    async fn test_load_config() -> Result<()> {
        let mut tmp_file = NamedTempFile::new()?;
        let json = r#"{"log_file_path": "test.log", "max_size_bytes": 100, "max_backups": 2, "compression": true, "dry_run": true, "strategy": "Daily", "check_interval_secs": 30, "backup_pattern": "backup_{timestamp}.log", "max_age_days": 14}"#;
        tmp_file.write_all(json.as_bytes())?;

        let config = RotationConfig::load_from_file(tmp_file.path()).await?;
        assert_eq!(config.log_file_path, "test.log");
        assert_eq!(config.max_size_bytes, 100);
        assert!(config.compression);
        assert!(config.dry_run);
        assert_eq!(config.strategy, RotationStrategy::Daily);
        assert_eq!(config.check_interval_secs, 30);
        assert_eq!(config.backup_pattern, Some("backup_{timestamp}.log".to_string()));
        assert_eq!(config.max_age_days, 14);
        Ok(())
    }
}