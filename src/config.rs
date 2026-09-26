use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use anyhow::{Context, Result, anyhow};
use std::env;

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
    pub max_total_backup_size_bytes: Option<u64>,
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
            max_total_backup_size_bytes: Some(100 * 1024 * 1024), // 100MB default
        }
    }
}

impl RotationConfig {
    pub async fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path).await
            .context("Failed to read configuration file")?;
        let mut config = serde_json::from_str(&content)
            .context("Failed to parse configuration JSON")?;
        
        config.apply_env_overrides();
        config.validate()?;
        Ok(config)
    }

    pub fn apply_env_overrides(&mut self) {
        if let Ok(val) = env::var("LOG_ROTATOR_FILE") { self.log_file_path = val; }
        if let Ok(val) = env::var("LOG_ROTATOR_MAX_SIZE") { 
            if let Ok(n) = val.parse() { self.max_size_bytes = n; }
        }
        if let Ok(val) = env::var("LOG_ROTATOR_MAX_BACKUPS") { 
            if let Ok(n) = val.parse() { self.max_backups = n; }
        }
        if let Ok(val) = env::var("LOG_ROTATOR_COMPRESSION") { 
            self.compression = val.to_lowercase() == "true";
        }
        if let Ok(val) = env::var("LOG_ROTATOR_DRY_RUN") { 
            self.dry_run = val.to_lowercase() == "true";
        }
        if let Ok(val) = env::var("LOG_ROTATOR_INTERVAL") { 
            if let Ok(n) = val.parse() { self.check_interval_secs = n; }
        }
        if let Ok(val) = env::var("LOG_ROTATOR_MAX_AGE") { 
            if let Ok(n) = val.parse() { self.max_age_days = n; }
        }
        if let Ok(val) = env::var("LOG_ROTATOR_TOTAL_SIZE") { 
            if let Ok(n) = val.parse() { self.max_total_backup_size_bytes = Some(n); }
        }
        if let Ok(val) = env::var("LOG_ROTATOR_STRATEGY") { 
            self.strategy = match val.to_lowercase().as_str() {
                "daily" => RotationStrategy::Daily,
                "age" => RotationStrategy::Age,
                _ => RotationStrategy::Size,
            };
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.log_file_path.is_empty() {
            return Err(anyhow!("log_file_path cannot be empty"));
        }
        if self.max_size_bytes == 0 {
            return Err(anyhow!("max_size_bytes must be greater than 0"));
        }
        if self.check_interval_secs == 0 {
            return Err(anyhow!("check_interval_secs must be greater than 0"));
        }
        if self.max_backups == 0 {
            return Err(anyhow!("max_backups must be at least 1"));
        }
        Ok(())
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
        assert_eq!(cfg.max_total_backup_size_bytes, Some(100 * 1024 * 1024));
    }

    #[tokio::test]
    async fn test_load_config() -> Result<()> {
        let mut tmp_file = NamedTempFile::new()?;
        let json = r#"{"log_file_path": "test.log", "max_size_bytes": 100, "max_backups": 2, "compression": true, "dry_run": true, "strategy": "Daily", "check_interval_secs": 30, "backup_pattern": "backup_{timestamp}.log", "max_age_days": 14, "max_total_backup_size_bytes": 500}"#;
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
        assert_eq!(config.max_total_backup_size_bytes, Some(500));
        Ok(())
    }

    #[test]
    fn test_validation() {
        let mut cfg = RotationConfig::default();
        assert!(cfg.validate().is_ok());

        cfg.max_size_bytes = 0;
        assert!(cfg.validate().is_err());

        cfg.max_size_bytes = 100;
        cfg.log_file_path = "".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_env_overrides() {
        env::set_var("LOG_ROTATOR_FILE", "env.log");
        env::set_var("LOG_ROTATOR_MAX_BACKUPS", "10");
        env::set_var("LOG_ROTATOR_STRATEGY", "Age");
        
        let mut cfg = RotationConfig::default();
        cfg.apply_env_overrides();
        
        assert_eq!(cfg.log_file_path, "env.log");
        assert_eq!(cfg.max_backups, 10);
        assert_eq!(cfg.strategy, RotationStrategy::Age);
        
        env::remove_var("LOG_ROTATOR_FILE");
        env::remove_var("LOG_ROTATOR_MAX_BACKUPS");
        env::remove_var("LOG_ROTATOR_STRATEGY");
    }
}