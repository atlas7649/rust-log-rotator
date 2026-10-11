use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use anyhow::{Context, Result, anyhow};
use std::env;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum RotationStrategy {
    Size,
    Daily,
    Age,
    Interval(u64), 
    Keyword(String),
    Regex(String),
    Truncate,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum BackupNaming {
    Timestamp,
    Sequential,
    Custom,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RotationTarget {
    pub log_file_path: PathBuf,
    pub max_size_bytes: Option<u64>,
    pub max_backups: Option<usize>,
    pub strategy: Option<RotationStrategy>,
    pub backup_pattern: Option<String>,
    pub backup_suffix: Option<String>,
    pub naming_style: Option<BackupNaming>,
    pub preserve_permissions: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RotationConfig {
    pub targets: Vec<RotationTarget>,
    pub compression: bool,
    pub dry_run: bool,
    pub check_interval_secs: u64,
    pub max_age_days: u64,
    pub max_total_backup_size_bytes: Option<u64>,
    pub max_total_backups: Option<usize>,
    pub rotation_grace_period_secs: u64,
    // Defaults for targets if they are not specified
    pub default_max_size_bytes: u64,
    pub default_max_backups: usize,
    pub default_strategy: RotationStrategy,
    pub default_naming_style: BackupNaming,
    pub default_preserve_permissions: bool,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            targets: vec![RotationTarget {
                log_file_path: PathBuf::from("app.log"),
                max_size_bytes: None,
                max_backups: None,
                strategy: None,
                backup_pattern: None,
                backup_suffix: None,
                naming_style: None,
                preserve_permissions: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: Some(100 * 1024 * 1024),
            max_total_backups: None,
            rotation_grace_period_secs: 30,
            default_max_size_bytes: 10 * 1024 * 1024,
            default_max_backups: 5,
            default_strategy: RotationStrategy::Size,
            default_naming_style: BackupNaming::Timestamp,
            default_preserve_permissions: true,
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
        let parse_bool = |var: &str| env::var(var).map(|v| v.to_lowercase() == "true");
        let parse_u64 = |var: &str| env::var(var).ok().and_then(|v| v.parse().ok());

        if let Ok(val) = parse_bool("LOG_ROTATOR_COMPRESSION") { 
            self.compression = val;
        }
        if let Ok(val) = parse_bool("LOG_ROTATOR_DRY_RUN") { 
            self.dry_run = val;
        }
        if let Some(n) = parse_u64("LOG_ROTATOR_INTERVAL") { 
            self.check_interval_secs = n; 
        }
        if let Some(n) = parse_u64("LOG_ROTATOR_MAX_AGE") { 
            self.max_age_days = n; 
        }
        if let Some(n) = parse_u64("LOG_ROTATOR_TOTAL_SIZE") { 
            self.max_total_backup_size_bytes = Some(n); 
        }
        if let Some(n) = parse_u64("LOG_ROTATOR_TOTAL_COUNT") { 
            self.max_total_backups = Some(n as usize); 
        }
        if let Some(n) = parse_u64("LOG_ROTATOR_GRACE_PERIOD") { 
            self.rotation_grace_period_secs = n; 
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.targets.is_empty() {
            return Err(anyhow!("At least one rotation target must be specified"));
        }
        for target in &self.targets {
            if target.log_file_path.as_os_str().is_empty() {
                return Err(anyhow!("log_file_path cannot be empty"));
            }
            if target.naming_style == Some(BackupNaming::Custom) && target.backup_pattern.is_none() {
                return Err(anyhow!("backup_pattern must be specified when naming_style is Custom for target {:?}", target.log_file_path));
            }
        }
        if self.check_interval_secs < 1 || self.check_interval_secs > 86400 {
            return Err(anyhow!("check_interval_secs must be between 1 and 86400"));
        }
        if let Some(size) = self.max_total_backup_size_bytes {
            if size == 0 {
                return Err(anyhow!("max_total_backup_size_bytes cannot be 0"));
            }
        }
        if let Some(count) = self.max_total_backups {
            if count == 0 {
                return Err(anyhow!("max_total_backups cannot be 0"));
            }
        }
        Ok(())
    }

    pub fn resolve_strategy(&self, target: &RotationTarget) -> &RotationStrategy {
        target.strategy.as_ref().unwrap_or(&self.default_strategy)
    }

    pub fn resolve_max_size(&self, target: &RotationTarget) -> u64 {
        target.max_size_bytes.unwrap_or(self.default_max_size_bytes)
    }

    pub fn resolve_max_backups(&self, target: &RotationTarget) -> usize {
        target.max_backups.unwrap_or(self.default_max_backups)
    }

    pub fn resolve_naming_style(&self, target: &RotationTarget) -> &BackupNaming {
        target.naming_style.as_ref().unwrap_or(&self.default_naming_style)
    }

    pub fn resolve_preserve_permissions(&self, target: &RotationTarget) -> bool {
        target.preserve_permissions.unwrap_or(self.default_preserve_permissions)
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
        assert_eq!(cfg.targets.len(), 1);
        assert_eq!(cfg.targets[0].log_file_path, PathBuf::from("app.log"));
        assert_eq!(cfg.default_max_backups, 5);
        assert_eq!(cfg.default_strategy, RotationStrategy::Size);
        assert_eq!(cfg.check_interval_secs, 60);
        assert_eq!(cfg.max_age_days, 7);
        assert_eq!(cfg.max_total_backup_size_bytes, Some(100 * 1024 * 1024));
        assert_eq!(cfg.rotation_grace_period_secs, 30);
        assert_eq!(cfg.default_naming_style, BackupNaming::Timestamp);
        assert!(cfg.default_preserve_permissions);
    }

    #[tokio::test]
    async fn test_load_config() -> Result<()> {
        let mut tmp_file = NamedTempFile::new()?;
        let json = r#"{
            "targets": [ 
                {"log_file_path": "test1.log", "max_size_bytes": 100}, 
                {"log_file_path": "test2.log", "strategy": "Daily"}
            ], 
            "compression": true, 
            "dry_run": true, 
            "check_interval_secs": 30, 
            "max_age_days": 14, 
            "max_total_backup_size_bytes": 500,
            "max_total_backups": 20,
            "rotation_grace_period_secs": 45,
            "default_max_size_bytes": 1000,
            "default_max_backups": 10,
            "default_strategy": "Size"
        }"#;
        tmp_file.write_all(json.as_bytes())?;

        let config = RotationConfig::load_from_file(tmp_file.path()).await?;
        assert_eq!(config.targets.len(), 2);
        assert_eq!(config.targets[0].log_file_path, PathBuf::from("test1.log"));
        assert_eq!(config.targets[1].log_file_path, PathBuf::from("test2.log"));
        assert!(config.compression);
        assert!(config.dry_run);
        assert_eq!(config.check_interval_secs, 30);
        assert_eq!(config.max_age_days, 14);
        assert_eq!(config.max_total_backup_size_bytes, Some(500));
        assert_eq!(config.max_total_backups, Some(20));
        assert_eq!(config.rotation_grace_period_secs, 45);
        Ok(())
    }

    #[test]
    fn test_validation() {
        let mut cfg = RotationConfig::default();
        assert!(cfg.validate().is_ok());

        cfg.targets = vec![];
        assert!(cfg.validate().is_err());

        cfg.targets = vec![RotationTarget {
            log_file_path: PathBuf::from(""),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: None,
            backup_suffix: None,
            naming_style: None,
            preserve_permissions: None,
        }];
        assert!(cfg.validate().is_err());

        cfg.targets = vec![RotationTarget {
            log_file_path: PathBuf::from("test.log"),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: None,
            backup_suffix: None,
            naming_style: Some(BackupNaming::Custom),
            preserve_permissions: None,
        }];
        assert!(cfg.validate().is_err());

        let mut cfg_invalid = RotationConfig::default();
        cfg_invalid.check_interval_secs = 0;
        assert!(cfg_invalid.validate().is_err());
        cfg_invalid.check_interval_secs = 90000;
        assert!(cfg_invalid.validate().is_err());
    }

    #[test]
    fn test_resolution() {
        let mut cfg = RotationConfig::default();
        cfg.default_strategy = RotationStrategy::Size;
        
        let target_with_strategy = RotationTarget {
            log_file_path: PathBuf::from("test.log"),
            max_size_bytes: Some(500),
            max_backups: Some(2),
            strategy: Some(RotationStrategy::Truncate),
            backup_pattern: None,
            backup_suffix: None,
            naming_style: Some(BackupNaming::Sequential),
            preserve_permissions: Some(false),
        };

        assert_eq!(cfg.resolve_strategy(&target_with_strategy), &RotationStrategy::Truncate);
        assert_eq!(cfg.resolve_max_size(&target_with_strategy), 500);
        assert_eq!(cfg.resolve_max_backups(&target_with_strategy), 2);
        assert_eq!(cfg.resolve_naming_style(&target_with_strategy), &BackupNaming::Sequential);
        assert_eq!(cfg.resolve_preserve_permissions(&target_with_strategy), false);

        let target_default = RotationTarget {
            log_file_path: PathBuf::from("default.log"),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: None,
            backup_suffix: None,
            naming_style: None,
            preserve_permissions: None,
        };

        assert_eq!(cfg.resolve_strategy(&target_default), &RotationStrategy::Size);
        assert_eq!(cfg.resolve_max_size(&target_default), cfg.default_max_size_bytes);
        assert_eq!(cfg.resolve_max_backups(&target_default), cfg.default_max_backups);
        assert_eq!(cfg.resolve_naming_style(&target_default), &BackupNaming::Timestamp);
        assert_eq!(cfg.resolve_preserve_permissions(&target_default), cfg.default_preserve_permissions);
    }
}
