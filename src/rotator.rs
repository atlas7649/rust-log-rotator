use crate::config::RotationConfig;
use anyhow::{Context, Result};
use std::path::Path;
use tokio::fs;
use chrono::Local;

pub struct LogRotator {
    config: RotationConfig,
}

impl LogRotator {
    pub fn new(config: RotationConfig) -> Self {
        Self { config }
    }

    pub async fn check_and_rotate(&self) -> Result<bool> {
        let path = Path::new(&self.config.log_file_path);
        if !path.exists() {
            return Ok(false);
        }

        let metadata = fs::metadata(path).await?;
        if metadata.len() >= self.config.max_size_bytes {
            self.rotate().await?;
            return Ok(true);
        }

        Ok(false)
    }

    async fn rotate(&self) -> Result<()> {
        // Rotate old backups
        for i in (1..self.config.max_backups).rev() {
            let old_path = format!("{}.{}", self.config.log_file_path, i);
            let new_path = format!("{}.{}", self.config.log_file_path, i + 1);
            if Path::new(&old_path).exists() {
                fs::rename(old_path, new_path).await?;
            }
        }

        // Move current log to .1
        let backup_path = format!("{}.1", self.config.log_file_path);
        fs::rename(&self.config.log_file_path, backup_path).await
            .context("Failed to rename log file during rotation")?;

        Ok(())
    }
}