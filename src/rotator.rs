use crate::config::RotationConfig;
use anyhow::{Context, Result};
use std::path::Path;
use tokio::fs;

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
        // 1. Remove the oldest backup if it exists to make room
        let oldest_path = format!("{}.{}", self.config.log_file_path, self.config.max_backups);
        if Path::new(&oldest_path).exists() {
            fs::remove_file(oldest_path).await
                .context("Failed to remove oldest backup file")?;
        }

        // 2. Shift existing backups (max-1 -> max, ..., 1 -> 2)
        for i in (1..self.config.max_backups).rev() {
            let current_backup = format!("{}.{}", self.config.log_file_path, i);
            let next_backup = format!("{}.{}", self.config.log_file_path, i + 1);
            if Path::new(&current_backup).exists() {
                fs::rename(current_backup, next_backup).await
                    .context(format!("Failed to rotate backup {} to {}", i, i + 1))?;
            }
        }

        // 3. Move current log to .1
        let first_backup = format!("{}.1", self.config.log_file_path);
        fs::rename(&self.config.log_file_path, first_backup).await
            .context("Failed to rename log file to first backup")?;

        Ok(())
    }
}