use crate::config::{RotationConfig, RotationStrategy};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

pub struct LogRotator {
    config: RotationConfig,
    last_rotation_date: Option<chrono::NaiveDate>,
}

impl LogRotator {
    pub fn new(config: RotationConfig) -> Self {
        Self {
            config,
            last_rotation_date: None,
        }
    }

    pub async fn check_and_rotate(&mut self) -> Result<bool> {
        let path = Path::new(&self.config.log_file_path);
        if !path.exists() {
            return Ok(false);
        }

        let should_rotate = match self.config.strategy {
            RotationStrategy::Size => {
                let metadata = fs::metadata(path).await?;
                metadata.len() >= self.config.max_size_bytes
            }
            RotationStrategy::Daily => {
                let today = chrono::Local::now().date_naive();
                let needs_rotation = match self.last_rotation_date {
                    Some(last_date) => last_date != today,
                    None => false,
                };
                if needs_rotation {
                    true
                } else {
                    if self.last_rotation_date.is_none() {
                        self.last_rotation_date = Some(today);
                    }
                    false
                }
            }
        };

        if should_rotate {
            if self.config.dry_run {
                println!("[Dry Run] Log file {} triggered rotation strategy {:?}, would rotate", self.config.log_file_path, self.config.strategy);
                return Ok(false);
            }
            self.rotate().await?;
            self.last_rotation_date = Some(chrono::Local::now().date_naive());
            return Ok(true);
        }

        Ok(false)
    }

    async fn rotate(&self) -> Result<()> {
        let ext = if self.config.compression { ".gz" } else { "" };
        let timestamp = chrono::Local::now().format("%Y%m%d-%H%M%S").to_string();

        // 1. Identify existing backups to maintain the limit
        let backups = self.list_backups().await?;
        
        // Sort backups by creation time (oldest first)
        let mut metadata_list = Vec::new();
        for path in &backups {
            if let Ok(meta) = fs::metadata(path).await {
                if let Ok(created) = meta.created() {
                    metadata_list.push((path, created));
                }
            }
        }
        metadata_list.sort_by_key(|&(_, created)| created);

        // Remove oldest if we exceed the limit (including the one we are about to create)
        let to_remove_count = backups.len().saturating_sub(self.config.max_backups - 1);
        for i in 0..to_remove_count {
            if let Some((path, _)) = metadata_list.get(i) {
                fs::remove_file(path).await
                    .context("Failed to remove oldest backup file")?;
            }
        }

        // 2. Rotate current log to a new timestamped backup
        let backup_name = if let Some(ref pattern) = self.config.backup_pattern {
            pattern.replace("{timestamp}", &timestamp) + ext
        } else {
            format!("{}_{}{}", self.config.log_file_path, timestamp, ext)
        };

        if self.config.compression {
            self.compress_and_move(&self.config.log_file_path, &backup_name).await?
        } else {
            fs::rename(&self.config.log_file_path, backup_name).await
                .context("Failed to rename log file to backup")?;
        }

        Ok(())
    }

    async fn list_backups(&self) -> Result<Vec<PathBuf>> {
        let path = Path::new(&self.config.log_file_path);
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let filename = path.file_name().context("Invalid log file path")?;
        let filename_str = filename.to_string_lossy();
        let ext = if self.config.compression { ".gz" } else { "" };

        let mut backups = Vec::new();
        let mut entries = fs::read_dir(parent).await
            .context("Failed to read log directory")?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(name) = path.file_name() {
                let name_str = name.to_string_lossy();
                
                if name_str == filename_str {
                    continue;
                }

                if !name_str.ends_with(ext) {
                    continue;
                }

                let is_backup = if let Some(ref pattern) = self.config.backup_pattern {
                    if let Some(prefix) = pattern.split("{timestamp}").next() {
                        name_str.starts_with(prefix)
                    } else {
                        false
                    }
                } else {
                    name_str.starts_with(&filename_str)
                };

                if is_backup {
                    backups.push(path);
                }
            }
        }
        Ok(backups)
    }

    async fn compress_and_move(&self, src: &str, dst: &str) -> Result<()> {
        let src_path_owned = src.to_string();
        let dst_path_owned = dst.to_string();

        tokio::task::spawn_blocking(move || {
            let mut input = std::fs::File::open(&src_path_owned)
                .context("Failed to open log file for compression")?;
            let output = std::fs::File::create(&dst_path_owned)
                .context("Failed to create compressed log file")?;
            
            let mut encoder = GzEncoder::new(output, Compression::default());
            std::io::copy(&mut input, &mut encoder)
                .context("Failed to stream data to gzip encoder")?;
            
            encoder.finish().context("Failed to finish gzip compression")?;
            Ok::<(), anyhow::Error>(())
        }).await.context("Join error during compression")?;

        fs::remove_file(src).await
            .context("Failed to remove original log file after compression")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RotationConfig, RotationStrategy};
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_size_rotation_trigger() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("test.log");
        let log_path_str = log_path.to_str().unwrap().to_string();

        fs::write(&log_path, "small content").await?;

        let config = RotationConfig {
            log_file_path: log_path_str.clone(),
            max_size_bytes: 100,
            max_backups: 3,
            compression: false,
            dry_run: false,
            strategy: RotationStrategy::Size,
            check_interval_secs: 60,
            backup_pattern: None,
        };
        let mut rotator = LogRotator::new(config);

        assert!(!rotator.check_and_rotate().await?);

        let large_content = "a".repeat(101);
        fs::write(&log_path, large_content).await?;

        assert!(rotator.check_and_rotate().await?);
        assert!(!log_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_backup_limit() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("limit.log");
        let log_path_str = log_path.to_str().unwrap().to_string();

        let config = RotationConfig {
            log_file_path: log_path_str.clone(),
            max_size_bytes: 10,
            max_backups: 2,
            compression: false,
            dry_run: false,
            strategy: RotationStrategy::Size,
            check_interval_secs: 60,
            backup_pattern: None,
        };
        let mut rotator = LogRotator::new(config);

        for _ in 0..3 {
            fs::write(&log_path, "some content").await?;
            rotator.check_and_rotate().await?;
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        }

        let backups = rotator.list_backups().await?;
        assert_eq!(backups.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_rotation_trigger() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("daily.log");
        let log_path_str = log_path.to_str().unwrap().to_string();
        fs::write(&log_path, "content").await?;

        let config = RotationConfig {
            log_file_path: log_path_str.clone(),
            max_size_bytes: 1024 * 1024,
            max_backups: 3,
            compression: false,
            dry_run: false,
            strategy: RotationStrategy::Daily,
            check_interval_secs: 60,
            backup_pattern: None,
        };
        let mut rotator = LogRotator::new(config);

        assert!(!rotator.check_and_rotate().await?);

        rotator.last_rotation_date = Some(chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());

        assert!(rotator.check_and_rotate().await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_rotation() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("compress.log");
        let log_path_str = log_path.to_str().unwrap().to_string();
        fs::write(&log_path, "compressed content").await?;

        let config = RotationConfig {
            log_file_path: log_path_str.clone(),
            max_size_bytes: 1,
            max_backups: 3,
            compression: true,
            dry_run: false,
            strategy: RotationStrategy::Size,
            check_interval_secs: 60,
            backup_pattern: None,
        };
        let mut rotator = LogRotator::new(config);

        assert!(rotator.check_and_rotate().await?);
        
        let backups = rotator.list_backups().await?;
        assert_eq!(backups.len(), 1);
        assert!(backups[0].to_str().unwrap().ends_with(".gz"));

        Ok(())
    }

    #[tokio::test]
    async fn test_custom_backup_pattern() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("pattern.log");
        let log_path_str = log_path.to_str().unwrap().to_string();
        fs::write(&log_path, "content").await?;

        let config = RotationConfig {
            log_file_path: log_path_str.clone(),
            max_size_bytes: 1,
            max_backups: 3,
            compression: false,
            dry_run: false,
            strategy: RotationStrategy::Size,
            check_interval_secs: 60,
            backup_pattern: Some("archived_{timestamp}.bak".to_string()),
        };
        let mut rotator = LogRotator::new(config);

        assert!(rotator.check_and_rotate().await?);
        
        let backups = rotator.list_backups().await?;
        assert_eq!(backups.len(), 1);
        let name = backups[0].file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("archived_"));
        assert!(name.ends_with(".bak"));

        Ok(())
    }
}