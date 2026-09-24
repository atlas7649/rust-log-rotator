use crate::config::{RotationConfig, RotationStrategy};
use anyhow::{Context, Result};
use std::path::Path;
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

        let oldest_path = format!("{}.{}{}", self.config.log_file_path, self.config.max_backups, ext);
        if Path::new(&oldest_path).exists() {
            fs::remove_file(oldest_path).await
                .context("Failed to remove oldest backup file")?;
        }

        for i in (1..self.config.max_backups).rev() {
            let current_backup = format!("{}.{}{}", self.config.log_file_path, i, ext);
            let next_backup = format!("{}.{}{}", self.config.log_file_path, i + 1, ext);
            if Path::new(&current_backup).exists() {
                fs::rename(current_backup, next_backup).await
                    .context(format!("Failed to rotate backup {} to {}", i, i + 1))?;
            }
        }

        let first_backup = format!("{}.1{}", self.config.log_file_path, ext);
        if self.config.compression {
            self.compress_and_move(&self.config.log_file_path, &first_backup).await?
        } else {
            fs::rename(&self.config.log_file_path, first_backup).await
                .context("Failed to rename log file to first backup")?;
        }

        Ok(())
    }

    async fn compress_and_move(&self, src: &str, dst: &str) -> Result<()> {
        let src_path = Path::new(src);
        let dst_path = Path::new(dst);

        let src_path_owned = src_path.to_path_buf();
        let dst_path_owned = dst_path.to_path_buf();

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

        fs::remove_file(src_path).await
            .context("Failed to remove original log file after compression")?;

        Ok(())
    }
}