use crate::config::RotationConfig;
use anyhow::{Context, Result};
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

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
            if self.config.dry_run {
                println!("[Dry Run] Log file {} exceeded size, would rotate", self.config.log_file_path);
                return Ok(false);
            }
            self.rotate().await?;
            return Ok(true);
        }

        Ok(false)
    }

    async fn rotate(&self) -> Result<()> {
        let ext = if self.config.compression { ".gz" } else { "" };

        // 1. Remove the oldest backup if it exists to make room
        let oldest_path = format!("{}.{}{}", self.config.log_file_path, self.config.max_backups, ext);
        if Path::new(&oldest_path).exists() {
            fs::remove_file(oldest_path).await
                .context("Failed to remove oldest backup file")?;
        }

        // 2. Shift existing backups (max-1 -> max, ..., 1 -> 2)
        for i in (1..self.config.max_backups).rev() {
            let current_backup = format!("{}.{}{}", self.config.log_file_path, i, ext);
            let next_backup = format!("{}.{}{}", self.config.log_file_path, i + 1, ext);
            if Path::new(&current_backup).exists() {
                fs::rename(current_backup, next_backup).await
                    .context(format!("Failed to rotate backup {} to {}", i, i + 1))?;
            }
        }

        // 3. Move current log to .1 (and compress if enabled)
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

        // Use spawn_blocking for the synchronous compression part to avoid blocking the tokio executor
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