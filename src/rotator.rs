use crate::config::{RotationConfig, RotationStrategy, RotationTarget, BackupNaming};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use tracing::{info, debug, warn};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub struct LogRotator {
    pub config: RotationConfig,
    last_rotation_dates: std::collections::HashMap<String, chrono::NaiveDate>,
    last_rotation_times: std::collections::HashMap<String, chrono::DateTime<chrono::Local>>,
}

impl LogRotator {
    pub fn new(config: RotationConfig) -> Self {
        Self {
            config,
            last_rotation_dates: std::collections::HashMap::new(),
            last_rotation_times: std::collections::HashMap::new(),
        }
    }

    pub fn update_config(&mut self, config: RotationConfig) {
        info!("Updating rotator configuration");
        self.config = config;
    }

    pub async fn get_operational_stats(&self) -> String {
        let mut total_count = 0;
        let mut total_size = 0u64;
        let mut target_stats = Vec::new();

        for target in &self.config.targets {
            let mut count = 0;
            let mut size = 0u64;
            if let Ok(backups) = self.list_backups(target).await {
                count = backups.len();
                for path in backups {
                    if let Ok(meta) = fs::metadata(path).await {
                        size += meta.len();
                    }
                }
            }
            total_count += count;
            total_size += size;
            target_stats.push(format!("  - {}: count={}, size={} bytes", target.log_file_path, count, size));
        }

        format!(
            "Operational Stats:\nTotal Backups: {}\nTotal Size: {} bytes\nPer-Target Details:\n{}", 
            total_count, 
            total_size, 
            target_stats.join("\n")
        )
    }

    pub async fn check_and_rotate_all(&mut self) -> Result<usize> {
        let mut rotated_count = 0;
        for target in &self.config.targets {
            if self.check_and_rotate_target(target).await? {
                rotated_count += 1;
            }
        }
        Ok(rotated_count)
    }

    async fn check_and_rotate_target(&mut self, target: &RotationTarget) -> Result<bool> {
        let path = Path::new(&target.log_file_path);
        if !path.exists() {
            debug!("Log file does not exist, skipping check: {}", target.log_file_path);
            return Ok(false);
        }

        // Grace period check: prevent rotating too frequently
        if let Some(last_time) = self.last_rotation_times.get(&target.log_file_path) {
            let elapsed = chrono::Local::now().signed_duration_since(*last_time);
            if elapsed.num_seconds() < self.config.rotation_grace_period_secs as i64 {
                return Ok(false);
            }
        }

        let strategy = target.strategy.as_ref().unwrap_or(&self.config.default_strategy);
        let max_size = target.max_size_bytes.unwrap_or(self.config.default_max_size_bytes);

        let should_rotate = match strategy {
            RotationStrategy::Size => {
                let metadata = fs::metadata(path).await?;
                metadata.len() >= max_size
            }
            RotationStrategy::Daily => {
                let today = chrono::Local::now().date_naive();
                let last_date = self.last_rotation_dates.get(&target.log_file_path);
                match last_date {
                    Some(date) if *date != today => true,
                    Some(_) => false,
                    None => {
                        self.last_rotation_dates.insert(target.log_file_path.clone(), today);
                        false
                    }
                }
            }
            RotationStrategy::Age => {
                let metadata = fs::metadata(path).await?;
                let created = metadata.created().with_context(|| format!("Failed to get creation time for {}", target.log_file_path))?;
                let age = chrono::Local::now().signed_duration_since(chrono::DateTime::from(created));
                age.num_days() >= self.config.max_age_days as i64
            }
        };

        if should_rotate {
            if self.config.dry_run {
                info!(
                    "[Dry Run] Log file {} triggered rotation strategy {:?}, would rotate", 
                    target.log_file_path, 
                    strategy
                );
                return Ok(false);
            }
            self.rotate_target(target).await?;
            let now = chrono::Local::now();
            self.last_rotation_dates.insert(target.log_file_path.clone(), now.date_naive());
            self.last_rotation_times.insert(target.log_file_path.clone(), now);
            return Ok(true);
        }

        Ok(false)
    }

    async fn rotate_target(&self, target: &RotationTarget) -> Result<()> {
        let ext = if self.config.compression { ".gz" } else { "" };
        
        // 1. Local Pruning: Maintain per-target limits first
        let backups = self.list_backups(target).await?;
        
        let mut metadata_list = Vec::new();
        for path in &backups {
            if let Ok(meta) = fs::metadata(path).await {
                if let Ok(created) = meta.created() {
                    metadata_list.push((path.clone(), created, meta.len()));
                }
            }
        }
        metadata_list.sort_by_key(|&(_, created, _)| created);

        let now = chrono::Local::now();
        let mut remaining_local = Vec::new();
        for (path, created, size) in metadata_list {
            let age = now.signed_duration_since(chrono::DateTime::from(created));
            if age.num_days() >= self.config.max_age_days as i64 {
                debug!("Removing expired local backup: {:?}", path);
                let _ = fs::remove_file(path).await;
            } else {
                remaining_local.push((path, created, size));
            }
        }

        let max_backups = target.max_backups.unwrap_or(self.config.default_max_backups);
        if remaining_local.len() >= max_backups {
            let to_remove = remaining_local.len() - max_backups + 1;
            for i in 0..to_remove {
                let (path, _, _) = &remaining_local[i];
                debug!("Removing oldest local backup to maintain limit: {:?}", path);
                let _ = fs::remove_file(path).await;
            }
        }

        // 2. Global Pruning: Only if config specifies a global limit
        if let Some(max_total_size) = self.config.max_total_backup_size_bytes {
            self.prune_global_backups(max_total_size).await?;
        }

        // 3. Rotate current log
        let naming_style = target.naming_style.as_ref().unwrap_or(&self.config.default_naming_style);
        let backup_name = match naming_style {
            BackupNaming::Timestamp => {
                let timestamp = chrono::Local::now().format("%Y%m%d-%H%M%S").to_string();
                if let Some(ref pattern) = target.backup_pattern {
                    pattern.replace("{timestamp}", &timestamp) + ext
                } else {
                    format!("{}_{}{}", target.log_file_path, timestamp, ext)
                }
            },
            BackupNaming::Sequential => {
                let mut next_idx = 1;
                loop {
                    let candidate = format!("{}.{}{}", target.log_file_path, next_idx, ext);
                    if !Path::new(&candidate).exists() {
                        break candidate;
                    }
                    next_idx += 1;
                }
            }
        };

        if fs::metadata(&backup_name).await.is_ok() {
            return Err(anyhow::anyhow!("Backup file {} already exists, skipping rotation to prevent overwrite", backup_name));
        }

        // Ensure target directory exists
        if let Some(parent) = Path::new(&backup_name).parent() {
            fs::create_dir_all(parent).await.context("Failed to create backup directory")?;
        }

        // Capture original permissions
        let original_permissions = fs::metadata(&target.log_file_path).await?.permissions();

        if self.config.compression {
            self.compress_and_move(&target.log_file_path, &backup_name).await?;
        } else {
            let content = fs::read(&target.log_file_path).await
                .context("Failed to read log file for rotation")?;
            fs::write(&backup_name, content).await
                .context("Failed to write rotated log file")?;
            
            fs::write(&target.log_file_path, b"").await
                .context("Failed to truncate original log file")?;
        }

        // Preserve permissions on the backup file
        if let Err(e) = fs::set_permissions(&backup_name, original_permissions).await {
            warn!(error = %e, "Failed to preserve permissions for backup file {}", backup_name);
        }

        Ok(())
    }

    async fn prune_global_backups(&self, max_total_size: u64) -> Result<()> {
        let mut all_backups = Vec::new();
        for t in &self.config.targets {
            let t_backups = self.list_backups(t).await?;
            for pb in t_backups {
                if let Ok(meta) = fs::metadata(&pb).await {
                    if let Ok(created) = meta.created() {
                        all_backups.push((pb, created, meta.len()));
                    }
                }
            }
        }
        
        all_backups.sort_by_key(|&(_, created, _)| created);

        let mut current_total_size: u64 = all_backups.iter().map(|(_, _, size)| *size).sum();
        if current_total_size <= max_total_size {
            return Ok(());
        }

        for (path, _, size) in all_backups {
            if current_total_size <= max_total_size {
                break;
            }
            debug!("Removing backup {:?} to maintain global size limit", path);
            if fs::remove_file(path).await.is_ok() {
                current_total_size -= size;
            }
        }
        Ok(())
    }

    async fn list_backups(&self, target: &RotationTarget) -> Result<Vec<PathBuf>> {
        let path = Path::new(&target.log_file_path);
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let filename = path.file_name().context("Invalid log file path")?;
        let filename_str = filename.to_string_lossy();
        let ext = if self.config.compression { ".gz" } else { "" };

        let mut backups = Vec::new();
        let mut entries = fs::read_dir(parent).await
            .context("Failed to read log directory")?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            
            if path == Path::new(&target.log_file_path) {
                continue;
            }

            if let Some(name) = path.file_name() {
                let name_str = name.to_string_lossy();
                
                if !name_str.ends_with(ext) {
                    continue;
                }

                let is_backup = if let Some(ref pattern) = target.backup_pattern {
                    if let Some(prefix) = pattern.split("{timestamp}").next() {
                        if let Some(suffix) = pattern.split("{timestamp}").last() {
                            name_str.starts_with(prefix) && name_str.contains(suffix)
                        } else {
                            name_str.starts_with(prefix)
                        }
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

        fs::write(src, b"").await
            .context("Failed to truncate original log file after compression")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RotationConfig, RotationStrategy, RotationTarget};
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_config_update() {
        let config1 = RotationConfig::default();
        let mut rotator = LogRotator::new(config1.clone());
        
        let mut config2 = config1.clone();
        config2.compression = !config1.compression;
        
        rotator.update_config(config2.clone());
        assert_eq!(rotator.config.compression, config2.compression);
    }

    #[tokio::test]
    async fn test_size_rotation_trigger() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("test.log");
        let log_path_str = log_path.to_str().unwrap().to_string();

        fs::write(&log_path, "small content").await?;

        let config = RotationConfig {
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: Some(100),
                max_backups: Some(3),
                strategy: Some(RotationStrategy::Size),
                backup_pattern: None,
                naming_style: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: None,
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 1024,
            default_max_backups: 5,
            default_strategy: RotationStrategy::Size,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        assert_eq!(rotator.check_and_rotate_all().await?, 0);

        let large_content = "a".repeat(101);
        fs::write(&log_path, large_content).await?;

        assert_eq!(rotator.check_and_rotate_all().await?, 1);
        assert!(log_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_backup_limit() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("limit.log");
        let log_path_str = log_path.to_str().unwrap().to_string();

        let config = RotationConfig {
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: Some(10),
                max_backups: Some(2),
                strategy: Some(RotationStrategy::Size),
                backup_pattern: None,
                naming_style: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: None,
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 10,
            default_max_backups: 2,
            default_strategy: RotationStrategy::Size,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        for _ in 0..3 {
            fs::write(&log_path, "some content").await?;
            rotator.check_and_rotate_all().await?;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let backups = rotator.list_backups(&RotationTarget {
            log_file_path: log_path_str.clone(),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: None,
            naming_style: None,
        }).await?;
        assert_eq!(backups.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_total_size_limit() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("size_limit.log");
        let log_path_str = log_path.to_str().unwrap().to_string();

        let config = RotationConfig {
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: Some(10),
                max_backups: Some(10),
                strategy: Some(RotationStrategy::Size),
                backup_pattern: None,
                naming_style: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: Some(25),
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 10,
            default_max_backups: 10,
            default_strategy: RotationStrategy::Size,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        for _ in 0..3 {
            fs::write(&log_path, "123456789012").await?;
            rotator.check_and_rotate_all().await?;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let backups = rotator.list_backups(&RotationTarget {
            log_file_path: log_path_str.clone(),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: None,
            naming_style: None,
        }).await?;
        assert!(backups.len() < 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_rotation_trigger() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("daily.log");
        let log_path_str = log_path.to_str().unwrap().to_string();
        fs::write(&log_path, "content").await?;

        let config = RotationConfig {
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: None,
                max_backups: None,
                strategy: Some(RotationStrategy::Daily),
                backup_pattern: None,
                naming_style: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: None,
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 1024 * 1024,
            default_max_backups: 3,
            default_strategy: RotationStrategy::Daily,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        assert_eq!(rotator.check_and_rotate_all().await?, 0);

        rotator.last_rotation_dates.insert(log_path_str.clone(), chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());

        assert_eq!(rotator.check_and_rotate_all().await?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_age_rotation_trigger() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("age.log");
        let log_path_str = log_path.to_str().unwrap().to_string();
        fs::write(&log_path, "content").await?;

        let config = RotationConfig {
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: None,
                max_backups: None,
                strategy: Some(RotationStrategy::Age),
                backup_pattern: None,
                naming_style: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 0, // Trigger immediately
            max_total_backup_size_bytes: None,
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 1024 * 1024,
            default_max_backups: 3,
            default_strategy: RotationStrategy::Age,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        assert_eq!(rotator.check_and_rotate_all().await?, 1);
        assert!(log_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_rotation() -> Result<()> {
        let dir = tempdir()?;
        let log_path = dir.path().join("compress.log");
        let log_path_str = log_path.to_str().unwrap().to_string();
        fs::write(&log_path, "compressed content").await?;

        let config = RotationConfig {
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: Some(1),
                max_backups: Some(3),
                strategy: Some(RotationStrategy::Size),
                backup_pattern: None,
                naming_style: None,
            }],
            compression: true,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: None,
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 1,
            default_max_backups: 3,
            default_strategy: RotationStrategy::Size,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        assert_eq!(rotator.check_and_rotate_all().await?, 1);
        
        let backups = rotator.list_backups(&RotationTarget {
            log_file_path: log_path_str.clone(),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: None,
            naming_style: None,
        }).await?;
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
            targets: vec![RotationTarget {
                log_file_path: log_path_str.clone(),
                max_size_bytes: Some(1),
                max_backups: Some(3),
                strategy: Some(RotationStrategy::Size),
                backup_pattern: Some("archived_{timestamp}.bak".to_string()),
                naming_style: None,
            }],
            compression: false,
            dry_run: false,
            check_interval_secs: 60,
            max_age_days: 7,
            max_total_backup_size_bytes: None,
            rotation_grace_period_secs: 0,
            default_max_size_bytes: 1,
            default_max_backups: 3,
            default_strategy: RotationStrategy::Size,
            default_naming_style: BackupNaming::Timestamp,
        };
        let mut rotator = LogRotator::new(config);

        assert_eq!(rotator.check_and_rotate_all().await?, 1);
        
        let backups = rotator.list_backups(&RotationTarget {
            log_file_path: log_path_str.clone(),
            max_size_bytes: None,
            max_backups: None,
            strategy: None,
            backup_pattern: Some("archived_{timestamp}.bak".to_string()),
            naming_style: None,
        }).await?;
        assert_eq!(backups.len(), 1);
        let name = backups[0].file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("archived_"));
        assert!(name.ends_with(".bak"));

        Ok(())
    }
}
