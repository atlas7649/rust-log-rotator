mod config;
mod rotator;

use config::RotationConfig;
use rotator::LogRotator;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use std::env;
use tokio::signal;
use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let config_path = env::args().nth(1).unwrap_or_else(|| "config.json".to_string());
    
    let config = match RotationConfig::load_from_file(&config_path).await {
        Ok(cfg) => {
            info!(path = %config_path, "Loaded configuration");
            cfg
        },
        Err(e) => {
            warn!(path = %config_path, error = %e, "Could not load config, using defaults");
            RotationConfig::default()
        }
    };

    let mut rotator = LogRotator::new(config.clone());

    info!(
        log_file = %config.log_file_path, 
        strategy = ?config.strategy, 
        interval = config.check_interval_secs, 
        "Monitoring started"
    );
    
    if config.dry_run {
        info!("Dry run mode enabled - no files will be modified");
    }

    let mut check_interval = interval(Duration::from_secs(config.check_interval_secs));
    check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Shutdown signal received. Performing final check...");
                match rotator.check_and_rotate().await {
                    Ok(true) => info!("Final rotation completed successfully"),
                    Ok(false) => info!("No rotation needed during shutdown"),
                    Err(e) => error!(error = %e, "Error during final rotation check"),
                }
                info!("Shutting down log rotator...");
                break;
            }
            _ = check_interval.tick() => {
                match rotator.check_and_rotate().await {
                    Ok(true) => info!(timestamp = %chrono::Local::now(), "Log rotated successfully"),
                    Ok(false) => {},
                    Err(e) => error!(error = %e, "Error during rotation check"),
                }
            }
        }
    }

    Ok(())
}