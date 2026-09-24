mod config;
mod rotator;

use config::RotationConfig;
use rotator::LogRotator;
use std::time::Duration;
use tokio::time::sleep;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = env::args().nth(1).unwrap_or_else(|| "config.json".to_string());
    
    let config = match RotationConfig::load_from_file(&config_path).await {
        Ok(cfg) => {
            println!("Loaded configuration from {}", config_path);
            cfg
        },
        Err(e) => {
            eprintln!("Could not load config from {}: {}. Using defaults.", config_path, e);
            RotationConfig::default()
        }
    };

    let rotator = LogRotator::new(config.clone());

    println!("Monitoring log file: {}", config.log_file_path);
    println!("Max size: {} bytes", config.max_size_bytes);

    loop {
        match rotator.check_and_rotate().await {
            Ok(true) => println!("Log rotated successfully at {}", chrono::Local::now()),
            Ok(false) => {},
            Err(e) => eprintln!("Error during rotation check: {}", e),
        }
        sleep(Duration::from_secs(60)).await;
    }
}
