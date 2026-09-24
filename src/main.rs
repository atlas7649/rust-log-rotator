mod config;
mod rotator;

use config::RotationConfig;
use rotator::LogRotator;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = RotationConfig::default();
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
