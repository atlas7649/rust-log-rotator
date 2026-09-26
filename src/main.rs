mod config;
mod rotator;

use config::RotationConfig;
use rotator::LogRotator;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use std::env;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::net::UnixListener;
use tokio::io::AsyncReadExt;
use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug)]
enum ControlSignal {
    RotateNow,
    Shutdown,
}

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
    let (tx, mut rx) = mpsc::channel::<ControlSignal>(32);

    info!(
        targets = ?config.targets.len(), 
        interval = config.check_interval_secs, 
        "Monitoring started"
    );
    
    if config.dry_run {
        info!("Dry run mode enabled - no files will be modified");
    }

    let mut check_interval = interval(Duration::from_secs(config.check_interval_secs));
    check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Handle Ctrl-C in a separate task to send Shutdown signal
    let tx_shutdown = tx.clone();
    tokio::spawn(async move {
        if let Ok(_) = signal::ctrl_c().await {
            let _ = tx_shutdown.send(ControlSignal::Shutdown).await;
        }
    });

    // Unix domain socket for external triggers
    let tx_socket = tx.clone();
    tokio::spawn(async move {
        let socket_path = "/tmp/rust-log-rotator.sock";
        let _ = std::fs::remove_file(socket_path);
        
        let listener = match UnixListener::bind(socket_path) {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, "Failed to bind unix socket");
                return;
            }
        };
        
        info!(socket = %socket_path, "Listening for external rotation triggers");
        
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                if let Ok(_) = stream.read(&mut buf).await {
                    info!("External rotation trigger received via socket");
                    let _ = tx_socket.send(ControlSignal::RotateNow).await;
                }
            }
        }
    });

    loop {
        tokio::select! {
            Some(sig) = rx.recv() => {
                match sig {
                    ControlSignal::RotateNow => {
                        info!("Manual rotation trigger received");
                        match rotator.check_and_rotate_all().await {
                            Ok(n) if n > 0 => info!(count = n, "Manual rotation successful"),
                            Ok(_) => info!("Manual rotation not needed"),
                            Err(e) => error!(error = %e, "Error during manual rotation"),
                        }
                    }
                    ControlSignal::Shutdown => {
                        info!("Shutdown signal received. Performing final check...");
                        match rotator.check_and_rotate_all().await {
                            Ok(n) if n > 0 => info!(count = n, "Final rotation completed successfully"),
                            Ok(_) => info!("No rotation needed during shutdown"),
                            Err(e) => error!(error = %e, "Error during final rotation check"),
                        }
                        info!("Shutting down log rotator...");
                        break;
                    }
                }
            }
            _ = check_interval.tick() => {
                match rotator.check_and_rotate_all().await {
                    Ok(n) if n > 0 => info!(timestamp = %chrono::Local::now(), count = n, "Logs rotated successfully"),
                    Ok(_) => {},
                    Err(e) => error!(error = %e, "Error during rotation check"),
                }
            }
        }
    }

    Ok(())
}