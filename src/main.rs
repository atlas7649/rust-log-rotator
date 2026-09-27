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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug)]
enum ControlSignal {
    RotateNow,
    ReloadConfig,
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

    // Handle SIGHUP for config reload (Unix only)
    let tx_reload = tx.clone();
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut stream = signal(SignalKind::hangup()).expect("failed to install SIGHUP handler");
        while stream.recv().await.is_some() {
            info!("SIGHUP received, requesting config reload");
            let _ = tx_reload.send(ControlSignal::ReloadConfig).await;
        }
    });

    // Unix domain socket for external triggers
    let tx_socket = tx.clone();
    let socket_config = config.clone();
    tokio::spawn(async move {
        let socket_path = "/tmp/rust-log-rotator.sock";
        
        // Try to remove existing socket before binding
        if std::path::Path::new(socket_path).exists() {
            if let Err(e) = std::fs::remove_file(socket_path) {
                error!(error = %e, "Failed to remove existing unix socket");
            }
        }
        
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
                if let Ok(n) = stream.read(&mut buf).await {
                    let msg = String::from_utf8_lossy(&buf[..n]);
                    let command = msg.trim();
                    
                    if command == "ping" {
                        let _ = stream.write_all(b"pong\n").await;
                    } else if command == "status" {
                        let status_msg = format!(
                            "rotator is running. targets: {}, compression: {}, grace_period: {}s\n", 
                            socket_config.targets.len(),
                            socket_config.compression,
                            socket_config.rotation_grace_period_secs
                        );
                        let _ = stream.write_all(status_msg.as_bytes()).await;
                    } else if command == "reload" {
                        info!("External reload trigger received via socket");
                        let _ = tx_socket.send(ControlSignal::ReloadConfig).await;
                        let _ = stream.write_all(b"reloading\n").await;
                    } else {
                        info!("External rotation trigger received via socket: {}", command);
                        let _ = tx_socket.send(ControlSignal::RotateNow).await;
                        let _ = stream.write_all(b"rotating\n").await;
                    }
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
                    ControlSignal::ReloadConfig => {
                        info!(path = %config_path, "Reloading configuration");
                        match RotationConfig::load_from_file(&config_path).await {
                            Ok(new_config) => {
                                rotator.update_config(new_config);
                                info!("Configuration reloaded successfully");
                            }
                            Err(e) => error!(error = %e, "Failed to reload configuration, keeping old config"),
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
                        let _ = std::fs::remove_file("/tmp/rust-log-rotator.sock");
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
