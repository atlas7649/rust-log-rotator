mod config;
mod rotator;

use config::RotationConfig;
use rotator::LogRotator;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior, timeout};
use std::env;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;
use serde::Serialize;

#[derive(Debug)]
enum ControlSignal {
    RotateNow,
    RotateTarget(String),
    ReloadConfig,
    Shutdown,
    GetStats(tokio::sync::oneshot::Sender<String>),
    GetStatus(tokio::sync::oneshot::Sender<String>),
}

#[derive(Serialize)]
struct TargetStatus {
    path: String,
    max_size: Option<u64>,
    strategy: String,
}

#[derive(Serialize)]
struct StatusResponse {
    targets_count: usize,
    targets: Vec<TargetStatus>,
    compression: bool,
    grace_period_secs: u64,
    max_age_days: u64,
    dry_run: bool,
    uptime_secs: u64,
    status: String,
    last_check_interval_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Determine log level from environment variable RUST_LOG, default to INFO
    let log_level_str = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let log_level = match log_level_str.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // Initialize tracing subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
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

    let start_time = std::time::Instant::now();
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

    // Handle Termination signals
    let tx_shutdown = tx.clone();
    tokio::spawn(async move {
        let ctrl_c = signal::ctrl_c();
        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::{signal, SignalKind};
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler").recv()
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("SIGINT received"),
            _ = terminate => info!("SIGTERM received"),
        }
        let _ = tx_shutdown.send(ControlSignal::Shutdown).await;
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
    tokio::spawn(async move {
        let socket_path = "/tmp/rust-log-rotator.sock";
        
        // Try to remove existing socket before binding
        if std::path::Path::new(socket_path).exists() {
            if let Err(e) = std::fs::remove_file(socket_path) {
                error!(error = %e, "Failed to remove existing unix socket at {}", socket_path);
            }
        }
        
        let listener = match UnixListener::bind(socket_path) {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, "Failed to bind unix socket at {}", socket_path);
                return;
            }
        };
        
        info!(socket = %socket_path, "Listening for external rotation triggers");
        
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                // Add a timeout to prevent hanging on dead connections
                if let Ok(Ok(n)) = timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                    if n == 0 { continue; }
                    let msg = String::from_utf8_lossy(&buf[..n]);
                    let command = msg.trim();
                    
                    if command.is_empty() {
                        continue;
                    }

                    if command == "ping" {
                        let _ = stream.write_all(b"pong\n").await;
                    } else if command == "health" {
                        let _ = stream.write_all(b"ok\n").await;
                    } else if command == "status" {
                        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                        let _ = tx_socket.send(ControlSignal::GetStatus(resp_tx)).await;
                        if let Ok(status) = resp_rx.await {
                            let _ = stream.write_all(status.as_bytes()).await;
                            let _ = stream.write_all(b"\n").await;
                        } else {
                            let _ = stream.write_all(b"error: could not retrieve status from rotator\n").await;
                        }
                    } else if command == "config" {
                        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                        let _ = tx_socket.send(ControlSignal::GetStatus(resp_tx)).await;
                        if let Ok(status_json) = resp_rx.await {
                            let _ = stream.write_all(format!("Current status/config: {}\n", status_json).as_bytes()).await;
                        } else {
                            let _ = stream.write_all(b"error: failed to retrieve configuration\n").await;
                        }
                    } else if command == "stats" {
                        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                        let _ = tx_socket.send(ControlSignal::GetStats(resp_tx)).await;
                        if let Ok(stats) = resp_rx.await {
                            let _ = stream.write_all(format!("Operational Statistics:\n{}", stats).as_bytes()).await;
                            let _ = stream.write_all(b"\n").await;
                        }
                    } else if command == "reload" {
                        info!("External reload trigger received via socket");
                        let _ = tx_socket.send(ControlSignal::ReloadConfig).await;
                        let _ = stream.write_all(b"configuration reload requested\n").await;
                    } else if command == "force" {
                        info!("External force rotation trigger received via socket");
                        let _ = tx_socket.send(ControlSignal::RotateNow).await;
                        let _ = stream.write_all(b"global rotation forced\n").await;
                    } else if command.starts_with("rotate ") {
                        let target_file = command["rotate ".len()..].trim().to_string();
                        info!(target = %target_file, "External targeted rotation trigger received via socket");
                        let _ = tx_socket.send(ControlSignal::RotateTarget(target_file)).await;
                        let _ = stream.write_all(b"targeted rotation requested\n").await;
                    } else {
                        info!(command = %command, "External rotation trigger received via socket");
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
                    ControlSignal::RotateTarget(target_path) => {
                        info!(target = %target_path, "Manual targeted rotation trigger received");
                        if let Err(e) = rotator.rotate_specific_target(&target_path).await {
                            error!(target = %target_path, error = %e, "Error during targeted rotation");
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
                    ControlSignal::GetStats(resp_tx) => {
                        let stats = rotator.get_operational_stats().await;
                        let _ = resp_tx.send(stats);
                    }
                    ControlSignal::GetStatus(resp_tx) => {
                        let targets = rotator.config.targets.iter().map(|t| TargetStatus {
                            path: t.log_file_path.clone(),
                            max_size: t.max_size_bytes,
                            strategy: format!("{:?}", rotator.config.resolve_strategy(t)),
                        }).collect();

                        let status_data = StatusResponse {
                            targets_count: rotator.config.targets.len(),
                            targets,
                            compression: rotator.config.compression,
                            grace_period_secs: rotator.config.rotation_grace_period_secs,
                            max_age_days: rotator.config.max_age_days,
                            dry_run: rotator.config.dry_run,
                            uptime_secs: start_time.elapsed().as_secs(),
                            status: "running".to_string(),
                            last_check_interval_secs: rotator.config.check_interval_secs,
                        };
                        let status = match serde_json::to_string(&status_data) {
                            Ok(json) => json,
                            Err(_) => "error: failed to serialize status".to_string(),
                        };
                        let _ = resp_tx.send(status);
                    }
                    ControlSignal::Shutdown => {
                        info!("Shutdown signal received. Performing final check...");
                        match rotator.check_and_rotate_all().await {
                            Ok(n) if n > 0 => info!(count = n, "Final rotation completed successfully"),
                            Ok(_) => info!("No rotation needed during shutdown"),
                            Err(e) => error!(error = %e, "Error during final rotation check"),
                        }
                        info!("Shutting down log rotator...");
                        if let Err(e) = std::fs::remove_file("/tmp/rust-log-rotator.sock") {
                            warn!(error = %e, "Failed to remove unix socket on shutdown");
                        }
                        break;
                    }
                }
            }
            _ = check_interval.tick() => {
                match rotator.check_and_rotate_all().await {
                    Ok(n) if n > 0 => debug!(count = n, "Scheduled rotation performed"),
                    Ok(_) => {},
                    Err(e) => error!(error = %e, "Unexpected error during scheduled rotation check"),
                }
            }
        }
    }

    Ok(())
}
