# Rust Log Rotator

A lightweight, asynchronous log rotation utility built with Rust and Tokio.

## Features
- Size-based rotation
- Configurable backup retention
- Asynchronous I/O for minimal blocking
- Socket-based control interface for external triggers

## Usage
1. Configure the `RotationConfig` in `src/main.rs` or extend it to read from a JSON file.
2. Run with `cargo run`.

## Control Interface

The utility opens a Unix domain socket at `/tmp/rust-log-rotator.sock`. You can send commands using `nc` or similar tools:

- `ping`: Check if the rotator is alive (responds with `pong`)
- `health`: Health check (responds with `ok`)
- `status`: Get a JSON summary of the current operational status
- `config`: Get the current active configuration in JSON format
- `stats`: Get operational statistics
- `reload`: Trigger a configuration reload from the source file
- `force`: Force a rotation check for all targets
- `rotate <path>`: Force rotation for a specific log file

Example:
```bash
echo "status" | nc -U /tmp/rust-log-rotator.sock
```