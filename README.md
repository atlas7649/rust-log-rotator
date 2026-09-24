# Rust Log Rotator

A lightweight, asynchronous log rotation utility built with Rust and Tokio.

## Features
- Size-based rotation
- Configurable backup retention
- Asynchronous I/O for minimal blocking

## Usage
1. Configure the `RotationConfig` in `src/main.rs` or extend it to read from a JSON file.
2. Run with `cargo run`.