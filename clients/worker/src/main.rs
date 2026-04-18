use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};
use tlbus_client::{DEFAULT_BUS_SOCKET, WorkerLoopConfig, WorkerReply, run_worker_loop};
use tlbus_core::{BusError, Result};

#[derive(Debug, Clone)]
struct WorkerArgs {
    bus_socket: PathBuf,
    service_name: String,
    service_secret: String,
    capability_address: String,
    timeout_seconds: u64,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(BusError::Configuration(message)) if message == "help requested" => {
            print_usage();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tlbus-worker: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<()> {
    let args = parse_args(env::args().skip(1))?;
    run_worker(args)
}

fn parse_args<I>(mut args: I) -> Result<WorkerArgs>
where
    I: Iterator<Item = String>,
{
    let mut bus_socket = PathBuf::from(DEFAULT_BUS_SOCKET);
    let mut service_name = None;
    let mut service_secret = None;
    let mut capability_address = None;
    let mut timeout_seconds = 20_u64;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bus-socket" => bus_socket = PathBuf::from(next_value(&mut args, "--bus-socket")?),
            "--service-name" => service_name = Some(next_value(&mut args, "--service-name")?),
            "--service-secret" => service_secret = Some(next_value(&mut args, "--service-secret")?),
            "--capability-address" => {
                capability_address = Some(next_value(&mut args, "--capability-address")?)
            }
            "--timeout-seconds" => {
                let raw = next_value(&mut args, "--timeout-seconds")?;
                timeout_seconds = raw.parse().map_err(|_| {
                    BusError::Configuration(format!(
                        "invalid `--timeout-seconds` value `{raw}`: expected integer"
                    ))
                })?;
            }
            "-h" | "--help" => return Err(BusError::Configuration("help requested".to_string())),
            _ => return Err(BusError::Configuration(format!("unknown argument `{arg}`"))),
        }
    }

    let service_name = require_value(service_name, "--service-name")?;
    let capability_address = capability_address.unwrap_or_else(|| format!("{service_name}.handle"));

    Ok(WorkerArgs {
        bus_socket,
        service_name,
        service_secret: require_value(service_secret, "--service-secret")?,
        capability_address,
        timeout_seconds,
    })
}

fn run_worker(args: WorkerArgs) -> Result<()> {
    let worker_name = args.service_name.clone();
    let config = WorkerLoopConfig::new(
        worker_name.clone(),
        args.service_secret,
        args.capability_address,
    )
    .with_bus_socket(args.bus_socket)
    .with_timeout(Duration::from_secs(args.timeout_seconds));

    run_worker_loop(config, move |envelope| {
        let payload = parse_payload(&envelope.payload);
        let response = json!({
            "status": "ok",
            "worker": worker_name.clone(),
            "from": envelope.from,
            "target": envelope.to,
            "payload": payload,
        });
        Ok(Some(WorkerReply::json(response)))
    })
}

fn parse_payload(payload: &[u8]) -> Value {
    serde_json::from_slice(payload)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(payload).into_owned()))
}

fn next_value<I>(args: &mut I, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| BusError::Configuration(format!("missing value after `{flag}`")))
}

fn require_value<T>(value: Option<T>, flag: &str) -> Result<T> {
    value.ok_or_else(|| BusError::Configuration(format!("missing required argument `{flag}`")))
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!(
        "  tlbus-worker --service-name <name> --service-secret <secret> [--capability-address <service.action>] [--bus-socket /run/tlb.sock] [--timeout-seconds 20]"
    );
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::parse_payload;

    #[test]
    fn parse_payload_accepts_json() {
        let parsed = parse_payload(br#"{"status":"ok"}"#);
        assert_eq!(parsed["status"], Value::String("ok".to_string()));
    }

    #[test]
    fn parse_payload_falls_back_to_utf8() {
        let parsed = parse_payload(b"hello");
        assert_eq!(parsed, Value::String("hello".to_string()));
    }
}
