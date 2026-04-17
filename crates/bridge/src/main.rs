use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use tlbus_bridge::{Bridge, BridgeConfig, DEFAULT_MANIFEST_SYNC_INTERVAL};
use tlbus_core::{BusError, Result};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(BusError::Configuration(message)) if message == "help requested" => {
            print_usage();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tlbus-bridge: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let config = parse_config(env::args().skip(1))?;
    Bridge::new(config).serve().await
}

fn parse_config<I>(args: I) -> Result<BridgeConfig>
where
    I: IntoIterator<Item = String>,
{
    let mut pool = None;
    let mut bus_socket = PathBuf::from("/run/tlb.sock");
    let mut ingress_socket = PathBuf::from("/run/tlbnet.sock");
    let mut listen_addr: Option<std::net::SocketAddr> = None;
    let mut advertise_addr = None;
    let mut peers = Vec::new();
    let mut manifest_sync_interval = DEFAULT_MANIFEST_SYNC_INTERVAL;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--pool" => pool = Some(next_value(&mut args, "--pool")?),
            "--bus-socket" => bus_socket = PathBuf::from(next_value(&mut args, "--bus-socket")?),
            "--ingress-socket" => {
                ingress_socket = PathBuf::from(next_value(&mut args, "--ingress-socket")?)
            }
            "--listen" => {
                let value = next_value(&mut args, "--listen")?;
                listen_addr = Some(value.parse().map_err(|_| {
                    BusError::Configuration(format!(
                        "invalid `--listen` address `{value}`: expected host:port"
                    ))
                })?);
            }
            "--advertise" => advertise_addr = Some(next_value(&mut args, "--advertise")?),
            "--peer" => peers.push(parse_peer_addr(&next_value(&mut args, "--peer")?)?),
            "--manifest-sync-ms" => {
                let value = next_value(&mut args, "--manifest-sync-ms")?;
                let millis = value.parse::<u64>().map_err(|_| {
                    BusError::Configuration(format!(
                        "invalid `--manifest-sync-ms` value `{value}`: expected integer milliseconds"
                    ))
                })?;
                manifest_sync_interval = Duration::from_millis(millis.max(250));
            }
            "-h" | "--help" => return Err(BusError::Configuration("help requested".to_string())),
            _ => return Err(BusError::Configuration(format!("unknown argument `{arg}`"))),
        }
    }

    let listen_addr = require_value(listen_addr, "--listen")?;
    let advertise_addr = advertise_addr.unwrap_or_else(|| listen_addr.to_string());
    let pool_secret = env::var("TLB_POOL_SECRET")
        .ok()
        .filter(|value| !value.trim().is_empty());

    Ok(BridgeConfig::new(
        require_value(pool, "--pool")?,
        bus_socket,
        ingress_socket,
        listen_addr,
        advertise_addr,
        tlbus_core::Router::new(),
        peers,
        manifest_sync_interval,
        pool_secret,
    ))
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

fn parse_peer_addr(value: &str) -> Result<String> {
    if value.trim().is_empty() || !value.contains(':') {
        return Err(BusError::Configuration(format!(
            "invalid peer `{value}`: expected `host:port`"
        )));
    }

    Ok(value.to_string())
}

fn print_usage() {
    eprintln!("usage: tlbus-bridge --pool ps1 --listen 127.0.0.1:8101 \\");
    eprintln!(
        "                    [--bus-socket /run/tlb.sock] [--ingress-socket /run/tlbnet.sock] \\"
    );
    eprintln!(
        "                    [--advertise host:port] [--peer host:port ...] [--manifest-sync-ms 5000]"
    );
    eprintln!("       set TLB_POOL_SECRET to enable sidecar federation handshake");
}
