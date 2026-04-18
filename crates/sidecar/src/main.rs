use std::env;
use std::process::ExitCode;

use tlbus_core::{BusError, Result};
use tlbus_sidecar::{Sidecar, parse_config};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(BusError::Configuration(message)) if message == "help requested" => {
            print_usage();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tlbus-sidecar: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let hmac_key = env::var("TLB_HMAC_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(String::into_bytes);
    let config = parse_config(env::args().skip(1), hmac_key)?;
    Sidecar::new(config)?.serve().await
}

fn print_usage() {
    eprintln!("usage: tlbus-sidecar [--peer host:port ...] [--listen host:port] \\");
    eprintln!(
        "                     [--bus-socket /run/tlbus/tlb.sock] [--bridge-socket /run/tlbus/tlbnet.sock] [--service-socket-dir /run/tlbus] \\"
    );
    eprintln!("                     [--advertise host:port] [--manifest-sync-ms 2000] \\");
    eprintln!("                     [service=/path/to/socket.sock ...]");
    eprintln!("defaults and env:");
    eprintln!("  TLB_BUS_MODE=sidecar");
    eprintln!("  TLB_BUS_POOL=<required if --pool is omitted>");
    eprintln!("  TLB_BUS_SOCKET=/run/tlbus/tlb.sock");
    eprintln!("  TLB_BUS_BRIDGE_SOCKET=/run/tlbus/tlbnet.sock");
    eprintln!("  TLB_BUS_PORT=8202");
    eprintln!("  TLB_BUS_ADVERTISE_ADDR=127.0.0.1:<TLB_BUS_PORT>");
    eprintln!("  TLB_REFRESH_MANIFEST_MS=2000");
    eprintln!("  TLBUS_PLUGINS=lineage,auth,protocol[,hmac][,observability]");
    eprintln!("       set TLB_HMAC_KEY to enable the HMAC plugin");
    eprintln!("       set TLB_SERVICE_SECRET to enable service registration handshake");
    eprintln!("       set TLB_POOL_SECRET to enable pool-to-pool federation handshake");
    eprintln!(
        "       set TLB_METRICS_ADDR=host:port for the observability plugin (default 127.0.0.1:9090)"
    );
}
