use std::env;
use std::process::ExitCode;

use tlbus_core::{BusError, Result};
use tlbus_send::{SendConfig, send};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(BusError::Configuration(message)) if message == "help requested" => {
            print_usage();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tlbus-send: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let config = SendConfig::parse_from(env::args().skip(1))?;
    send(config).await
}

fn print_usage() {
    eprintln!("usage: tlbus-send [--socket /run/tlb.sock] --from service-a --to service-b \\");
    eprintln!(
        "                  [--ttl-ms 1000] [--transport tl-bus] [--protocol raw] [--protocol-version 2025-06-18] \\"
    );
    eprintln!(
        "                  [--content-type text/plain] [--header key=value ...] --payload \"hello\""
    );
}
