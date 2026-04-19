use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use tlbus_core::{BusError, Result, Router};
use tlbus_daemon::{Daemon, DaemonConfig, FederationConfig, build_pipeline, plugin_names_from_env};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tlbus-daemon: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let mut listen_path = PathBuf::from("/run/tlb.sock");
    let mut service_socket_dir = None;
    let mut routes = Vec::new();
    let mut local_pool = None;
    let mut bridge_socket = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                return Ok(());
            }
            "--listen" => {
                let value = args.next().ok_or_else(|| {
                    BusError::Configuration("missing value after `--listen`".to_string())
                })?;
                listen_path = PathBuf::from(value);
            }
            "--service-socket-dir" => {
                let value = args.next().ok_or_else(|| {
                    BusError::Configuration(
                        "missing value after `--service-socket-dir`".to_string(),
                    )
                })?;
                service_socket_dir = Some(PathBuf::from(value));
            }
            "--local-pool" => {
                local_pool = Some(args.next().ok_or_else(|| {
                    BusError::Configuration("missing value after `--local-pool`".to_string())
                })?);
            }
            "--bridge-socket" => {
                bridge_socket = Some(PathBuf::from(args.next().ok_or_else(|| {
                    BusError::Configuration("missing value after `--bridge-socket`".to_string())
                })?));
            }
            _ => {
                if let Some(value) = arg.strip_prefix("--listen=") {
                    listen_path = PathBuf::from(value);
                } else if let Some(value) = arg.strip_prefix("--service-socket-dir=") {
                    service_socket_dir = Some(PathBuf::from(value));
                } else if let Some(value) = arg.strip_prefix("--local-pool=") {
                    local_pool = Some(value.to_string());
                } else if let Some(value) = arg.strip_prefix("--bridge-socket=") {
                    bridge_socket = Some(PathBuf::from(value));
                } else {
                    routes.push(parse_route_arg(&arg)?);
                }
            }
        }
    }

    let hmac_key = env::var("TLB_HMAC_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(String::into_bytes);

    let plugin_names = plugin_names_from_env(
        env::var("TLBUS_PLUGINS").ok().as_deref(),
        hmac_key.as_ref().is_some_and(|key| !key.is_empty()),
    );

    let router = Router::from_routes(routes);
    let pipeline = build_pipeline(router.clone(), &plugin_names, hmac_key, local_pool.clone())?;

    let mut config = DaemonConfig::new(listen_path, router, pipeline)
        .with_service_secret(env::var("TLB_SERVICE_SECRET").ok());
    if let Some(service_socket_dir) = service_socket_dir {
        config = config.with_service_socket_dir(service_socket_dir);
    }

    match (local_pool, bridge_socket) {
        (Some(local_pool), Some(bridge_socket)) => {
            config = config.with_federation(FederationConfig {
                local_pool,
                bridge_socket,
            });
        }
        (None, None) => {}
        _ => {
            return Err(BusError::Configuration(
                "`--local-pool` and `--bridge-socket` must be provided together".to_string(),
            ));
        }
    }

    Daemon::new(config).serve().await
}

fn parse_route_arg(arg: &str) -> Result<(String, PathBuf)> {
    let (service, socket_path) = arg.split_once('=').ok_or_else(|| {
        BusError::Configuration(format!(
            "invalid route `{arg}`: expected `service=/path/to/socket.sock`"
        ))
    })?;

    if service.trim().is_empty() || socket_path.trim().is_empty() {
        return Err(BusError::Configuration(format!(
            "invalid route `{arg}`: service and path must both be present"
        )));
    }

    Ok((service.to_string(), PathBuf::from(socket_path)))
}

fn print_usage() {
    eprintln!(
        "usage: tlbus-daemon [--listen /run/tlb.sock] [--service-socket-dir /run/tlbus] [--local-pool ps1 --bridge-socket /run/tlbnet.sock] [service=/path/to/socket.sock ...]"
    );
    eprintln!("defaults and env:");
    eprintln!("  TLBUS_PLUGINS=lineage,auth,protocol[,hmac][,observability]");
    eprintln!("       set TLB_HMAC_KEY to enable the HMAC plugin");
    eprintln!("       set TLB_SERVICE_SECRET to enable service registration handshake");
    eprintln!(
        "       set TLB_METRICS_ADDR=host:port for the observability plugin (default 127.0.0.1:9090)"
    );
}
