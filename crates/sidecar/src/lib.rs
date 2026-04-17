use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tlbus_bridge::{Bridge, BridgeConfig};
use tlbus_core::{BusError, Result, Router};
use tlbus_daemon::{Daemon, DaemonConfig, FederationConfig, build_pipeline, plugin_names_from_env};

const DEFAULT_BUS_MODE: &str = "sidecar";
const DEFAULT_BUS_SOCKET: &str = "/run/tlbus/tlb.sock";
const DEFAULT_BRIDGE_SOCKET: &str = "/run/tlbus/tlbnet.sock";
const DEFAULT_LISTEN_HOST: &str = "0.0.0.0";
const DEFAULT_ADVERTISE_HOST: &str = "127.0.0.1";
const DEFAULT_BUS_PORT: u16 = 8202;
const DEFAULT_MANIFEST_SYNC_MS: u64 = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarConfig {
    pub pool: String,
    pub bus_socket: PathBuf,
    pub bridge_socket: PathBuf,
    pub service_socket_dir: PathBuf,
    pub listen_addr: SocketAddr,
    pub advertise_addr: String,
    pub routes: Vec<(String, PathBuf)>,
    pub peers: Vec<String>,
    pub manifest_sync_interval: Duration,
    pub plugin_names: Vec<String>,
    pub hmac_key: Option<Vec<u8>>,
    pub service_secret: Option<String>,
    pub pool_secret: Option<String>,
}

impl SidecarConfig {
    pub fn new(
        pool: impl Into<String>,
        bus_socket: impl Into<PathBuf>,
        bridge_socket: impl Into<PathBuf>,
        service_socket_dir: impl Into<PathBuf>,
        listen_addr: SocketAddr,
        advertise_addr: impl Into<String>,
        routes: Vec<(String, PathBuf)>,
        peers: Vec<String>,
        manifest_sync_interval: Duration,
        plugin_names: Vec<String>,
        hmac_key: Option<Vec<u8>>,
        service_secret: Option<String>,
        pool_secret: Option<String>,
    ) -> Self {
        Self {
            pool: pool.into(),
            bus_socket: bus_socket.into(),
            bridge_socket: bridge_socket.into(),
            service_socket_dir: service_socket_dir.into(),
            listen_addr,
            advertise_addr: advertise_addr.into(),
            routes,
            peers,
            manifest_sync_interval,
            plugin_names,
            hmac_key,
            service_secret,
            pool_secret,
        }
    }
}

#[derive(Clone)]
pub struct Sidecar {
    daemon: Daemon,
    bridge: Bridge,
}

impl Sidecar {
    pub fn new(config: SidecarConfig) -> Result<Self> {
        let SidecarConfig {
            pool,
            bus_socket,
            bridge_socket,
            service_socket_dir,
            listen_addr,
            advertise_addr,
            routes,
            peers,
            manifest_sync_interval,
            plugin_names,
            hmac_key,
            service_secret,
            pool_secret,
        } = config;

        let router = Router::from_routes(routes);
        let pipeline = build_pipeline(router.clone(), &plugin_names, hmac_key)?;
        let daemon = Daemon::new(
            DaemonConfig::new(bus_socket.clone(), router.clone(), pipeline)
                .with_service_socket_dir(service_socket_dir)
                .with_service_secret(service_secret)
                .with_federation(FederationConfig {
                    local_pool: pool.clone(),
                    bridge_socket: bridge_socket.clone(),
                }),
        );

        let bridge = Bridge::new(BridgeConfig::new(
            pool,
            bus_socket,
            bridge_socket,
            listen_addr,
            advertise_addr,
            router,
            peers,
            manifest_sync_interval,
            pool_secret,
        ));

        Ok(Self { daemon, bridge })
    }

    pub async fn serve(&self) -> Result<()> {
        tokio::try_join!(self.daemon.serve(), self.bridge.serve())?;
        Ok(())
    }
}

pub fn parse_config<I>(args: I, hmac_key: Option<Vec<u8>>) -> Result<SidecarConfig>
where
    I: IntoIterator<Item = String>,
{
    parse_config_with_env(args, hmac_key, &|key| std::env::var(key).ok())
}

fn parse_config_with_env<I, F>(
    args: I,
    hmac_key: Option<Vec<u8>>,
    env_lookup: &F,
) -> Result<SidecarConfig>
where
    I: IntoIterator<Item = String>,
    F: Fn(&str) -> Option<String>,
{
    let mode = env_lookup("TLB_BUS_MODE").unwrap_or_else(|| DEFAULT_BUS_MODE.to_string());
    if mode != DEFAULT_BUS_MODE {
        return Err(BusError::Configuration(format!(
            "unsupported `TLB_BUS_MODE` `{mode}` for `tlbus-sidecar`: expected `{DEFAULT_BUS_MODE}`"
        )));
    }

    let mut pool = env_lookup("TLB_BUS_POOL");
    let mut bus_socket = PathBuf::from(
        env_lookup("TLB_BUS_SOCKET").unwrap_or_else(|| DEFAULT_BUS_SOCKET.to_string()),
    );
    let mut bridge_socket = PathBuf::from(
        env_lookup("TLB_BUS_BRIDGE_SOCKET").unwrap_or_else(|| DEFAULT_BRIDGE_SOCKET.to_string()),
    );
    let mut service_socket_dir = None;
    let listen_port = env_lookup("TLB_BUS_PORT")
        .map(|value| parse_port_env(&value, "TLB_BUS_PORT"))
        .transpose()?
        .unwrap_or(DEFAULT_BUS_PORT);
    let mut listen_addr = Some(socket_addr(DEFAULT_LISTEN_HOST, listen_port)?);
    // `advertise_addr` is what peers call back, which can differ from the local bind
    // address when the sidecar listens on `0.0.0.0` inside a container.
    let mut advertise_addr =
        env_lookup("TLB_BUS_ADVERTISE_ADDR").filter(|value| !value.trim().is_empty());
    let mut routes = Vec::new();
    let mut peers = Vec::new();
    let mut manifest_sync_interval = Duration::from_millis(
        env_lookup("TLB_REFRESH_MANIFEST_MS")
            .map(|value| parse_duration_env(&value, "TLB_REFRESH_MANIFEST_MS"))
            .transpose()?
            .unwrap_or(DEFAULT_MANIFEST_SYNC_MS)
            .max(250),
    );
    let plugin_names = plugin_names_from_env(
        env_lookup("TLBUS_PLUGINS").as_deref(),
        hmac_key.as_ref().is_some_and(|key| !key.is_empty()),
    );

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--pool" => pool = Some(next_value(&mut args, "--pool")?),
            "--bus-socket" => bus_socket = PathBuf::from(next_value(&mut args, "--bus-socket")?),
            "--bridge-socket" => {
                bridge_socket = PathBuf::from(next_value(&mut args, "--bridge-socket")?)
            }
            "--service-socket-dir" => {
                service_socket_dir = Some(PathBuf::from(next_value(
                    &mut args,
                    "--service-socket-dir",
                )?))
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
                let millis = parse_duration_env(&value, "--manifest-sync-ms")?;
                manifest_sync_interval = Duration::from_millis(millis.max(250));
            }
            "-h" | "--help" => return Err(BusError::Configuration("help requested".to_string())),
            _ => routes.push(parse_route_arg(&arg)?),
        }
    }
    let service_socket_dir = service_socket_dir.unwrap_or_else(|| {
        bus_socket
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    });
    let service_secret = env_lookup("TLB_SERVICE_SECRET").filter(|value| !value.trim().is_empty());
    let listen_addr = require_value(listen_addr, "--listen")?;
    let pool_secret = env_lookup("TLB_POOL_SECRET").filter(|value| !value.trim().is_empty());

    let pool = pool.ok_or_else(|| {
        BusError::Configuration("missing required `TLB_BUS_POOL` or `--pool`".to_string())
    })?;

    Ok(SidecarConfig::new(
        pool,
        bus_socket,
        bridge_socket,
        service_socket_dir,
        listen_addr,
        advertise_addr
            .unwrap_or_else(|| format!("{DEFAULT_ADVERTISE_HOST}:{}", listen_addr.port())),
        routes,
        peers,
        manifest_sync_interval,
        plugin_names,
        hmac_key,
        service_secret,
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

fn parse_port_env(value: &str, source: &str) -> Result<u16> {
    value.parse::<u16>().map_err(|_| {
        BusError::Configuration(format!(
            "invalid `{source}` value `{value}`: expected an integer port"
        ))
    })
}

fn parse_duration_env(value: &str, source: &str) -> Result<u64> {
    value.parse::<u64>().map_err(|_| {
        BusError::Configuration(format!(
            "invalid `{source}` value `{value}`: expected integer milliseconds"
        ))
    })
}

fn socket_addr(host: &str, port: u16) -> Result<SocketAddr> {
    format!("{host}:{port}").parse().map_err(|_| {
        BusError::Configuration(format!(
            "invalid socket address `{host}:{port}` built from defaults"
        ))
    })
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

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use tempfile::tempdir;
    use tlbus_core::{Envelope, ServiceManifest, read_envelope, register_service, write_envelope};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::time::sleep;

    use crate::{DEFAULT_LISTEN_HOST, Sidecar, SidecarConfig, parse_config_with_env, socket_addr};

    fn loopback_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
    }

    #[test]
    fn parses_sidecar_configuration() {
        let config = parse_config_with_env(
            vec![
                "--pool".to_string(),
                "ps1".to_string(),
                "--bus-socket".to_string(),
                "/tmp/tlb.sock".to_string(),
                "--bridge-socket".to_string(),
                "/tmp/tlbnet.sock".to_string(),
                "--listen".to_string(),
                "127.0.0.1:8201".to_string(),
                "--advertise".to_string(),
                "host.docker.internal:8201".to_string(),
                "--peer".to_string(),
                "host.docker.internal:8202".to_string(),
                "--manifest-sync-ms".to_string(),
                "2500".to_string(),
            ],
            Some(b"shared-key".to_vec()),
            &|_| None,
        )
        .unwrap();

        assert_eq!(config.pool, "ps1");
        assert!(config.routes.is_empty());
        assert_eq!(config.advertise_addr, "host.docker.internal:8201");
        assert_eq!(config.peers, vec!["host.docker.internal:8202".to_string()]);
        assert_eq!(
            config.plugin_names,
            vec![
                "lineage".to_string(),
                "auth".to_string(),
                "protocol".to_string(),
                "hmac".to_string()
            ]
        );
        assert_eq!(config.hmac_key, Some(b"shared-key".to_vec()));
        assert_eq!(config.manifest_sync_interval, Duration::from_millis(2500));
    }

    #[test]
    fn reads_sidecar_configuration_from_environment_defaults() {
        let config = parse_config_with_env(Vec::<String>::new(), None, &|key| match key {
            "TLB_BUS_MODE" => Some("sidecar".to_string()),
            "TLB_BUS_POOL" => Some("ps2".to_string()),
            "TLB_BUS_PORT" => Some("8302".to_string()),
            "TLB_BUS_SOCKET" => Some("/tmp/custom-bus.sock".to_string()),
            "TLB_BUS_BRIDGE_SOCKET" => Some("/tmp/custom-bridge.sock".to_string()),
            "TLB_BUS_ADVERTISE_ADDR" => Some("host.docker.internal:8302".to_string()),
            "TLB_REFRESH_MANIFEST_MS" => Some("1500".to_string()),
            _ => None,
        })
        .unwrap();

        assert_eq!(config.pool, "ps2");
        assert_eq!(config.bus_socket, PathBuf::from("/tmp/custom-bus.sock"));
        assert_eq!(
            config.bridge_socket,
            PathBuf::from("/tmp/custom-bridge.sock")
        );
        assert_eq!(
            config.listen_addr,
            socket_addr(DEFAULT_LISTEN_HOST, 8302).unwrap()
        );
        assert_eq!(config.advertise_addr, "host.docker.internal:8302");
        assert_eq!(
            config.plugin_names,
            vec![
                "lineage".to_string(),
                "auth".to_string(),
                "protocol".to_string()
            ]
        );
        assert_eq!(config.manifest_sync_interval, Duration::from_millis(1500));
    }

    #[tokio::test]
    async fn sidecar_serves_bus_and_bridge_together() {
        let tempdir = tempdir().unwrap();
        let bus_socket = tempdir.path().join("tlb.sock");
        let bridge_socket = tempdir.path().join("tlbnet.sock");

        let sidecar = Sidecar::new(SidecarConfig::new(
            "ps1",
            bus_socket.clone(),
            bridge_socket.clone(),
            tempdir.path(),
            loopback_addr(),
            "127.0.0.1:9001",
            Vec::new(),
            Vec::new(),
            Duration::from_secs(30),
            vec![
                "lineage".to_string(),
                "auth".to_string(),
                "protocol".to_string(),
            ],
            None,
            Some("shared-secret".to_string()),
            Some("shared-pool-secret".to_string()),
        ))
        .unwrap();

        let task = tokio::spawn({
            let sidecar = sidecar.clone();
            async move { sidecar.serve().await }
        });

        wait_for_socket(&bus_socket).await;
        wait_for_socket(&bridge_socket).await;
        let registration = register_service(
            &bus_socket,
            ServiceManifest {
                name: "ps1.echo".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: Default::default(),
                capabilities: Default::default(),
                modes: Default::default(),
            },
        )
        .await
        .unwrap();
        let listener = UnixListener::bind(PathBuf::from(registration.service_socket)).unwrap();

        let mut client = UnixStream::connect(&bus_socket).await.unwrap();
        let outbound = Envelope::new("client", "ps1.echo.say", b"ping".to_vec());
        write_envelope(&mut client, &outbound).await.unwrap();

        let (mut inbound, _) = listener.accept().await.unwrap();
        let delivered = read_envelope(&mut inbound).await.unwrap();
        assert_eq!(delivered.to, "ps1.echo.say");
        assert_eq!(delivered.payload, b"ping".to_vec());

        task.abort();
    }

    async fn wait_for_socket(path: &Path) {
        for _ in 0..40 {
            if path.exists() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
        panic!("socket `{}` was not created in time", path.display());
    }
}
