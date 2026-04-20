use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;
use tlbus_core::{
    BusError, BusFrame, Envelope, PoolManifest, REPLY_TO_HEADER, RegistryListResponse, Result,
    ServiceCapability, ServiceDescriptor, ServiceManifest, ServiceMode, TXN_ID_HEADER,
    read_frame_sync, register_service_sync, write_frame_sync,
};
use uuid::Uuid;

pub const DEFAULT_BUS_SOCKET: &str = "/run/tlb.sock";
pub const DEFAULT_TRANSPORT: &str = "tl-bus";
pub const DEFAULT_PROTOCOL: &str = "standard";
pub const DEFAULT_CONTENT_TYPE: &str = "application/json";
pub const DISCOVERY_SERVICE_NAME: &str = "__tlbus__";
pub const DISCOVERY_SERVICES_ACTION: &str = "services";
pub const REGISTRY_SERVICE_NAME: &str = "registry";
pub const REGISTRY_LIST_ACTION: &str = "list";
pub const REGISTRY_GET_MANIFEST_ACTION: &str = "get_manifest";
pub const REGISTRY_GET_PROTOCOL_MANIFEST_ACTION: &str = "get_protocol_manifest";

const ACCEPT_RETRY_SLEEP: Duration = Duration::from_millis(50);
const DEFAULT_WORKER_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub bus_socket: PathBuf,
    pub service_name: String,
    pub service_secret: String,
    pub is_client: bool,
    pub features: BTreeMap<String, String>,
    pub capabilities: Vec<ServiceCapability>,
    pub modes: Vec<ServiceMode>,
    pub default_transport: String,
    pub default_protocol: String,
    pub default_content_type: String,
}

impl EndpointConfig {
    pub fn new(service_name: impl Into<String>, service_secret: impl Into<String>) -> Self {
        let service_name = service_name.into();
        let inbox_address = format!("{service_name}.inbox");

        Self {
            bus_socket: PathBuf::from(DEFAULT_BUS_SOCKET),
            service_name,
            service_secret: service_secret.into(),
            is_client: true,
            features: BTreeMap::from([("role".to_string(), "client".to_string())]),
            capabilities: vec![ServiceCapability {
                name: "inbox".to_string(),
                address: inbox_address,
                description: "Receives replies routed to the client inbox".to_string(),
            }],
            modes: vec![ServiceMode {
                transport: DEFAULT_TRANSPORT.to_string(),
                protocol: DEFAULT_PROTOCOL.to_string(),
                protocol_version: None,
                content_type: Some(DEFAULT_CONTENT_TYPE.to_string()),
            }],
            default_transport: DEFAULT_TRANSPORT.to_string(),
            default_protocol: DEFAULT_PROTOCOL.to_string(),
            default_content_type: DEFAULT_CONTENT_TYPE.to_string(),
        }
    }

    pub fn with_bus_socket(mut self, bus_socket: impl Into<PathBuf>) -> Self {
        self.bus_socket = bus_socket.into();
        self
    }

    pub fn register(self) -> Result<Endpoint> {
        let manifest = ServiceManifest {
            name: self.service_name.clone(),
            secret: self.service_secret.clone(),
            is_client: self.is_client,
            features: self.features.clone(),
            capabilities: self.capabilities.clone(),
            modes: self.modes.clone(),
        };

        let response = register_service_sync(&self.bus_socket, manifest)?;
        if !response.allowed || !response.active {
            return Err(BusError::Configuration(format!(
                "service registration rejected: {}",
                response
                    .reason
                    .unwrap_or_else(|| "registration denied".to_string())
            )));
        }

        let service_socket = map_remote_socket_path(
            &self.bus_socket,
            &response.bus_socket,
            &response.service_socket,
        );
        Ok(Endpoint {
            bus_socket: self.bus_socket,
            service_name: self.service_name,
            service_socket,
            default_transport: self.default_transport,
            default_protocol: self.default_protocol,
            default_content_type: self.default_content_type,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    bus_socket: PathBuf,
    service_name: String,
    pub service_socket: PathBuf,
    default_transport: String,
    default_protocol: String,
    default_content_type: String,
}

impl Endpoint {
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn send_json(
        &self,
        target: &str,
        payload: &Value,
        headers: BTreeMap<String, String>,
    ) -> Result<()> {
        let payload = serde_json::to_vec(payload)
            .map_err(|error| BusError::Configuration(format!("invalid JSON payload: {error}")))?;
        self.send_bytes(target, payload, headers)
    }

    pub fn send_bytes(
        &self,
        target: &str,
        payload: Vec<u8>,
        mut headers: BTreeMap<String, String>,
    ) -> Result<()> {
        if target.trim().is_empty() {
            return Err(BusError::Configuration(
                "target must not be empty".to_string(),
            ));
        }

        headers
            .entry(TXN_ID_HEADER.to_string())
            .or_insert_with(|| Uuid::new_v4().to_string());
        headers
            .entry("transport".to_string())
            .or_insert_with(|| self.default_transport.clone());
        headers
            .entry("protocol".to_string())
            .or_insert_with(|| self.default_protocol.clone());
        headers
            .entry("content_type".to_string())
            .or_insert_with(|| self.default_content_type.clone());

        let mut envelope = Envelope::new(self.service_name.clone(), target.to_string(), payload);
        envelope.headers = headers;
        envelope.validate()?;

        let mut stream = std::os::unix::net::UnixStream::connect(&self.bus_socket)?;
        write_frame_sync(&mut stream, &BusFrame::Envelope(envelope))
    }

    pub fn receive_once(&self, timeout: Duration) -> Result<Envelope> {
        let (listener, _cleanup_guard) = bind_inbox_listener(&self.service_socket)?;
        wait_for_envelope(&listener, timeout, &self.service_socket)
    }

    pub fn discover_pool_manifest(&self, pool: &str, timeout: Duration) -> Result<PoolManifest> {
        if pool.trim().is_empty() {
            return Err(BusError::Configuration(
                "pool must not be empty".to_string(),
            ));
        }

        let target = format!("{pool}.{DISCOVERY_SERVICE_NAME}.{DISCOVERY_SERVICES_ACTION}");
        let response = self.request_bytes(&target, b"{}".to_vec(), BTreeMap::new(), timeout)?;
        serde_json::from_slice(&response.payload)
            .map_err(|error| BusError::Codec(format!("json decode failed: {error}")))
    }

    pub fn discover_service_manifest(
        &self,
        service: &str,
        timeout: Duration,
    ) -> Result<ServiceDescriptor> {
        if service.trim().is_empty() {
            return Err(BusError::Configuration(
                "service must not be empty".to_string(),
            ));
        }

        let target = format!("{service}.manifest");
        let response = self.request_bytes(&target, b"{}".to_vec(), BTreeMap::new(), timeout)?;
        serde_json::from_slice(&response.payload)
            .map_err(|error| BusError::Codec(format!("json decode failed: {error}")))
    }

    pub fn discover_services(
        &self,
        pool: &str,
        timeout: Duration,
    ) -> Result<Vec<ServiceDescriptor>> {
        Ok(self.discover_pool_manifest(pool, timeout)?.services)
    }

    pub fn discover_registry_services(
        &self,
        timeout: Duration,
    ) -> Result<Vec<tlbus_core::RegistryService>> {
        let response = self.request_registry(
            REGISTRY_LIST_ACTION,
            Value::Object(Default::default()),
            timeout,
        )?;
        let payload: RegistryListResponse = serde_json::from_slice(&response.payload)
            .map_err(|error| BusError::Codec(format!("json decode failed: {error}")))?;
        Ok(payload.services)
    }

    pub fn discover_registry_manifest(
        &self,
        service: &str,
        timeout: Duration,
    ) -> Result<tlbus_core::RegistryServiceManifest> {
        if service.trim().is_empty() {
            return Err(BusError::Configuration(
                "service must not be empty".to_string(),
            ));
        }

        let response = self.request_registry(
            REGISTRY_GET_MANIFEST_ACTION,
            serde_json::json!({ "service": service }),
            timeout,
        )?;
        serde_json::from_slice(&response.payload)
            .map_err(|error| BusError::Codec(format!("json decode failed: {error}")))
    }

    pub fn discover_registry_protocol_manifest(
        &self,
        service: &str,
        protocol: &str,
        timeout: Duration,
    ) -> Result<Value> {
        if service.trim().is_empty() {
            return Err(BusError::Configuration(
                "service must not be empty".to_string(),
            ));
        }
        if protocol.trim().is_empty() {
            return Err(BusError::Configuration(
                "protocol must not be empty".to_string(),
            ));
        }

        let response = self.request_registry(
            REGISTRY_GET_PROTOCOL_MANIFEST_ACTION,
            serde_json::json!({ "service": service, "protocol": protocol }),
            timeout,
        )?;
        serde_json::from_slice(&response.payload)
            .map_err(|error| BusError::Codec(format!("json decode failed: {error}")))
    }

    fn request_bytes(
        &self,
        target: &str,
        payload: Vec<u8>,
        mut headers: BTreeMap<String, String>,
        timeout: Duration,
    ) -> Result<Envelope> {
        let reply_to = format!("{}.inbox", self.service_name());
        headers
            .entry(REPLY_TO_HEADER.to_string())
            .or_insert(reply_to);

        let (listener, _cleanup_guard) = bind_inbox_listener(&self.service_socket)?;
        self.send_bytes(target, payload, headers)?;
        wait_for_envelope(&listener, timeout, &self.service_socket)
    }

    fn request_registry(
        &self,
        action: &str,
        payload: Value,
        timeout: Duration,
    ) -> Result<Envelope> {
        let target = format!("{REGISTRY_SERVICE_NAME}.{action}");
        let payload = serde_json::to_vec(&payload)
            .map_err(|error| BusError::Configuration(format!("invalid JSON payload: {error}")))?;
        self.request_bytes(&target, payload, BTreeMap::new(), timeout)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerLoopConfig {
    pub bus_socket: PathBuf,
    pub service_name: String,
    pub service_secret: String,
    pub capability_name: String,
    pub capability_address: String,
    pub capability_description: String,
    pub timeout: Duration,
}

impl WorkerLoopConfig {
    pub fn new(
        service_name: impl Into<String>,
        service_secret: impl Into<String>,
        capability_address: impl Into<String>,
    ) -> Self {
        Self {
            bus_socket: PathBuf::from(DEFAULT_BUS_SOCKET),
            service_name: service_name.into(),
            service_secret: service_secret.into(),
            capability_name: "handle".to_string(),
            capability_address: capability_address.into(),
            capability_description: "Base worker capability endpoint".to_string(),
            timeout: DEFAULT_WORKER_TIMEOUT,
        }
    }

    pub fn with_bus_socket(mut self, bus_socket: impl Into<PathBuf>) -> Self {
        self.bus_socket = bus_socket.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Debug, Clone)]
pub struct WorkerReply {
    pub payload: Value,
    pub headers: BTreeMap<String, String>,
}

impl WorkerReply {
    pub fn json(payload: Value) -> Self {
        Self {
            payload,
            headers: BTreeMap::new(),
        }
    }
}

pub fn run_worker_loop<F>(config: WorkerLoopConfig, mut handler: F) -> Result<()>
where
    F: FnMut(&Envelope) -> Result<Option<WorkerReply>>,
{
    let mut endpoint_config =
        EndpointConfig::new(config.service_name.clone(), config.service_secret.clone())
            .with_bus_socket(config.bus_socket.clone());
    endpoint_config.is_client = false;
    endpoint_config.features = BTreeMap::from([
        ("role".to_string(), "worker".to_string()),
        ("runtime".to_string(), "rust".to_string()),
    ]);
    endpoint_config.capabilities = vec![ServiceCapability {
        name: config.capability_name.clone(),
        address: config.capability_address.clone(),
        description: config.capability_description.clone(),
    }];
    endpoint_config.modes = vec![ServiceMode {
        transport: DEFAULT_TRANSPORT.to_string(),
        protocol: DEFAULT_PROTOCOL.to_string(),
        protocol_version: None,
        content_type: Some(DEFAULT_CONTENT_TYPE.to_string()),
    }];

    let endpoint = endpoint_config.register()?;
    eprintln!(
        "tlbus-worker: event=register service={} capability={} service_socket={}",
        endpoint.service_name(),
        config.capability_address,
        endpoint.service_socket.display()
    );

    loop {
        let envelope = endpoint.receive_once(config.timeout)?;
        eprintln!(
            "tlbus-worker: event=recv service={} {}",
            endpoint.service_name(),
            envelope.trace_fields()
        );

        let Some(reply_to) = envelope.reply_to() else {
            eprintln!(
                "tlbus-worker: event=drop service={} reason=missing_reply_to",
                endpoint.service_name(),
            );
            continue;
        };

        let Some(mut reply) = handler(&envelope)? else {
            eprintln!(
                "tlbus-worker: event=drop service={} reason=handler_returned_none target={} txn_id={}",
                endpoint.service_name(),
                reply_to,
                envelope.txn_id().unwrap_or("missing")
            );
            continue;
        };

        if let Some(txn_id) = envelope.txn_id() {
            reply
                .headers
                .entry(TXN_ID_HEADER.to_string())
                .or_insert_with(|| txn_id.to_string());
        }

        endpoint.send_json(reply_to, &reply.payload, reply.headers)?;
        eprintln!(
            "tlbus-worker: event=reply service={} target={} txn_id={}",
            endpoint.service_name(),
            reply_to,
            envelope.txn_id().unwrap_or("missing")
        );
    }
}

pub fn parse_header_pairs(entries: &[String]) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for entry in entries {
        let (key, value) = entry.split_once('=').ok_or_else(|| {
            BusError::Configuration(format!("invalid header `{entry}`: expected key=value"))
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(BusError::Configuration(format!(
                "invalid header `{entry}`: key must not be empty"
            )));
        }
        parsed.insert(key.to_string(), value.to_string());
    }
    Ok(parsed)
}

pub fn map_remote_socket_path(
    local_bus_socket: &Path,
    remote_bus_socket: &str,
    remote_service_socket: &str,
) -> PathBuf {
    let local_bus_dir = local_bus_socket.parent().unwrap_or_else(|| Path::new("/"));
    let remote_bus_dir = Path::new(remote_bus_socket)
        .parent()
        .unwrap_or_else(|| Path::new("/"));
    let remote_service_path = Path::new(remote_service_socket);

    let relative = remote_service_path
        .strip_prefix(remote_bus_dir)
        .map(Path::to_path_buf)
        .unwrap_or_else(|_| {
            remote_service_path
                .file_name()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(OsStr::new("service.sock")))
        });

    local_bus_dir.join(relative)
}

fn remove_file_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

struct SocketCleanup {
    path: PathBuf,
}

impl SocketCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = remove_file_if_present(&self.path);
    }
}

fn bind_inbox_listener(socket_path: &Path) -> Result<(UnixListener, SocketCleanup)> {
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_file_if_present(socket_path)?;
    let cleanup_guard = SocketCleanup::new(socket_path.to_path_buf());

    let listener = UnixListener::bind(socket_path)?;
    listener.set_nonblocking(true)?;
    Ok((listener, cleanup_guard))
}

fn wait_for_envelope(
    listener: &UnixListener,
    timeout: Duration,
    socket_path: &Path,
) -> Result<Envelope> {
    let deadline = Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                let frame = read_frame_sync(&mut stream)?;
                return frame.into_envelope();
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(BusError::Transport(format!(
                        "timed out while waiting for incoming envelope on {}",
                        socket_path.display()
                    )));
                }
                thread::sleep(ACCEPT_RETRY_SLEEP);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    use tempfile::tempdir;
    use tlbus_core::{ServiceCapability, ServiceManifest, ServiceMode, register_service};
    use tlbus_daemon::{Daemon, DaemonConfig, FederationConfig, build_pipeline};
    use tokio::net::UnixListener;
    use tokio::time::sleep;

    use super::{EndpointConfig, WorkerLoopConfig, map_remote_socket_path, parse_header_pairs};

    #[test]
    fn maps_remote_socket_path_to_local_mount() {
        let local = std::path::Path::new("/tmp/tlbus/tlb.sock");
        let mapped =
            map_remote_socket_path(local, "/run/tlbus/tlb.sock", "/run/tlbus/ps1.client.sock");
        assert_eq!(
            mapped,
            std::path::PathBuf::from("/tmp/tlbus/ps1.client.sock")
        );
    }

    #[test]
    fn parses_header_pairs() {
        let headers = parse_header_pairs(&[
            "txn_id=abc".to_string(),
            "reply_to=ps1.client.inbox".to_string(),
        ])
        .unwrap();

        assert_eq!(headers.get("txn_id"), Some(&"abc".to_string()));
        assert_eq!(
            headers.get("reply_to"),
            Some(&"ps1.client.inbox".to_string())
        );
    }

    #[test]
    fn worker_loop_config_defaults() {
        let config = WorkerLoopConfig::new("ps1.worker", "shared-secret", "ps1.worker.handle");
        assert_eq!(config.service_name, "ps1.worker");
        assert_eq!(config.capability_name, "handle");
        assert_eq!(config.capability_address, "ps1.worker.handle");
        assert_eq!(config.timeout, Duration::from_secs(20));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_discovers_pool_and_service_manifests() {
        let tempdir = tempdir().unwrap();
        let bus_socket = tempdir.path().join("tlb.sock");

        let router = tlbus_core::Router::new();
        let pipeline = build_pipeline(
            router.clone(),
            &["protocol".to_string()],
            None,
            Some("ps2".to_string()),
        )
        .unwrap();
        let daemon = Daemon::new(
            DaemonConfig::new(bus_socket.clone(), router, pipeline)
                .with_service_socket_dir(tempdir.path())
                .with_service_secret(Some("shared-secret".to_string()))
                .with_federation(FederationConfig {
                    local_pool: "ps2".to_string(),
                    bridge_socket: tempdir.path().join("tlbnet.sock"),
                }),
        );

        let task = tokio::spawn({
            let daemon = daemon.clone();
            async move { daemon.serve().await }
        });

        wait_for_socket(&bus_socket).await;

        let echo_registration = register_service(
            &bus_socket,
            ServiceManifest {
                name: "ps2.echo".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: vec![ServiceCapability {
                    name: "echo".to_string(),
                    address: "ps2.echo.say".to_string(),
                    description: "Echoes the incoming payload".to_string(),
                }],
                modes: vec![ServiceMode {
                    transport: "http2".to_string(),
                    protocol: "mcp".to_string(),
                    protocol_version: Some("2025-06-18".to_string()),
                    content_type: Some("application/json".to_string()),
                }],
            },
        )
        .await
        .unwrap();
        let echo_listener = UnixListener::bind(&echo_registration.service_socket).unwrap();
        let echo_task = tokio::spawn(async move {
            while echo_listener.accept().await.is_ok() {}
        });

        let endpoint = EndpointConfig::new("ps2.client", "shared-secret")
            .with_bus_socket(bus_socket.clone())
            .register()
            .unwrap();

        let pool_manifest = endpoint
            .discover_pool_manifest("ps2", Duration::from_secs(2))
            .unwrap();
        assert_eq!(pool_manifest.pool, "ps2");
        assert!(
            pool_manifest
                .services
                .iter()
                .any(|service| service.service == "ps2.echo")
        );

        let service_manifest = endpoint
            .discover_service_manifest("ps2.echo", Duration::from_secs(2))
            .unwrap();
        assert_eq!(service_manifest.service, "ps2.echo");
        assert!(service_manifest.service_capability("echo").is_some());
        assert!(service_manifest.supports_mode("http2", "mcp").is_some());

        let registry_services = endpoint
            .discover_registry_services(Duration::from_secs(2))
            .unwrap();
        assert!(
            registry_services
                .iter()
                .any(|service| service.name == "echo")
        );

        let registry_manifest = endpoint
            .discover_registry_manifest("echo", Duration::from_secs(2))
            .unwrap();
        assert_eq!(registry_manifest.name, "echo");
        assert!(
            registry_manifest
                .capabilities
                .iter()
                .any(|capability| capability == "echo.say")
        );
        assert!(
            registry_manifest
                .protocols
                .iter()
                .any(|protocol| protocol == "mcp")
        );

        let protocol_manifest = endpoint
            .discover_registry_protocol_manifest("echo", "mcp", Duration::from_secs(2))
            .unwrap();
        assert_eq!(protocol_manifest["service"], "echo");
        assert_eq!(protocol_manifest["protocol"], "mcp");
        assert!(protocol_manifest["manifest"]["modes"].is_array());

        echo_task.abort();
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
