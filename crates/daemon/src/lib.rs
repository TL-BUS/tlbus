use std::path::{Path, PathBuf};

use tlbus_core::{
    BusError, BusFrame, Envelope, Pipeline, PluginContext, Result, Router, ServiceManifest,
    ServiceRegistrationRequest, ServiceRegistrationResponse, TargetAddress, read_frame,
    write_envelope, write_frame,
};
use tlbus_plugin_auth::AuthPlugin;
use tlbus_plugin_hmac::HmacPlugin;
use tlbus_plugin_lineage::LineagePlugin;
use tlbus_plugin_manifest::ProtocolPlugin;
use tokio::net::{UnixListener, UnixStream};

const DEFAULT_PLUGINS: &[&str] = &["lineage", "auth", "protocol"];

pub fn plugin_names_from_env(raw_value: Option<&str>, enable_hmac_by_default: bool) -> Vec<String> {
    match raw_value {
        Some(value) => value
            .split(',')
            .map(str::trim)
            .filter(|plugin| !plugin.is_empty())
            .map(|plugin| plugin.to_lowercase())
            .collect(),
        None => {
            let mut plugins = DEFAULT_PLUGINS
                .iter()
                .map(|plugin| plugin.to_string())
                .collect::<Vec<_>>();
            if enable_hmac_by_default {
                plugins.push("hmac".to_string());
            }
            plugins
        }
    }
}

pub fn build_pipeline(
    router: Router,
    plugin_names: &[String],
    hmac_key: Option<Vec<u8>>,
) -> Result<Pipeline> {
    let mut pipeline = Pipeline::default();

    for plugin_name in plugin_names {
        match plugin_name.trim().to_lowercase().as_str() {
            "" => {}
            "lineage" => pipeline.add(LineagePlugin),
            "auth" => pipeline.add(AuthPlugin),
            "hmac" => {
                let shared_key =
                    hmac_key
                        .clone()
                        .filter(|key| !key.is_empty())
                        .ok_or_else(|| {
                            BusError::Configuration(
                                "plugin `hmac` requires a non-empty `TLB_HMAC_KEY`".to_string(),
                            )
                        })?;
                pipeline.add(HmacPlugin::new(shared_key));
            }
            "protocol" | "manifest" => pipeline.add(ProtocolPlugin::new(router.clone())),
            other => {
                return Err(BusError::Configuration(format!(
                    "unknown plugin `{other}` in `TLBUS_PLUGINS`"
                )));
            }
        }
    }

    Ok(pipeline)
}

#[derive(Clone)]
pub struct FederationConfig {
    pub local_pool: String,
    pub bridge_socket: PathBuf,
}

#[derive(Clone)]
pub struct DaemonConfig {
    pub listen_path: PathBuf,
    pub service_socket_dir: PathBuf,
    pub router: Router,
    pub pipeline: Pipeline,
    pub federation: Option<FederationConfig>,
    pub service_secret: Option<String>,
}

impl DaemonConfig {
    pub fn new(listen_path: impl Into<PathBuf>, router: Router, pipeline: Pipeline) -> Self {
        let listen_path = listen_path.into();
        let service_socket_dir = listen_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));

        Self {
            listen_path,
            service_socket_dir,
            router,
            pipeline,
            federation: None,
            service_secret: None,
        }
    }

    pub fn with_federation(mut self, federation: FederationConfig) -> Self {
        self.federation = Some(federation);
        self
    }

    pub fn with_service_socket_dir(mut self, service_socket_dir: impl Into<PathBuf>) -> Self {
        self.service_socket_dir = service_socket_dir.into();
        self
    }

    pub fn with_service_secret(mut self, service_secret: Option<String>) -> Self {
        self.service_secret = service_secret.filter(|value| !value.trim().is_empty());
        self
    }
}

#[derive(Clone)]
pub struct Daemon {
    config: DaemonConfig,
}

impl Daemon {
    pub fn new(config: DaemonConfig) -> Self {
        Self { config }
    }

    pub async fn serve(&self) -> Result<()> {
        let listener = self.bind().await?;

        loop {
            // Each client connection carries one frame in v0.1, so per-connection tasks
            // keep the accept loop responsive without introducing a queueing subsystem.
            let (stream, _) = listener.accept().await?;
            let daemon = self.clone();

            tokio::spawn(async move {
                let _ = daemon.handle_connection(stream).await;
            });
        }
    }

    pub async fn bind(&self) -> Result<UnixListener> {
        if let Some(parent) = self.config.listen_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(&self.config.service_socket_dir).await?;

        if self.config.listen_path.exists() {
            // Rebind cleanly after restarts; the socket file is local process state.
            match tokio::fs::remove_file(&self.config.listen_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }

        Ok(UnixListener::bind(&self.config.listen_path)?)
    }

    pub async fn handle_connection(&self, mut stream: UnixStream) -> Result<()> {
        match read_frame(&mut stream).await? {
            BusFrame::Envelope(message) => self.process_envelope(message).await,
            BusFrame::ServiceRegistrationRequest(request) => {
                self.handle_registration(&mut stream, request).await
            }
            BusFrame::ServiceRegistrationResponse(_) => Err(BusError::InvalidEnvelope(
                "daemon cannot accept registration responses".to_string(),
            )),
        }
    }

    pub async fn process_envelope(&self, mut message: Envelope) -> Result<()> {
        log_envelope_event("recv", &message, "stage=ingress");

        if let Err(error) = message.validate() {
            log_envelope_event(
                "rejected",
                &message,
                &format!("stage=validate error={error}"),
            );
            return Err(error);
        }

        if message.is_expired_at(std::time::SystemTime::now()) {
            log_envelope_event("expired", &message, "stage=ttl");
            return Err(BusError::Expired);
        }

        // One context follows the message through the whole plugin pipeline.
        let mut ctx = PluginContext::default();
        if let Err(error) = self.config.pipeline.run_receive(&mut ctx, &mut message) {
            log_envelope_event(
                "rejected",
                &message,
                &format!("stage=receive error={error}"),
            );
            return Err(error);
        }

        if let Some(response) = ctx.take_terminal_response() {
            log_envelope_event("handled_by_plugin", &message, "stage=receive");
            Box::pin(self.process_envelope(response)).await?;
            return Ok(());
        }

        // Local delivery still uses service -> socket, but federated addresses can be
        // redirected to the bridge sidecar when the pool differs from the local one.
        let routing = match self.resolve_route(&message) {
            Ok(routing) => routing,
            Err(error) => {
                log_envelope_event("route_failed", &message, &format!("error={error}"));
                return Err(error);
            }
        };
        let original_target = message.to.clone();
        let route_key = routing.route_key;
        let route = routing.socket_path;
        ctx.set_route(route.clone());

        if let Err(error) = self.config.pipeline.run_route(&mut ctx, &mut message) {
            log_envelope_event(
                "route_failed",
                &message,
                &format!("stage=route error={error}"),
            );
            return Err(error);
        }
        if let Some(response) = ctx.take_terminal_response() {
            log_envelope_event("handled_by_plugin", &message, "stage=route");
            Box::pin(self.process_envelope(response)).await?;
            return Ok(());
        }
        // Routing already happened, so changing `to` after this point would make the
        // envelope disagree with the actual delivery target.
        if message.to != original_target {
            let error = BusError::InvalidEnvelope("`to` cannot change after routing".to_string());
            log_envelope_event(
                "route_failed",
                &message,
                &format!("stage=route error={error}"),
            );
            return Err(error);
        }

        // Delivery is best-effort: a connect/write failure drops the message.
        let mut destination = match UnixStream::connect(&route).await {
            Ok(destination) => destination,
            Err(error) => {
                self.config.router.set_active(&route_key, false);
                log_envelope_event(
                    "connect_failed",
                    &message,
                    &format!(
                        "route_key={route_key} route={} error={error}",
                        route.display()
                    ),
                );
                return Err(error.into());
            }
        };
        if let Err(error) = write_envelope(&mut destination, &message).await {
            log_envelope_event(
                "deliver_failed",
                &message,
                &format!(
                    "route_key={route_key} route={} error={error}",
                    route.display()
                ),
            );
            return Err(error);
        }
        log_envelope_event(
            "send",
            &message,
            &format!(
                "stage=deliver route_key={route_key} route={}",
                route.display()
            ),
        );
        if let Err(error) = self.config.pipeline.run_deliver(&mut ctx, &mut message) {
            log_envelope_event(
                "deliver_failed",
                &message,
                &format!("stage=deliver error={error}"),
            );
            return Err(error);
        }
        log_envelope_event(
            "delivered",
            &message,
            &format!(
                "from={} to={} route_key={route_key} route={}",
                message.from,
                message.to,
                route.display()
            ),
        );

        Ok(())
    }
    async fn handle_registration(
        &self,
        stream: &mut UnixStream,
        request: ServiceRegistrationRequest,
    ) -> Result<()> {
        let response = self.register_service(request.manifest);
        write_frame(stream, &BusFrame::ServiceRegistrationResponse(response)).await
    }

    fn register_service(&self, manifest: ServiceManifest) -> ServiceRegistrationResponse {
        let service_socket = self.service_socket_path(&manifest.name);
        let bus_socket = self.config.listen_path.to_string_lossy().to_string();
        let pool = self.registration_pool(&manifest.name);

        let denial_reason = self.registration_denial_reason(&manifest);
        if let Some(reason) = denial_reason {
            return ServiceRegistrationResponse {
                secret: manifest.secret,
                pool,
                allowed: false,
                active: false,
                service_socket: service_socket.to_string_lossy().to_string(),
                bus_socket,
                reason: Some(reason),
            };
        }

        let descriptor = self
            .config
            .router
            .register_manifest(&manifest, service_socket.clone());

        ServiceRegistrationResponse {
            secret: manifest.secret,
            pool,
            allowed: true,
            active: descriptor.active,
            service_socket: service_socket.to_string_lossy().to_string(),
            bus_socket,
            reason: None,
        }
    }

    fn registration_denial_reason(&self, manifest: &ServiceManifest) -> Option<String> {
        if manifest.name.trim().is_empty() {
            return Some("service name must not be empty".to_string());
        }

        if manifest.secret.trim().is_empty() {
            return Some("service secret must not be empty".to_string());
        }

        if let Some(expected_secret) = &self.config.service_secret {
            if &manifest.secret != expected_secret {
                return Some("service secret mismatch".to_string());
            }
        }

        if let Some(federation) = &self.config.federation {
            let expected_prefix = format!("{}.", federation.local_pool);
            if !manifest.name.starts_with(&expected_prefix) {
                return Some(format!(
                    "service `{}` does not belong to local pool `{}`",
                    manifest.name, federation.local_pool
                ));
            }
        }

        None
    }

    fn registration_pool(&self, service_name: &str) -> String {
        self.config
            .federation
            .as_ref()
            .map(|federation| federation.local_pool.clone())
            .or_else(|| service_name.split('.').next().map(str::to_string))
            .unwrap_or_else(|| "local".to_string())
    }

    fn service_socket_path(&self, service_name: &str) -> PathBuf {
        let basename = service_name
            .rsplit('.')
            .next()
            .unwrap_or(service_name)
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                    ch
                } else {
                    '-'
                }
            })
            .collect::<String>();

        self.config
            .service_socket_dir
            .join(format!("{basename}.sock"))
    }

    fn resolve_route(&self, message: &Envelope) -> Result<ResolvedRoute> {
        let target = TargetAddress::parse(&message.to)?;

        if let Some(federation) = &self.config.federation {
            if let Some(pool) = target.pool() {
                if pool != federation.local_pool {
                    return Ok(ResolvedRoute {
                        route_key: target.route_key(),
                        socket_path: federation.bridge_socket.clone(),
                    });
                }

                return self.resolve_local_route(&target);
            }
        }

        Ok(ResolvedRoute {
            route_key: target.route_key(),
            socket_path: self.config.router.resolve(&target.route_key())?,
        })
    }

    fn resolve_local_route(&self, target: &TargetAddress) -> Result<ResolvedRoute> {
        let route_key = target.route_key();
        if let Ok(socket_path) = self.config.router.resolve(&route_key) {
            return Ok(ResolvedRoute {
                route_key,
                socket_path,
            });
        }

        Ok(ResolvedRoute {
            route_key: target.service().to_string(),
            socket_path: self.config.router.resolve(target.service())?,
        })
    }
}

struct ResolvedRoute {
    route_key: String,
    socket_path: PathBuf,
}

fn log_envelope_event(event: &str, envelope: &Envelope, details: &str) {
    eprintln!(
        "tlbus-daemon: event={event} {} {details}",
        envelope.trace_fields()
    );
}

#[cfg(test)]
mod tests {
    use serde_json::from_slice;
    use tempfile::tempdir;
    use tlbus_core::{
        Envelope, Pipeline, Router, ServiceCapability, ServiceDescriptor, ServiceManifest,
        ServiceMode, read_envelope, register_service, write_envelope,
    };
    use tlbus_plugin_auth::AuthPlugin;
    use tlbus_plugin_hmac::HmacPlugin;
    use tlbus_plugin_lineage::LineagePlugin;
    use tokio::net::{UnixListener, UnixStream};

    use crate::{Daemon, DaemonConfig, FederationConfig, build_pipeline, plugin_names_from_env};

    #[test]
    fn default_plugin_selection_includes_protocol() {
        let plugins = plugin_names_from_env(None, false);
        assert_eq!(
            plugins,
            vec![
                "lineage".to_string(),
                "auth".to_string(),
                "protocol".to_string()
            ]
        );
    }

    #[test]
    fn explicit_plugin_selection_preserves_requested_order() {
        let plugins = plugin_names_from_env(Some("hmac,lineage,protocol"), false);
        assert_eq!(
            plugins,
            vec![
                "hmac".to_string(),
                "lineage".to_string(),
                "protocol".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn daemon_routes_messages_through_pipeline() {
        let tempdir = tempdir().unwrap();
        let service_socket = tempdir.path().join("echo.sock");
        let listener = UnixListener::bind(&service_socket).unwrap();

        let mut pipeline = Pipeline::default();
        pipeline.add(LineagePlugin::default());
        pipeline.add(AuthPlugin::default());
        pipeline.add(HmacPlugin::new(b"shared-key".to_vec()));

        let daemon = Daemon::new(DaemonConfig::new(
            tempdir.path().join("tlb.sock"),
            Router::from_routes([("echo", service_socket.clone())]),
            pipeline,
        ));

        let (mut client, server) = UnixStream::pair().unwrap();
        let daemon_task = tokio::spawn({
            let daemon = daemon.clone();
            async move { daemon.handle_connection(server).await.unwrap() }
        });

        let outbound = Envelope::new("client", "echo", b"ping".to_vec());
        write_envelope(&mut client, &outbound).await.unwrap();

        let (mut inbound_stream, _) = listener.accept().await.unwrap();
        let delivered = read_envelope(&mut inbound_stream).await.unwrap();

        daemon_task.await.unwrap();
        assert_eq!(delivered.from, "client");
        assert_eq!(delivered.to, "echo");
        assert_eq!(delivered.payload, b"ping".to_vec());
        assert!(delivered.headers.contains_key("txn_id"));
        assert_eq!(
            delivered.headers.get("auth.service"),
            Some(&"client".to_string())
        );
        assert!(delivered.headers.contains_key("hmac.proof"));
    }

    #[tokio::test]
    async fn daemon_routes_remote_pool_targets_to_the_bridge_socket() {
        let tempdir = tempdir().unwrap();
        let bridge_socket = tempdir.path().join("bridge.sock");
        let bridge_listener = UnixListener::bind(&bridge_socket).unwrap();

        let daemon = Daemon::new(
            DaemonConfig::new(
                tempdir.path().join("tlb.sock"),
                Router::new(),
                Pipeline::default(),
            )
            .with_federation(FederationConfig {
                local_pool: "ps1".to_string(),
                bridge_socket: bridge_socket.clone(),
            }),
        );

        let (mut client, server) = UnixStream::pair().unwrap();
        let daemon_task = tokio::spawn({
            let daemon = daemon.clone();
            async move { daemon.handle_connection(server).await.unwrap() }
        });

        let outbound = Envelope::new("client", "ps2.echo.say", b"ping".to_vec());
        write_envelope(&mut client, &outbound).await.unwrap();

        let (mut inbound_stream, _) = bridge_listener.accept().await.unwrap();
        let delivered = read_envelope(&mut inbound_stream).await.unwrap();

        daemon_task.await.unwrap();
        assert_eq!(delivered.to, "ps2.echo.say");
        assert_eq!(delivered.payload, b"ping".to_vec());
    }

    #[tokio::test]
    async fn daemon_registers_services_via_handshake() {
        let tempdir = tempdir().unwrap();
        let daemon = Daemon::new(
            DaemonConfig::new(
                tempdir.path().join("tlb.sock"),
                Router::new(),
                Pipeline::default(),
            )
            .with_service_socket_dir(tempdir.path())
            .with_service_secret(Some("shared-secret".to_string()))
            .with_federation(FederationConfig {
                local_pool: "ps2".to_string(),
                bridge_socket: tempdir.path().join("tlbnet.sock"),
            }),
        );

        let listener = daemon.bind().await.unwrap();
        let server = tokio::spawn({
            let daemon = daemon.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                daemon.handle_connection(stream).await.unwrap();
            }
        });

        let response = register_service(
            &tempdir.path().join("tlb.sock"),
            ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: Default::default(),
                capabilities: Default::default(),
                modes: Default::default(),
            },
        )
        .await
        .unwrap();

        server.await.unwrap();
        assert!(response.allowed);
        assert!(response.active);
        assert_eq!(response.pool, "ps2");
        assert!(response.service_socket.ends_with("calcola.sock"));
        assert_eq!(
            daemon.config.router.resolve("ps2.calcola").unwrap(),
            tempdir.path().join("calcola.sock")
        );
    }

    #[tokio::test]
    async fn daemon_serves_registered_manifests_via_manifest_target() {
        let tempdir = tempdir().unwrap();
        let client_socket = tempdir.path().join("client.sock");
        let client_listener = UnixListener::bind(&client_socket).unwrap();
        let router = Router::from_routes([("ps2.client", client_socket.clone())]);
        let pipeline = build_pipeline(router.clone(), &["protocol".to_string()], None).unwrap();
        let daemon = Daemon::new(
            DaemonConfig::new(tempdir.path().join("tlb.sock"), router, pipeline)
                .with_service_socket_dir(tempdir.path())
                .with_service_secret(Some("shared-secret".to_string()))
                .with_federation(FederationConfig {
                    local_pool: "ps2".to_string(),
                    bridge_socket: tempdir.path().join("tlbnet.sock"),
                }),
        );

        let listener = daemon.bind().await.unwrap();
        let server = tokio::spawn({
            let daemon = daemon.clone();
            async move {
                for _ in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    daemon.handle_connection(stream).await.unwrap();
                }
            }
        });

        register_service(
            &tempdir.path().join("tlb.sock"),
            ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: Default::default(),
                capabilities: vec![ServiceCapability {
                    name: "calculate".to_string(),
                    address: "ps2.calcola.compute".to_string(),
                    description: "Evaluates an arithmetic expression".to_string(),
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

        let mut request_stream = UnixStream::connect(tempdir.path().join("tlb.sock"))
            .await
            .unwrap();
        let mut request = Envelope::new("ps2.client", "ps2.calcola.manifest", b"{}".to_vec());
        request
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());
        request
            .headers
            .insert("txn_id".to_string(), "txn-123".to_string());
        write_envelope(&mut request_stream, &request).await.unwrap();

        let (mut inbound_stream, _) = client_listener.accept().await.unwrap();
        let response = read_envelope(&mut inbound_stream).await.unwrap();
        server.await.unwrap();

        assert_eq!(response.from, "ps2.calcola");
        assert_eq!(response.to, "ps2.client.inbox");
        assert_eq!(response.txn_id(), Some("txn-123"));
        let manifest: ServiceDescriptor = from_slice(&response.payload).unwrap();
        assert_eq!(manifest.service, "ps2.calcola");
        assert!(manifest.service_capability("calculate").is_some());
        assert!(manifest.service_capability("manifest").is_some());
        assert!(manifest.supports_mode("http2", "mcp").is_some());
    }
}
