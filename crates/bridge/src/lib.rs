use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use h2::client;
use h2::server;
use http::{Method, Request, Response, StatusCode};
use serde_json::json;
use tlbus_core::{
    BusError, Envelope, PoolHandshakeRequest, PoolHandshakeResponse, PoolManifest, Result, Router,
    TXN_ID_HEADER, TargetAddress, decode_envelope, encode_envelope, write_envelope,
};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::time::sleep;

const FEDERATION_PATH: &str = "/tlbusnet/v0/envelopes";
const HANDSHAKE_PATH: &str = "/tlbusnet/v0/handshake";
const DEFAULT_PROTOCOL: &str = "raw";
const TRANSPORT_HTTP2: &str = "http2";
const REGISTRY_SERVICE: &str = "registry";
const PROTOCOL_HEADER: &str = "protocol";
const CONTENT_TYPE_HEADER: &str = "content_type";
const CONTENT_TYPE_JSON: &str = "application/json";
const STANDARD_PROTOCOL: &str = "standard";
pub const DEFAULT_MANIFEST_SYNC_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub pool: String,
    pub bus_socket: PathBuf,
    pub ingress_socket: PathBuf,
    pub listen_addr: SocketAddr,
    pub advertise_addr: String,
    pub router: Router,
    pub peers: Vec<String>,
    pub manifest_sync_interval: Duration,
    pub pool_secret: Option<String>,
}

impl BridgeConfig {
    pub fn new(
        pool: impl Into<String>,
        bus_socket: impl Into<PathBuf>,
        ingress_socket: impl Into<PathBuf>,
        listen_addr: SocketAddr,
        advertise_addr: impl Into<String>,
        router: Router,
        peers: Vec<String>,
        manifest_sync_interval: Duration,
        pool_secret: Option<String>,
    ) -> Self {
        Self {
            pool: pool.into(),
            bus_socket: bus_socket.into(),
            ingress_socket: ingress_socket.into(),
            listen_addr,
            advertise_addr: advertise_addr.into(),
            router,
            peers,
            manifest_sync_interval,
            pool_secret: pool_secret.filter(|value| !value.trim().is_empty()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerState {
    addr: String,
    manifest: PoolManifest,
    connected: bool,
}

#[derive(Debug, Clone)]
pub struct Bridge {
    config: BridgeConfig,
    peers: Arc<RwLock<BTreeMap<String, PeerState>>>,
    known_peer_addrs: Arc<RwLock<BTreeSet<String>>>,
}

impl Bridge {
    pub fn new(config: BridgeConfig) -> Self {
        let known_peer_addrs = config.peers.iter().cloned().collect();
        Self {
            config,
            peers: Arc::new(RwLock::new(BTreeMap::new())),
            known_peer_addrs: Arc::new(RwLock::new(known_peer_addrs)),
        }
    }

    pub async fn serve(&self) -> Result<()> {
        let local_listener = self.bind_local_ingress().await?;
        let network_listener = TcpListener::bind(self.config.listen_addr).await?;

        tokio::try_join!(
            self.serve_local_ingress(local_listener),
            self.serve_network_ingress(network_listener),
            self.run_peer_sync_loop(),
        )?;
        Ok(())
    }

    pub async fn bind_local_ingress(&self) -> Result<UnixListener> {
        if let Some(parent) = self.config.ingress_socket.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        if self.config.ingress_socket.exists() {
            match tokio::fs::remove_file(&self.config.ingress_socket).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }

        Ok(UnixListener::bind(&self.config.ingress_socket)?)
    }

    pub async fn handle_local_stream(&self, mut stream: UnixStream) -> Result<()> {
        let envelope = tlbus_core::read_envelope(&mut stream).await?;
        if let Err(error) = self.forward_remote(envelope.clone()).await {
            self.try_send_local_error_reply(&envelope, &error).await;
            return Err(error);
        }
        Ok(())
    }

    pub async fn forward_remote(&self, mut envelope: Envelope) -> Result<()> {
        let target = match TargetAddress::parse(&envelope.to) {
            Ok(target) => target,
            Err(error) => {
                log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
                return Err(error);
            }
        };
        let remote_pool = match target.pool() {
            Some(remote_pool) => remote_pool,
            None => {
                let error = BusError::InvalidEnvelope(format!(
                    "bridge expects federated targets, got `{}`",
                    target.raw()
                ));
                log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
                return Err(error);
            }
        };

        if remote_pool == self.config.pool {
            let error = BusError::InvalidEnvelope(format!(
                "target `{}` resolves to the local pool and should not enter the bridge",
                target.raw()
            ));
            log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
            return Err(error);
        }

        let peer_state = match self.peer_state(remote_pool) {
            Some(peer_state) => peer_state,
            None => {
                let error = BusError::ServiceUnavailable {
                    service: target.route_key(),
                };
                log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
                return Err(error);
            }
        };

        if !peer_state.connected {
            let error = BusError::ServiceUnavailable {
                service: target.route_key(),
            };
            log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
            return Err(error);
        }

        let descriptor = peer_state.manifest.service(&target.route_key());
        if descriptor.is_none() && !allows_unlisted_remote_target(&target) {
            let error = BusError::RouteNotFound {
                service: target.route_key(),
            };
            log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
            return Err(error);
        }

        if let Some(descriptor) = descriptor {
            if !descriptor.active {
                let error = BusError::ServiceUnavailable {
                    service: descriptor.service.clone(),
                };
                log_envelope_event("export_rejected", &envelope, &format!("error={error}"));
                return Err(error);
            }
        }

        envelope
            .headers
            .entry("protocol".to_string())
            .or_insert_with(|| DEFAULT_PROTOCOL.to_string());
        envelope
            .headers
            .insert("transport".to_string(), TRANSPORT_HTTP2.to_string());
        envelope
            .headers
            .insert("from_pool".to_string(), self.config.pool.clone());
        envelope
            .headers
            .insert("to_pool".to_string(), remote_pool.to_string());

        if let Err(error) = self
            .send_to_peer(peer_state.addr.clone(), envelope.clone())
            .await
        {
            self.mark_pool_unavailable(remote_pool);
            log_envelope_event(
                "export_failed",
                &envelope,
                &format!(
                    "from_pool={} to_pool={} target={} peer={} error={error}",
                    self.config.pool,
                    remote_pool,
                    target.route_key(),
                    peer_state.addr
                ),
            );
            return Err(match error {
                BusError::RouteNotFound { .. } => BusError::RouteNotFound {
                    service: target.route_key(),
                },
                BusError::ServiceUnavailable { .. } => BusError::ServiceUnavailable {
                    service: target.route_key(),
                },
                other => other,
            });
        }

        log_envelope_event(
            "exported",
            &envelope,
            &format!(
                "from_pool={} to_pool={} target={} peer={}",
                self.config.pool,
                remote_pool,
                target.route_key(),
                peer_state.addr
            ),
        );
        Ok(())
    }

    pub async fn forward_imported(&self, envelope: Envelope) -> Result<()> {
        let target = match TargetAddress::parse(&envelope.to) {
            Ok(target) => target,
            Err(error) => {
                log_envelope_event("import_rejected", &envelope, &format!("error={error}"));
                return Err(error);
            }
        };
        let target_pool = match target.pool() {
            Some(target_pool) => target_pool,
            None => {
                let error = BusError::InvalidEnvelope(format!(
                    "received non-federated remote target `{}`",
                    target.raw()
                ));
                log_envelope_event("import_rejected", &envelope, &format!("error={error}"));
                return Err(error);
            }
        };

        if target_pool != self.config.pool {
            let error = BusError::InvalidEnvelope(format!(
                "target `{}` does not belong to local pool `{}`",
                target.raw(),
                self.config.pool
            ));
            log_envelope_event("import_rejected", &envelope, &format!("error={error}"));
            return Err(error);
        }

        let protocol = envelope
            .headers
            .get("protocol")
            .map(String::as_str)
            .unwrap_or_default();
        if protocol.is_empty() {
            let error =
                BusError::InvalidEnvelope("missing required remote header `protocol`".to_string());
            log_envelope_event("import_rejected", &envelope, &format!("error={error}"));
            return Err(error);
        }

        if let Err(error) = self.validate_local_target(&target) {
            log_envelope_event("import_rejected", &envelope, &format!("error={error}"));
            return Err(error);
        }

        let mut stream = match UnixStream::connect(&self.config.bus_socket).await {
            Ok(stream) => stream,
            Err(error) => {
                log_envelope_event(
                    "import_failed",
                    &envelope,
                    &format!(
                        "pool={} target={} bus_socket={} error={error}",
                        self.config.pool,
                        target.route_key(),
                        self.config.bus_socket.display()
                    ),
                );
                return Err(error.into());
            }
        };
        if let Err(error) = write_envelope(&mut stream, &envelope).await {
            log_envelope_event(
                "import_failed",
                &envelope,
                &format!(
                    "pool={} target={} bus_socket={} error={error}",
                    self.config.pool,
                    target.route_key(),
                    self.config.bus_socket.display()
                ),
            );
            return Err(error);
        }

        log_envelope_event(
            "imported",
            &envelope,
            &format!(
                "pool={} target={} from_pool={}",
                self.config.pool,
                target.route_key(),
                envelope
                    .headers
                    .get("from_pool")
                    .map(String::as_str)
                    .unwrap_or("unknown")
            ),
        );
        Ok(())
    }

    pub async fn sync_peer(&self, peer_addr: &str) -> Result<PoolHandshakeResponse> {
        let request = PoolHandshakeRequest {
            pool: self.config.pool.clone(),
            secret: self.config.pool_secret.clone().unwrap_or_default(),
            advertise_addr: self.config.advertise_addr.clone(),
            manifest: self.local_manifest(),
        };

        let response = self
            .send_handshake_request(peer_addr.to_string(), request)
            .await?;
        if !response.allowed {
            self.mark_addr_unavailable(peer_addr);
            return Err(BusError::Transport(format!(
                "peer `{peer_addr}` rejected manifest sync: {}",
                response
                    .reason
                    .unwrap_or_else(|| "manifest sync rejected".to_string())
            )));
        }

        self.store_peer_manifest(
            response.pool.clone(),
            response.advertise_addr.clone(),
            response.manifest.clone(),
            true,
        );
        Ok(response)
    }

    pub fn local_manifest(&self) -> PoolManifest {
        PoolManifest {
            pool: self.config.pool.clone(),
            services: self.config.router.manifest_for_pool(&self.config.pool),
        }
    }

    pub fn peer_manifest(&self, pool: &str) -> Option<PoolManifest> {
        self.peers
            .read()
            .expect("peer read lock poisoned")
            .get(pool)
            .map(|state| state.manifest.clone())
    }

    async fn run_peer_sync_loop(&self) -> Result<()> {
        loop {
            let peer_addrs = self
                .known_peer_addrs
                .read()
                .expect("peer address read lock poisoned")
                .iter()
                .cloned()
                .collect::<Vec<_>>();

            for peer_addr in peer_addrs {
                if peer_addr == self.config.advertise_addr {
                    continue;
                }

                if let Err(error) = self.sync_peer(&peer_addr).await {
                    eprintln!("tlbus-bridge: manifest sync with `{peer_addr}` failed: {error}");
                }
            }

            sleep(self.config.manifest_sync_interval).await;
        }
    }

    async fn serve_local_ingress(&self, listener: UnixListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let bridge = self.clone();

            tokio::spawn(async move {
                if let Err(error) = bridge.handle_local_stream(stream).await {
                    eprintln!("tlbus-bridge: local ingress rejected message: {error}");
                }
            });
        }
    }

    async fn serve_network_ingress(&self, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let bridge = self.clone();

            tokio::spawn(async move {
                if let Err(error) = bridge.handle_peer_stream(stream).await {
                    eprintln!("tlbus-bridge: peer ingress rejected message: {error}");
                }
            });
        }
    }

    async fn handle_peer_stream(&self, stream: TcpStream) -> Result<()> {
        let mut connection = server::handshake(stream)
            .await
            .map_err(|error| BusError::Transport(error.to_string()))?;

        while let Some(result) = connection.accept().await {
            let (request, mut respond) =
                result.map_err(|error| BusError::Transport(error.to_string()))?;

            let peer_response = match self.process_peer_request(request).await {
                Ok(response) => response,
                Err(error) => PeerHttpResponse {
                    status: status_for_error(&error),
                    body: error.to_string().into_bytes(),
                },
            };

            let response = Response::builder()
                .status(peer_response.status)
                .body(())
                .map_err(|error| BusError::Transport(error.to_string()))?;
            let has_body = !peer_response.body.is_empty();
            let mut send_stream = respond
                .send_response(response, !has_body)
                .map_err(|error| BusError::Transport(error.to_string()))?;

            if has_body {
                send_stream
                    .send_data(Bytes::from(peer_response.body), true)
                    .map_err(|error| BusError::Transport(error.to_string()))?;
            }
        }

        Ok(())
    }

    async fn process_peer_request(
        &self,
        request: Request<h2::RecvStream>,
    ) -> Result<PeerHttpResponse> {
        if request.method() != Method::POST {
            return Err(BusError::Transport(format!(
                "unexpected request target {} {}",
                request.method(),
                request.uri().path()
            )));
        }

        match request.uri().path() {
            FEDERATION_PATH => {
                let body = collect_body(request.into_body()).await?;
                let envelope = decode_envelope(&body)?;
                self.forward_imported(envelope).await?;
                Ok(PeerHttpResponse {
                    status: StatusCode::ACCEPTED,
                    body: Vec::new(),
                })
            }
            HANDSHAKE_PATH => {
                let body = collect_body(request.into_body()).await?;
                let handshake: PoolHandshakeRequest = rmp_serde::from_slice(&body)
                    .map_err(|error| BusError::Codec(error.to_string()))?;
                let response = self.handle_handshake(handshake);
                let status = if response.allowed {
                    StatusCode::OK
                } else {
                    StatusCode::FORBIDDEN
                };
                let body = rmp_serde::to_vec(&response)
                    .map_err(|error| BusError::Codec(error.to_string()))?;
                Ok(PeerHttpResponse { status, body })
            }
            path => Err(BusError::Transport(format!(
                "unexpected request path `{path}`"
            ))),
        }
    }

    fn handle_handshake(&self, handshake: PoolHandshakeRequest) -> PoolHandshakeResponse {
        if let Some(reason) = self.handshake_denial_reason(&handshake) {
            return PoolHandshakeResponse {
                pool: self.config.pool.clone(),
                allowed: false,
                advertise_addr: String::new(),
                manifest: PoolManifest {
                    pool: self.config.pool.clone(),
                    services: Vec::new(),
                },
                reason: Some(reason),
            };
        }

        self.store_peer_manifest(
            handshake.pool.clone(),
            handshake.advertise_addr,
            handshake.manifest,
            true,
        );

        PoolHandshakeResponse {
            pool: self.config.pool.clone(),
            allowed: true,
            advertise_addr: self.config.advertise_addr.clone(),
            manifest: self.local_manifest(),
            reason: None,
        }
    }

    fn handshake_denial_reason(&self, handshake: &PoolHandshakeRequest) -> Option<String> {
        if handshake.pool.trim().is_empty() {
            return Some("peer pool must not be empty".to_string());
        }

        if handshake.pool == self.config.pool {
            return Some("peer pool must differ from local pool".to_string());
        }

        if handshake.secret.trim().is_empty() {
            return Some("pool secret must not be empty".to_string());
        }

        let Some(expected_secret) = &self.config.pool_secret else {
            return Some("pool federation handshake is disabled".to_string());
        };

        if &handshake.secret != expected_secret {
            return Some("pool secret mismatch".to_string());
        }

        if handshake.advertise_addr.trim().is_empty() || !handshake.advertise_addr.contains(':') {
            return Some("peer advertise address must be host:port".to_string());
        }

        if handshake.manifest.pool != handshake.pool {
            return Some("pool manifest does not match the declared peer pool".to_string());
        }

        let prefix = format!("{}.", handshake.pool);
        if handshake
            .manifest
            .services
            .iter()
            .any(|service| !service.service.starts_with(&prefix))
        {
            return Some("peer manifest includes services outside the declared pool".to_string());
        }

        None
    }

    async fn send_handshake_request(
        &self,
        peer_addr: String,
        request: PoolHandshakeRequest,
    ) -> Result<PoolHandshakeResponse> {
        let body =
            rmp_serde::to_vec(&request).map_err(|error| BusError::Codec(error.to_string()))?;
        let (status, response_body) = self
            .send_peer_request(peer_addr.clone(), HANDSHAKE_PATH, body)
            .await?;

        if status != StatusCode::OK && status != StatusCode::FORBIDDEN {
            return Err(BusError::Transport(format!(
                "peer `{peer_addr}` rejected the handshake with status {status}"
            )));
        }

        rmp_serde::from_slice(&response_body).map_err(|error| BusError::Codec(error.to_string()))
    }

    async fn send_to_peer(&self, peer_addr: String, envelope: Envelope) -> Result<()> {
        let body = encode_envelope(&envelope)?;
        let (status, _) = self
            .send_peer_request(peer_addr.clone(), FEDERATION_PATH, body)
            .await?;

        if status.is_success() {
            return Ok(());
        }

        match status {
            StatusCode::NOT_FOUND => Err(BusError::RouteNotFound {
                service: envelope.to,
            }),
            StatusCode::SERVICE_UNAVAILABLE => Err(BusError::ServiceUnavailable {
                service: envelope.to,
            }),
            _ => Err(BusError::Transport(format!(
                "peer `{peer_addr}` rejected the envelope with status {status}"
            ))),
        }
    }

    async fn send_peer_request(
        &self,
        peer_addr: String,
        path: &'static str,
        body: Vec<u8>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let stream = TcpStream::connect(&peer_addr).await?;
        let (mut sender, connection) = client::handshake(stream)
            .await
            .map_err(|error| BusError::Transport(error.to_string()))?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("tlbus-bridge: http2 client connection ended: {error}");
            }
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .body(())
            .map_err(|error| BusError::Transport(error.to_string()))?;

        let (response_future, mut send_stream) = sender
            .send_request(request, false)
            .map_err(|error| BusError::Transport(error.to_string()))?;

        send_stream
            .send_data(Bytes::from(body), true)
            .map_err(|error| BusError::Transport(error.to_string()))?;

        let response = response_future
            .await
            .map_err(|error| BusError::Transport(error.to_string()))?;
        let status = response.status();
        let body = collect_body(response.into_body()).await?;
        Ok((status, body))
    }

    fn validate_local_target(&self, target: &TargetAddress) -> Result<()> {
        if target.service() == REGISTRY_SERVICE {
            return Ok(());
        }

        let route_key = target.route_key();
        if let Some(descriptor) = self.config.router.descriptor(&route_key) {
            return if descriptor.active {
                Ok(())
            } else {
                Err(BusError::ServiceUnavailable {
                    service: descriptor.service,
                })
            };
        }

        if let Some(descriptor) = self.config.router.descriptor(target.service()) {
            return if descriptor.active {
                Ok(())
            } else {
                Err(BusError::ServiceUnavailable {
                    service: descriptor.service,
                })
            };
        }

        Err(BusError::RouteNotFound { service: route_key })
    }

    fn store_peer_manifest(
        &self,
        pool: String,
        addr: String,
        manifest: PoolManifest,
        connected: bool,
    ) {
        self.known_peer_addrs
            .write()
            .expect("peer address write lock poisoned")
            .insert(addr.clone());
        self.peers
            .write()
            .expect("peer write lock poisoned")
            .insert(
                pool,
                PeerState {
                    addr,
                    manifest,
                    connected,
                },
            );
    }

    fn mark_addr_unavailable(&self, addr: &str) {
        let mut peers = self.peers.write().expect("peer write lock poisoned");
        for state in peers.values_mut() {
            if state.addr == addr {
                state.connected = false;
            }
        }
    }

    fn mark_pool_unavailable(&self, pool: &str) {
        if let Some(state) = self
            .peers
            .write()
            .expect("peer write lock poisoned")
            .get_mut(pool)
        {
            state.connected = false;
        }
    }

    fn peer_state(&self, pool: &str) -> Option<PeerState> {
        self.peers
            .read()
            .expect("peer read lock poisoned")
            .get(pool)
            .cloned()
    }

    async fn try_send_local_error_reply(&self, request: &Envelope, error: &BusError) {
        let Some(reply_to) = request.reply_to().map(str::to_string) else {
            return;
        };

        let payload = match serde_json::to_vec(&json!({
            "error": {
                "code": error_code_for_bus_error(error),
                "message": error.to_string(),
            }
        })) {
            Ok(payload) => payload,
            Err(encode_error) => {
                eprintln!(
                    "tlbus-bridge: event=local_error_reply_failed {} error=json encode failed: {encode_error}",
                    request.trace_fields()
                );
                return;
            }
        };

        let sender = TargetAddress::parse(&request.to)
            .map(|target| target.route_key())
            .unwrap_or_else(|_| format!("{}.bridge", self.config.pool));
        let mut response = Envelope::new(sender, reply_to, payload);
        response.ttl_ms = request.ttl_ms;
        response.headers.insert(
            CONTENT_TYPE_HEADER.to_string(),
            CONTENT_TYPE_JSON.to_string(),
        );
        response
            .headers
            .insert(PROTOCOL_HEADER.to_string(), STANDARD_PROTOCOL.to_string());
        if let Some(txn_id) = request.txn_id() {
            response
                .headers
                .insert(TXN_ID_HEADER.to_string(), txn_id.to_string());
        }

        let mut stream = match UnixStream::connect(&self.config.bus_socket).await {
            Ok(stream) => stream,
            Err(connect_error) => {
                eprintln!(
                    "tlbus-bridge: event=local_error_reply_failed {} bus_socket={} error={connect_error}",
                    request.trace_fields(),
                    self.config.bus_socket.display(),
                );
                return;
            }
        };

        if let Err(write_error) = write_envelope(&mut stream, &response).await {
            eprintln!(
                "tlbus-bridge: event=local_error_reply_failed {} bus_socket={} error={write_error}",
                request.trace_fields(),
                self.config.bus_socket.display(),
            );
            return;
        }

        eprintln!(
            "tlbus-bridge: event=local_error_reply {} code={} to={}",
            request.trace_fields(),
            error_code_for_bus_error(error),
            response.to
        );
    }
}

#[derive(Debug, Clone)]
struct PeerHttpResponse {
    status: StatusCode,
    body: Vec<u8>,
}

async fn collect_body(mut body: h2::RecvStream) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|error| BusError::Transport(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}

fn status_for_error(error: &BusError) -> StatusCode {
    match error {
        BusError::RouteNotFound { .. } => StatusCode::NOT_FOUND,
        BusError::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        BusError::Rejected { .. } => StatusCode::FORBIDDEN,
        BusError::InvalidEnvelope(_) | BusError::Codec(_) | BusError::Configuration(_) => {
            StatusCode::BAD_REQUEST
        }
        BusError::Expired => StatusCode::GONE,
        BusError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        BusError::Transport(_) | BusError::Io(_) => StatusCode::BAD_GATEWAY,
    }
}

fn error_code_for_bus_error(error: &BusError) -> &'static str {
    match error {
        BusError::RouteNotFound { .. } => "service_not_found",
        BusError::ServiceUnavailable { .. } => "service_unavailable",
        BusError::Rejected { .. } => "rejected",
        BusError::InvalidEnvelope(_) => "invalid_envelope",
        BusError::Codec(_) => "codec_error",
        BusError::Configuration(_) => "configuration_error",
        BusError::Unsupported(_) => "unsupported",
        BusError::Expired => "expired",
        BusError::Transport(_) | BusError::Io(_) => "transport_error",
    }
}

fn allows_unlisted_remote_target(target: &TargetAddress) -> bool {
    if target.service() == REGISTRY_SERVICE {
        return true;
    }
    matches!(target.action(), Some("inbox"))
}

fn log_envelope_event(event: &str, envelope: &Envelope, details: &str) {
    eprintln!(
        "tlbus-bridge: event={event} {} {details}",
        envelope.trace_fields()
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::path::PathBuf;
    use std::time::Duration;

    use serde_json::Value;
    use tempfile::tempdir;
    use tlbus_core::{
        BusError, Envelope, Router, ServiceCapability, ServiceManifest, ServiceMode, read_envelope,
    };
    use tokio::net::{TcpListener, UnixListener, UnixStream};

    use crate::{Bridge, BridgeConfig};

    fn loopback_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
    }

    fn service_manifest(name: &str) -> ServiceManifest {
        ServiceManifest {
            name: name.to_string(),
            secret: "shared-secret".to_string(),
            is_client: false,
            features: BTreeMap::new(),
            capabilities: vec![ServiceCapability {
                name: "echo".to_string(),
                address: format!("{name}.say"),
                description: "Echoes the incoming payload".to_string(),
            }],
            modes: vec![ServiceMode {
                transport: "http2".to_string(),
                protocol: "standard".to_string(),
                protocol_version: None,
                content_type: Some("application/json".to_string()),
            }],
        }
    }

    #[tokio::test]
    async fn bridge_discovers_peer_manifest_and_forwards_messages() {
        let left_dir = tempdir().unwrap();
        let right_dir = tempdir().unwrap();

        let bus_socket = right_dir.path().join("bus.sock");
        let bus_listener = UnixListener::bind(&bus_socket).unwrap();

        let right_peer_listener = TcpListener::bind(loopback_addr()).await.unwrap();
        let right_peer_addr = right_peer_listener.local_addr().unwrap();

        let right_router = Router::new();
        let right_service_socket = right_dir.path().join("ps2-echo.sock");
        let _right_service_listener = UnixListener::bind(&right_service_socket).unwrap();
        right_router.register_manifest(&service_manifest("ps2.echo"), right_service_socket);
        let right_bridge = Bridge::new(BridgeConfig::new(
            "ps2",
            bus_socket.clone(),
            right_dir.path().join("ingress.sock"),
            right_peer_addr,
            right_peer_addr.to_string(),
            right_router,
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let right_server = tokio::spawn({
            let right_bridge = right_bridge.clone();
            let right_peer_listener = right_peer_listener;
            async move {
                for _ in 0..2 {
                    let (stream, _) = right_peer_listener.accept().await.unwrap();
                    right_bridge.handle_peer_stream(stream).await.unwrap();
                }
            }
        });

        let left_router = Router::new();
        left_router.register_manifest(
            &ServiceManifest {
                name: "ps1.client".to_string(),
                secret: "shared-secret".to_string(),
                is_client: true,
                features: BTreeMap::from([("kind".to_string(), "client".to_string())]),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            PathBuf::from("/tmp/ps1-client.sock"),
        );
        let left_bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            left_dir.path().join("bus.sock"),
            left_dir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9001",
            left_router,
            vec![right_peer_addr.to_string()],
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let response = left_bridge
            .sync_peer(&right_peer_addr.to_string())
            .await
            .unwrap();
        assert_eq!(response.pool, "ps2");

        let remote_manifest = left_bridge.peer_manifest("ps2").unwrap();
        let remote_descriptor = remote_manifest.service("ps2.echo").unwrap();
        assert_eq!(remote_descriptor.capabilities[0].name, "echo");
        assert_eq!(remote_descriptor.modes[0].transport, "http2");

        let mut message = Envelope::new("ps1.client", "ps2.echo.say", b"hello federation".to_vec());
        message
            .headers
            .insert("protocol".to_string(), "raw".to_string());
        message
            .headers
            .insert("txn_id".to_string(), "txn-123".to_string());
        left_bridge.forward_remote(message).await.unwrap();

        let (mut inbound, _) = bus_listener.accept().await.unwrap();
        let delivered = read_envelope(&mut inbound).await.unwrap();
        right_server.abort();

        assert_eq!(delivered.to, "ps2.echo.say");
        assert_eq!(delivered.payload, b"hello federation".to_vec());
        assert_eq!(delivered.headers.get("from_pool"), Some(&"ps1".to_string()));
        assert_eq!(delivered.headers.get("to_pool"), Some(&"ps2".to_string()));
        assert_eq!(delivered.txn_id(), Some("txn-123"));
    }

    #[tokio::test]
    async fn bridge_rejects_targets_missing_from_peer_manifest() {
        let left_dir = tempdir().unwrap();
        let right_dir = tempdir().unwrap();

        let right_peer_listener = TcpListener::bind(loopback_addr()).await.unwrap();
        let right_peer_addr = right_peer_listener.local_addr().unwrap();

        let right_router = Router::new();
        let right_service_socket = right_dir.path().join("ps2-echo.sock");
        let _right_service_listener = UnixListener::bind(&right_service_socket).unwrap();
        right_router.register_manifest(&service_manifest("ps2.echo"), right_service_socket);
        let right_bridge = Bridge::new(BridgeConfig::new(
            "ps2",
            right_dir.path().join("bus.sock"),
            right_dir.path().join("ingress.sock"),
            right_peer_addr,
            right_peer_addr.to_string(),
            right_router,
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let right_server = tokio::spawn({
            let right_bridge = right_bridge.clone();
            let right_peer_listener = right_peer_listener;
            async move {
                let (stream, _) = right_peer_listener.accept().await.unwrap();
                right_bridge.handle_peer_stream(stream).await.unwrap();
            }
        });

        let left_bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            left_dir.path().join("bus.sock"),
            left_dir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9001",
            Router::new(),
            vec![right_peer_addr.to_string()],
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));
        left_bridge
            .sync_peer(&right_peer_addr.to_string())
            .await
            .unwrap();

        let mut message = Envelope::new("ps1.client", "ps2.calcola.evaluate", b"payload".to_vec());
        message
            .headers
            .insert("protocol".to_string(), "raw".to_string());

        let error = left_bridge.forward_remote(message).await.unwrap_err();
        right_server.abort();
        assert!(matches!(
            error,
            BusError::RouteNotFound { service } if service == "ps2.calcola"
        ));
    }

    #[tokio::test]
    async fn bridge_allows_registry_requests_even_if_registry_is_not_in_peer_manifest() {
        let left_dir = tempdir().unwrap();
        let right_dir = tempdir().unwrap();

        let bus_socket = right_dir.path().join("bus.sock");
        let bus_listener = UnixListener::bind(&bus_socket).unwrap();

        let right_peer_listener = TcpListener::bind(loopback_addr()).await.unwrap();
        let right_peer_addr = right_peer_listener.local_addr().unwrap();

        let right_router = Router::new();
        let right_service_socket = right_dir.path().join("ps2-echo.sock");
        let _right_service_listener = UnixListener::bind(&right_service_socket).unwrap();
        right_router.register_manifest(&service_manifest("ps2.echo"), right_service_socket);
        let right_bridge = Bridge::new(BridgeConfig::new(
            "ps2",
            bus_socket.clone(),
            right_dir.path().join("ingress.sock"),
            right_peer_addr,
            right_peer_addr.to_string(),
            right_router,
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let right_server = tokio::spawn({
            let right_bridge = right_bridge.clone();
            let right_peer_listener = right_peer_listener;
            async move {
                for _ in 0..2 {
                    let (stream, _) = right_peer_listener.accept().await.unwrap();
                    right_bridge.handle_peer_stream(stream).await.unwrap();
                }
            }
        });

        let left_bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            left_dir.path().join("bus.sock"),
            left_dir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9001",
            Router::new(),
            vec![right_peer_addr.to_string()],
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));
        left_bridge
            .sync_peer(&right_peer_addr.to_string())
            .await
            .unwrap();

        let mut message = Envelope::new(
            "ps1.agent",
            "ps2.registry.get_manifest",
            br#"{"service":"ps2.invoice"}"#.to_vec(),
        );
        message
            .headers
            .insert("protocol".to_string(), "raw".to_string());
        message
            .headers
            .insert("txn_id".to_string(), "txn-registry-1".to_string());

        left_bridge.forward_remote(message).await.unwrap();

        let (mut inbound, _) = bus_listener.accept().await.unwrap();
        let delivered = read_envelope(&mut inbound).await.unwrap();
        right_server.abort();

        assert_eq!(delivered.to, "ps2.registry.get_manifest");
        assert_eq!(delivered.txn_id(), Some("txn-registry-1"));
    }

    #[tokio::test]
    async fn bridge_allows_inbox_targets_even_if_peer_manifest_is_stale() {
        let left_dir = tempdir().unwrap();
        let right_dir = tempdir().unwrap();

        let bus_socket = right_dir.path().join("bus.sock");
        let bus_listener = UnixListener::bind(&bus_socket).unwrap();

        let right_peer_listener = TcpListener::bind(loopback_addr()).await.unwrap();
        let right_peer_addr = right_peer_listener.local_addr().unwrap();

        let right_router = Router::new();
        let right_seed_socket = right_dir.path().join("ps1-seed.sock");
        let _right_seed_listener = UnixListener::bind(&right_seed_socket).unwrap();
        right_router.register_manifest(&service_manifest("ps1.seed"), right_seed_socket);
        let right_bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            bus_socket.clone(),
            right_dir.path().join("ingress.sock"),
            right_peer_addr,
            right_peer_addr.to_string(),
            right_router,
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let right_server = tokio::spawn({
            let right_bridge = right_bridge.clone();
            let right_peer_listener = right_peer_listener;
            async move {
                for _ in 0..3 {
                    let (stream, _) = right_peer_listener.accept().await.unwrap();
                    right_bridge.handle_peer_stream(stream).await.unwrap();
                }
            }
        });

        let left_bridge = Bridge::new(BridgeConfig::new(
            "ps2",
            left_dir.path().join("bus.sock"),
            left_dir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9002",
            Router::new(),
            vec![right_peer_addr.to_string()],
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));
        left_bridge
            .sync_peer(&right_peer_addr.to_string())
            .await
            .unwrap();

        // Register the inbox owner only after handshake so peer manifest on ps2 stays stale.
        let right_probe_socket = right_dir.path().join("ps1-probe.sock");
        let _right_probe_listener = UnixListener::bind(&right_probe_socket).unwrap();
        right_bridge.config.router.register_manifest(
            &ServiceManifest {
                name: "ps1.probe".to_string(),
                secret: "shared-secret".to_string(),
                is_client: true,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            right_probe_socket,
        );

        let mut message = Envelope::new("ps2.registry", "ps1.probe.inbox", b"reply".to_vec());
        message
            .headers
            .insert("protocol".to_string(), "raw".to_string());
        message
            .headers
            .insert("txn_id".to_string(), "txn-inbox-stale".to_string());

        left_bridge.forward_remote(message).await.unwrap();

        let (mut inbound, _) = bus_listener.accept().await.unwrap();
        let delivered = read_envelope(&mut inbound).await.unwrap();
        right_server.abort();

        assert_eq!(delivered.to, "ps1.probe.inbox");
        assert_eq!(delivered.txn_id(), Some("txn-inbox-stale"));
    }

    #[tokio::test]
    async fn bridge_rejects_inactive_services_from_peer_manifest() {
        let left_dir = tempdir().unwrap();
        let right_dir = tempdir().unwrap();

        let right_peer_listener = TcpListener::bind(loopback_addr()).await.unwrap();
        let right_peer_addr = right_peer_listener.local_addr().unwrap();

        let right_router = Router::new();
        right_router.register_manifest(
            &service_manifest("ps2.calcola"),
            PathBuf::from("/tmp/ps2-calcola.sock"),
        );
        right_router.set_active("ps2.calcola", false);
        let right_bridge = Bridge::new(BridgeConfig::new(
            "ps2",
            right_dir.path().join("bus.sock"),
            right_dir.path().join("ingress.sock"),
            right_peer_addr,
            right_peer_addr.to_string(),
            right_router,
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let right_server = tokio::spawn({
            let right_bridge = right_bridge.clone();
            let right_peer_listener = right_peer_listener;
            async move {
                let (stream, _) = right_peer_listener.accept().await.unwrap();
                right_bridge.handle_peer_stream(stream).await.unwrap();
            }
        });

        let left_bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            left_dir.path().join("bus.sock"),
            left_dir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9001",
            Router::new(),
            vec![right_peer_addr.to_string()],
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));
        left_bridge
            .sync_peer(&right_peer_addr.to_string())
            .await
            .unwrap();

        let mut message = Envelope::new("ps1.client", "ps2.calcola.evaluate", b"payload".to_vec());
        message
            .headers
            .insert("protocol".to_string(), "raw".to_string());

        let error = left_bridge.forward_remote(message).await.unwrap_err();
        right_server.abort();
        assert!(matches!(
            error,
            BusError::ServiceUnavailable { service } if service == "ps2.calcola"
        ));
    }

    #[tokio::test]
    async fn local_ingress_reads_length_prefixed_envelopes() {
        let tempdir = tempdir().unwrap();
        let bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            tempdir.path().join("bus.sock"),
            tempdir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9001",
            Router::new(),
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let ingress_listener = bridge.bind_local_ingress().await.unwrap();
        let (mut client, server) = UnixStream::pair().unwrap();

        let handler = tokio::spawn({
            let bridge = bridge.clone();
            async move {
                let envelope = Envelope::new("client", "ps2.echo.say", b"payload".to_vec());
                tlbus_core::write_envelope(&mut client, &envelope)
                    .await
                    .unwrap();
                let error = bridge.handle_local_stream(server).await.unwrap_err();
                assert!(matches!(error, BusError::ServiceUnavailable { .. }));
            }
        });

        drop(ingress_listener);
        handler.await.unwrap();
    }

    #[tokio::test]
    async fn local_ingress_sends_error_reply_on_forward_failure() {
        let tempdir = tempdir().unwrap();
        let bus_socket = tempdir.path().join("bus.sock");
        let bus_listener = UnixListener::bind(&bus_socket).unwrap();
        let bridge = Bridge::new(BridgeConfig::new(
            "ps1",
            bus_socket,
            tempdir.path().join("ingress.sock"),
            loopback_addr(),
            "127.0.0.1:9001",
            Router::new(),
            Vec::new(),
            Duration::from_secs(30),
            Some("shared-pool-secret".to_string()),
        ));

        let (mut client, server) = UnixStream::pair().unwrap();
        let handler = tokio::spawn({
            let bridge = bridge.clone();
            async move {
                let mut envelope = Envelope::new("ps1.agent", "ps2.registry.list", b"{}".to_vec());
                envelope
                    .headers
                    .insert("reply_to".to_string(), "ps1.agent.inbox".to_string());
                envelope
                    .headers
                    .insert("txn_id".to_string(), "txn-bridge-error".to_string());
                tlbus_core::write_envelope(&mut client, &envelope)
                    .await
                    .unwrap();

                let error = bridge.handle_local_stream(server).await.unwrap_err();
                assert!(matches!(
                    error,
                    BusError::ServiceUnavailable { service } if service == "ps2.registry"
                ));
            }
        });

        let (mut inbound, _) = bus_listener.accept().await.unwrap();
        let response = read_envelope(&mut inbound).await.unwrap();
        handler.await.unwrap();

        assert_eq!(response.from, "ps2.registry");
        assert_eq!(response.to, "ps1.agent.inbox");
        assert_eq!(response.txn_id(), Some("txn-bridge-error"));
        let body: Value = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(body["error"]["code"], "service_unavailable");
    }
}
