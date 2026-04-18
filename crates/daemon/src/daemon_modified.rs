//Copyright (c) 2026 ThinkStudio LLC. All rights reserved.
// Licensed under the MIT License.

use std::path::{Path, PathBuf};
use std::time::Instant;
use std::net::SocketAddr;

use tlbus_core::{
    BusError, BusFrame, Envelope, Pipeline, PluginContext, Result, Router, ServiceManifest,
    ServiceRegistrationRequest, ServiceRegistrationResponse, TargetAddress, read_frame, write_envelope, write_frame,
};
use tlbus_plugin_auth::AuthPlugin;
use tlbus_plugin_hmac::HmacPlugin;
use tlbus_plugin_lineage::LineagePlugin;
use tlbus_plugin_manifest::ProtocolPlugin;
use tokio::net::{UnixListener, UnixStream};

use crate::observability::{record_message_metrics, init_tracer, health_check,start_metrics_server};

#[tokio::test]
async fn test_health_check() {
    // Start the health check server on a random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = tokio::spawn(async move {
       if let Err(error) = health_check(addr).await {
            eprintln!("Health check server error: {error}");
        }
    });

    // Wait for the server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a request to the health checkendpoint
    let client = Client::new();
    let uri = format!("http://{}:{}", addr.ip(), addr.port());
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap();

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Stop the server
    server.abort();
}

#[tokio::test]
async fn test_metrics_collection() {
    // Start the metrics server on a random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = tokio::spawn(async move {
        if let Err(error) = start_metrics_server(addr).await {
            eprintln!("Metrics server error: {error}");
        }
    });

    // Wait for the server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a request to the metrics endpoint
    let client = Client::new();
    let uri = format!("http://{}:{}", addr.ip(), addr.port());
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap();

    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Checkthat we get metrics data
    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(body_str.contains("tlbus_messages_total"));
    assert!(body_str.contains("tlbus_message_latency_seconds"));

// Stop the server
    server.abort();
}

#[tokio::test]
async fn test_tracing() {
    // Initialize tracing
    if let Err(error) = init_tracer() {
        eprintln!("Failed to initialize tracer: {error}");
        return;
    }

    // Create a test span
let span = tracing::info_span!("test_span");
    let _guard = span.enter();

    // Verify that tracing is working
    let spans = global::tracer_provider().get_tracer("test", None).get_spans();
    assert!(!spans.is_empty());
}

pubfninit_tracer() -> Result<()> {
    // Create a stdout exporter
    let exporter = opentelemetry::sdk::export::trace::stdout::new_pipeline().install_simple();
    
    // Create a tracer provider with the exporter
    let provider = opentelemetry::sdk::trace::TracerProvider::builder()
.with_simple_exporter(exporter)
        .build();
    
    // Set the global tracer provider
   opentelemetry::global::set_tracer_provider(provider);
    
    Ok(())
}

pub struct Daemon {
    config: DaemonConfig,
}

impl Daemon {
    pub fn new(config: DaemonConfig) ->Self {
        Self { config }
    }

    pub async fn serve(&self) -> Result<()> {
        let mutlisten_path = self.config.listen_path.clone();
        if listen_path.exists() {
            // Rebind cleanly after restarts; the socket file is local process state.
            std::fs::remove_file(&listen_path)?;
        }
        let listener = UnixListener::bind(&listen_path)?;
        
        // Initialize OpenTelemetrytracing
        if let Err(error) = init_tracer() {
            eprintln!("Failed to initialize tracer: {error}");
            return Err(BusError::Configuration("failed to initialize tracer".to_string()));
        }
        
        // Spawn metrics server
        let metrics_addr: std::net::SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let daemon = self.clone();
        tokio::spawn(async move {
            if let Err(error)= daemon.config.start_metrics_server(metrics_addr).await {
                eprintln!("Metrics server error: {error}");
            }
       });
        
        // Spawn health check server
        let health_check_addr: std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let daemon = self.clone();
        tokio::spawn(async move {
            if let Err(error) = health_check(health_check_addr).await {
                eprintln!("Health check server error: {error}");
            }
        });
        
        loop {
            // Each client connection carries one frame in v0.1, so per-connection tasks
            // keep the accept loop responsive without introducing a queueing subsystem.
            let (stream, _) = listener.accept().await?;
            let daemon = self.clone();
            
            tokio::spawn(async move {
                let start = Instant::now();
                let _span = tracing::info_span!("handle_connection");
                let _enter = _span.enter();
                
                let result = daemon.handle_connection(stream).await;
                
                // Record metrics
                let duration = start.elapsed();
                record_message_metrics(&Envelope::default(), duration);
                
                result
            });
        }
    }

    pub async fnhandle_connection(&self, mut stream: UnixStream) -> Result<()> {
        match read_frame(&mut stream).await?{
            BusFrame::Envelope(message) => self.process_envelope(message).await,
            BusFrame::ServiceRegistrationRequest(request) => self.handle_registration(&mut stream, request).await,
            BusFrame::ServiceRegistrationResponse(_) => Err(BusError::InvalidEnvelope(
                "daemon cannot accept registration responses".to_string(),
            )),
        }
    }

    async fn process_envelope(&self, mut message: Envelope) -> Result<()> {
        let _span = tracing::info_span!("process_envelope",message.from = %message.from, message.to = %message.to);
        let _enter = _span.enter();
        
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
                &format!("stage=receiveerror={error}"),
            );
            return Err(error);
        }
        
        if let Some(response) =ctx.take_terminal_response() {
            log_envelope_event("handled_by_plugin", &message, "stage=receive");
            Box::pin(self.process_envelope(response)).await??;
            return Ok(());
       }
        
        // Local delivery still uses service -> socket, but federated addresses can be
        // redirectedto the bridge sidecar when the pool differs from the local one.
        let routing = self.resolve_route(&message)?;
        let original_target = message.to.clone();
        let route_key = routing.route_key;
       let route = routing.socket_path;
        ctx.set_route(route.clone());
        
        if let Err(error) =self.config.pipeline.run_route(&mut ctx, &mut message) {
            log_envelope_event(
                "route_failed",
                &message,
                &format!("stage=route error={error}"),
            );
return Err(error);
        }
        
        // Routing already happened, so changing `to` after this pointwould make the
        // envelope disagree with the actual delivery target.
        if message.to != original_target {
            return Err(BusError::InvalidEnvelope(
                "`to` cannot change after routing".to_string(),
            ));
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
            "sent",
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

    fn resolve_route(&self, message:&Envelope) -> Result<ResolvedRoute> {
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

    fn handle_registration(
        &self,
        stream: &mut UnixStream,
        request: ServiceRegistrationRequest,
    ) -> Result<()> {
let response = self.register_service(request.manifest);
        write_envelope(stream, &Envelope::default()).await
    }

    fn register_service(&self, manifest: ServiceManifest) -> ServiceRegistrationResponse {
        let service_socket = self.service_socket_path(&manifest.name);
        let bus_socket = self.config.listen_path.to_string_lossy().to_string();
        
        let denial_reason = self.registration_denial_reason(&manifest);
        if let Some(reason) = denial_reason {
            return ServiceRegistrationResponse {
                secret: manifest.secret,
                pool: self.registration_pool(&manifest.name),
                allowed: false,
                active: false,
               service_socket: service_socket.to_string_lossy().to_string(),
                bus_socket: bus_socket.clone(),
                reason: Some(reason),
            };
        }
        
        let descriptor = self.config.router.register_manifest(&manifest, service_socket.clone());
        
        ServiceRegistrationResponse {
            secret: manifest.secret,
            pool: descriptor.pool.unwrap_or("local".to_string()),
            allowed: true,
            active: descriptor.active,
            service_socket: service_socket.to_string_lossy().to_string(),
            bus_socket: bus_socket,
            reason: None,
        }
    }

    fnregistration_denial_reason(&self, manifest: &ServiceManifest) ->Option<String> {
        if manifest.name.trim().is_empty() {
            return Some("service name must not be empty".to_string());
        }
        
        if let Some(expected_secret) = &self.config.service_secret {
            if &manifest.secret !=expected_secret {
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
        self.config.federation.as_ref().map(|federation| federation.local_pool.clone()).unwrap_or_else(|| {
            service_name.split('.').next().map(str::to_string).unwrap_or("local".to_string())
        })
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
        
        self.config.service_socket_dir.join(format!("{basename}.sock"))
    }
}

pub struct DaemonConfig {
pub listen_path: PathBuf,
    pub service_socket_dir: PathBuf,
    pub router: Router,
    pub pipeline: Pipeline,
    pub federation: Option<FederationConfig>,
    pub service_secret: Option<String>,
}

impl DaemonConfig {
    pubfn new(listen_path: impl Into<PathBuf>, router: Router, pipeline: Pipeline) -> Self {
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

    pub fn with_service_socket_dir(mut self, service_socket_dir: impl Into<PathBuf>) -> Self {
        self.service_socket_dir = service_socket_dir.into();
        self
    }

    pub fn with_service_secret(mut self, service_secret: Option<String>) -> Self {
        self.service_secret = service_secret.filter(|value| !value.trim().is_empty());
        self
    }

    pub fn with_federation(mut self, federation: FederationConfig) -> Self {
self.federation = Some(federation);
        self
}
}

pub struct FederationConfig {
    pub local_pool: String,
    pub bridge_socket: PathBuf,
}

pub struct ResolvedRoute {
    pub route_key: String,
    pub socket_path: PathBuf,
}

pub fn log_envelope_event(event:&str, envelope: &Envelope, details: &str) {
    eprintln!("tlbus-daemon: event={event} {} {details}", envelope.trace_fields());
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
use std::io;
    use std::io::ErrorKind;
    use std::io::Error;
    use std::io::Read;
    use std::io::Write;
    use std::io::Cursor;
    use std::io::BufReader;
    use std::io::BufWriter;
use std::io::ReadBuf;
    use std::io::ReadHalf;
    use std::io::WriteHalf;
    use std::io::split;
    use std::io::copy;
    use std::io::sink;
    use std::io::stdout;
    use std::io::stderr;
    use std::io::BufRead;
    use std::io::BufReadExt;
    use std::io::BufWriter;
    use std::io::BufWriter;
    use std::io::BufWriter;
    use hyper::Client;
    usehyper::Request;
    use hyper::Body;
    use prometheus::Encoder;
    use prometheus::TextEncoder;
    use opentelemetry::trace::Tracer;
    use opentelemetry::trace::TraceContextExt;
    use tower_http::trace::OpenTelemetrySpanExt;
}
