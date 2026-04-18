use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Instant;

use hyper::server::conn::AddrIncoming;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
use tlbus_core::{BusError, Envelope, Plugin, PluginContext, Result};

const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9090";

fn messages_total() -> &'static IntCounter {
    static COUNTER: OnceLock<IntCounter> = OnceLock::new();
    COUNTER.get_or_init(|| {
        let counter = IntCounter::new("tlbus_messages_total", "Total number of messages")
            .expect("create tlbus_messages_total");
        prometheus::default_registry()
            .register(Box::new(counter.clone()))
            .expect("register tlbus_messages_total");
        counter
    })
}

fn message_latency_seconds() -> &'static Histogram {
    static HISTOGRAM: OnceLock<Histogram> = OnceLock::new();
    HISTOGRAM.get_or_init(|| {
        let histogram = Histogram::with_opts(HistogramOpts::new(
            "tlbus_message_latency_seconds",
            "Latency of message processing in seconds",
        ))
        .expect("create tlbus_message_latency_seconds");
        prometheus::default_registry()
            .register(Box::new(histogram.clone()))
            .expect("register tlbus_message_latency_seconds");
        histogram
    })
}

async fn metrics_handler(
    request: Request<Body>,
) -> std::result::Result<Response<Body>, hyper::Error> {
    match request.uri().path() {
        "/metrics" => {
            let metric_families = prometheus::gather();
            let encoder = TextEncoder::new();
            let mut buffer = Vec::new();

            let response = match encoder.encode(&metric_families, &mut buffer) {
                Ok(()) => Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::CONTENT_TYPE, encoder.format_type())
                    .body(Body::from(buffer))
                    .expect("build metrics response"),
                Err(error) => Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(format!("failed to encode metrics: {error}")))
                    .expect("build metrics error response"),
            };

            Ok(response)
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .expect("build 404 response")),
    }
}

fn parse_metrics_addr() -> Result<SocketAddr> {
    let raw = std::env::var("TLB_METRICS_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_METRICS_ADDR.to_string());

    raw.parse().map_err(|_| {
        BusError::Configuration(format!(
            "invalid `TLB_METRICS_ADDR` `{raw}`: expected host:port"
        ))
    })
}

fn spawn_metrics_server(addr: SocketAddr) -> Result<()> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(BusError::Configuration(
            "observability plugin requires a Tokio runtime".to_string(),
        ));
    }

    // Ensure metrics exist even before any message is processed.
    let _ = messages_total();
    let _ = message_latency_seconds();

    let incoming = AddrIncoming::bind(&addr).map_err(|error| {
        BusError::Configuration(format!("failed to bind metrics server: {error}"))
    })?;

    let make_service =
        make_service_fn(|_conn| async { Ok::<_, hyper::Error>(service_fn(metrics_handler)) });

    tokio::spawn(async move {
        let server = Server::builder(incoming).serve(make_service);
        if let Err(error) = server.await {
            eprintln!("metrics server error: {error}");
        }
    });

    Ok(())
}

pub struct ObservabilityPlugin {
    _metrics_addr: SocketAddr,
}

impl ObservabilityPlugin {
    pub fn new() -> Result<Self> {
        let metrics_addr = parse_metrics_addr()?;
        spawn_metrics_server(metrics_addr)?;
        Ok(Self {
            _metrics_addr: metrics_addr,
        })
    }
}

impl Plugin for ObservabilityPlugin {
    fn name(&self) -> &'static str {
        "observability"
    }

    fn on_receive(&self, ctx: &mut PluginContext, _msg: &mut Envelope) -> Result<()> {
        if ctx.flow_started_at().is_none() {
            ctx.set_flow_started_at(Instant::now());
        }
        messages_total().inc();
        Ok(())
    }

    fn on_deliver(&self, ctx: &mut PluginContext, _msg: &mut Envelope) -> Result<()> {
        if let Some(elapsed) = ctx.flow_elapsed() {
            message_latency_seconds().observe(elapsed.as_secs_f64());
        }
        Ok(())
    }
}
