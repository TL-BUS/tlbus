use serde_json::{Value, json};
use tlbus_core::{
    BusError, Envelope, Plugin, PluginContext, PoolManifest, RegistryListResponse,
    RegistryManifestRequest, RegistryProtocolManifestRequest, RegistryServiceManifest, Result,
    Router, ServiceDescriptor, TXN_ID_HEADER, TargetAddress,
};

const CONTENT_TYPE_HEADER: &str = "content_type";
const PROTOCOL_HEADER: &str = "protocol";
const REPLY_TO_HEADER: &str = "reply_to";
const CONTENT_TYPE_JSON: &str = "application/json";
const STANDARD_PROTOCOL: &str = "standard";
const MANIFEST_ACTION: &str = "manifest";
const SERVICES_ACTION: &str = "services";
const DISCOVERY_SERVICE: &str = "__tlbus__";
const REGISTRY_SERVICE: &str = "registry";
const REGISTRY_LIST_ACTION: &str = "list";
const REGISTRY_GET_MANIFEST_ACTION: &str = "get_manifest";
const REGISTRY_GET_PROTOCOL_MANIFEST_ACTION: &str = "get_protocol_manifest";

#[derive(Debug, Clone, Default)]
pub struct ProtocolPlugin {
    router: Router,
    local_pool: Option<String>,
}

impl ProtocolPlugin {
    pub fn new(router: Router) -> Self {
        Self {
            router,
            local_pool: None,
        }
    }

    pub fn with_local_pool(router: Router, local_pool: Option<String>) -> Self {
        Self {
            router,
            local_pool: local_pool.filter(|pool| !pool.trim().is_empty()),
        }
    }
}

impl Plugin for ProtocolPlugin {
    fn name(&self) -> &'static str {
        "protocol"
    }

    fn on_receive(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        let target = TargetAddress::parse(&msg.to)?;

        if let Some(action) = self.registry_action(&target) {
            return match action {
                REGISTRY_LIST_ACTION => self.handle_registry_list(ctx, msg, &target),
                REGISTRY_GET_MANIFEST_ACTION => {
                    self.handle_registry_get_manifest(ctx, msg, &target)
                }
                REGISTRY_GET_PROTOCOL_MANIFEST_ACTION => {
                    self.handle_registry_get_protocol_manifest(ctx, msg, &target)
                }
                _ => Ok(()),
            };
        }

        match target.action() {
            Some(MANIFEST_ACTION) => self.handle_manifest(ctx, msg, &target),
            Some(SERVICES_ACTION) => self.handle_services(ctx, msg, &target),
            _ => Ok(()),
        }
    }
}

impl ProtocolPlugin {
    fn handle_manifest(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        target: &TargetAddress,
    ) -> Result<()> {
        let Some(descriptor) = self
            .router
            .descriptor(&target.route_key())
            .or_else(|| self.router.descriptor(target.service()))
        else {
            return Ok(());
        };

        let payload = serde_json::to_vec(&descriptor)
            .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        self.set_terminal_json_response(ctx, msg, descriptor.service.clone(), payload, "manifest")
    }

    fn handle_services(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        target: &TargetAddress,
    ) -> Result<()> {
        if target.service() != DISCOVERY_SERVICE {
            return Ok(());
        }

        let Some(requested_pool) = target.pool() else {
            return Ok(());
        };

        if self
            .local_pool
            .as_deref()
            .is_some_and(|local_pool| local_pool != requested_pool)
        {
            return Ok(());
        }

        let services = self.router.manifest_for_pool(requested_pool);
        if self.local_pool.is_none() && services.is_empty() {
            return Ok(());
        }

        let payload = serde_json::to_vec(&PoolManifest {
            pool: requested_pool.to_string(),
            services,
        })
        .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        self.set_terminal_json_response(
            ctx,
            msg,
            format!("{requested_pool}.{DISCOVERY_SERVICE}"),
            payload,
            "discovery",
        )
    }

    fn handle_registry_list(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        target: &TargetAddress,
    ) -> Result<()> {
        if self.is_remote_registry_request(target) {
            return Ok(());
        }

        let mut services = self
            .registry_descriptors(target)
            .into_iter()
            .filter(|descriptor| !descriptor.is_client && descriptor.active)
            .map(|descriptor| descriptor.registry_service())
            .collect::<Vec<_>>();
        services.sort_by(|left, right| left.name.cmp(&right.name));

        let payload = serde_json::to_vec(&RegistryListResponse { services })
            .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        self.set_terminal_json_response(
            ctx,
            msg,
            self.registry_sender(target),
            payload,
            "registry list",
        )
    }

    fn handle_registry_get_manifest(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        target: &TargetAddress,
    ) -> Result<()> {
        if self.is_remote_registry_request(target) {
            return Ok(());
        }

        let request = match serde_json::from_slice::<RegistryManifestRequest>(&msg.payload) {
            Ok(request) if !request.service.trim().is_empty() => request,
            Ok(_) => {
                return self.handle_registry_error(
                    ctx,
                    msg,
                    target,
                    "invalid_request",
                    "`service` must not be empty",
                );
            }
            Err(error) => {
                return self.handle_registry_error(
                    ctx,
                    msg,
                    target,
                    "invalid_request",
                    &format!("invalid `registry.get_manifest` payload: {error}"),
                );
            }
        };

        let Some(descriptor) = self.resolve_registry_descriptor(target, &request.service) else {
            return self.handle_registry_error(
                ctx,
                msg,
                target,
                "service_not_found",
                &format!("service `{}` not found", request.service),
            );
        };
        if !descriptor.active {
            return self.handle_registry_error(
                ctx,
                msg,
                target,
                "service_unavailable",
                &format!("service `{}` is inactive", request.service),
            );
        }

        let registry_manifest = self.registry_manifest(&descriptor);
        let payload = serde_json::to_vec(&registry_manifest)
            .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        self.set_terminal_json_response(
            ctx,
            msg,
            self.registry_sender(target),
            payload,
            "registry get_manifest",
        )
    }

    fn handle_registry_get_protocol_manifest(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        target: &TargetAddress,
    ) -> Result<()> {
        if self.is_remote_registry_request(target) {
            return Ok(());
        }

        let request = match serde_json::from_slice::<RegistryProtocolManifestRequest>(&msg.payload)
        {
            Ok(request)
                if !request.service.trim().is_empty() && !request.protocol.trim().is_empty() =>
            {
                request
            }
            Ok(_) => {
                return self.handle_registry_error(
                    ctx,
                    msg,
                    target,
                    "invalid_request",
                    "`service` and `protocol` must not be empty",
                );
            }
            Err(error) => {
                return self.handle_registry_error(
                    ctx,
                    msg,
                    target,
                    "invalid_request",
                    &format!("invalid `registry.get_protocol_manifest` payload: {error}"),
                );
            }
        };

        let Some(descriptor) = self.resolve_registry_descriptor(target, &request.service) else {
            return self.handle_registry_error(
                ctx,
                msg,
                target,
                "service_not_found",
                &format!("service `{}` not found", request.service),
            );
        };
        if !descriptor.active {
            return self.handle_registry_error(
                ctx,
                msg,
                target,
                "service_unavailable",
                &format!("service `{}` is inactive", request.service),
            );
        }

        let Some(protocol_manifest) = self.protocol_manifest_for(&descriptor, &request.protocol)
        else {
            return self.handle_registry_error(
                ctx,
                msg,
                target,
                "protocol_not_supported",
                &format!(
                    "service `{}` does not expose protocol `{}`",
                    descriptor.registry_service().name,
                    request.protocol
                ),
            );
        };

        let payload = serde_json::to_vec(&json!({
            "service": descriptor.registry_service().name,
            "protocol": request.protocol,
            "manifest": protocol_manifest,
        }))
        .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        self.set_terminal_json_response(
            ctx,
            msg,
            self.registry_sender(target),
            payload,
            "registry get_protocol_manifest",
        )
    }

    fn handle_registry_error(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        target: &TargetAddress,
        code: &str,
        message: &str,
    ) -> Result<()> {
        let payload = serde_json::to_vec(&json!({
            "error": {
                "code": code,
                "message": message,
            }
        }))
        .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        self.set_terminal_json_response(ctx, msg, self.registry_sender(target), payload, "registry")
    }

    fn set_terminal_json_response(
        &self,
        ctx: &mut PluginContext,
        msg: &Envelope,
        from: String,
        payload: Vec<u8>,
        request_kind: &str,
    ) -> Result<()> {
        let reply_to = self.reply_to(msg).ok_or_else(|| {
            BusError::InvalidEnvelope(format!(
                "missing required header `{REPLY_TO_HEADER}` for {request_kind} requests"
            ))
        })?;

        let mut response = Envelope::new(from, reply_to, payload);
        response.ttl_ms = msg.ttl_ms;
        response.headers.insert(
            CONTENT_TYPE_HEADER.to_string(),
            CONTENT_TYPE_JSON.to_string(),
        );
        response
            .headers
            .insert(PROTOCOL_HEADER.to_string(), STANDARD_PROTOCOL.to_string());
        if let Some(txn_id) = msg.txn_id() {
            response
                .headers
                .insert(TXN_ID_HEADER.to_string(), txn_id.to_string());
        }

        ctx.set_terminal_response(response);
        Ok(())
    }

    fn registry_action<'a>(&self, target: &'a TargetAddress) -> Option<&'a str> {
        if let Some(action) = target.raw().strip_prefix("registry.") {
            if !action.trim().is_empty() {
                return Some(action);
            }
        }

        if target.service() == REGISTRY_SERVICE {
            return target.action();
        }

        None
    }

    fn registry_sender(&self, target: &TargetAddress) -> String {
        match target.pool() {
            Some(pool) => format!("{pool}.{REGISTRY_SERVICE}"),
            None => REGISTRY_SERVICE.to_string(),
        }
    }

    fn is_remote_registry_request(&self, target: &TargetAddress) -> bool {
        matches!(
            (target.pool(), self.local_pool.as_deref()),
            (Some(requested_pool), Some(local_pool)) if requested_pool != local_pool
        )
    }

    fn registry_descriptors(&self, target: &TargetAddress) -> Vec<ServiceDescriptor> {
        let Some(pool) = target.pool() else {
            return match self.local_pool.as_deref() {
                Some(local_pool) => self.router.manifest_for_pool(local_pool),
                None => self.router.manifest(),
            };
        };

        if self
            .local_pool
            .as_deref()
            .is_some_and(|local_pool| local_pool != pool)
        {
            return Vec::new();
        }

        self.router.manifest_for_pool(pool)
    }

    fn resolve_registry_descriptor(
        &self,
        target: &TargetAddress,
        requested_service: &str,
    ) -> Option<ServiceDescriptor> {
        let requested_service = requested_service.trim();
        if requested_service.is_empty() {
            return None;
        }

        let descriptors = self.registry_descriptors(target);

        if let Some(descriptor) = descriptors
            .iter()
            .find(|descriptor| descriptor.service == requested_service)
        {
            return Some(descriptor.clone());
        }

        if let Some(pool) = target.pool().or(self.local_pool.as_deref()) {
            let scoped_name = format!("{pool}.{requested_service}");
            if let Some(descriptor) = descriptors
                .iter()
                .find(|descriptor| descriptor.service == scoped_name)
            {
                return Some(descriptor.clone());
            }
        }

        descriptors
            .into_iter()
            .find(|descriptor| descriptor.registry_service().name == requested_service)
    }

    fn registry_manifest(&self, descriptor: &ServiceDescriptor) -> RegistryServiceManifest {
        let registry_service = descriptor.registry_service();
        let mut protocol_manifests = descriptor
            .registry_protocols()
            .into_iter()
            .filter_map(|protocol| {
                self.protocol_manifest_for(descriptor, &protocol)
                    .map(|manifest| (protocol, manifest))
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        if protocol_manifests.is_empty() {
            protocol_manifests = std::collections::BTreeMap::new();
        }

        RegistryServiceManifest {
            name: registry_service.name,
            capabilities: descriptor.registry_capabilities(),
            protocols: descriptor.registry_protocols(),
            protocol_manifests,
        }
    }

    fn protocol_manifest_for(
        &self,
        descriptor: &ServiceDescriptor,
        protocol: &str,
    ) -> Option<Value> {
        let protocol = protocol.trim();
        if protocol.is_empty() {
            return None;
        }

        let modes = descriptor
            .modes
            .iter()
            .filter(|mode| mode.protocol == protocol)
            .map(|mode| {
                json!({
                    "transport": mode.transport,
                    "protocol_version": mode.protocol_version,
                    "content_type": mode.content_type,
                })
            })
            .collect::<Vec<_>>();

        if modes.is_empty() {
            return None;
        }

        let mut manifest = serde_json::Map::new();
        manifest.insert("protocol".to_string(), Value::String(protocol.to_string()));
        manifest.insert("modes".to_string(), Value::Array(modes));

        if protocol == "mcp" {
            if let Some(schema) =
                self.first_feature(descriptor, &["protocol_manifest.mcp.schema", "mcp.schema"])
            {
                manifest.insert("schema".to_string(), Value::String(schema));
            }
        }

        if protocol == "rest-api" {
            if let Some(openapi) = self.first_feature(
                descriptor,
                &[
                    "protocol_manifest.rest-api.openapi",
                    "protocol_manifest.rest_api.openapi",
                    "rest-api.openapi",
                    "rest_api.openapi",
                ],
            ) {
                manifest.insert("openapi".to_string(), Value::String(openapi));
            }
        }

        Some(Value::Object(manifest))
    }

    fn first_feature(&self, descriptor: &ServiceDescriptor, keys: &[&str]) -> Option<String> {
        keys.iter()
            .find_map(|key| descriptor.features.get(*key).cloned())
    }

    fn reply_to(&self, msg: &Envelope) -> Option<String> {
        msg.headers.get(REPLY_TO_HEADER).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::{Value, json};
    use tlbus_core::{
        Plugin, PoolManifest, RegistryListResponse, RegistryServiceManifest, ServiceCapability,
        ServiceManifest, ServiceMode,
    };

    use crate::ProtocolPlugin;

    struct InvoiceServiceFixture {
        socket_path: PathBuf,
        #[cfg(unix)]
        _listener: StdUnixListener,
    }

    impl Drop for InvoiceServiceFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }

    fn unique_socket_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        PathBuf::from(format!("/tmp/{prefix}-{}-{nanos}.sock", std::process::id()))
    }

    fn register_invoice_service(router: &tlbus_core::Router) -> InvoiceServiceFixture {
        let socket_path = unique_socket_path("tlb-invoice");
        let _ = std::fs::remove_file(&socket_path);
        #[cfg(unix)]
        let listener = StdUnixListener::bind(&socket_path).unwrap();

        router.register_manifest(
            &ServiceManifest {
                name: "ps2.invoice".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::from([
                    (
                        "protocol_manifest.mcp.schema".to_string(),
                        "https://schemas.example.dev/invoice/mcp.json".to_string(),
                    ),
                    (
                        "protocol_manifest.rest-api.openapi".to_string(),
                        "https://api.example.dev/invoice/openapi.json".to_string(),
                    ),
                ]),
                capabilities: vec![
                    ServiceCapability {
                        name: "create".to_string(),
                        address: "ps2.invoice.create".to_string(),
                        description: "Creates invoices".to_string(),
                    },
                    ServiceCapability {
                        name: "status".to_string(),
                        address: "ps2.invoice.status".to_string(),
                        description: "Checks invoice status".to_string(),
                    },
                ],
                modes: vec![
                    ServiceMode {
                        transport: "http2".to_string(),
                        protocol: "mcp".to_string(),
                        protocol_version: Some("2025-06-18".to_string()),
                        content_type: Some("application/json".to_string()),
                    },
                    ServiceMode {
                        transport: "http1".to_string(),
                        protocol: "rest-api".to_string(),
                        protocol_version: None,
                        content_type: Some("application/json".to_string()),
                    },
                ],
            },
            socket_path.clone(),
        );

        InvoiceServiceFixture {
            socket_path,
            #[cfg(unix)]
            _listener: listener,
        }
    }

    #[test]
    fn intercepts_manifest_requests_and_returns_descriptor() {
        let router = tlbus_core::Router::new();
        router.register_manifest(
            &ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: vec![ServiceCapability {
                    name: "calculate".to_string(),
                    address: "ps2.calcola.compute".to_string(),
                    description: "Evaluates arithmetic expressions".to_string(),
                }],
                modes: vec![ServiceMode {
                    transport: "http2".to_string(),
                    protocol: "mcp".to_string(),
                    protocol_version: Some("2025-06-18".to_string()),
                    content_type: Some("application/json".to_string()),
                }],
            },
            PathBuf::from("/tmp/calcola.sock"),
        );

        let plugin = ProtocolPlugin::new(router);
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message =
            tlbus_core::Envelope::new("ps1.client", "ps2.calcola.manifest", b"{}".to_vec());
        message
            .headers
            .insert("reply_to".to_string(), "ps1.client.inbox".to_string());
        message
            .headers
            .insert("txn_id".to_string(), "txn-123".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let descriptor: tlbus_core::ServiceDescriptor =
            serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(response.from, "ps2.calcola");
        assert_eq!(response.to, "ps1.client.inbox");
        assert_eq!(response.txn_id(), Some("txn-123"));
        assert!(descriptor.service_capability("calculate").is_some());
        assert!(descriptor.service_capability("manifest").is_some());
    }

    #[test]
    fn ignores_non_manifest_requests() {
        let plugin = ProtocolPlugin::new(tlbus_core::Router::new());
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message =
            tlbus_core::Envelope::new("ps1.client", "ps2.calcola.compute", b"{}".to_vec());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        assert!(!ctx.has_terminal_response());
    }

    #[test]
    fn returns_pool_services_for_reserved_discovery_target() {
        let router = tlbus_core::Router::new();
        router.register_manifest(
            &ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: vec![ServiceCapability {
                    name: "calculate".to_string(),
                    address: "ps2.calcola.compute".to_string(),
                    description: "Evaluates arithmetic expressions".to_string(),
                }],
                modes: vec![ServiceMode {
                    transport: "http2".to_string(),
                    protocol: "mcp".to_string(),
                    protocol_version: Some("2025-06-18".to_string()),
                    content_type: Some("application/json".to_string()),
                }],
            },
            PathBuf::from("/tmp/calcola.sock"),
        );

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message =
            tlbus_core::Envelope::new("ps2.client", "ps2.__tlbus__.services", b"{}".to_vec());
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());
        message
            .headers
            .insert("txn_id".to_string(), "txn-456".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let manifest: PoolManifest = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(response.from, "ps2.__tlbus__");
        assert_eq!(response.to, "ps2.client.inbox");
        assert_eq!(response.txn_id(), Some("txn-456"));
        assert_eq!(manifest.pool, "ps2");
        assert_eq!(manifest.services.len(), 1);
        assert_eq!(manifest.services[0].service, "ps2.calcola");
    }

    #[test]
    fn ignores_discovery_requests_for_remote_pools() {
        let router = tlbus_core::Router::new();
        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps1".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message =
            tlbus_core::Envelope::new("ps1.client", "ps2.__tlbus__.services", b"{}".to_vec());
        message
            .headers
            .insert("reply_to".to_string(), "ps1.client.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();
        assert!(!ctx.has_terminal_response());
    }

    #[test]
    fn ignores_registry_get_manifest_requests_for_remote_pools() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps1".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new(
            "ps1.agent",
            "ps2.registry.get_manifest",
            serde_json::to_vec(&json!({ "service": "ps2.invoice" })).unwrap(),
        );
        message
            .headers
            .insert("reply_to".to_string(), "ps1.agent.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();
        assert!(!ctx.has_terminal_response());
    }

    #[test]
    fn registry_list_returns_service_summaries() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new("ps2.client", "registry.list", b"{}".to_vec());
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());
        message
            .headers
            .insert("txn_id".to_string(), "txn-reg-1".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let body: RegistryListResponse = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(response.from, "registry");
        assert_eq!(body.services.len(), 1);
        assert_eq!(body.services[0].name, "invoice");
        assert_eq!(
            body.services[0].capabilities,
            vec!["invoice.create", "invoice.status"]
        );
        assert_eq!(body.services[0].protocols, vec!["mcp", "rest-api"]);
    }

    #[test]
    fn registry_list_omits_inactive_services() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);
        router.set_active("ps2.invoice", false);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new("ps2.client", "registry.list", b"{}".to_vec());
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let body: RegistryListResponse = serde_json::from_slice(&response.payload).unwrap();
        assert!(body.services.is_empty());
    }

    #[test]
    fn registry_get_manifest_returns_full_manifest() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new(
            "ps2.client",
            "registry.get_manifest",
            serde_json::to_vec(&json!({ "service": "invoice" })).unwrap(),
        );
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let body: RegistryServiceManifest = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(body.name, "invoice");
        assert_eq!(body.capabilities, vec!["invoice.create", "invoice.status"]);
        assert_eq!(body.protocols, vec!["mcp", "rest-api"]);
        assert!(body.protocol_manifests.contains_key("mcp"));
        assert!(body.protocol_manifests.contains_key("rest-api"));
    }

    #[test]
    fn registry_get_manifest_returns_service_unavailable_for_inactive_service() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);
        router.set_active("ps2.invoice", false);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new(
            "ps2.client",
            "registry.get_manifest",
            serde_json::to_vec(&json!({ "service": "invoice" })).unwrap(),
        );
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let body: Value = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(body["error"]["code"], "service_unavailable");
    }

    #[test]
    fn registry_get_protocol_manifest_returns_specific_protocol_metadata() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new(
            "ps2.client",
            "registry.get_protocol_manifest",
            serde_json::to_vec(&json!({ "service": "invoice", "protocol": "mcp" })).unwrap(),
        );
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let body: Value = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(body["service"], "invoice");
        assert_eq!(body["protocol"], "mcp");
        assert_eq!(
            body["manifest"]["schema"],
            "https://schemas.example.dev/invoice/mcp.json"
        );
        assert!(body["manifest"]["modes"].is_array());
    }

    #[test]
    fn registry_get_protocol_manifest_returns_structured_error_when_protocol_is_missing() {
        let router = tlbus_core::Router::new();
        let _invoice_fixture = register_invoice_service(&router);

        let plugin = ProtocolPlugin::with_local_pool(router, Some("ps2".to_string()));
        let mut ctx = tlbus_core::PluginContext::default();
        let mut message = tlbus_core::Envelope::new(
            "ps2.client",
            "registry.get_protocol_manifest",
            serde_json::to_vec(&json!({ "service": "invoice", "protocol": "graphql" })).unwrap(),
        );
        message
            .headers
            .insert("reply_to".to_string(), "ps2.client.inbox".to_string());

        plugin.on_receive(&mut ctx, &mut message).unwrap();

        let response = ctx.take_terminal_response().unwrap();
        let body: Value = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(body["error"]["code"], "protocol_not_supported");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("does not expose protocol")
        );
    }
}
