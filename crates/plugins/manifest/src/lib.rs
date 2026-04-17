use tlbus_core::{
    BusError, Envelope, Plugin, PluginContext, Result, Router, TXN_ID_HEADER, TargetAddress,
};

const CONTENT_TYPE_HEADER: &str = "content_type";
const PROTOCOL_HEADER: &str = "protocol";
const REPLY_TO_HEADER: &str = "reply_to";
const CONTENT_TYPE_JSON: &str = "application/json";
const STANDARD_PROTOCOL: &str = "standard";

#[derive(Debug, Clone, Default)]
pub struct ProtocolPlugin {
    router: Router,
}

impl ProtocolPlugin {
    pub fn new(router: Router) -> Self {
        Self { router }
    }
}

impl Plugin for ProtocolPlugin {
    fn name(&self) -> &'static str {
        "protocol"
    }

    fn on_receive(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        let target = TargetAddress::parse(&msg.to)?;
        if target.action() != Some("manifest") {
            return Ok(());
        }

        let Some(descriptor) = self
            .router
            .descriptor(&target.route_key())
            .or_else(|| self.router.descriptor(target.service()))
        else {
            return Ok(());
        };

        let reply_to = msg.headers.get(REPLY_TO_HEADER).cloned().ok_or_else(|| {
            BusError::InvalidEnvelope(format!(
                "missing required header `{REPLY_TO_HEADER}` for manifest requests"
            ))
        })?;
        let payload = serde_json::to_vec(&descriptor)
            .map_err(|error| BusError::Codec(format!("json encode failed: {error}")))?;

        let mut response = Envelope::new(descriptor.service.clone(), reply_to, payload);
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use tlbus_core::{Plugin, ServiceCapability, ServiceManifest, ServiceMode};

    use crate::ProtocolPlugin;

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
}
