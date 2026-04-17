use std::collections::BTreeMap;
use std::os::unix::net::UnixStream as StdUnixStream;

use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;

use crate::{BusError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceCapability {
    pub name: String,
    pub address: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceMode {
    pub transport: String,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceManifest {
    pub name: String,
    pub secret: String,
    #[serde(default)]
    pub is_client: bool,
    #[serde(default)]
    pub features: BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: Vec<ServiceCapability>,
    #[serde(default)]
    pub modes: Vec<ServiceMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceDescriptor {
    pub service: String,
    pub active: bool,
    #[serde(default)]
    pub is_client: bool,
    #[serde(default)]
    pub features: BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: Vec<ServiceCapability>,
    #[serde(default)]
    pub modes: Vec<ServiceMode>,
}

impl ServiceManifest {
    pub fn descriptor(&self) -> ServiceDescriptor {
        ServiceDescriptor {
            service: self.name.clone(),
            active: true,
            is_client: self.is_client,
            features: self.features.clone(),
            capabilities: descriptor_capabilities(&self.name, &self.capabilities),
            modes: self.modes.clone(),
        }
    }
}

impl ServiceDescriptor {
    pub fn service_capability(&self, name: &str) -> Option<&ServiceCapability> {
        self.capabilities
            .iter()
            .find(|capability| capability.name == name)
    }

    pub fn supports_mode(&self, transport: &str, protocol: &str) -> Option<&ServiceMode> {
        self.modes
            .iter()
            .find(|mode| mode.transport == transport && mode.protocol == protocol)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolManifest {
    pub pool: String,
    #[serde(default)]
    pub services: Vec<ServiceDescriptor>,
}

impl PoolManifest {
    pub fn service(&self, service: &str) -> Option<&ServiceDescriptor> {
        self.services.iter().find(|entry| entry.service == service)
    }
}

fn descriptor_capabilities(
    service_name: &str,
    capabilities: &[ServiceCapability],
) -> Vec<ServiceCapability> {
    let manifest_address = format!("{service_name}.manifest");
    let has_manifest_capability = capabilities
        .iter()
        .any(|capability| capability.name == "manifest" || capability.address == manifest_address);

    let mut descriptor_capabilities = capabilities.to_vec();
    if !has_manifest_capability {
        descriptor_capabilities.push(ServiceCapability {
            name: "manifest".to_string(),
            address: manifest_address,
            description: "Returns the TL-Bus service manifest registered for this service"
                .to_string(),
        });
    }

    descriptor_capabilities
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolHandshakeRequest {
    pub pool: String,
    pub secret: String,
    pub advertise_addr: String,
    pub manifest: PoolManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolHandshakeResponse {
    pub pool: String,
    pub allowed: bool,
    pub advertise_addr: String,
    pub manifest: PoolManifest,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceRegistrationRequest {
    pub manifest: ServiceManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceRegistrationResponse {
    pub secret: String,
    pub pool: String,
    pub allowed: bool,
    pub active: bool,
    pub service_socket: String,
    pub bus_socket: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case")]
pub enum BusFrame {
    Envelope(crate::Envelope),
    ServiceRegistrationRequest(ServiceRegistrationRequest),
    ServiceRegistrationResponse(ServiceRegistrationResponse),
}

impl BusFrame {
    pub fn into_envelope(self) -> Result<crate::Envelope> {
        match self {
            Self::Envelope(envelope) => Ok(envelope),
            other => Err(BusError::Codec(format!(
                "expected envelope frame, received `{other:?}`"
            ))),
        }
    }

    pub fn into_registration_response(self) -> Result<ServiceRegistrationResponse> {
        match self {
            Self::ServiceRegistrationResponse(response) => Ok(response),
            other => Err(BusError::Codec(format!(
                "expected registration response frame, received `{other:?}`"
            ))),
        }
    }
}

pub async fn register_service(
    bus_socket: &std::path::Path,
    manifest: ServiceManifest,
) -> Result<ServiceRegistrationResponse> {
    let mut stream = UnixStream::connect(bus_socket).await?;
    crate::write_frame(
        &mut stream,
        &BusFrame::ServiceRegistrationRequest(ServiceRegistrationRequest { manifest }),
    )
    .await?;

    let response = crate::read_frame(&mut stream).await?;
    response.into_registration_response()
}

pub fn register_service_sync(
    bus_socket: &std::path::Path,
    manifest: ServiceManifest,
) -> Result<ServiceRegistrationResponse> {
    let mut stream = StdUnixStream::connect(bus_socket)?;
    crate::write_frame_sync(
        &mut stream,
        &BusFrame::ServiceRegistrationRequest(ServiceRegistrationRequest { manifest }),
    )?;

    let response = crate::read_frame_sync(&mut stream)?;
    response.into_registration_response()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{ServiceCapability, ServiceManifest, ServiceMode};

    #[test]
    fn descriptor_adds_builtin_manifest_capability() {
        let descriptor = ServiceManifest {
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
        }
        .descriptor();

        assert!(descriptor.service_capability("calculate").is_some());
        assert!(descriptor.service_capability("manifest").is_some());
        assert!(descriptor.supports_mode("http2", "mcp").is_some());
    }
}
