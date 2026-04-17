use thiserror::Error;

use crate::plugin::PluginStage;

pub type Result<T> = std::result::Result<T, BusError>;

#[derive(Debug, Error)]
pub enum BusError {
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(String),
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("message expired")]
    Expired,
    #[error("route not found for service `{service}`")]
    RouteNotFound { service: String },
    #[error("service `{service}` is unavailable")]
    ServiceUnavailable { service: String },
    #[error("plugin `{plugin}` rejected the message during {stage}: {reason}")]
    Rejected {
        plugin: &'static str,
        stage: PluginStage,
        reason: String,
    },
    #[error("codec error: {0}")]
    Codec(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl BusError {
    pub fn reject(plugin: &'static str, stage: PluginStage, reason: impl Into<String>) -> Self {
        Self::Rejected {
            plugin,
            stage,
            reason: reason.into(),
        }
    }
}

impl From<rmp_serde::encode::Error> for BusError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self::Codec(value.to_string())
    }
}

impl From<rmp_serde::decode::Error> for BusError {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self::Codec(value.to_string())
    }
}
