use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BusError, Result};

pub const TXN_ID_HEADER: &str = "txn_id";
pub const REPLY_TO_HEADER: &str = "reply_to";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    // Stable bus-level identifier for correlating the same message across plugins.
    pub bus_id: Uuid,
    // Logical sender service name.
    pub from: String,
    // Logical destination service name resolved by the local router.
    pub to: String,
    // Unix timestamp in seconds, kept simple for v0.1 interoperability.
    pub ts: u64,
    // Local expiry budget; expired messages are dropped by the daemon.
    pub ttl_ms: u64,
    // Extensible metadata area owned by plugins.
    pub headers: BTreeMap<String, String>,
    // Payload stays opaque to the core and is forwarded as raw bytes.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl Envelope {
    pub fn new(
        from: impl Into<String>,
        to: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            bus_id: Uuid::new_v4(),
            from: from.into(),
            to: to.into(),
            ts: unix_timestamp_secs(SystemTime::now()),
            ttl_ms: 1_000,
            headers: BTreeMap::new(),
            payload: payload.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        // v0.1 validates only routing-critical fields and leaves richer policies to plugins.
        if self.from.trim().is_empty() {
            return Err(BusError::InvalidEnvelope(
                "`from` must not be empty".to_string(),
            ));
        }

        if self.to.trim().is_empty() {
            return Err(BusError::InvalidEnvelope(
                "`to` must not be empty".to_string(),
            ));
        }

        if self.ttl_ms == 0 {
            return Err(BusError::InvalidEnvelope(
                "`ttl_ms` must be greater than zero".to_string(),
            ));
        }

        Ok(())
    }

    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        // Convert everything to milliseconds so the TTL check remains exact.
        let expires_at_ms = (self.ts as u128 * 1_000) + self.ttl_ms as u128;
        unix_timestamp_millis(now) > expires_at_ms
    }

    pub fn txn_id(&self) -> Option<&str> {
        self.headers.get(TXN_ID_HEADER).map(String::as_str)
    }

    pub fn reply_to(&self) -> Option<&str> {
        self.headers.get(REPLY_TO_HEADER).map(String::as_str)
    }

    pub fn trace_fields(&self) -> String {
        format!(
            "bus_id={} txn_id={} from={} to={} reply_to={}",
            self.bus_id,
            self.txn_id().unwrap_or("missing"),
            self.from,
            self.to,
            self.reply_to().unwrap_or("missing"),
        )
    }
}

fn unix_timestamp_secs(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn unix_timestamp_millis(now: SystemTime) -> u128 {
    now.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{Envelope, TXN_ID_HEADER};

    #[test]
    fn expires_after_ttl_window() {
        let mut envelope = Envelope::new("svc-a", "svc-b", b"hello".to_vec());
        envelope.ts = 10;
        envelope.ttl_ms = 250;

        assert!(!envelope.is_expired_at(UNIX_EPOCH + Duration::from_millis(10_250)));
        assert!(envelope.is_expired_at(UNIX_EPOCH + Duration::from_millis(10_251)));
    }

    #[test]
    fn validation_rejects_blank_route_fields() {
        let mut envelope = Envelope::new("svc-a", "svc-b", []);
        envelope.from.clear();

        assert!(envelope.validate().is_err());
    }

    #[test]
    fn trace_fields_include_routing_and_reply_metadata() {
        let mut envelope = Envelope::new("svc-a", "svc-b", b"ping".to_vec());
        envelope
            .headers
            .insert(TXN_ID_HEADER.to_string(), "txn-123".to_string());
        envelope
            .headers
            .insert("reply_to".to_string(), "svc-a.inbox".to_string());

        let trace = envelope.trace_fields();
        assert!(trace.contains("txn_id=txn-123"));
        assert!(trace.contains("from=svc-a"));
        assert!(trace.contains("to=svc-b"));
        assert!(trace.contains("reply_to=svc-a.inbox"));
    }
}
