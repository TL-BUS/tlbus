use hmac::digest::KeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tlbus_core::{BusError, Envelope, Plugin, PluginContext, Result, encode_envelope};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct HmacPlugin {
    shared_key: Vec<u8>,
}

impl HmacPlugin {
    pub fn new(shared_key: Vec<u8>) -> Self {
        Self { shared_key }
    }
}

impl Plugin for HmacPlugin {
    fn name(&self) -> &'static str {
        "hmac"
    }

    fn on_route(&self, _ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        if self.shared_key.is_empty() {
            return Err(BusError::reject(
                self.name(),
                tlbus_core::PluginStage::Route,
                "shared key must not be empty",
            ));
        }

        let mut signable = msg.clone();
        // The proof signs the canonical envelope content, not the previous proof value.
        signable.headers.remove("hmac.proof");
        let encoded = encode_envelope(&signable)?;

        // Route-time signing keeps the proof aligned with the final local destination.
        let mut signer = HmacSha256::new_from_slice(&self.shared_key).map_err(|error| {
            BusError::reject(
                self.name(),
                tlbus_core::PluginStage::Route,
                error.to_string(),
            )
        })?;
        signer.update(&encoded);

        msg.headers.insert(
            "hmac.proof".to_string(),
            hex::encode(signer.finalize().into_bytes()),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tlbus_core::{Envelope, Plugin, PluginContext};

    use crate::HmacPlugin;

    #[test]
    fn adds_route_proof_header() {
        let plugin = HmacPlugin::new(b"shared-key".to_vec());
        let mut ctx = PluginContext::default();
        let mut envelope = Envelope::new("svc-a", "svc-b", b"payload".to_vec());

        plugin.on_route(&mut ctx, &mut envelope).unwrap();
        assert!(envelope.headers.contains_key("hmac.proof"));
    }

    #[test]
    fn proof_is_deterministic_for_same_message() {
        let plugin = HmacPlugin::new(b"shared-key".to_vec());
        let mut first = Envelope::new("svc-a", "svc-b", b"payload".to_vec());
        let mut second = first.clone();
        let mut ctx = PluginContext::default();

        plugin.on_route(&mut ctx, &mut first).unwrap();
        plugin.on_route(&mut ctx, &mut second).unwrap();

        assert_eq!(
            first.headers.get("hmac.proof"),
            second.headers.get("hmac.proof")
        );
    }
}
