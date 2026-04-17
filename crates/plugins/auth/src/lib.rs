use tlbus_core::{Envelope, Plugin, PluginContext, Result};

#[derive(Debug, Default, Clone, Copy)]
pub struct AuthPlugin;

impl Plugin for AuthPlugin {
    fn name(&self) -> &'static str {
        "auth"
    }

    fn on_receive(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        // v0.1 keeps auth intentionally lightweight: capture the claimed caller identity
        // so later policy work can build on a stable hook.
        ctx.insert_attribute("auth.subject", msg.from.clone());
        msg.headers
            .entry("auth.service".to_string())
            .or_insert_with(|| msg.from.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tlbus_core::{Envelope, Plugin, PluginContext};

    use crate::AuthPlugin;

    #[test]
    fn stubs_service_identity_from_sender() {
        let plugin = AuthPlugin;
        let mut ctx = PluginContext::default();
        let mut envelope = Envelope::new("svc-a", "svc-b", []);

        plugin.on_receive(&mut ctx, &mut envelope).unwrap();

        assert_eq!(ctx.attribute("auth.subject"), Some("svc-a"));
        assert_eq!(
            envelope.headers.get("auth.service"),
            Some(&"svc-a".to_string())
        );
    }
}
