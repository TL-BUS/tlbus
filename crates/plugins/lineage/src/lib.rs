use tlbus_core::{Envelope, Plugin, PluginContext, Result, TXN_ID_HEADER};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy)]
pub struct LineagePlugin;

impl Plugin for LineagePlugin {
    fn name(&self) -> &'static str {
        "lineage"
    }

    fn on_receive(&self, _ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        // The first hop creates a transaction identifier; downstream hops preserve it.
        msg.headers
            .entry(TXN_ID_HEADER.to_string())
            .or_insert_with(|| Uuid::new_v4().to_string());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tlbus_core::{Envelope, Plugin, PluginContext, TXN_ID_HEADER};

    use crate::LineagePlugin;

    #[test]
    fn injects_txn_id_only_when_missing() {
        let plugin = LineagePlugin;
        let mut ctx = PluginContext::default();
        let mut envelope = Envelope::new("svc-a", "svc-b", []);
        envelope
            .headers
            .insert(TXN_ID_HEADER.to_string(), "existing".to_string());

        plugin.on_receive(&mut ctx, &mut envelope).unwrap();
        assert_eq!(envelope.txn_id(), Some("existing"));
    }

    #[test]
    fn creates_txn_id_if_absent() {
        let plugin = LineagePlugin;
        let mut ctx = PluginContext::default();
        let mut envelope = Envelope::new("svc-a", "svc-b", []);

        plugin.on_receive(&mut ctx, &mut envelope).unwrap();
        assert!(envelope.txn_id().is_some());
    }
}
