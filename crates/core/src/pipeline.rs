use std::sync::Arc;

use crate::{Envelope, Plugin, PluginContext, Result};

#[derive(Default, Clone)]
pub struct Pipeline {
    plugins: Vec<Arc<dyn Plugin>>,
}

impl Pipeline {
    pub fn new(plugins: Vec<Arc<dyn Plugin>>) -> Self {
        Self { plugins }
    }

    pub fn add<P>(&mut self, plugin: P)
    where
        P: Plugin + 'static,
    {
        self.plugins.push(Arc::new(plugin));
    }

    pub fn plugins(&self) -> &[Arc<dyn Plugin>] {
        &self.plugins
    }

    pub fn run_receive(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        // All plugins observe the same mutable envelope so optional concerns can be
        // layered without hard-coding them into the core.
        for plugin in &self.plugins {
            plugin.on_receive(ctx, msg)?;
            if ctx.has_terminal_response() {
                break;
            }
        }

        Ok(())
    }

    pub fn run_route(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        for plugin in &self.plugins {
            plugin.on_route(ctx, msg)?;
            if ctx.has_terminal_response() {
                break;
            }
        }

        Ok(())
    }

    pub fn run_deliver(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
        for plugin in &self.plugins {
            plugin.on_deliver(ctx, msg)?;
            if ctx.has_terminal_response() {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Envelope, Pipeline, Plugin, PluginContext, Result};

    struct RecordingPlugin;

    struct InterceptingPlugin;

    impl Plugin for RecordingPlugin {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn on_receive(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
            ctx.insert_attribute("receive", "seen");
            msg.headers
                .insert("stage.receive".to_string(), "ok".to_string());
            Ok(())
        }

        fn on_route(&self, ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
            let receive = ctx.attribute("receive").unwrap_or("missing");
            msg.headers
                .insert("stage.route".to_string(), receive.to_string());
            Ok(())
        }

        fn on_deliver(&self, _ctx: &mut PluginContext, msg: &mut Envelope) -> Result<()> {
            msg.headers
                .insert("stage.deliver".to_string(), "ok".to_string());
            Ok(())
        }
    }

    impl Plugin for InterceptingPlugin {
        fn name(&self) -> &'static str {
            "intercepting"
        }

        fn on_receive(&self, ctx: &mut PluginContext, _msg: &mut Envelope) -> Result<()> {
            ctx.set_terminal_response(Envelope::new("svc-b", "svc-a", b"handled".to_vec()));
            Ok(())
        }
    }

    #[test]
    fn pipeline_runs_stages_in_order() {
        let mut pipeline = Pipeline::default();
        pipeline.add(RecordingPlugin);

        let mut ctx = PluginContext::default();
        let mut msg = Envelope::new("svc-a", "svc-b", []);

        pipeline.run_receive(&mut ctx, &mut msg).unwrap();
        pipeline.run_route(&mut ctx, &mut msg).unwrap();
        pipeline.run_deliver(&mut ctx, &mut msg).unwrap();

        assert_eq!(msg.headers.get("stage.receive"), Some(&"ok".to_string()));
        assert_eq!(msg.headers.get("stage.route"), Some(&"seen".to_string()));
        assert_eq!(msg.headers.get("stage.deliver"), Some(&"ok".to_string()));
    }

    #[test]
    fn pipeline_stops_after_terminal_response() {
        let mut pipeline = Pipeline::default();
        pipeline.add(InterceptingPlugin);
        pipeline.add(RecordingPlugin);

        let mut ctx = PluginContext::default();
        let mut msg = Envelope::new("svc-a", "svc-b", []);

        pipeline.run_receive(&mut ctx, &mut msg).unwrap();

        assert!(ctx.has_terminal_response());
        assert!(msg.headers.get("stage.receive").is_none());
    }
}
