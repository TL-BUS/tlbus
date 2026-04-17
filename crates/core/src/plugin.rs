use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::{Envelope, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginStage {
    Receive,
    Route,
    Deliver,
}

impl fmt::Display for PluginStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Receive => write!(f, "receive"),
            Self::Route => write!(f, "route"),
            Self::Deliver => write!(f, "deliver"),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PluginContext {
    // Scratchpad for cross-stage coordination inside a single delivery flow.
    attributes: BTreeMap<String, String>,
    // Resolved local route chosen by the daemon before `on_route`.
    route: Option<PathBuf>,
    // Optional plugin-owned terminal response that short-circuits normal delivery.
    terminal_response: Option<Envelope>,
}

impl PluginContext {
    pub fn insert_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), value.into());
    }

    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(String::as_str)
    }

    pub fn set_route(&mut self, route: impl Into<PathBuf>) {
        self.route = Some(route.into());
    }

    pub fn route(&self) -> Option<&Path> {
        self.route.as_deref()
    }

    pub fn set_terminal_response(&mut self, response: Envelope) {
        self.terminal_response = Some(response);
    }

    pub fn take_terminal_response(&mut self) -> Option<Envelope> {
        self.terminal_response.take()
    }

    pub fn has_terminal_response(&self) -> bool {
        self.terminal_response.is_some()
    }
}

pub trait Plugin: Send + Sync {
    // Stable human-readable identifier used in diagnostics.
    fn name(&self) -> &'static str;

    fn on_receive(&self, _ctx: &mut PluginContext, _msg: &mut Envelope) -> Result<()> {
        Ok(())
    }

    fn on_route(&self, _ctx: &mut PluginContext, _msg: &mut Envelope) -> Result<()> {
        Ok(())
    }

    fn on_deliver(&self, _ctx: &mut PluginContext, _msg: &mut Envelope) -> Result<()> {
        Ok(())
    }
}
