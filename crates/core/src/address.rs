use crate::{BusError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetAddress {
    raw: String,
    pool: Option<String>,
    service: String,
    action: Option<String>,
}

impl TargetAddress {
    pub fn parse(target: impl Into<String>) -> Result<Self> {
        let raw = target.into();
        if raw.trim().is_empty() {
            return Err(BusError::InvalidEnvelope(
                "`to` must not be empty".to_string(),
            ));
        }

        let parts = raw.split('.').map(str::to_string).collect::<Vec<_>>();
        if parts.len() >= 3 {
            let pool = parts[0].trim();
            let service = parts[1].trim();
            let action = parts[2..].join(".");
            if pool.is_empty() || service.is_empty() || action.trim().is_empty() {
                return Err(BusError::InvalidEnvelope(format!(
                    "invalid federated target `{raw}`"
                )));
            }

            return Ok(Self {
                raw,
                pool: Some(pool.to_string()),
                service: service.to_string(),
                action: Some(action),
            });
        }

        Ok(Self {
            service: raw.clone(),
            raw,
            pool: None,
            action: None,
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub fn pool(&self) -> Option<&str> {
        self.pool.as_deref()
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn route_key(&self) -> String {
        match self.pool() {
            Some(pool) => format!("{pool}.{}", self.service),
            None => self.raw.clone(),
        }
    }

    pub fn action(&self) -> Option<&str> {
        self.action.as_deref()
    }

    pub fn is_federated(&self) -> bool {
        self.pool.is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::TargetAddress;

    #[test]
    fn parses_simple_local_target() {
        let target = TargetAddress::parse("echo").unwrap();
        assert_eq!(target.pool(), None);
        assert_eq!(target.service(), "echo");
        assert_eq!(target.action(), None);
        assert!(!target.is_federated());
    }

    #[test]
    fn parses_federated_target() {
        let target = TargetAddress::parse("ps2.invoice.create").unwrap();
        assert_eq!(target.pool(), Some("ps2"));
        assert_eq!(target.service(), "invoice");
        assert_eq!(target.action(), Some("create"));
        assert_eq!(target.route_key(), "ps2.invoice");
        assert!(target.is_federated());
    }

    #[test]
    fn rejects_malformed_federated_target() {
        assert!(TargetAddress::parse("ps2..create").is_err());
    }
}
