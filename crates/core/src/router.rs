use std::collections::BTreeMap;
#[cfg(unix)]
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::{BusError, Result, ServiceDescriptor, ServiceManifest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRecord {
    pub descriptor: ServiceDescriptor,
    pub socket_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct Router {
    routes: Arc<RwLock<BTreeMap<String, ServiceRecord>>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_routes<I, S, P>(routes: I) -> Self
    where
        I: IntoIterator<Item = (S, P)>,
        S: Into<String>,
        P: Into<PathBuf>,
    {
        let mut router = Self::new();
        for (service, path) in routes {
            router.insert(service, path);
        }

        router
    }

    pub fn insert(&mut self, service: impl Into<String>, path: impl Into<PathBuf>) {
        self.register_static(service, path);
    }

    pub fn register(&self, service: impl Into<String>, path: impl Into<PathBuf>) {
        self.register_static(service, path);
    }

    pub fn register_manifest(
        &self,
        manifest: &ServiceManifest,
        path: impl Into<PathBuf>,
    ) -> ServiceDescriptor {
        let descriptor = manifest.descriptor();

        self.routes
            .write()
            .expect("router write lock poisoned")
            .insert(
                descriptor.service.clone(),
                ServiceRecord {
                    descriptor: descriptor.clone(),
                    socket_path: path.into(),
                },
            );

        descriptor
    }

    pub fn remove(&self, service: &str) -> Option<PathBuf> {
        self.routes
            .write()
            .expect("router write lock poisoned")
            .remove(service)
            .map(|record| record.socket_path)
    }

    pub fn set_active(&self, service: &str, active: bool) -> Option<ServiceDescriptor> {
        let mut routes = self.routes.write().expect("router write lock poisoned");
        let record = routes.get_mut(service)?;
        record.descriptor.active = active;
        Some(record.descriptor.clone())
    }

    pub fn descriptor(&self, service: &str) -> Option<ServiceDescriptor> {
        self.routes
            .read()
            .expect("router read lock poisoned")
            .get(service)
            .map(|record| record.descriptor.clone())
    }

    pub fn manifest_for_pool(&self, pool: &str) -> Vec<ServiceDescriptor> {
        self.refresh_activity(Some(pool));
        let prefix = format!("{pool}.");
        self.routes
            .read()
            .expect("router read lock poisoned")
            .values()
            .filter(|record| record.descriptor.service.starts_with(&prefix))
            .map(|record| record.descriptor.clone())
            .collect()
    }

    pub fn manifest(&self) -> Vec<ServiceDescriptor> {
        self.refresh_activity(None);
        self.routes
            .read()
            .expect("router read lock poisoned")
            .values()
            .map(|record| record.descriptor.clone())
            .collect()
    }

    pub fn resolve(&self, service: &str) -> Result<PathBuf> {
        let record = self
            .routes
            .read()
            .expect("router read lock poisoned")
            .get(service)
            .cloned()
            .ok_or_else(|| BusError::RouteNotFound {
                service: service.to_string(),
            })?;

        if !record.descriptor.active {
            return Err(BusError::ServiceUnavailable {
                service: record.descriptor.service,
            });
        }

        Ok(record.socket_path)
    }

    fn register_static(&self, service: impl Into<String>, path: impl Into<PathBuf>) {
        let service = service.into();
        self.routes
            .write()
            .expect("router write lock poisoned")
            .insert(
                service.clone(),
                ServiceRecord {
                    descriptor: ServiceDescriptor {
                        service,
                        active: true,
                        is_client: false,
                        features: BTreeMap::new(),
                        capabilities: Vec::new(),
                        modes: Vec::new(),
                    },
                    socket_path: path.into(),
                },
            );
    }

    fn refresh_activity(&self, pool: Option<&str>) {
        let prefix = pool.map(|value| format!("{value}."));
        let probes = self
            .routes
            .read()
            .expect("router read lock poisoned")
            .iter()
            .filter(|(_, record)| !record.descriptor.is_client)
            .filter(|(_, record)| record.descriptor.active)
            .filter(|(_, record)| {
                if let Some(prefix) = prefix.as_ref() {
                    record.descriptor.service.starts_with(prefix)
                } else {
                    true
                }
            })
            .map(|(service, record)| (service.clone(), record.socket_path.clone()))
            .collect::<Vec<_>>();

        if probes.is_empty() {
            return;
        }

        let updates = probes
            .into_iter()
            .map(|(service, socket_path)| (service, !Self::socket_is_reachable(&socket_path)))
            .collect::<Vec<_>>();

        let mut routes = self.routes.write().expect("router write lock poisoned");
        for (service, should_deactivate) in updates {
            if should_deactivate {
                if let Some(record) = routes.get_mut(&service) {
                    record.descriptor.active = false;
                }
            }
        }
    }

    fn socket_is_reachable(path: &PathBuf) -> bool {
        #[cfg(unix)]
        {
            StdUnixStream::connect(path).is_ok()
        }
        #[cfg(not(unix))]
        {
            path.exists()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::path::PathBuf;

    use crate::{Router, ServiceManifest};

    #[test]
    fn resolves_static_service_routes() {
        let router = Router::from_routes([("svc-a", PathBuf::from("/tmp/svc-a.sock"))]);

        let route = router.resolve("svc-a").unwrap();
        assert_eq!(route, PathBuf::from("/tmp/svc-a.sock"));
    }

    #[test]
    fn missing_route_is_an_error() {
        let router = Router::new();
        assert!(router.resolve("missing").is_err());
    }

    #[test]
    fn inactive_routes_are_reported_as_unavailable() {
        let router = Router::new();
        router.register_manifest(
            &ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            PathBuf::from("/tmp/calcola.sock"),
        );
        router.set_active("ps2.calcola", false);

        let error = router.resolve("ps2.calcola").unwrap_err();
        assert!(matches!(
            error,
            crate::BusError::ServiceUnavailable { service } if service == "ps2.calcola"
        ));
    }

    #[test]
    fn manifest_filters_services_by_pool_prefix() {
        let router = Router::new();
        router.register_manifest(
            &ServiceManifest {
                name: "ps1.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            PathBuf::from("/tmp/calcola.sock"),
        );
        router.register_manifest(
            &ServiceManifest {
                name: "ps2.somma".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            PathBuf::from("/tmp/somma.sock"),
        );

        let manifest = router.manifest_for_pool("ps1");
        assert_eq!(manifest.len(), 1);
        assert_eq!(manifest[0].service, "ps1.calcola");
        assert_eq!(manifest[0].capabilities[0].name, "manifest");
    }

    #[test]
    fn manifest_returns_all_registered_services() {
        let router = Router::new();
        router.register_manifest(
            &ServiceManifest {
                name: "ps1.echo".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            PathBuf::from("/tmp/echo.sock"),
        );
        router.register_manifest(
            &ServiceManifest {
                name: "ps2.somma".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            PathBuf::from("/tmp/somma.sock"),
        );

        let mut services = router
            .manifest()
            .into_iter()
            .map(|descriptor| descriptor.service)
            .collect::<Vec<_>>();
        services.sort();

        assert_eq!(services, vec!["ps1.echo", "ps2.somma"]);
    }

    #[test]
    #[cfg(unix)]
    fn manifest_refreshes_service_activity_from_socket_liveness() {
        let socket_path = PathBuf::from(format!(
            "/tmp/tlb-live-{}.sock",
            &uuid::Uuid::new_v4().to_string()[..8]
        ));
        let _ = std::fs::remove_file(&socket_path);

        let listener = StdUnixListener::bind(&socket_path).unwrap();
        let router = Router::new();
        router.register_manifest(
            &ServiceManifest {
                name: "ps2.invoice".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: Vec::new(),
                modes: Vec::new(),
            },
            socket_path.clone(),
        );

        let active_descriptor = router
            .manifest_for_pool("ps2")
            .into_iter()
            .find(|descriptor| descriptor.service == "ps2.invoice")
            .unwrap();
        assert!(active_descriptor.active);

        drop(listener);
        let _ = std::fs::remove_file(&socket_path);

        let inactive_descriptor = router
            .manifest_for_pool("ps2")
            .into_iter()
            .find(|descriptor| descriptor.service == "ps2.invoice")
            .unwrap();
        assert!(!inactive_descriptor.active);
    }
}
