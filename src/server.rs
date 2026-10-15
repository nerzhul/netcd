//! Tonic server wiring with mTLS support.
//!
//! Per-request peer cert extraction is documented as a future
//! enhancement in `crate::auth`. v0.1 uses a single-tenant model.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::transport::{Certificate, Identity as TlsIdentity, Server, ServerTlsConfig};
use tracing::{info, warn};

use crate::auth::Authenticator;
use crate::proto::etcdserverpb::{
    kv_server::KvServer, lease_server::LeaseServer, maintenance_server::MaintenanceServer,
    watch_server::WatchServer,
};
use crate::proto::grpc::health::v1::health_server::HealthServer;
use crate::services::health::HealthService;
use crate::services::kv::KvService;
use crate::services::lease::LeaseService;
use crate::services::maintenance::MaintenanceService;
use crate::services::watch::WatchService;
use crate::store::Store;

// Marker type used by future tower layer; keep import live.
#[allow(dead_code)]
type PeerInfo = TlsConnectInfo<TcpConnectInfo>;

pub struct EtcdServerOptions {
    pub bind_addr: String,
    pub mtls: bool,
    pub server_cert_pem: Option<Vec<u8>>,
    pub server_key_pem: Option<Vec<u8>>,
    pub client_ca_pem: Option<Vec<u8>>,
}

pub async fn build_and_serve(
    store: Arc<Store>,
    auth: Authenticator,
    max_ttl: Duration,
    _history_capacity: usize,
    progress_interval: Duration,
    opts: EtcdServerOptions,
) -> anyhow::Result<()> {
    let kv_svc = KvService::new(store.clone(), auth.clone());
    let watch_svc = WatchService::new(store.clone(), auth.clone(), progress_interval);
    let lease_svc = LeaseService::new(store.clone(), auth.clone(), max_ttl);
    let maintenance_svc = MaintenanceService::new(store.clone());

    let current_rev = Arc::new(std::sync::atomic::AtomicU64::new(1));
    {
        let mirror = current_rev.clone();
        let store_for_mirror = store.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                mirror.store(store_for_mirror.current_rev(), Ordering::Relaxed);
            }
        });
    }
    let health_svc = HealthService::new(current_rev);

    let kv = KvServer::new(kv_svc)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let watch = WatchServer::new(watch_svc)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let lease = LeaseServer::new(lease_svc)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let maintenance = MaintenanceServer::new(maintenance_svc);
    let health = HealthServer::new(health_svc);

    let mut builder = Server::builder();
    if opts.mtls {
        let cert_pem = opts
            .server_cert_pem
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("mTLS: --server-cert required"))?;
        let key_pem = opts
            .server_key_pem
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("mTLS: --server-key required"))?;
        let ca_pem = opts
            .client_ca_pem
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("mTLS: --client-ca required"))?;
        let server_id = TlsIdentity::from_pem(cert_pem, key_pem);
        let ca = Certificate::from_pem(ca_pem);
        let tls = ServerTlsConfig::new()
            .identity(server_id)
            .client_ca_root(ca);
        builder = builder.tls_config(tls)?;
        info!("mTLS enabled; client certs required");
    } else {
        warn!("mTLS DISABLED - all requests share the default tenant");
    }

    let router = builder
        .add_service(kv)
        .add_service(watch)
        .add_service(lease)
        .add_service(maintenance)
        .add_service(health);

    let addr = opts.bind_addr.clone();
    info!(addr = %addr, "etcd gRPC server listening");
    router.serve(addr.parse()?).await?;
    Ok(())
}
