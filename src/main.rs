//! netcd: single-node etcd v3-compatible in-memory KV store.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use netcd::admin::AdminState;
use netcd::auth::Authenticator;
use netcd::config::{Config, ConfigRef};
use netcd::server::{build_and_serve, EtcdServerOptions};
use netcd::store::Store;

#[derive(Parser, Debug)]
#[command(name = "netcd", version, about)]
struct Cli {
    /// gRPC listen address.
    #[arg(long, default_value = "0.0.0.0:2379")]
    listen: String,

    /// Admin HTTP API listen address. Set to `disabled` to turn off.
    #[arg(long, default_value = "0.0.0.0:2378")]
    admin_listen: String,

    /// Config file path.
    #[arg(long, default_value = "config.json", env = "NETCD_CONFIG")]
    config: PathBuf,

    /// Enable mTLS (server cert + client CA required).
    #[arg(long, default_value_t = false)]
    mtls: bool,

    /// PEM-encoded server certificate (full chain).
    #[arg(long)]
    server_cert: Option<PathBuf>,

    /// PEM-encoded server private key.
    #[arg(long)]
    server_key: Option<PathBuf>,

    /// PEM-encoded client CA bundle for verifying client certs.
    #[arg(long)]
    client_ca: Option<PathBuf>,

    /// Tracing filter (env-filter syntax). Default: `info,netcd=debug`.
    #[arg(long, default_value = "info,netcd=debug")]
    log: String,
}

fn init_tracing(filter: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_target(false)
        .compact()
        .try_init();
}

fn read_bytes(p: &Option<PathBuf>, what: &str) -> Result<Option<Vec<u8>>> {
    match p {
        Some(path) => {
            let b = std::fs::read(path)
                .with_context(|| format!("reading {what} from {}", path.display()))?;
            Ok(Some(b))
        }
        None => Ok(None),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    // Config: load if present, otherwise start with a dev config and
    // persist it so the admin API can mutate it.
    let config = if cli.config.exists() {
        Config::load(&cli.config)
            .with_context(|| format!("loading config {}", cli.config.display()))?
    } else {
        warn!(
            "config {} not found; creating default dev config",
            cli.config.display()
        );
        let cfg = Config::default_dev();
        cfg.save_atomic(&cli.config)
            .with_context(|| format!("writing default config {}", cli.config.display()))?;
        cfg
    };
    let cfg_ref = ConfigRef::new(config.clone(), cli.config.clone());

    // Store.
    let store = Store::new(config.max_cache_bytes, config.history_capacity);

    // Authenticator.
    let cfg_arcswap: Arc<ArcSwap<Config>> = Arc::new(ArcSwap::from_pointee(config.clone()));
    let auth = Authenticator::new(cfg_arcswap.clone(), cli.mtls);

    // Spawn background tasks: lease expiry, history trim, SIGHUP reload.
    let lease_tick = Duration::from_millis((config.server_max_ttl_seconds as u64 / 100).max(250));
    netcd::store::lease::spawn_expiry_loop(store.clone(), lease_tick);

    // SIGHUP -> reload config from disk and update live state.
    {
        let cfg_ref_signal = cfg_ref.clone();
        let store_signal = store.clone();
        let mut sighup = signal(SignalKind::hangup())?;
        tokio::spawn(async move {
            while sighup.recv().await.is_some() {
                match cfg_ref_signal.reload() {
                    Ok(new_cfg) => {
                        info!("config reloaded via SIGHUP");
                        store_signal.resize_history(new_cfg.history_capacity);
                    }
                    Err(e) => {
                        warn!(error = %e, "SIGHUP reload failed");
                    }
                }
            }
        });
    }

    // Wire the same ConfigRef into both the live config and the admin
    // API: every admin edit updates `cfg_arcswap` and persists, so the
    // gRPC services see the new tenant list on their next read.
    // (Authenticator already holds cfg_arcswap.)

    // Admin API.
    if cli.admin_listen != "disabled" {
        let state = AdminState {
            cfg: cfg_ref.clone(),
            store: store.clone(),
            admin_fingerprints: Arc::new(parking_lot::Mutex::new(Vec::new())),
            addr: cli.admin_listen.clone(),
        };
        let admin_addr = cli.admin_listen.clone();
        tokio::spawn(async move {
            if let Err(e) = netcd::admin::serve(state, &admin_addr).await {
                warn!(error = %e, "admin API exited");
            }
        });
    }

    // gRPC server.
    let opts = EtcdServerOptions {
        bind_addr: cli.listen.clone(),
        mtls: cli.mtls,
        server_cert_pem: read_bytes(&cli.server_cert, "server cert")?,
        server_key_pem: read_bytes(&cli.server_key, "server key")?,
        client_ca_pem: read_bytes(&cli.client_ca, "client CA")?,
    };

    // Graceful shutdown on SIGINT / SIGTERM.
    let shutdown = async {
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigint.recv() => info!("SIGINT received, shutting down"),
            _ = sigterm.recv() => info!("SIGTERM received, shutting down"),
        }
        Ok::<(), anyhow::Error>(())
    };

    tokio::select! {
        r = build_and_serve(
            store.clone(),
            auth.clone(),
            Duration::from_secs(config.server_max_ttl_seconds.max(1) as u64),
            config.history_capacity,
            Duration::from_secs(
                (config.server_max_ttl_seconds.max(60) / 10) as u64,
            ),
            opts,
        ) => {
            if let Err(e) = r {
                tracing::error!(error = %e, "gRPC server error");
            }
        }
        _ = shutdown => {
            info!("shutdown signal received");
        }
    }
    Ok(())
}
