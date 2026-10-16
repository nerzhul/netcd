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
use netcd::config::{Config, ConfigRef, Overrides};
use netcd::server::{build_and_serve, EtcdServerOptions};
use netcd::store::Store;

#[derive(Parser, Debug)]
#[command(name = "netcd", version, about)]
struct Cli {
    /// gRPC listen address. Overridable via `NETCD_LISTEN`.
    #[arg(long, default_value = "0.0.0.0:2379", env = "NETCD_LISTEN")]
    listen: String,

    /// Admin HTTP API listen address. Set to `disabled` to turn off.
    /// Overridable via `NETCD_ADMIN_LISTEN`.
    #[arg(long, default_value = "0.0.0.0:2378", env = "NETCD_ADMIN_LISTEN")]
    admin_listen: String,

    /// Config file path. Overridable via `NETCD_CONFIG`.
    #[arg(long, default_value = "config.json", env = "NETCD_CONFIG")]
    config: PathBuf,

    /// Enable mTLS (server cert + client CA required). Overridable via
    /// `NETCD_MTLS` (set to `1`/`true`/`yes`/`on` to enable).
    #[arg(long, default_value_t = false, env = "NETCD_MTLS")]
    mtls: bool,

    /// PEM-encoded server certificate (full chain). Overridable via
    /// `NETCD_SERVER_CERT`.
    #[arg(long, env = "NETCD_SERVER_CERT")]
    server_cert: Option<PathBuf>,

    /// PEM-encoded server private key. Overridable via `NETCD_SERVER_KEY`.
    #[arg(long, env = "NETCD_SERVER_KEY")]
    server_key: Option<PathBuf>,

    /// PEM-encoded client CA bundle for verifying client certs. Overridable
    /// via `NETCD_CLIENT_CA`.
    #[arg(long, env = "NETCD_CLIENT_CA")]
    client_ca: Option<PathBuf>,

    /// Tracing filter (env-filter syntax). Default: `info,netcd=debug`.
    /// Overridable via `NETCD_LOG`.
    #[arg(long, default_value = "info,netcd=debug", env = "NETCD_LOG")]
    log: String,

    /// Override `server_max_ttl_seconds` from `config.json`. Overridable via
    /// `NETCD_SERVER_MAX_TTL_SECONDS`.
    #[arg(long, env = "NETCD_SERVER_MAX_TTL_SECONDS")]
    server_max_ttl_seconds: Option<i64>,

    /// Override `max_cache_bytes` from `config.json`. Overridable via
    /// `NETCD_MAX_CACHE_BYTES`.
    #[arg(long, env = "NETCD_MAX_CACHE_BYTES")]
    max_cache_bytes: Option<u64>,

    /// Override `history_capacity` from `config.json`. Overridable via
    /// `NETCD_HISTORY_CAPACITY`.
    #[arg(long, env = "NETCD_HISTORY_CAPACITY")]
    history_capacity: Option<usize>,
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

/// Where a single config knob ultimately came from. The 12-factor
/// precedence is `cli > env > file > default`; this enum lists them in
/// that order.
#[derive(Debug, Clone, Copy)]
enum Origin {
    Cli,
    Env,
    File,
    Default,
}

/// Classify the origin of one knob. `cli_present` is whether the clap
/// field parsed as `Some(...)` (either a CLI flag or an env-var fallback).
/// `file_present` is whether a `config.json` existed on disk at boot —
/// needed to tell `file` apart from `default` even when the explicit file
/// value happens to equal the in-code default.
fn classify<T: PartialEq>(
    cli_present: bool,
    env_var_set: bool,
    file_present: bool,
    effective: &T,
    default_value: &T,
) -> Origin {
    if cli_present {
        if env_var_set {
            // Both the env var *and* (maybe) the CLI flag are present;
            // clap gives CLI precedence, but we cannot tell from the
            // derive layer which one actually won. Report `Env`
            // because the env var definitely participated.
            Origin::Env
        } else {
            Origin::Cli
        }
    } else if file_present || effective != default_value {
        Origin::File
    } else {
        Origin::Default
    }
}

/// Emit one `info!` line per numeric knob recording the value that the
/// process is about to run with and the layer it came from.
///
/// See [`classify`] for the (slightly conservative) rules. The
/// classification is informational; the effective values used by the
/// runtime are exactly `config.*` and are the only ones the rest of
/// `main` cares about.
fn log_effective_config(cli: &Cli, config: &Config, file_present: bool) {
    let defaults = Config::default_dev();

    let ttl_origin = classify(
        cli.server_max_ttl_seconds.is_some(),
        std::env::var_os("NETCD_SERVER_MAX_TTL_SECONDS").is_some(),
        file_present,
        &config.server_max_ttl_seconds,
        &defaults.server_max_ttl_seconds,
    );
    let cache_origin = classify(
        cli.max_cache_bytes.is_some(),
        std::env::var_os("NETCD_MAX_CACHE_BYTES").is_some(),
        file_present,
        &config.max_cache_bytes,
        &defaults.max_cache_bytes,
    );
    let hist_origin = classify(
        cli.history_capacity.is_some(),
        std::env::var_os("NETCD_HISTORY_CAPACITY").is_some(),
        file_present,
        &config.history_capacity,
        &defaults.history_capacity,
    );

    info!(
        "effective config: server_max_ttl_seconds={} ({:?})",
        config.server_max_ttl_seconds, ttl_origin
    );
    info!(
        "effective config: max_cache_bytes={} ({:?})",
        config.max_cache_bytes, cache_origin
    );
    info!(
        "effective config: history_capacity={} ({:?})",
        config.history_capacity, hist_origin
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    // Config: load if present, otherwise start with a dev config and
    // persist it so the admin API can mutate it. `file_was_present` is
    // captured before the auto-create branch so the boot log can tell
    // `File` apart from `Default` even when the file's explicit value
    // happens to equal the in-code default.
    let file_was_present = cli.config.exists();
    let config = if file_was_present {
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

    // Layer CLI / env-var overrides on top of the JSON-loaded config.
    // `tenants` cannot be set through `Overrides` — RBAC stays admin-only.
    let overrides = Overrides {
        server_max_ttl_seconds: cli.server_max_ttl_seconds,
        max_cache_bytes: cli.max_cache_bytes,
        history_capacity: cli.history_capacity,
    };
    let config = config.with_overrides(&overrides);
    config
        .validate()
        .with_context(|| "config failed validation (env-supplied or default values)")?;

    // Emit one boot line that records the resolved effective value of
    // each numeric knob and its origin (cli > env > file > default).
    log_effective_config(&cli, &config, file_was_present);

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
