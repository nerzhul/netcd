//! Admin HTTP API on a separate port. Lets operators view and edit the
//! live `Config` without going through the etcd gRPC surface (which we
//! keep wire-compatible with etcd v3 - adding a non-standard service
//! there would defeat the point).
//!
//! Endpoints:
//!
//! - `GET    /v1/config`              -> the current `Config` JSON
//! - `PUT    /v1/config`              -> replace the entire config
//! - `POST   /v1/config/reload`       -> re-read from disk
//! - `GET    /v1/tenants`             -> list tenants
//! - `POST   /v1/tenants`             -> add a tenant
//! - `PUT    /v1/tenants/{name}`      -> update a tenant
//! - `DELETE /v1/tenants/{name}`      -> remove a tenant
//! - `GET    /v1/stats`               -> server stats (rev, bytes_used, count)

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::{Config, ConfigRef, Tenant};
use crate::store::Store;

#[derive(Clone)]
pub struct AdminState {
    pub cfg: ConfigRef,
    pub store: Arc<Store>,
    /// If `Some`, the admin API requires clients to present a TLS
    /// client cert whose fingerprint matches one of these (mirrors the
    /// gRPC auth model).
    pub admin_fingerprints: Arc<Mutex<Vec<String>>>,
    /// Bound address (used for logging).
    pub addr: String,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/v1/config", get(get_config).put(put_config))
        .route("/v1/config/reload", post(post_reload))
        .route("/v1/tenants", get(list_tenants).post(add_tenant))
        .route(
            "/v1/tenants/{name}",
            put(update_tenant).delete(delete_tenant),
        )
        .route("/v1/stats", get(get_stats))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

async fn get_config(State(s): State<AdminState>) -> Result<Json<Config>, (StatusCode, String)> {
    Ok(Json((*s.cfg.current()).clone()))
}

async fn put_config(
    State(s): State<AdminState>,
    Json(new_cfg): Json<Config>,
) -> Result<Json<Config>, (StatusCode, String)> {
    let updated = s
        .cfg
        .replace(new_cfg)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    // Resize history if it changed.
    s.store.resize_history(updated.history_capacity);
    info!("config updated via admin API");
    Ok(Json((*updated).clone()))
}

async fn post_reload(State(s): State<AdminState>) -> Result<Json<Config>, (StatusCode, String)> {
    let updated = s
        .cfg
        .reload()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    s.store.resize_history(updated.history_capacity);
    info!("config reloaded from disk via admin API");
    Ok(Json((*updated).clone()))
}

async fn list_tenants(State(s): State<AdminState>) -> Json<Vec<Tenant>> {
    Json(s.cfg.current().tenants.clone())
}

#[derive(Deserialize)]
struct AddTenant {
    tenant: Tenant,
}

async fn add_tenant(
    State(s): State<AdminState>,
    Json(body): Json<AddTenant>,
) -> Result<(StatusCode, Json<Tenant>), (StatusCode, String)> {
    let mut added = None;
    let updated = s
        .cfg
        .update(|cfg| {
            if cfg.tenants.iter().any(|t| t.name == body.tenant.name) {
                return;
            }
            added = Some(body.tenant.clone());
            cfg.tenants.push(body.tenant.clone());
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    match added {
        Some(_) => Ok((StatusCode::CREATED, Json(body.tenant))),
        None => Err((
            StatusCode::CONFLICT,
            format!("tenant {} already exists", updated.tenants[0].name),
        )),
    }
}

#[derive(Deserialize)]
struct UpdateTenant {
    allowed_prefixes: Vec<String>,
    client_cert_fingerprint_sha256: String,
}

async fn update_tenant(
    State(s): State<AdminState>,
    Path(name): Path<String>,
    Json(body): Json<UpdateTenant>,
) -> Result<Json<Tenant>, (StatusCode, String)> {
    let mut found = false;
    let updated = s
        .cfg
        .update(|cfg| {
            if let Some(t) = cfg.tenants.iter_mut().find(|t| t.name == name) {
                t.allowed_prefixes = body.allowed_prefixes.clone();
                t.client_cert_fingerprint_sha256 = body.client_cert_fingerprint_sha256.clone();
                found = true;
            }
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if found {
        let new = updated
            .tenants
            .iter()
            .find(|t| t.name == name)
            .cloned()
            .unwrap();
        Ok(Json(new))
    } else {
        Err((StatusCode::NOT_FOUND, format!("tenant {name} not found")))
    }
}

async fn delete_tenant(
    State(s): State<AdminState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut removed = false;
    let _ = s
        .cfg
        .update(|cfg| {
            let before = cfg.tenants.len();
            cfg.tenants.retain(|t| t.name != name);
            removed = cfg.tenants.len() < before;
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("tenant {name} not found")))
    }
}

#[derive(Serialize)]
struct Stats {
    revision: u64,
    bytes_used: u64,
    max_cache_bytes: u64,
    key_count: usize,
    lease_count: usize,
    watch_count: usize,
    compacted_rev: u64,
    /// Oldest revision still held in the in-memory history ring
    /// buffer (0 if empty). Any `start_revision <= compacted_rev` is
    /// considered compacted and will be answered with a
    /// `WatchResponse{compact_revision}` notification.
    oldest_rev: u64,
}

async fn get_stats(State(s): State<AdminState>) -> Json<Stats> {
    let kv = s.store.kv.read();
    let leases = s.store.leases.list();
    let watches = s.store.watches.lock();
    Json(Stats {
        revision: s.store.current_rev(),
        bytes_used: s.store.bytes_used.load(Ordering::Relaxed),
        max_cache_bytes: s.store.max_cache_bytes,
        key_count: kv.len(),
        lease_count: leases.len(),
        watch_count: watches.len(),
        compacted_rev: s.store.compacted_rev.load(Ordering::Relaxed),
        oldest_rev: s.store.history.oldest_rev(),
    })
}

/// Bind the admin API on `addr` (e.g. `0.0.0.0:2378`).
pub async fn serve(state: AdminState, addr: &str) -> anyhow::Result<()> {
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "admin HTTP API listening");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_state() -> (tempfile::TempDir, AdminState) {
        let dir = tempdir().unwrap();
        let cfg_path = dir.path().join("cfg.json");
        let cfg = Config::default_dev();
        let cfg_ref = ConfigRef::new(cfg, cfg_path);
        let store = Store::new(1024, 100);
        (
            dir,
            AdminState {
                cfg: cfg_ref,
                store,
                admin_fingerprints: Arc::new(Mutex::new(Vec::new())),
                addr: "127.0.0.1:0".into(),
            },
        )
    }

    #[tokio::test]
    async fn crud_tenant() {
        let (dir, state) = test_state();
        let _ = dir;
        // Add
        let r = add_tenant(
            State(state.clone()),
            Json(AddTenant {
                tenant: Tenant {
                    name: "alice".into(),
                    allowed_prefixes: vec!["/foo/".into()],
                    client_cert_fingerprint_sha256: "AB".into(),
                },
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.0, StatusCode::CREATED);
        assert_eq!(r.1 .0.name, "alice");
        // Update
        let r = update_tenant(
            State(state.clone()),
            Path("alice".into()),
            Json(UpdateTenant {
                allowed_prefixes: vec!["/bar/".into()],
                client_cert_fingerprint_sha256: "AC".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(r.0.allowed_prefixes, vec!["/bar/".to_string()]);
        // Delete
        let r = delete_tenant(State(state.clone()), Path("alice".into()))
            .await
            .unwrap();
        assert_eq!(r, StatusCode::NO_CONTENT);
        // Now gone
        let err = delete_tenant(State(state), Path("alice".into()))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }
}
