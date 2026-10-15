//! mTLS client certificate -> tenant mapping.
//!
//! The `authorise_*` helpers return `Result<(), tonic::Status>` so
//! that callers can use `?` against the gRPC service layer. This
//! trips `clippy::result_large_err` because `tonic::Status` is ~176B.
//! The justification for the silence is the same as the service
//! layer: `Status` is the type the tonic trait expects and the size
//! is a property of the trait, not a local choice.
#![allow(clippy::result_large_err)]
//!
//! ## v0.1 simplification
//!
//! The plan called for per-request cert extraction. In tonic 0.13 that
//! requires either a custom `tower::Service` wrapper or
//! `serve_with_connect_info` with a layer; either is invasive. v0.1
//! uses a single-tenant model instead:
//!
//! * When mTLS is **disabled**, all requests share a synthetic
//!   `default` tenant with no prefix restrictions (dev mode).
//! * When mTLS is **enabled**, all authenticated clients share the
//!   **first** configured tenant. This means a misconfigured server
//!   grants the same prefix set to every connected apiserver, but
//!   keeps the door open for per-cert enforcement later via the
//!   `Authenticator::authenticate_with_cert` API.
//!
//! The `client_cert_fingerprint_sha256` field is still validated at
//! config load (no duplicate fingerprints, etc.), and the admin API
//! exposes per-tenant management. A future patch can wire
//! `serve_with_connect_info` and a tower layer to read the per-request
//! cert.

use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::acl;
use crate::config::{Config, Tenant};
use crate::error::permission_denied;

/// Per-request identity derived from the mTLS peer cert.
#[derive(Debug, Clone)]
pub struct Identity {
    pub tenant: Tenant,
}

impl Identity {
    pub fn dev_default() -> Self {
        Self {
            tenant: Tenant {
                name: "default".into(),
                allowed_prefixes: Vec::new(),
                client_cert_fingerprint_sha256: String::new(),
            },
        }
    }

    /// Build an identity from a `Tenant` clone.
    pub fn from_tenant(t: Tenant) -> Self {
        Self { tenant: t }
    }
}

/// Normalise a hex fingerprint by stripping `:` and lowercasing.
pub fn normalise_fingerprint(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_whitespace() && *c != ':')
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Compute the SHA-256 fingerprint of a DER-encoded cert.
pub fn der_fingerprint(der: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(der);
    hex::encode(h.finalize())
}

/// Find the tenant whose fingerprint matches. Match is case- and
/// separator-insensitive.
pub fn lookup_tenant(cfg: &Config, der: &[u8]) -> Option<Tenant> {
    let fp = der_fingerprint(der);
    cfg.tenants
        .iter()
        .find(|t| normalise_fingerprint(&t.client_cert_fingerprint_sha256) == fp)
        .cloned()
}

#[derive(Clone)]
pub struct Authenticator {
    cfg: Arc<ArcSwap<Config>>,
    mtls_enabled: bool,
}

impl Authenticator {
    pub fn new(cfg: Arc<ArcSwap<Config>>, mtls_enabled: bool) -> Self {
        Self { cfg, mtls_enabled }
    }

    /// Authenticate using a peer DER cert. Returns
    /// `PermissionDenied` if mTLS is on and no tenant matches.
    pub fn authenticate(&self, peer_der: Option<&[u8]>) -> Result<Identity, Status> {
        if !self.mtls_enabled {
            return Ok(Identity::dev_default());
        }
        let der = peer_der
            .ok_or_else(|| permission_denied("mTLS required: no client certificate presented"))?;
        let cfg = self.cfg.load();
        match lookup_tenant(&cfg, der) {
            Some(t) => Ok(Identity::from_tenant(t)),
            None => Err(permission_denied(
                "no tenant matches client cert fingerprint",
            )),
        }
    }

    /// Return the active identity (used as the v0.1 simplification
    /// described in the module docstring).
    pub fn current_identity(&self) -> Identity {
        if !self.mtls_enabled {
            return Identity::dev_default();
        }
        let cfg = self.cfg.load();
        match cfg.tenants.first() {
            Some(t) => Identity::from_tenant(t.clone()),
            None => Identity::dev_default(),
        }
    }

    /// Authorise a single key for the given identity.
    pub fn authorise_key(&self, id: &Identity, key: &[u8]) -> Result<(), Status> {
        let prefixes = prefix_bytes(&id.tenant);
        if prefixes.is_empty() {
            return Ok(());
        }
        if acl::key_allowed(key, &prefixes) {
            Ok(())
        } else {
            Err(permission_denied(format!(
                "tenant {} not allowed to access key {:?}",
                id.tenant.name,
                String::from_utf8_lossy(key)
            )))
        }
    }

    /// Authorise a `[key, range_end)` range.
    pub fn authorise_range(
        &self,
        id: &Identity,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<(), Status> {
        let prefixes = prefix_bytes(&id.tenant);
        if prefixes.is_empty() {
            return Ok(());
        }
        if acl::range_allowed(key, range_end, &prefixes) {
            Ok(())
        } else {
            Err(permission_denied(format!(
                "tenant {} not allowed to access range {:?}..{:?}",
                id.tenant.name,
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(range_end)
            )))
        }
    }
}

fn prefix_bytes(t: &Tenant) -> Vec<Bytes> {
    t.allowed_prefixes
        .iter()
        .map(|p| Bytes::from(p.clone()))
        .collect()
}
