//! Tenant / capacity / TTL configuration.
//!
//! `Config` is loaded at boot from a JSON file, persisted atomically (write
//! to `path.tmp`, fsync, rename) and hot-reloaded on `SIGHUP` via
//! `arc-swap::ArcSwap` so that all request handlers read it lock-free.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

/// Default fallback for any missing optional field. Matches the example in
/// the plan.
fn default_server_max_ttl() -> i64 {
    86_400
}
fn default_max_cache_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_history_capacity() -> usize {
    1000
}

/// Per-tenant ACL and identity binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tenant {
    pub name: String,
    /// Key prefixes this tenant is allowed to read/write/watch.
    pub allowed_prefixes: Vec<String>,
    /// SHA-256 fingerprint of the DER-encoded client cert.
    /// Accepts uppercase or lowercase hex, with or without `:` separators.
    pub client_cert_fingerprint_sha256: String,
}

/// Top-level server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_server_max_ttl")]
    pub server_max_ttl_seconds: i64,
    #[serde(default = "default_max_cache_bytes")]
    pub max_cache_bytes: u64,
    #[serde(default = "default_history_capacity")]
    pub history_capacity: usize,
    pub tenants: Vec<Tenant>,
}

impl Config {
    /// Build a sane default config (no tenants, permissive defaults).
    pub fn default_dev() -> Self {
        Self {
            server_max_ttl_seconds: default_server_max_ttl(),
            max_cache_bytes: default_max_cache_bytes(),
            history_capacity: default_history_capacity(),
            tenants: Vec::new(),
        }
    }

    /// Read the config from `path`. Missing file is a hard error.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Atomically write the config: `<path>.tmp` -> fsync -> rename.
    /// Rename is atomic on POSIX so a concurrent reader never sees a
    /// half-written file.
    pub fn save_atomic(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
        }
        let tmp = tmp_path(path);
        let bytes = serde_json::to_vec_pretty(self).context("serialising config")?;
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)
                .with_context(|| format!("creating {}", tmp.display()))?;
            f.write_all(&bytes)?;
            f.sync_all()
                .with_context(|| format!("fsync {}", tmp.display()))?;
        }
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        // fsync the parent dir so the rename itself is durable.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    /// Sanity-check the config at load time.
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.server_max_ttl_seconds > 0,
            "server_max_ttl_seconds must be > 0"
        );
        anyhow::ensure!(self.max_cache_bytes > 0, "max_cache_bytes must be > 0");
        anyhow::ensure!(self.history_capacity > 0, "history_capacity must be > 0");
        let mut seen = std::collections::HashSet::new();
        for t in &self.tenants {
            anyhow::ensure!(!t.name.is_empty(), "tenant name must be non-empty");
            anyhow::ensure!(
                !t.allowed_prefixes.is_empty(),
                "tenant {} must have at least one allowed_prefixes entry",
                t.name
            );
            anyhow::ensure!(
                !t.client_cert_fingerprint_sha256.is_empty(),
                "tenant {} must have a client_cert_fingerprint_sha256",
                t.name
            );
            anyhow::ensure!(
                seen.insert(t.client_cert_fingerprint_sha256.to_lowercase()),
                "duplicate client_cert_fingerprint_sha256 across tenants"
            );
        }
        Ok(())
    }
}

fn tmp_path(p: &Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// A thread-safe, hot-reloadable handle to the live config.
///
/// Internally an `ArcSwap<Config>`: reads are lock-free, writes (reload) are
/// wait-free. The current config can be obtained with
/// [`ConfigRef::current`].
#[derive(Clone)]
pub struct ConfigRef {
    inner: Arc<ArcSwap<Config>>,
    path: PathBuf,
}

impl ConfigRef {
    /// Build a new `ConfigRef` from a freshly loaded config. The file
    /// at `path` will be reloaded on `SIGHUP`.
    pub fn new(cfg: Config, path: PathBuf) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(cfg)),
            path,
        }
    }

    /// Take a snapshot of the current config.
    pub fn current(&self) -> Arc<Config> {
        self.inner.load_full()
    }

    /// Re-read the config from disk and swap it in. Returns the new
    /// `Arc<Config>` for inspection. On error, the previous config is
    /// retained and the error is returned.
    pub fn reload(&self) -> Result<Arc<Config>> {
        let cfg = Config::load(&self.path)?;
        self.inner.store(Arc::new(cfg.clone()));
        Ok(Arc::new(cfg))
    }

    /// Replace the live config with `new`, validate it, persist it
    /// atomically to disk, then swap it in. If validation or save
    /// fails the live config is unchanged.
    pub fn replace(&self, new: Config) -> Result<Arc<Config>> {
        new.validate()?;
        new.save_atomic(&self.path)?;
        self.inner.store(Arc::new(new.clone()));
        Ok(Arc::new(new))
    }

    /// Mutate the live config in-memory via a closure, then persist +
    /// swap. The closure receives a `&mut Config`; the mutation is
    /// validated before being published.
    pub fn update<F>(&self, mutator: F) -> Result<Arc<Config>>
    where
        F: FnOnce(&mut Config),
    {
        // Clone the current config (cheap: it's an Arc), mutate,
        // validate, persist, swap.
        let mut next = (**self.inner.load()).clone();
        mutator(&mut next);
        self.replace(next)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.json");
        let mut cfg = Config::default_dev();
        cfg.tenants.push(Tenant {
            name: "t".into(),
            allowed_prefixes: vec!["/foo/".into()],
            client_cert_fingerprint_sha256: "AB:CD".into(),
        });
        cfg.save_atomic(&p).unwrap();
        let back = Config::load(&p).unwrap();
        assert_eq!(back.tenants.len(), 1);
        assert_eq!(back.tenants[0].name, "t");
    }

    #[test]
    fn validate_rejects_empty_prefixes() {
        let mut cfg = Config::default_dev();
        cfg.tenants.push(Tenant {
            name: "t".into(),
            allowed_prefixes: vec![],
            client_cert_fingerprint_sha256: "AB".into(),
        });
        assert!(cfg.validate().is_err());
    }
}
