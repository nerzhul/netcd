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
// Deliberately smaller than `std::thread::available_parallelism()` so the
// process does not gobble every core on a busy host; the gRPC workload is
// mostly I/O-bound and does not benefit from a 1-thread-per-core model.
fn default_worker_threads() -> usize {
    3
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
    /// Number of OS threads the async runtime drives. Name is deliberately
    /// runtime-agnostic so swapping the implementation does not invalidate
    /// operator tooling or persisted config files. Boot-time only —
    /// changing it requires a restart.
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,
    pub tenants: Vec<Tenant>,
}

impl Config {
    /// Build a sane default config (no tenants, permissive defaults).
    pub fn default_dev() -> Self {
        Self {
            server_max_ttl_seconds: default_server_max_ttl(),
            max_cache_bytes: default_max_cache_bytes(),
            history_capacity: default_history_capacity(),
            worker_threads: default_worker_threads(),
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

    /// Return a copy of `self` with any `Some(...)` fields in `o` applied
    /// on top. Fields left in `None` are preserved from the receiver.
    ///
    /// `tenants` is deliberately **not** overridable through this path:
    /// the `Overrides` type has no `tenants` field, so RBAC edits must go
    /// through the admin HTTP API. This invariant is asserted in the
    /// unit tests below.
    pub fn with_overrides(&self, o: &Overrides) -> Config {
        Config {
            server_max_ttl_seconds: o
                .server_max_ttl_seconds
                .unwrap_or(self.server_max_ttl_seconds),
            max_cache_bytes: o.max_cache_bytes.unwrap_or(self.max_cache_bytes),
            history_capacity: o.history_capacity.unwrap_or(self.history_capacity),
            worker_threads: o.worker_threads.unwrap_or(self.worker_threads),
            tenants: self.tenants.clone(),
        }
    }

    /// Sanity-check the config at load time.
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.server_max_ttl_seconds > 0,
            "server_max_ttl_seconds must be > 0"
        );
        anyhow::ensure!(self.max_cache_bytes > 0, "max_cache_bytes must be > 0");
        anyhow::ensure!(self.history_capacity > 0, "history_capacity must be > 0");
        anyhow::ensure!(self.worker_threads > 0, "worker_threads must be > 0");
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

/// Field-level overrides layered on top of a JSON-loaded `Config`.
///
/// Populated from CLI flags and/or environment variables. A `None` means
/// "do not override" — the underlying `Config` value is kept.
///
/// This struct deliberately has no `tenants` (or `Tenant`-shaped) field.
/// RBAC state is only mutable through the admin HTTP API; if a future
/// change adds such a field, the `overrides_do_not_touch_tenants` test
/// will catch the unintended re-friction through `with_overrides`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Overrides {
    pub server_max_ttl_seconds: Option<i64>,
    pub max_cache_bytes: Option<u64>,
    pub history_capacity: Option<usize>,
    pub worker_threads: Option<usize>,
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

    #[test]
    fn override_applies_each_field_when_some() {
        let base = Config::default_dev();
        let o = Overrides {
            server_max_ttl_seconds: Some(42),
            max_cache_bytes: Some(99),
            history_capacity: Some(7),
            worker_threads: Some(5),
        };
        let out = base.with_overrides(&o);
        assert_eq!(out.server_max_ttl_seconds, 42);
        assert_eq!(out.max_cache_bytes, 99);
        assert_eq!(out.history_capacity, 7);
        assert_eq!(out.worker_threads, 5);
        // Default-constructed base has no tenants; the override path must
        // not introduce any.
        assert!(out.tenants.is_empty());
    }

    #[test]
    fn override_preserves_value_when_none() {
        let mut base = Config::default_dev();
        base.server_max_ttl_seconds = 123;
        base.max_cache_bytes = 456;
        base.history_capacity = 789;
        base.worker_threads = 11;
        let o = Overrides::default();
        let out = base.with_overrides(&o);
        assert_eq!(out.server_max_ttl_seconds, 123);
        assert_eq!(out.max_cache_bytes, 456);
        assert_eq!(out.history_capacity, 789);
        assert_eq!(out.worker_threads, 11);
    }

    #[test]
    fn override_partially_applies() {
        let base = Config::default_dev();
        let o = Overrides {
            history_capacity: Some(99),
            ..Overrides::default()
        };
        let out = base.with_overrides(&o);
        assert_eq!(out.server_max_ttl_seconds, base.server_max_ttl_seconds);
        assert_eq!(out.max_cache_bytes, base.max_cache_bytes);
        assert_eq!(out.history_capacity, 99);
        assert_eq!(out.worker_threads, base.worker_threads);
    }

    #[test]
    fn overrides_do_not_touch_tenants() {
        // Build a config that has RBAC state, run `with_overrides` with a
        // fully populated `Overrides`, and assert the tenants come back
        // bit-for-bit identical. Combined with the type-level invariant
        // that `Overrides` has no `tenants` field, this guards against
        // any future contributor re-introducing tenant material on the
        // CLI/env surface.
        let mut base = Config::default_dev();
        base.tenants.push(Tenant {
            name: "team-a".into(),
            allowed_prefixes: vec!["/registry/events/team-a/".into()],
            client_cert_fingerprint_sha256: "ab:cd".into(),
        });
        base.tenants.push(Tenant {
            name: "team-b".into(),
            allowed_prefixes: vec!["/registry/events/team-b/".into()],
            client_cert_fingerprint_sha256: "de:ad:be:ef".into(),
        });
        let original_tenants = base.tenants.clone();

        let o = Overrides {
            server_max_ttl_seconds: Some(1),
            max_cache_bytes: Some(2),
            history_capacity: Some(3),
            worker_threads: Some(4),
        };
        let out = base.with_overrides(&o);

        assert_eq!(out.tenants, original_tenants);
        assert_eq!(out.tenants.len(), 2);
        assert_eq!(out.tenants[0].name, "team-a");
        assert_eq!(out.tenants[1].name, "team-b");
    }

    #[test]
    fn default_dev_worker_threads_is_three() {
        // Locked-in default; if a future contributor wants to change it,
        // they have to update this test too, which surfaces the change in
        // code review.
        assert_eq!(Config::default_dev().worker_threads, 3);
    }

    #[test]
    fn validate_rejects_zero_worker_threads() {
        let mut cfg = Config::default_dev();
        cfg.worker_threads = 0;
        assert!(cfg.validate().is_err());
    }
}
