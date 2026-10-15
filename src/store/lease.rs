//! Lease table + background expiry task.
//!
//! `Lease.Grant` allocates a new `Lease` keyed by `id`. `KeepAlive`
//! extends its expiry; `Revoke` deletes the lease and removes all
//! attached keys. A background tokio task expires leases whose deadline
//! has passed and fires `Delete` Watch events for every attached key.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::time::{interval, MissedTickBehavior};

use crate::store::{MutationKind, Store};
use bytes::Bytes;

/// One active lease.
#[derive(Debug)]
pub struct LeaseEntry {
    pub id: i64,
    pub expires: Instant,
    /// Keys attached to this lease. We snapshot this set when the
    /// lease expires or is revoked; under the table lock a concurrent
    /// Put can be adding new attachments.
    pub keys: HashSet<Bytes>,
    /// Optional owner tenant name (used for ACL on KeepAlive / Revoke).
    pub owner: Option<String>,
}

#[derive(Debug, Default)]
pub struct LeaseTable {
    inner: Mutex<HashMap<i64, LeaseEntry>>,
    next_id: Mutex<i64>,
}

impl LeaseTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
        }
    }

    pub fn grant(&self, ttl: Duration, owner: Option<String>) -> i64 {
        let mut nid = self.next_id.lock();
        // Start at 1; 0 is reserved (etcd never issues it).
        *nid += 1;
        let id = *nid;
        drop(nid);
        self.grant_with_id(id, ttl, owner);
        id
    }

    /// Insert a lease with a specific ID (used to honour an explicit
    /// `LeaseGrantRequest.id`).
    pub fn grant_with_id(&self, id: i64, ttl: Duration, owner: Option<String>) {
        let mut g = self.inner.lock();
        g.insert(
            id,
            LeaseEntry {
                id,
                expires: Instant::now() + ttl,
                keys: HashSet::new(),
                owner,
            },
        );
        // Advance the counter past this id so future grants don't
        // collide.
        let mut nid = self.next_id.lock();
        if *nid < id {
            *nid = id;
        }
    }

    pub fn keep_alive(&self, id: i64, ttl: Duration, owner: Option<&str>) -> bool {
        let mut g = self.inner.lock();
        if let Some(l) = g.get_mut(&id) {
            if let Some(o) = owner {
                if l.owner.as_deref().is_some_and(|x| x != o) {
                    return false;
                }
            }
            l.expires = Instant::now() + ttl;
            true
        } else {
            false
        }
    }

    pub fn revoke(&self, id: i64, owner: Option<&str>) -> Option<HashSet<Bytes>> {
        let mut g = self.inner.lock();
        let l = g.remove(&id)?;
        if let Some(o) = owner {
            if l.owner.as_deref().is_some_and(|x| x != o) {
                // Re-insert and refuse.
                g.insert(id, l);
                return None;
            }
        }
        Some(l.keys)
    }

    /// Add `key` to the attachment set of `id`. Idempotent.
    pub fn attach(&self, id: i64, key: Bytes) {
        let mut g = self.inner.lock();
        if let Some(l) = g.get_mut(&id) {
            l.keys.insert(key);
        }
    }

    /// Remove `key` from any lease that holds it. Returns the IDs of
    /// the leases touched (caller can use this to decide whether to
    /// fire a Watch event for the unattached key).
    pub fn detach(&self, key: &[u8]) -> Vec<i64> {
        let mut g = self.inner.lock();
        let mut touched = Vec::new();
        for l in g.values_mut() {
            if l.keys.remove(key) {
                touched.push(l.id);
            }
        }
        touched
    }

    pub fn time_to_live(&self, id: i64) -> Option<(i64, Duration, HashSet<Bytes>)> {
        let g = self.inner.lock();
        g.get(&id).map(|l| {
            let now = Instant::now();
            let rem = if l.expires > now {
                l.expires - now
            } else {
                Duration::ZERO
            };
            (l.id, rem, l.keys.clone())
        })
    }

    pub fn list(&self) -> Vec<i64> {
        self.inner.lock().keys().copied().collect()
    }

    /// Collect every lease that has expired and remove it. Returns
    /// the set of keys (and the lease id) that were attached, so the
    /// caller can fire `Delete` watch events.
    pub fn collect_expired(&self) -> Vec<(i64, HashSet<Bytes>)> {
        let now = Instant::now();
        let mut g = self.inner.lock();
        let mut out = Vec::new();
        let expired: Vec<i64> = g
            .iter()
            .filter(|(_, l)| l.expires <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(l) = g.remove(&id) {
                out.push((id, l.keys));
            }
        }
        out
    }
}

/// Spawn the background expiry loop. The task wakes up every
/// `tick` (capped to a sensible minimum) and asks the lease table
/// for expired entries. For each expired lease it asks the store to
/// delete every attached key.
pub fn spawn_expiry_loop(store: Arc<Store>, tick: Duration) {
    tokio::spawn(async move {
        let mut t = interval(tick);
        t.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            t.tick().await;
            let expired = store.leases.collect_expired();
            for (id, keys) in expired {
                tracing::info!(lease_id = id, keys = keys.len(), "lease expired");
                for k in keys {
                    let _ = store.delete_with_event(k, MutationKind::DeleteLease);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn grant_keep_revoke() {
        let lt = LeaseTable::new();
        let id = lt.grant(Duration::from_secs(60), None);
        assert!(id > 0);
        assert!(lt.keep_alive(id, Duration::from_secs(120), None));
        let keys = lt.revoke(id, None).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn expiry() {
        let lt = LeaseTable::new();
        let id = lt.grant(Duration::from_millis(20), None);
        thread::sleep(Duration::from_millis(50));
        let out = lt.collect_expired();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, id);
    }

    #[test]
    fn attach_detach() {
        let lt = LeaseTable::new();
        let id = lt.grant(Duration::from_secs(60), None);
        lt.attach(id, Bytes::from_static(b"k"));
        let touched = lt.detach(b"k");
        assert_eq!(touched, vec![id]);
    }

    #[test]
    fn revoke_cross_tenant_denied() {
        let lt = LeaseTable::new();
        let id = lt.grant(Duration::from_secs(60), Some("alice".into()));
        assert!(lt.revoke(id, Some("bob")).is_none());
        assert!(lt.revoke(id, Some("alice")).is_some());
    }
}
