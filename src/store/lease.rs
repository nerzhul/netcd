//! Lease table + background expiry task.
//!
//! `Lease.Grant` allocates a new `Lease` keyed by `id`. `KeepAlive`
//! extends its expiry; `Revoke` deletes the lease and removes all
//! attached keys. A background tokio task expires leases whose deadline
//! has passed and fires `Delete` Watch events for every attached key.
//!
//! A lease remembers the TTL it was granted with, and every renewal
//! uses that value. Renewing with the server-wide cap instead would
//! silently turn every short-lived lease into a long-lived one: an
//! apiserver asking for a 60 s lease would get a 24 h one, and the
//! objects bound to it would never be reaped.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::time::{interval, MissedTickBehavior};

use crate::store::{MutationKind, Store};
use bytes::Bytes;

/// How often the background task scans the lease table for expired
/// leases.
///
/// etcd reaps leases every 500 ms. netcd used to derive the interval
/// from `server_max_ttl_seconds / 100`, which with the default 24 h
/// cap meant one scan every 864 s: a lease granted for 60 s could then
/// stay alive for a quarter of an hour. The scan is O(number of
/// leases), so a short interval is cheap and keeps the observed
/// lifetime close to the granted one.
pub const DEFAULT_EXPIRY_TICK: Duration = Duration::from_secs(1);

/// How many expiry ticks between two heap trims.
///
/// The trim walks the allocator's free lists and gives the pages back to
/// the kernel, which is not free. Once every 30 s is enough for the
/// resident set to follow the real usage of an idle server.
pub const TRIM_EVERY_TICKS: u32 = 30;

/// One active lease.
#[derive(Debug)]
pub struct LeaseEntry {
    pub id: i64,
    pub expires: Instant,
    /// TTL this lease was granted with, already clamped to the server
    /// cap. Renewals extend the deadline by this much, so the client
    /// gets the lifetime it asked for.
    pub ttl: Duration,
    /// Keys attached to this lease. We snapshot this set when the
    /// lease expires or is revoked; under the table lock a concurrent
    /// Put can be adding new attachments.
    pub keys: HashSet<Bytes>,
    /// Optional owner tenant name (used for ACL on KeepAlive / Revoke).
    pub owner: Option<String>,
}

/// Snapshot of a lease's state, as reported by `Lease.TimeToLive`.
#[derive(Debug)]
pub struct LeaseStatus {
    pub id: i64,
    /// Time left before the lease expires.
    pub remaining: Duration,
    /// TTL the lease was granted with (already clamped to the cap).
    pub granted_ttl: Duration,
    /// Keys currently attached to the lease.
    pub keys: HashSet<Bytes>,
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
                ttl,
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

    /// Renew `id`.
    ///
    /// The deadline is extended by the TTL the lease was *granted*
    /// with, not by the server-wide cap: honouring the TTL the client
    /// asked for is the whole point of a lease. Returns the time left
    /// after the renewal, or `None` when the lease is unknown or
    /// owned by another tenant.
    pub fn keep_alive(&self, id: i64, owner: Option<&str>) -> Option<Duration> {
        let mut g = self.inner.lock();
        let l = g.get_mut(&id)?;
        if let Some(o) = owner {
            if l.owner.as_deref().is_some_and(|x| x != o) {
                return None;
            }
        }
        let now = Instant::now();
        l.expires = now + l.ttl;
        Some(l.expires - now)
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

    /// Snapshot a lease's state. `None` when the lease is unknown.
    pub fn time_to_live(&self, id: i64) -> Option<LeaseStatus> {
        let g = self.inner.lock();
        let l = g.get(&id)?;
        let remaining = l.expires.saturating_duration_since(Instant::now());
        Some(LeaseStatus {
            id: l.id,
            remaining,
            granted_ttl: l.ttl,
            keys: l.keys.clone(),
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
        // The heap is trimmed every `TRIM_EVERY_TICKS` ticks: glibc keeps
        // the freed pages in its arena, so the resident set of the
        // process would stay at its high-water mark after a burst of
        // events was purged.
        let mut ticks_since_trim: u32 = 0;
        loop {
            t.tick().await;
            let expired = store.leases.collect_expired();
            for (id, keys) in expired {
                tracing::info!(lease_id = id, keys = keys.len(), "lease expired");
                for k in keys {
                    let _ = store.delete_with_event(k, MutationKind::DeleteLease);
                }
            }
            ticks_since_trim += 1;
            if ticks_since_trim >= TRIM_EVERY_TICKS {
                ticks_since_trim = 0;
                crate::alloc::trim_heap();
            }
            // Periodic housekeeping: release the payloads of the deleted
            // keys, compact the eviction queue.
            store.housekeeping();
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
        assert!(lt.keep_alive(id, None).is_some());
        let keys = lt.revoke(id, None).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn keep_alive_renews_with_the_granted_ttl() {
        // Regression: `keep_alive` used to take the TTL from its caller,
        // and the only caller passed the server-wide cap. A lease the
        // apiserver granted for 60 s was therefore renewed for the
        // full cap (24 h by default) and never expired.
        let lt = LeaseTable::new();
        let granted = Duration::from_secs(60);
        let id = lt.grant(granted, None);

        let remaining = lt.keep_alive(id, None).expect("the lease is alive");
        assert!(
            remaining <= granted && remaining > granted - Duration::from_secs(1),
            "the renewal must use the granted TTL ({granted:?}), got {remaining:?}",
        );

        let status = lt.time_to_live(id).expect("the lease exists");
        assert_eq!(status.granted_ttl, granted);
        assert!(
            status.remaining <= granted && status.remaining > granted - Duration::from_secs(1),
            "the remaining TTL must follow the granted TTL, got {:?}",
            status.remaining,
        );
    }

    #[test]
    fn keep_alive_on_an_unknown_lease_is_none() {
        let lt = LeaseTable::new();
        assert!(lt.keep_alive(42, None).is_none());
    }

    #[test]
    fn keep_alive_refuses_another_tenant() {
        let lt = LeaseTable::new();
        let id = lt.grant(Duration::from_secs(60), Some("alice".into()));
        assert!(lt.keep_alive(id, Some("bob")).is_none());
        assert!(lt.keep_alive(id, Some("alice")).is_some());
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
