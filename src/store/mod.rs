//! In-memory store: KV, leases, history, eviction, watches.

pub mod history;
pub mod lease;
pub mod revision;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::error::StoreError;
use crate::services::watch::WatchEvent;
use crate::store::history::{HistoryBuffer, Mutation};
use crate::store::lease::LeaseTable;
use crate::store::revision::AtomicRevision;

/// Distinguish Put vs Delete in the mutation event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Put,
    Delete,
    DeleteLease,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Bytes,
    pub create_rev: u64,
    pub mod_rev: u64,
    pub lease_id: Option<i64>,
    /// Cached `key.len() + value.len()` for fast eviction accounting.
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct EvictEntry {
    pub key: Bytes,
    pub size: u64,
    pub rev: u64,
}

/// A live Watch subscription, keyed by `watch_id`.
pub struct WatchHandle {
    pub id: i64,
    /// Inclusive lower bound key.
    pub key: Bytes,
    /// Exclusive upper bound (`b"\x00"` for "to the end", empty for
    /// single-key).
    pub range_end: Bytes,
    /// `prev_kv` requested?
    pub prev_kv: bool,
    /// Sender half of the event channel. Cloning is cheap (mpsc).
    pub sender: tokio::sync::mpsc::UnboundedSender<WatchEvent>,
}

pub struct Store {
    /// Monotonic revision; starts at 1.
    pub rev: AtomicRevision,
    /// Sum of `key.len() + value.len()` for all live entries.
    pub bytes_used: std::sync::atomic::AtomicU64,
    /// Main KV map.
    pub kv: RwLock<BTreeMap<Bytes, Entry>>,
    /// Insertion order; front is the next to evict.
    pub eviction: Mutex<VecDeque<EvictEntry>>,
    /// History ring buffer.
    pub history: HistoryBuffer,
    /// Lease table.
    pub leases: LeaseTable,
    /// Live Watch subscriptions.
    pub watches: Mutex<HashMap<i64, WatchHandle>>,
    /// Compacted rev (last successful `Compact` call).
    pub compacted_rev: std::sync::atomic::AtomicU64,
    /// Configured capacity for `max_cache_bytes`.
    pub max_cache_bytes: u64,
    /// Next watch id.
    next_watch_id: std::sync::atomic::AtomicI64,
}

impl Store {
    pub fn new(max_cache_bytes: u64, history_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            rev: AtomicRevision::new(1),
            bytes_used: std::sync::atomic::AtomicU64::new(0),
            kv: RwLock::new(BTreeMap::new()),
            eviction: Mutex::new(VecDeque::new()),
            history: HistoryBuffer::new(history_capacity),
            leases: LeaseTable::new(),
            watches: Mutex::new(HashMap::new()),
            compacted_rev: std::sync::atomic::AtomicU64::new(0),
            max_cache_bytes,
            next_watch_id: std::sync::atomic::AtomicI64::new(0),
        })
    }

    /// Allocate a new watch id.
    pub fn alloc_watch_id(&self) -> i64 {
        self.next_watch_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Snapshot the current revision.
    pub fn current_rev(&self) -> u64 {
        self.rev.get()
    }

    /// Reconfigure the history ring buffer's capacity. Used by SIGHUP.
    pub fn resize_history(&self, new_cap: usize) {
        self.history.resize(new_cap);
    }

    /// Put (insert or upsert) a key. Returns the new revision.
    pub fn put(
        &self,
        key: Bytes,
        value: Bytes,
        lease_id: Option<i64>,
    ) -> Result<u64, StoreError> {
        if key.is_empty() {
            return Err(StoreError::InvalidArgument("key must not be empty".into()));
        }
        let new_rev = self.rev.next();
        let new_size = (key.len() + value.len()) as u64;

        // Compute create_rev: if the key already exists we preserve the
        // original create_rev; otherwise this is the new create_rev.
        let create_rev = {
            let kv = self.kv.read();
            kv.get(&key).map(|e| e.create_rev).unwrap_or(new_rev)
        };

        // Compute the byte delta. We need the old size under the
        // write lock to avoid a TOCTOU.
        let old_size = {
            let mut kv = self.kv.write();
            let old = kv.get(&key).map(|e| e.size);
            kv.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    create_rev,
                    mod_rev: new_rev,
                    lease_id,
                    size: new_size,
                },
            );
            old.unwrap_or(0)
        };

        // Eviction queue: always push the *new* entry at the back. If
        // a previous version existed, the old eviction entry becomes
        // a no-op when popped (its rev won't match the live entry).
        {
            let mut ev = self.eviction.lock();
            ev.push_back(EvictEntry {
                key: key.clone(),
                size: new_size,
                rev: new_rev,
            });
        }

        // Update byte counter.
        let delta = (new_size as i64) - (old_size as i64);
        if delta >= 0 {
            self.bytes_used.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.bytes_used
                .fetch_sub((-delta) as u64, Ordering::Relaxed);
        }

        // Lease attachment.
        if let Some(lid) = lease_id {
            self.leases.attach(lid, key.clone());
        }

        // Record in history.
        let m = Mutation {
            rev: new_rev,
            kind: MutationKind::Put,
            key: key.clone(),
            value: value.clone(),
            create_rev,
            lease_id,
        };
        self.history.push(m.clone());

        // Evict opportunistically.
        self.evict_to_capacity();

        // Fan out to watchers.
        self.fanout(m);

        Ok(new_rev)
    }

    /// Delete a single key. Returns the deletion's revision and the
    /// number of keys removed.
    pub fn delete(&self, key: &[u8]) -> Result<(u64, i64), StoreError> {
        self.delete_inner(Bytes::copy_from_slice(key), MutationKind::Delete)
    }

    /// Delete with a specific mutation kind (used by lease expiry).
    pub fn delete_with_event(
        &self,
        key: Bytes,
        kind: MutationKind,
    ) -> Result<(u64, i64), StoreError> {
        self.delete_inner(key, kind)
    }

    fn delete_inner(
        &self,
        key: Bytes,
        kind: MutationKind,
    ) -> Result<(u64, i64), StoreError> {
        if key.is_empty() {
            return Err(StoreError::InvalidArgument("key must not be empty".into()));
        }
        let entry = {
            let mut kv = self.kv.write();
            kv.remove(&key)
        };
        let entry = match entry {
            Some(e) => e,
            None => return Ok((self.current_rev(), 0)),
        };
        self.bytes_used.fetch_sub(entry.size, Ordering::Relaxed);
        // Detach from any lease.
        let _ = self.leases.detach(&key);

        let new_rev = self.rev.next();
        let m = Mutation {
            rev: new_rev,
            kind,
            key: key.clone(),
            value: Bytes::new(),
            create_rev: entry.create_rev,
            lease_id: entry.lease_id,
        };
        self.history.push(m.clone());
        self.fanout(m);
        Ok((new_rev, 1))
    }

    /// Read a single key. Returns `None` if not present.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>, StoreError> {
        let kv = self.kv.read();
        Ok(kv.get(key).cloned())
    }

    /// Read a range `[key, range_end)`. Range is half-open. `range_end`
    /// of `b"\x00"` is treated as "to the end"; empty `range_end` is
    /// equivalent to a single key.
    ///
    /// `range_end` may also be in the "key + 1" form produced by
    /// `etcdctl --prefix` and the etcd v3 client's `WithPrefix()`:
    /// `getPrefix(key)` returns `key` with the rightmost byte that is
    /// `< 0xFF` incremented by 1, and the rest of the trailing bytes
    /// being `0xFF` in the original. Per the etcd v3 proto docstring,
    /// this form means "all keys prefixed with key" and is NOT a
    /// literal half-open BTreeMap range. We detect it and do a manual
    /// prefix scan.
    pub fn range(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
    ) -> Result<Vec<(Bytes, Entry)>, StoreError> {
        if revision != 0 && revision != self.current_rev() as i64 {
            return Err(StoreError::OutOfRange(format!(
                "min_rev={} max_rev={} requested={}",
                self.compacted_rev.load(Ordering::Relaxed),
                self.current_rev(),
                revision
            )));
        }
        let kv = self.kv.read();
        let mut out = Vec::new();
        let limit_u = if limit <= 0 { usize::MAX } else { limit as usize };
        use std::ops::Bound;
        if range_end.is_empty() {
            if let Some(e) = kv.get(key) {
                out.push((Bytes::copy_from_slice(key), e.clone()));
            }
        } else if range_end == b"\x00".as_slice() {
            // "from key onwards" - inclusive lower bound, open upper.
            for (k, e) in kv.range::<Bytes, _>((
                Bound::Included(Bytes::copy_from_slice(key)),
                Bound::Unbounded,
            )) {
                out.push((k.clone(), e.clone()));
                if out.len() >= limit_u {
                    break;
                }
            }
        } else if is_prefix_range_end(key, range_end) {
            // "key + 1" form produced by `etcdctl --prefix` /
            // `client.WithPrefix()`. Per the etcd v3 proto docstring,
            // this means "all keys prefixed with key". We do a manual
            // scan because the BTreeMap range would not include
            // keys like "/foo/a" (lexically > "/foo/0").
            for (k, e) in kv.range::<Bytes, _>((
                Bound::Included(Bytes::copy_from_slice(key)),
                Bound::Unbounded,
            )) {
                if !k.starts_with(key) {
                    break;
                }
                out.push((k.clone(), e.clone()));
                if out.len() >= limit_u {
                    break;
                }
            }
        } else {
            // Literal half-open range [key, range_end).
            for (k, e) in kv.range::<Bytes, _>((
                Bound::Included(Bytes::copy_from_slice(key)),
                Bound::Excluded(Bytes::copy_from_slice(range_end)),
            )) {
                out.push((k.clone(), e.clone()));
                if out.len() >= limit_u {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Evict from the front of the queue until
    /// `bytes_used <= max_cache_bytes`. No-op if under the limit.
    pub fn evict_to_capacity(&self) {
        let cap = self.max_cache_bytes;
        loop {
            let used = self.bytes_used.load(Ordering::Relaxed);
            if used <= cap {
                return;
            }
            let front = {
                let mut ev = self.eviction.lock();
                match ev.pop_front() {
                    Some(f) => f,
                    None => return,
                }
            };
            let removed: Option<Entry> = {
                let mut kv = self.kv.write();
                let cur = kv.get(&front.key);
                match cur {
                    Some(e) if e.mod_rev == front.rev => {
                        let e2 = e.clone();
                        kv.remove(&front.key);
                        Some(e2)
                    }
                    _ => None,
                }
            };
            if let Some(e) = removed {
                self.bytes_used.fetch_sub(e.size, Ordering::Relaxed);
                let _ = self.leases.detach(&front.key);
                let new_rev = self.rev.next();
                let m = Mutation {
                    rev: new_rev,
                    kind: MutationKind::Delete,
                    key: front.key.clone(),
                    value: Bytes::new(),
                    create_rev: e.create_rev,
                    lease_id: e.lease_id,
                };
                self.history.push(m.clone());
                self.fanout(m);
                tracing::debug!(key = ?front.key, "evicted by capacity");
            }
        }
    }

    /// Fan a mutation out to all live watchers whose range covers
    /// `m.key`. We hold the `watches` lock while sending; mpsc send
    /// is non-blocking, so this is fine.
    fn fanout(&self, m: Mutation) {
        let g = self.watches.lock();
        for (_id, w) in g.iter() {
            if !watch_matches(w, &m.key) {
                continue;
            }
            let _ = w
                .sender
                .send(WatchEvent::from_mutation(m.clone(), w.prev_kv));
        }
    }
}

/// Check whether `key` falls within the `[w.key, w.range_end)` range
/// (with the same `b"\x00"` "to the end" semantics as `range`, and the
/// same prefix-form handling).
pub fn watch_matches(w: &WatchHandle, key: &[u8]) -> bool {
    if key < w.key.as_ref() {
        return false;
    }
    if w.range_end.is_empty() {
        return key == w.key.as_ref();
    }
    if w.range_end.as_ref() == b"\x00" {
        return true;
    }
    if is_prefix_range_end(w.key.as_ref(), w.range_end.as_ref()) {
        return key.starts_with(w.key.as_ref());
    }
    key < w.range_end.as_ref()
}

/// True iff `range_end` is in the etcd "key + 1" prefix form produced
/// by `client.WithPrefix()` and `etcdctl --prefix`. The form is: take
/// `key`, find the rightmost byte that is < 0xFF, increment it, and
/// truncate. The remaining bytes of `key` past the incremented
/// position are all 0xFF.
fn is_prefix_range_end(key: &[u8], range_end: &[u8]) -> bool {
    if range_end.is_empty() || range_end.len() > key.len() {
        return false;
    }
    for (i, (&k, &r)) in key.iter().zip(range_end.iter()).enumerate() {
        if k != r {
            // The first differing byte must be exactly +1 in range_end.
            if r != k.wrapping_add(1) {
                return false;
            }
            // All bytes after the differing position in `key` must be
            // 0xFF (the "carry" bytes that couldn't be incremented).
            if !key[i + 1..].iter().all(|&b| b == 0xFF) {
                return false;
            }
            return true;
        }
    }
    // range_end is a strict prefix of key with no difference - not a
    // valid prefix range.
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_half_open_with_prefix_form() {
        // Mirrors what etcdctl sends for `get --prefix /foo/`.
        // The wire form is: key="/foo/" (5 bytes), range_end="/foo/0"
        // (also 5 bytes: last byte of key replaced by key+1, NOT
        // appended). etcd's `getPrefix` produces rangeBytes of the
        // same length as the key.
        let s = Store::new(1024, 100);
        s.put(Bytes::from_static(b"/foo/a"), Bytes::from_static(b"v-a"), None)
            .unwrap();
        s.put(Bytes::from_static(b"/foo/b"), Bytes::from_static(b"v-b"), None)
            .unwrap();
        s.put(Bytes::from_static(b"/foo/c"), Bytes::from_static(b"v-c"), None)
            .unwrap();
        // 5-byte key, 5-byte range_end (last byte '/' -> '0').
        let key: &[u8] = b"/foo/";
        let range_end: &[u8] = b"/foo0";
        assert_eq!(key.len(), 5);
        assert_eq!(range_end.len(), 5);
        let got = s.range(key, range_end, 0, 0).unwrap();
        let keys: Vec<&[u8]> = got.iter().map(|(k, _)| k.as_ref()).collect();
        assert_eq!(keys.len(), 3, "got {:?}", keys);
        assert!(keys.contains(&b"/foo/a".as_slice()));
        assert!(keys.contains(&b"/foo/b".as_slice()));
        assert!(keys.contains(&b"/foo/c".as_slice()));
    }

    #[test]
    fn range_open_end_nul() {
        let s = Store::new(1024, 100);
        s.put(Bytes::from_static(b"/foo/a"), Bytes::from_static(b"v-a"), None)
            .unwrap();
        s.put(Bytes::from_static(b"/foo/b"), Bytes::from_static(b"v-b"), None)
            .unwrap();
        let got = s.range(b"/foo/", b"\x00", 0, 0).unwrap();
        assert_eq!(got.len(), 2);
    }
}
