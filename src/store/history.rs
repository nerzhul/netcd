//! Bounded ring buffer of recent mutations.
//!
//! Used **only** to bootstrap late watchers (`Watch` with
//! `start_revision`). It is not a public Compact MVCC store; `Compact`
//! is accepted as a no-op (other than recording `compacted_rev`).
//!
//! Concurrency: a single `parking_lot::Mutex` covers the deque; reads
//! are cheap (just a slice), and mutations only happen on the request
//! hot path, where contention is naturally limited by the request rate.
//!
//! ## Payloads
//!
//! The ring keeps the payload of the mutations it holds, because a late
//! watcher needs the value of the events it replays. When the key a
//! payload belongs to is deleted, the payload must not stay readable in
//! the process' memory: [`HistoryBuffer::release_dead_payloads`] drops
//! it.
//!
//! The release is a sweep rather than an index: maintaining an index
//! keyed by the object key would cost a hash lookup and a map insert on
//! *every* write, which is the path that has to stay fast when an
//! application writes a lot of events. The sweep runs once per second,
//! from the background task.

use std::collections::VecDeque;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::store::secure::SecureBytes;
use crate::store::MutationKind;

/// One mutation as recorded in the ring buffer.
#[derive(Debug, Clone)]
pub struct Mutation {
    pub rev: u64,
    pub kind: MutationKind,
    pub key: Bytes,
    /// Value for `Put`, empty for `Delete`.
    ///
    /// `None` once the payload has been released, which happens when the
    /// key it belongs to is deleted: the payload must not stay readable
    /// after the key is gone.
    pub value: Option<SecureBytes>,
    /// `create_rev` for `Put`, ignored for `Delete`.
    pub create_rev: u64,
    /// Lease the key is bound to, if any.
    pub lease_id: Option<i64>,
}

pub struct HistoryBuffer {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Mutations in chronological order; oldest at index 0.
    deque: VecDeque<Mutation>,
    capacity: usize,
}

impl HistoryBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                deque: VecDeque::with_capacity(capacity),
                capacity,
            }),
        }
    }

    /// Reconfigure the buffer's capacity. If the new capacity is
    /// smaller, oldest entries are dropped.
    pub fn resize(&self, new_cap: usize) {
        let mut g = self.inner.lock();
        g.capacity = new_cap;
        while g.deque.len() > new_cap {
            g.deque.pop_front();
        }
    }

    /// Append a mutation. Drops the oldest if the buffer is full.
    pub fn push(&self, m: Mutation) -> Option<u64> {
        let mut g = self.inner.lock();
        let dropped = if g.deque.len() == g.capacity {
            g.deque.pop_front().map(|m| m.rev)
        } else {
            None
        };
        g.deque.push_back(m);
        dropped
    }

    /// Drop the payloads of the mutations whose key is no longer alive.
    ///
    /// `is_live` tells whether a key is still in the store. Dropping the
    /// payload releases the last reference to the buffer, which wipes it.
    ///
    /// Cost: one pass over the ring, with a lookup per entry. The store
    /// calls it from its background task, once per second, so it never
    /// shows up on the write path.
    pub fn release_dead_payloads(&self, is_live: impl Fn(&[u8]) -> bool) {
        let mut g = self.inner.lock();
        for m in g.deque.iter_mut() {
            if m.value.is_some() && !is_live(&m.key) {
                m.value = None;
            }
        }
    }

    /// Snapshot the current buffer. The result is a `Vec<Mutation>`
    /// ordered oldest -> newest.
    pub fn snapshot(&self) -> Vec<Mutation> {
        self.inner.lock().deque.iter().cloned().collect()
    }

    /// The oldest rev still in the buffer (0 if empty).
    pub fn oldest_rev(&self) -> u64 {
        self.inner.lock().deque.front().map(|m| m.rev).unwrap_or(0)
    }

    /// The newest rev in the buffer (0 if empty).
    pub fn newest_rev(&self) -> u64 {
        self.inner.lock().deque.back().map(|m| m.rev).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(rev: u64, key: &str) -> Mutation {
        Mutation {
            rev,
            kind: MutationKind::Put,
            key: Bytes::from(key.as_bytes().to_vec()),
            value: Some(SecureBytes::from(b"payload".to_vec())),
            create_rev: rev,
            lease_id: None,
        }
    }

    #[test]
    fn ring_drops_oldest() {
        let h = HistoryBuffer::new(2);
        for i in 1..=5u64 {
            h.push(put(i, "k"));
        }
        let snap = h.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].rev, 4);
        assert_eq!(snap[1].rev, 5);
        assert_eq!(h.oldest_rev(), 4);
        assert_eq!(h.newest_rev(), 5);
    }

    #[test]
    fn resize_shrinks() {
        let h = HistoryBuffer::new(10);
        for i in 1..=5u64 {
            h.push(put(i, "k"));
        }
        h.resize(2);
        let snap = h.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].rev, 4);
    }

    #[test]
    fn push_returns_dropped_rev() {
        // The `Store` uses this return value to bump `compacted_rev`
        // so watchers can detect a revision they cached is no longer
        // reachable.
        let h = HistoryBuffer::new(2);
        assert_eq!(h.push(put(1, "k")), None, "no drop on first push");
        assert_eq!(h.push(put(2, "k")), None, "no drop when the buffer fills");
        assert_eq!(h.push(put(3, "k")), Some(1), "drop returns the oldest rev");
        assert_eq!(h.push(put(4, "k")), Some(2));
    }

    #[test]
    fn the_sweep_drops_the_payload_of_a_dead_key() {
        // The payload of a deleted key must not stay readable in the
        // ring.
        let h = HistoryBuffer::new(10);
        h.push(put(1, "/a"));
        h.push(put(2, "/b"));
        h.push(put(3, "/a"));

        assert!(
            h.snapshot()[0].value.is_some(),
            "the payload is there while the key is alive",
        );

        // "/a" is gone, "/b" is still there.
        h.release_dead_payloads(|key| key == b"/b");

        let snap = h.snapshot();
        assert_eq!(snap.len(), 3, "the revisions are kept");
        assert!(
            snap[0].value.is_none() && snap[2].value.is_none(),
            "the payloads of the dead key are gone: {:?}",
            snap.iter().map(|m| &m.value).collect::<Vec<_>>(),
        );
        assert!(
            snap[1].value.is_some(),
            "the payload of the live key is untouched",
        );
    }

    #[test]
    fn the_sweep_frees_the_payload_buffer() {
        // The payload is shared with the store's KV entry. Releasing it
        // from the ring must drop the last reference so the buffer is
        // wiped.
        let payload = SecureBytes::from(b"confidential".to_vec());
        let h = HistoryBuffer::new(10);
        h.push(Mutation {
            rev: 1,
            kind: MutationKind::Put,
            key: Bytes::from_static(b"/a"),
            value: Some(payload.clone()),
            create_rev: 1,
            lease_id: None,
        });
        assert_eq!(
            h.snapshot()[0].value.as_ref().unwrap().as_slice(),
            b"confidential"
        );
        drop(payload);
        h.release_dead_payloads(|_| false);
        assert!(h.snapshot()[0].value.is_none());
    }

    #[test]
    fn a_released_key_does_not_leak_into_the_next_mutation() {
        // After a release, a new mutation for the same key must start
        // with a fresh payload.
        let h = HistoryBuffer::new(10);
        h.push(put(1, "/a"));
        h.release_dead_payloads(|_| false);
        h.push(put(2, "/a"));
        let snap = h.snapshot();
        assert!(snap[0].value.is_none());
        assert!(
            snap[1].value.is_some(),
            "the new mutation must carry its own payload",
        );
    }
}
