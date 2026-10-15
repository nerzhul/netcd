//! Bounded ring buffer of recent mutations.
//!
//! Used **only** to bootstrap late watchers (`Watch` with
//! `start_revision`). It is not a public Compact MVCC store; `Compact`
//! is accepted as a no-op (other than recording `compacted_rev`).
//!
//! Concurrency: a single `parking_lot::Mutex` covers the deque; reads
//! are cheap (just a slice), and mutations only happen on the request
//! hot path, where contention is naturally limited by the request rate.

use std::collections::VecDeque;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::store::MutationKind;

/// One mutation as recorded in the ring buffer.
#[derive(Debug, Clone)]
pub struct Mutation {
    pub rev: u64,
    pub kind: MutationKind,
    pub key: Bytes,
    /// Value for `Put`, empty for `Delete`.
    pub value: Bytes,
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
    pub fn push(&self, m: Mutation) {
        let mut g = self.inner.lock();
        if g.deque.len() == g.capacity {
            g.deque.pop_front();
        }
        g.deque.push_back(m);
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

    #[test]
    fn ring_drops_oldest() {
        let h = HistoryBuffer::new(2);
        for i in 1..=5u64 {
            h.push(Mutation {
                rev: i,
                kind: MutationKind::Put,
                key: Bytes::from_static(b"k"),
                value: Bytes::new(),
                create_rev: i,
                lease_id: None,
            });
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
            h.push(Mutation {
                rev: i,
                kind: MutationKind::Put,
                key: Bytes::from_static(b"k"),
                value: Bytes::new(),
                create_rev: i,
                lease_id: None,
            });
        }
        h.resize(2);
        let snap = h.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].rev, 4);
    }
}
