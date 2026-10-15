//! Monotonic revision counter.
//!
//! Every successful mutation increments the revision by exactly one and
//! the new value is returned to the caller. The initial revision is 1
//! (matching etcd semantics, where rev 0 is reserved for "no events").
//!
//! Lock-free via `AtomicU64`.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct AtomicRevision {
    v: AtomicU64,
}

impl AtomicRevision {
    pub fn new(start: u64) -> Self {
        Self {
            v: AtomicU64::new(start),
        }
    }

    /// Current revision (last issued).
    pub fn get(&self) -> u64 {
        self.v.load(Ordering::Relaxed)
    }

    /// Allocate the next revision and return it. The first call after
    /// `new(1)` returns 2, etc. `next` is reserved for the very first
    /// event so callers always observe `rev >= 1`.
    pub fn next(&self) -> u64 {
        self.v.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Set the revision (used at startup to honour a pre-existing
    /// `compact_rev`).
    pub fn set(&self, v: u64) {
        self.v.store(v, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic() {
        let r = AtomicRevision::new(1);
        assert_eq!(r.get(), 1);
        assert_eq!(r.next(), 2);
        assert_eq!(r.next(), 3);
        assert_eq!(r.get(), 3);
    }
}
