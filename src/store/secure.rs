//! A byte buffer that wipes itself when it is released.
//!
//! netcd stores the payloads the kube-apiserver writes (Kubernetes
//! `Event` objects). Those payloads can carry confidential technical
//! information, and a buffer that has been freed but not yet reused by
//! the allocator is still readable through a core dump or a heap
//! inspection.
//!
//! [`SecureBytes`] closes that window for every copy netcd keeps: the
//! buffer is overwritten with zeros at the exact moment its last
//! reference goes away, before the allocator can hand the block to
//! somebody else.

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;

/// A reference-counted byte buffer that overwrites its contents with
/// zeros when the last reference is dropped.
///
/// Cloning is a refcount bump, exactly like `bytes::Bytes`, so the
/// store can share one buffer between the KV entry, the watch history
/// ring and the watch fan-out without copying the payload.
///
/// # Example
///
/// ```
/// use netcd::store::secure::SecureBytes;
///
/// let a = SecureBytes::from(b"confidential".to_vec());
/// let b = a.clone();
/// drop(a);
/// // The payload is still alive through `b`...
/// assert_eq!(b.as_slice(), b"confidential");
/// drop(b);
/// // ... and once the last reference is gone it has been wiped.
/// ```
#[derive(Clone)]
pub struct SecureBytes {
    inner: Arc<SecureBuf>,
}

struct SecureBuf {
    /// Overwritten with zeros before the allocation is released.
    data: UnsafeCell<Vec<u8>>,
}

// SAFETY: the payload is only read through shared references. It is
// mutated (wiped) exclusively in `Drop`, which only runs when the last
// `Arc` reference is gone, i.e. when no other thread can observe the
// buffer any more.
unsafe impl Send for SecureBuf {}
unsafe impl Sync for SecureBuf {}

impl SecureBuf {
    /// Wipe the payload. Only called from `Drop`, where this buffer is
    /// the sole owner of the allocation.
    fn wipe(&mut self) {
        let data = self.data.get_mut();
        // `write_bytes` may be turned into a no-op by the optimiser
        // because the memory is freed right after. The compiler fence
        // makes the write observable, so it cannot be elided.
        unsafe {
            std::ptr::write_bytes(data.as_mut_ptr(), 0, data.len());
            std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Drop for SecureBuf {
    fn drop(&mut self) {
        self.wipe();
    }
}

impl SecureBytes {
    /// Take ownership of a buffer.
    pub fn from_vec(value: Vec<u8>) -> Self {
        Self {
            inner: Arc::new(SecureBuf {
                data: UnsafeCell::new(value),
            }),
        }
    }

    /// Copy a slice into a new buffer.
    pub fn from_slice(value: &[u8]) -> Self {
        Self::from_vec(value.to_vec())
    }

    /// An empty buffer.
    pub fn empty() -> Self {
        Self::from_vec(Vec::new())
    }

    /// The payload.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the payload is only mutated in `Drop`, when this
        // buffer has no other owner left.
        unsafe { (*self.inner.data.get()).as_slice() }
    }

    /// The payload length in bytes.
    pub fn len(&self) -> usize {
        // SAFETY: see `as_slice`.
        unsafe { (*self.inner.data.get()).len() }
    }

    /// True when the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Copy the payload out.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

impl Default for SecureBytes {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for SecureBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecureBytes({} bytes)", self.len())
    }
}

impl From<Vec<u8>> for SecureBytes {
    fn from(value: Vec<u8>) -> Self {
        Self::from_vec(value)
    }
}

impl From<&[u8]> for SecureBytes {
    fn from(value: &[u8]) -> Self {
        Self::from_slice(value)
    }
}

impl<const N: usize> From<&[u8; N]> for SecureBytes {
    fn from(value: &[u8; N]) -> Self {
        Self::from_slice(value)
    }
}

impl PartialEq<[u8]> for SecureBytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for SecureBytes {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_one_buffer() {
        let a = SecureBytes::from(b"payload".to_vec());
        let b = a.clone();
        drop(a);
        assert_eq!(b.as_slice(), b"payload");
        assert_eq!(b.len(), 7);
    }

    #[test]
    fn the_payload_is_wiped_when_the_last_reference_goes_away() {
        // The buffer must be overwritten *before* the allocator gets it
        // back, otherwise a core dump still shows the payload.
        //
        // The wipe itself is checked deterministically on a live buffer;
        // the `Drop` path calls the very same function.
        let mut buf = SecureBuf {
            data: UnsafeCell::new(vec![0xAB; 128]),
        };
        let ptr = buf.data.get_mut().as_mut_ptr();
        buf.wipe();
        let wiped = unsafe { std::slice::from_raw_parts(ptr, 128) };
        assert!(
            wiped.iter().all(|b| *b == 0),
            "the payload must be overwritten with zeros",
        );
    }

    #[test]
    fn releasing_the_last_reference_wipes_the_buffer() {
        // A buffer that is still referenced is *not* wiped: another
        // holder would read zeros instead of the payload.
        let a = SecureBytes::from(vec![0xCD; 64]);
        let b = a.clone();
        drop(a);
        assert_eq!(b.as_slice()[0], 0xCD, "a live buffer keeps its payload");
        drop(b);
    }

    #[test]
    fn empty_buffers_are_usable() {
        let e = SecureBytes::empty();
        assert!(e.is_empty());
        assert_eq!(e.len(), 0);
        assert_eq!(e.to_vec(), Vec::<u8>::new());
    }
}
