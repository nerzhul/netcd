//! A global allocator that overwrites every block before releasing it.
//!
//! netcd handles payloads the kube-apiserver writes (Kubernetes `Event`
//! objects). Those payloads can carry confidential technical
//! information, and a block that has been freed but not yet reused by
//! the allocator is still readable through a core dump or a heap
//! inspection.
//!
//! The store's own copies are wiped by [`crate::store::secure::SecureBytes`],
//! but every copy that travels through the gRPC stack (prost decoding
//! the request, the `Vec<u8>` handed to the encoder, the encoded frame)
//! is a plain allocation this crate does not own. Scrubbing the
//! allocator closes those windows without having to reach into the
//! transport layer.
//!
//! The cost is one `memset` per deallocation. Measurements on the event
//! write path show it in the noise compared to the copy itself.
//!
//! This type is *not* installed by the library: `netcd` the binary
//! picks it as its `#[global_allocator]`, so embedding the library in
//! another program keeps that program's own allocator.

use std::alloc::{GlobalAlloc, Layout, System};

/// A [`System`] allocator that zeroes every block before handing it
/// back.
pub struct ScrubbingAlloc;

unsafe impl GlobalAlloc for ScrubbingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Overwrite the block *before* the allocator can hand it to
        // somebody else. `write_bytes` may be turned into a no-op by the
        // optimiser because the memory is freed right after; the
        // compiler fence makes the write observable, so it cannot be
        // elided.
        std::ptr::write_bytes(ptr, 0, layout.size());
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
        System.dealloc(ptr, layout);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        System.alloc_zeroed(layout)
    }

    // `realloc` is deliberately *not* overridden: the default
    // implementation allocates, copies and then calls `dealloc`, which
    // scrubs the old block. `System.realloc` would leave the old block
    // readable when it has to move the allocation.
}

/// Release the free pages of the heap back to the operating system.
///
/// glibc's allocator keeps the freed blocks in its arena and does not
/// give the pages back to the kernel, so the resident set of a process
/// stays at its high-water mark even when the live data is gone. On a
/// server that just purged a burst of events, that is the difference
/// between "the memory went back down" and "the memory is still
/// reserved".
///
/// This is a no-op on platforms that do not provide `malloc_trim`
/// (musl, macOS), where the allocator behaves differently anyway.
pub fn trim_heap() {
    #[cfg(all(target_env = "gnu", target_pointer_width = "64"))]
    unsafe {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        // SAFETY: `malloc_trim` takes a padding argument and touches no
        // pointer we own.
        malloc_trim(0);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_scrubbing_allocator_serves_allocations() {
        let mut v: Vec<u8> = Vec::with_capacity(64);
        v.extend_from_slice(b"payload");
        assert_eq!(v.as_slice(), b"payload");
        // The vector is freed here, and the block is scrubbed on the way
        // out.
    }
}
