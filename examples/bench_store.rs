//! Store benchmark: the write path a kube-apiserver drives when it
//! writes Kubernetes `Event` objects.
//!
//! Usage:
//!   cargo run --release --example bench_store -- [events] [value_bytes]
//!
//! It writes `events` payloads of `value_bytes` bytes under a lease,
//! then expires the lease the way the background reaper does, and
//! reports the throughput and what stays resident.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use netcd::store::secure::SecureBytes;
use netcd::store::{MutationKind, Store};

#[cfg(feature = "scrub-alloc")]
#[global_allocator]
static GLOBAL_ALLOC: netcd::alloc::ScrubbingAlloc = netcd::alloc::ScrubbingAlloc;

fn rss_kib() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    pages * 4
}

fn main() {
    let n: u32 = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let value_size: usize = std::env::args()
        .nth(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);

    let s = Store::new(256 * 1024 * 1024, 1000);
    let lease = s.leases.grant(Duration::from_secs(3600), None);
    let payload = vec![b'x'; value_size];

    let rss_start = rss_kib();

    // --- write path ---
    let t = Instant::now();
    for i in 0..n {
        s.put(
            Bytes::from(format!("/registry/events/default/evt-{i:08}")),
            SecureBytes::from(payload.clone()),
            Some(lease),
        )
        .unwrap();
    }
    let write = t.elapsed();
    println!(
        "write : {n} events of {value_size} B in {write:?} -> {:.0} events/s",
        n as f64 / write.as_secs_f64()
    );

    let rss_peak = rss_kib();
    println!(
        "RSS   : {rss_start} KiB -> {rss_peak} KiB (live: {} keys, {} bytes)",
        s.kv.read().len(),
        s.bytes_used.load(Ordering::Relaxed)
    );
    println!(
        "queue : eviction={} history={}",
        s.eviction.lock().len(),
        s.history.snapshot().len()
    );

    // --- reap path: the lease expires ---
    let t = Instant::now();
    let keys = s.leases.revoke(lease, None).unwrap();
    for k in keys {
        s.delete_with_event(k, MutationKind::DeleteLease).unwrap();
    }
    let reap = t.elapsed();
    println!(
        "reap  : {n} deletions in {reap:?} -> {:.0} deletes/s",
        n as f64 / reap.as_secs_f64()
    );

    // --- what stays resident ---
    let rss_after = rss_kib();
    println!(
        "RSS   : {rss_after} KiB after the reap (live: {} keys, {} bytes)",
        s.kv.read().len(),
        s.bytes_used.load(Ordering::Relaxed)
    );
    println!(
        "queue : eviction={} history={}",
        s.eviction.lock().len(),
        s.history.snapshot().len()
    );

    s.compact_eviction_queue();
    println!(
        "queue : eviction={} after compaction",
        s.eviction.lock().len()
    );
    println!("RSS   : {} KiB after compaction", rss_kib());
    netcd::alloc::trim_heap();
    println!("RSS   : {} KiB after malloc_trim", rss_kib());
}
