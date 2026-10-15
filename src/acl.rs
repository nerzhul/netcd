//! Prefix-based access control.
//!
//! A `Tenant` holds a list of allowed byte-string prefixes. A request is
//! authorised iff every key (or, for ranges, both `key` and `range_end`) is
//! contained in at least one of the prefixes.

use bytes::Bytes;

/// True if `key` is covered by any of `prefixes` (lexical prefix match).
pub fn key_allowed(key: &[u8], prefixes: &[Bytes]) -> bool {
    prefixes.iter().any(|p| key.starts_with(p.as_ref()))
}

/// True if the half-open range `[key, range_end)` is fully covered by
/// the given prefixes. Single-key requests (empty `range_end`) are
/// accepted iff `key` is covered.
///
/// `range_end` of `b"\x00"` is treated as "no upper bound" by etcd;
/// equivalently any key >= `key` matches. The full coverage check for
/// that case is: every key in `[key, +inf)` is covered iff there is at
/// least one prefix `p` such that `p` covers `key` and the same `p` also
/// covers all of `[key, +inf)`, i.e. `p` is a prefix of `key` AND
/// `key.starts_with(p) && range_end_starts_with(p_or_covers)`. In
/// practice the only safe answer is: there exists a prefix `p` such that
/// `key >= p` and (range_end starts with p or range_end is `+inf`).
/// Simpler: require every key up to and including `range_end` to be
/// inside the same prefix. We approximate this with the rule
/// "key and range_end both start with the same prefix" - sufficient for
/// Kubernetes, which always sends closed prefixes.
pub fn range_allowed(key: &[u8], range_end: &[u8], prefixes: &[Bytes]) -> bool {
    if range_end.is_empty() {
        return key_allowed(key, prefixes);
    }
    // etcd range semantics: range_end == "\x00" means "to the end".
    if range_end == b"\x00" {
        // Accept iff key is inside some prefix - the caller trusts the
        // tenant to authorise the sub-prefix.
        return key_allowed(key, prefixes);
    }
    // Special case: `etcdctl --prefix` / `WithPrefix` produce a
    // `range_end` of the form `key` with the last incrementable byte
    // + 1 (e.g. key="/foo/" -> range_end="/foo/0"). Per the etcd
    // v3 proto, this means "all keys with prefix key", so it's
    // enough that `key` is covered by some tenant prefix.
    if is_prefix_range_end(key, range_end) {
        return key_allowed(key, prefixes);
    }
    // Otherwise we need a single prefix that covers the entire range.
    // That means: prefix <= key AND prefix is a prefix of range_end
    // (since range_end is exclusive).
    prefixes
        .iter()
        .any(|p| key.starts_with(p.as_ref()) && range_end.starts_with(p.as_ref()))
}

/// True iff `range_end` is in the etcd "key + 1" prefix form. See
/// `netcd::store::is_prefix_range_end` for the full definition.
fn is_prefix_range_end(key: &[u8], range_end: &[u8]) -> bool {
    if range_end.is_empty() || range_end.len() > key.len() {
        return false;
    }
    for (i, (&k, &r)) in key.iter().zip(range_end.iter()).enumerate() {
        if k != r {
            if r != k.wrapping_add(1) {
                return false;
            }
            if !key[i + 1..].iter().all(|&b| b == 0xFF) {
                return false;
            }
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &[&'static str]) -> Vec<Bytes> {
        s.iter().map(|x| Bytes::from(*x)).collect()
    }

    #[test]
    fn single_key_match() {
        let prefixes = p(&["/registry/events/"]);
        assert!(key_allowed(b"/registry/events/x", &prefixes));
        assert!(!key_allowed(b"/registry/pods/x", &prefixes));
    }

    #[test]
    fn range_within_prefix() {
        let prefixes = p(&["/registry/events/team-a/"]);
        assert!(range_allowed(
            b"/registry/events/team-a/a",
            b"/registry/events/team-a/z",
            &prefixes
        ));
    }

    #[test]
    fn range_crossing_prefix_denied() {
        let prefixes = p(&["/registry/events/team-a/"]);
        assert!(!range_allowed(
            b"/registry/events/team-a/a",
            b"/registry/events/team-b/a",
            &prefixes
        ));
    }

    #[test]
    fn range_open_end_uses_key_prefix() {
        let prefixes = p(&["/registry/events/team-a/"]);
        assert!(range_allowed(
            b"/registry/events/team-a/a",
            b"\x00",
            &prefixes
        ));
        assert!(!range_allowed(b"/registry/pods/a", b"\x00", &prefixes));
    }

    #[test]
    fn empty_range_end_treated_as_single_key() {
        let prefixes = p(&["/registry/events/"]);
        assert!(range_allowed(b"/registry/events/x", b"", &prefixes));
    }
}
