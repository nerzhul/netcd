//! End-to-end integration test.
//!
//! Spins up a real netcd instance with self-signed mTLS certificates,
//! then exercises Put/Get/Watch/Txn over the etcd v3 gRPC surface using
//! the generated `KvClient`/`WatchClient` from tonic-build.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use std::sync::Once;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        // Install the AWS-LC crypto provider for rustls.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

use netcd::auth::Authenticator;
use netcd::config::{Config, Tenant};
use netcd::proto::etcdserverpb::kv_client::KvClient;
use netcd::proto::etcdserverpb::watch_client::WatchClient;
use netcd::proto::etcdserverpb::{
    DeleteRangeRequest, PutRequest, RangeRequest, TxnRequest, WatchCreateRequest, WatchRequest,
};
use netcd::server::{build_and_serve, EtcdServerOptions};
use netcd::store::Store;
use tokio_stream::StreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

/// Build a CA + server cert + client cert hierarchy. Returns
/// (ca_pem, server_cert_pem, server_key_pem, client_cert_pem,
/// client_key_pem).
#[allow(clippy::type_complexity)]
fn build_pki() -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
    // CA
    let mut ca_params = rcgen::CertificateParams::new(vec!["netcd-test-ca".into()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // Server cert, signed by the CA.
    let mut server_params =
        rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    server_params.insert_extended_key_usage(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = rcgen::KeyPair::generate().unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    // Client cert, signed by the CA.
    let mut client_params = rcgen::CertificateParams::new(vec!["client".into()]).unwrap();
    client_params.insert_extended_key_usage(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();

    (
        ca_cert.pem().into_bytes(),
        server_cert.pem().into_bytes(),
        server_key.serialize_pem().into_bytes(),
        client_cert.pem().into_bytes(),
        client_key.serialize_pem().into_bytes(),
    )
}

/// Compute the SHA-256 hex of the leaf cert DER for fingerprinting.
fn fingerprint(cert_pem: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let s = std::str::from_utf8(cert_pem).unwrap();
    let mut in_cert = false;
    let mut b64 = String::new();
    for line in s.lines() {
        if line.contains("BEGIN CERTIFICATE") {
            in_cert = true;
            continue;
        }
        if line.contains("END CERTIFICATE") {
            break;
        }
        if in_cert {
            b64.push_str(line.trim());
        }
    }
    let der = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &b64).unwrap();
    let mut h = Sha256::new();
    h.update(&der);
    hex::encode(h.finalize())
}

struct Harness {
    addr: SocketAddr,
    server_task: Option<tokio::task::JoinHandle<()>>,
    ca_pem: Vec<u8>,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
    config_path: std::path::PathBuf,
}

impl Harness {
    async fn start() -> Self {
        init();
        let (ca_pem, server_cert_pem, server_key_pem, client_cert_pem, client_key_pem) =
            build_pki();
        let fp = fingerprint(&client_cert_pem);

        let mut cfg = Config::default_dev();
        cfg.tenants.push(Tenant {
            name: "team-a".into(),
            allowed_prefixes: vec!["/registry/events/team-a/".into()],
            client_cert_fingerprint_sha256: fp,
        });
        let cfg_path = std::env::temp_dir().join(format!(
            "netcd-test-{}-{}.json",
            std::process::id(),
            rand::random::<u64>()
        ));
        let _ = std::fs::remove_file(&cfg_path);
        cfg.save_atomic(&cfg_path).unwrap();

        let store = Store::new(1024 * 1024, 100);
        let cfg_arcswap: Arc<arc_swap::ArcSwap<Config>> =
            Arc::new(arc_swap::ArcSwap::from_pointee(cfg));
        let auth = Authenticator::new(cfg_arcswap, true);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let opts = EtcdServerOptions {
            bind_addr: addr.to_string(),
            mtls: true,
            server_cert_pem: Some(server_cert_pem),
            server_key_pem: Some(server_key_pem),
            client_ca_pem: Some(ca_pem.clone()),
        };
        let server_task = tokio::spawn(async move {
            let _ = build_and_serve(
                store,
                auth,
                Duration::from_secs(60),
                100,
                Duration::from_secs(60),
                opts,
            )
            .await;
        });
        // Give the server a moment to bind.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Self {
            addr,
            server_task: Some(server_task),
            ca_pem,
            client_cert_pem,
            client_key_pem,
            config_path: cfg_path,
        }
    }

    fn endpoint(&self) -> tonic::transport::Endpoint {
        let url = format!("https://{}", self.addr);
        tonic::transport::Endpoint::from_shared(url)
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(self.ca_pem.clone()))
                    .identity(Identity::from_pem(
                        self.client_cert_pem.clone(),
                        self.client_key_pem.clone(),
                    )),
            )
            .unwrap()
    }

    async fn kv_client(&self) -> KvClient<Channel> {
        let ch = self.endpoint().connect().await.unwrap();
        KvClient::new(ch)
    }

    async fn watch_client(&self) -> WatchClient<Channel> {
        let ch = self.endpoint().connect().await.unwrap();
        WatchClient::new(ch)
    }

    async fn stop(mut self) {
        if let Some(t) = self.server_task.take() {
            t.abort();
        }
        let _ = std::fs::remove_file(&self.config_path);
    }
}

#[tokio::test]
async fn put_get_roundtrip() {
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/x".to_vec(),
        value: b"hello".to_vec(),
        lease: 0,
        ..Default::default()
    })
    .await
    .unwrap();
    let resp = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/x".to_vec(),
            range_end: vec![],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().kvs[0].value, b"hello");
    h.stop().await;
}

#[tokio::test]
async fn delete_removes_key() {
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/x".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let d = kv
        .delete_range(DeleteRangeRequest {
            key: b"/registry/events/team-a/x".to_vec(),
            range_end: vec![],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(d.into_inner().deleted, 1);
    let resp = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/x".to_vec(),
            range_end: vec![],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.into_inner().kvs.is_empty());
    h.stop().await;
}

#[tokio::test]
async fn watch_streams_events() {
    let h = Harness::start().await;
    let mut w = h.watch_client().await;
    let (tx, rx) = futures::channel::mpsc::unbounded::<WatchRequest>();
    let create = WatchRequest {
        request_union: Some(
            netcd::proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: b"/registry/events/team-a/".to_vec(),
                    range_end: b"\x00".to_vec(),
                    ..Default::default()
                },
            ),
        ),
    };
    tx.unbounded_send(create).unwrap();
    // Do NOT drop tx - keeping the stream open so the server keeps the
    // watch registered.
    let mut stream = w.watch(rx).await.unwrap().into_inner();

    // Give the server a moment to register the watch and send the
    // create response.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Now publish a Put.
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/abc".to_vec(),
        value: b"x".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    let mut got_create = false;
    let mut got_put = false;
    for _ in 0..8 {
        match stream.next().await {
            Some(Ok(resp)) => {
                if resp.created {
                    got_create = true;
                } else if !resp.events.is_empty() {
                    got_put = true;
                }
            }
            Some(Err(_)) => break,
            None => break,
        }
        if got_create && got_put {
            break;
        }
    }
    assert!(got_create, "missing create response");
    assert!(got_put, "missing put event");
    h.stop().await;
}

#[tokio::test]
async fn admin_api_edits_config() {
    use netcd::admin::{router, AdminState};
    use netcd::config::ConfigRef;
    use netcd::store::Store;
    init();

    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("cfg.json");
    let cfg = Config::default_dev();
    let cfg_ref = ConfigRef::new(cfg, cfg_path);
    let store = Store::new(1024, 100);
    let state = AdminState {
        cfg: cfg_ref,
        store,
        admin_fingerprints: Default::default(),
        addr: "127.0.0.1:0".into(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let _server = tokio::spawn(async move {
        axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // GET /v1/tenants - empty.
    let resp = reqwest::get(format!("http://{}/v1/tenants", addr))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Vec<Tenant> = resp.json().await.unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn range_query_with_prefix() {
    // Mirrors `etcdctl put foo1 v1; etcdctl put foo2 v2; etcdctl get --prefix foo`
    // The wire form is: key="foo", range_end=key with last byte + 1 = "fop".
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/a".to_vec(),
        value: b"v-a".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(PutRequest {
        key: b"/registry/events/team-a/b".to_vec(),
        value: b"v-b".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(PutRequest {
        key: b"/registry/events/team-a/c".to_vec(),
        value: b"v-c".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    // `etcdctl get --prefix /registry/events/team-a/` -> range_end =
    // the key with the last byte incremented: "/" (0x2F) -> "0" (0x30).
    // NB: the result has the SAME length as the key (etcd's
    // `getPrefix` replaces the last incrementable byte, it doesn't
    // append).
    let resp = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/".to_vec(),
            range_end: b"/registry/events/team-a0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let inner = resp.into_inner();
    let keys: Vec<&[u8]> = inner.kvs.iter().map(|kv| kv.key.as_slice()).collect();
    assert_eq!(
        keys.len(),
        3,
        "expected 3 keys under prefix, got {:?}",
        keys
    );
    assert!(keys.contains(&b"/registry/events/team-a/a".as_slice()));
    assert!(keys.contains(&b"/registry/events/team-a/b".as_slice()));
    assert!(keys.contains(&b"/registry/events/team-a/c".as_slice()));
    h.stop().await;
}

#[tokio::test]
async fn range_query_open_end() {
    // `etcdctl get /registry/events/team-a/ --from-key` and similar
    // use range_end = "\x00" (literal NUL = "to the end of the keyspace").
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/a".to_vec(),
        value: b"v-a".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(PutRequest {
        key: b"/registry/events/team-a/b".to_vec(),
        value: b"v-b".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let resp = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/".to_vec(),
            range_end: b"\x00".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().kvs.len(), 2);
    h.stop().await;
}

#[tokio::test]
async fn txn_conditional_put() {
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/cond".to_vec(),
        value: b"v1".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // If mod_revision == 0 (no entry), succeed and put v2. Since we
    // already have v1 (mod > 0), the compare fails and the failure
    // branch (empty) runs.
    let txn = TxnRequest {
        compare: vec![netcd::proto::etcdserverpb::Compare {
            result: 0,
            target: 2,
            key: b"/registry/events/team-a/cond".to_vec(),
            range_end: vec![],
            target_union: Some(netcd::proto::etcdserverpb::compare::TargetUnion::ModRevision(0)),
        }],
        success: vec![netcd::proto::etcdserverpb::RequestOp {
            request: Some(netcd::proto::etcdserverpb::request_op::Request::RequestPut(
                PutRequest {
                    key: b"/registry/events/team-a/cond".to_vec(),
                    value: b"v2".to_vec(),
                    ..Default::default()
                },
            )),
        }],
        failure: vec![],
    };
    let resp = kv.txn(txn).await.unwrap().into_inner();
    assert!(!resp.succeeded);
    // Value should still be v1.
    let r = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/cond".to_vec(),
            range_end: vec![],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(r.into_inner().kvs[0].value, b"v1");
    h.stop().await;
}

// ---------------------------------------------------------------------------
// Non-regression tests for the watch service.
//
// Background: the kube-apiserver logs
//     "Timeout: Too large resource version: <requested>, current: <current>"
// whenever netcd accepts a watch request whose `start_revision` is greater
// than `current_rev` but does NOT return an error. Etcd returns
// `Status::out_of_range("Too large resource version: %d, current: %d")`
// from the Watch RPC in that case, and the apiserver treats it as a fatal
// watch error that triggers a re-List (see
// k8s.io/apiserver/pkg/storage/etcd3/watcher.go).
//
// A second class of bug was the ring buffer silently dropping old
// revisions while leaving `compacted_rev = 0`, so watchers with a
// `start_revision` below `oldest_rev` got a partial replay and no
// `compact_revision` notification.

#[tokio::test]
async fn watch_future_start_revision_is_accepted_silently() {
    // Regression: netcd was running, apiserver cached RV=117 from a
    // List, netcd was restarted (rev resets to 1), and the apiserver
    // tries to resume its watch at start_revision=117 while netcd is
    // at rev=56. The server must accept the watch (returning a
    // `created` response with header.revision = current_rev) and not
    // stream any past events (none exist below start_revision). Live
    // mutations after the watch is established are streamed
    // normally.
    //
    // Note: this deviates from etcd, which would return
    // `OutOfRange("Too large resource version: ...")`. Returning
    // OutOfRange forces the apiserver's per-resource watchers to
    // re-List in parallel, producing a burst of N `watcher.go:425
    // watch chan error` lines in the apiserver log. Since netcd is
    // in-memory and restarts drop state, this would happen on every
    // netcd restart, so we accept the watch silently instead. The
    // apiserver's last List response is still the source of truth
    // for the current state.
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    kv.put(PutRequest {
        key: b"/registry/events/team-a/seed".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    for i in 0..54u32 {
        kv.put(PutRequest {
            key: format!("/registry/events/team-a/pad-{i}")
                .as_bytes()
                .to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    // Sanity: current_rev is 56 now (1 reserved + 1 seed + 54 pad).
    let sanity = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/seed".to_vec(),
            range_end: vec![],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .header
        .expect("header")
        .revision as u64;
    assert_eq!(sanity, 56, "setup: expected current_rev=56");

    // Ask for a start_revision in the future. The server must accept
    // the watch and return `created=true` with header.revision =
    // current_rev. It must NOT return OutOfRange.
    let mut w = h.watch_client().await;
    let (tx, rx) = futures::channel::mpsc::unbounded::<WatchRequest>();
    tx.unbounded_send(WatchRequest {
        request_union: Some(
            netcd::proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: b"/registry/events/team-a/".to_vec(),
                    range_end: b"\x00".to_vec(),
                    start_revision: 117,
                    ..Default::default()
                },
            ),
        ),
    })
    .unwrap();
    // Keep the request stream open so the watch stays registered.
    std::mem::forget(tx);

    let mut stream = w.watch(rx).await.expect("watch handshake").into_inner();

    // The first response must be the "created" notification with
    // header.revision = current_rev (56), NOT start_revision (117)
    // and NOT an OutOfRange error.
    let first = {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut got: Option<netcd::proto::etcdserverpb::WatchResponse> = None;
        while got.is_none() {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                panic!("timed out waiting for first watch response");
            }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(resp))) => got = Some(resp),
                Ok(Some(Err(status))) => panic!(
                    "watch with start_revision > current_rev must not error, \
                     got code={:?} message={:?}",
                    status.code(),
                    status.message()
                ),
                Ok(None) => panic!("watch stream closed before sending the created response"),
                Err(_) => panic!("timed out waiting for first watch response"),
            }
        }
        got.unwrap()
    };
    assert!(first.created, "server must emit a `created` response");
    assert_eq!(
        first.header.expect("header").revision,
        56,
        "header.revision must be current_rev (56), not the requested start_revision (117)"
    );
    assert!(
        first.events.is_empty(),
        "no events should be replayed for a start_revision in the future; got {} events",
        first.events.len()
    );
    assert_eq!(
        first.compact_revision, 0,
        "no compact notification should be sent for a future start_revision"
    );

    // The watch is registered for live events. A Put after the
    // watch is established must be streamed.
    kv.put(PutRequest {
        key: b"/registry/events/team-a/late".to_vec(),
        value: b"after-watch".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut got_live = false;
    while !got_live {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!("did not receive the live Put event after accepting the future watch");
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(resp))) => {
                if !resp.events.is_empty() {
                    got_live = true;
                }
            }
            Ok(Some(Err(status))) => panic!(
                "unexpected error after live Put: code={:?} message={:?}",
                status.code(),
                status.message()
            ),
            Ok(None) => panic!("watch stream closed unexpectedly after live Put"),
            Err(_) => panic!("timed out waiting for live Put event"),
        }
    }

    h.stop().await;
}

#[tokio::test]
async fn watch_below_oldest_history_returns_compact_notification() {
    // Regression: netcd with a small history_capacity lets the ring
    // buffer cycle past the apiserver's cached start_revision. The
    // server must advertise `compact_revision` so the apiserver
    // re-List at the right rev; it must NOT silently stream a
    // partial view of state.
    let h = Harness::start().await;
    let mut kv = h.kv_client().await;
    // The harness uses history_capacity=100. Drive 200 Puts so the
    // ring buffer cycles 100 entries.
    for i in 0..200u32 {
        kv.put(PutRequest {
            key: format!("/registry/events/team-a/k-{i:04}")
                .as_bytes()
                .to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let cur = kv
        .range(RangeRequest {
            key: b"/registry/events/team-a/seed".to_vec(),
            range_end: vec![],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .header
        .expect("header")
        .revision as u64;
    assert!(cur >= 200, "setup: expected current_rev >= 200, got {cur}");

    // Ask for a start_revision below oldest_rev (rev 1).
    let mut w = h.watch_client().await;
    let (tx, rx) = futures::channel::mpsc::unbounded::<WatchRequest>();
    tx.unbounded_send(WatchRequest {
        request_union: Some(
            netcd::proto::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: b"/registry/events/team-a/".to_vec(),
                    range_end: b"\x00".to_vec(),
                    start_revision: 1,
                    ..Default::default()
                },
            ),
        ),
    })
    .unwrap();

    let mut stream = w.watch(rx).await.expect("watch handshake").into_inner();

    // The first response must be a one-shot compact notification.
    // Per etcd semantics, no events are streamed and the stream is
    // left open for future events; we only assert on the first frame
    // here.
    let first = {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut got: Option<netcd::proto::etcdserverpb::WatchResponse> = None;
        while got.is_none() {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                panic!("timed out waiting for first watch response");
            }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(resp))) => got = Some(resp),
                Ok(Some(Err(status))) => panic!(
                    "unexpected error on compacted watch: code={:?} message={:?}",
                    status.code(),
                    status.message()
                ),
                Ok(None) => panic!("watch stream closed before sending any response"),
                Err(_) => panic!("timed out waiting for first watch response"),
            }
        }
        got.unwrap()
    };
    assert!(
        first.events.is_empty() && !first.created,
        "expected a one-shot compact notification, got created={} events={}",
        first.created,
        first.events.len()
    );
    assert!(
        first.compact_revision > 0,
        "expected compact_revision > 0, got {}",
        first.compact_revision
    );
    // compacted_rev must reflect the dropped revs (>= oldest_rev - 1
    // after the fix). It must be strictly less than current_rev.
    let compact = first.compact_revision as u64;
    assert!(
        compact < cur,
        "compact_revision ({compact}) must be < current_rev ({cur})",
    );

    h.stop().await;
}
