//! Watch service + WatchEvent type used by the store fan-out.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::auth::Authenticator;
use crate::error::failed_precondition;
use crate::proto::etcdserverpb::{
    self, watch_request, ResponseHeader, WatchCancelRequest, WatchCreateRequest, WatchRequest,
    WatchResponse,
};
use crate::proto::mvccpb::{self, event::EventType, Event};
use crate::store::history::Mutation;
use crate::store::{MutationKind, Store};

/// One fan-out event between the store and a single watch stream.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub rev: u64,
    pub kind: MutationKind,
    pub key: Bytes,
    pub value: Bytes,
    pub create_rev: u64,
    pub lease_id: Option<i64>,
    pub prev_value: Option<Bytes>,
}

impl WatchEvent {
    pub fn from_mutation(m: Mutation, prev_kv: bool) -> Self {
        Self {
            rev: m.rev,
            kind: m.kind,
            key: m.key,
            value: m.value,
            create_rev: m.create_rev,
            lease_id: m.lease_id,
            prev_value: if prev_kv { Some(Bytes::new()) } else { None },
        }
    }
}

fn header(rev: i64) -> Option<ResponseHeader> {
    Some(ResponseHeader {
        cluster_id: 0,
        member_id: 0,
        revision: rev,
        raft_term: 0,
    })
}

pub struct WatchService {
    pub store: Arc<Store>,
    #[allow(dead_code)]
    pub auth: Authenticator,
    #[allow(dead_code)]
    pub progress_interval: Duration,
}

impl WatchService {
    pub fn new(store: Arc<Store>, auth: Authenticator, progress_interval: Duration) -> Self {
        Self {
            store,
            auth,
            progress_interval,
        }
    }
}

#[tonic::async_trait]
impl etcdserverpb::watch_server::Watch for WatchService {
    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<WatchResponse, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<WatchResponse, Status>>(128);
        let store = self.store.clone();

        tokio::spawn(async move {
            let active: Arc<parking_lot::Mutex<Vec<i64>>> =
                Arc::new(parking_lot::Mutex::new(Vec::new()));
            let active_for_cleanup = active.clone();

            loop {
                match in_stream.message().await {
                    Ok(Some(req)) => {
                        if let Err(e) = handle_request(req, &store, &tx, &active).await {
                            let _ = tx.send(Err(e)).await;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(error = %e, "watch stream error");
                        break;
                    }
                }
            }
            let owned = active_for_cleanup.lock().clone();
            {
                let mut g = store.watches.lock();
                for wid in owned {
                    g.remove(&wid);
                }
            }
            debug!("watch stream closed");
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

async fn handle_request(
    req: WatchRequest,
    store: &Arc<Store>,
    tx: &mpsc::Sender<Result<WatchResponse, Status>>,
    active: &Arc<parking_lot::Mutex<Vec<i64>>>,
) -> Result<(), Status> {
    let union = req
        .request_union
        .ok_or_else(|| failed_precondition("empty WatchRequest"))?;
    match union {
        watch_request::RequestUnion::CreateRequest(create) => {
            handle_create(create, store, tx, active).await
        }
        watch_request::RequestUnion::CancelRequest(cancel) => {
            handle_cancel(cancel, store, tx).await
        }
        watch_request::RequestUnion::ProgressRequest(_) => handle_progress(store, tx).await,
    }
}

async fn handle_create(
    create: WatchCreateRequest,
    store: &Arc<Store>,
    tx: &mpsc::Sender<Result<WatchResponse, Status>>,
    active: &Arc<parking_lot::Mutex<Vec<i64>>>,
) -> Result<(), Status> {
    let key = Bytes::from(create.key);
    let range_end = Bytes::from(create.range_end);
    let prev_kv = create.prev_kv;
    let start_revision = create.start_revision;

    // Validate `start_revision` up front so a rejected request leaves
    // no half-registered watch in `store.watches`. One rejection case
    // is etcd-compatible:
    //
    //   * `start_revision <= compacted_rev` (explicit `Compact()` call
    //     or the ring buffer dropping the entry): reply with a
    //     one-shot `WatchResponse{compact_revision}` so the client
    //     re-List at `compacted_rev + 1`.
    //
    // We deliberately do NOT reject `start_revision > current_rev`
    // (e.g. apiserver cached a RV from before netcd restarted and
    // the new netcd is at a lower rev). Etcd would return
    // `OutOfRange("Too large resource version: ...")` here, but
    // because netcd is in-memory and restarts lose state, the
    // apiserver's `ResourceVersion` cache is frequently one restart
    // ahead of netcd. Returning OutOfRange triggers a re-List on
    // every resource type in parallel, producing a burst of
    // `E... watcher.go:425 watch chan error: ... Too large resource
    // version: X, current: Y` lines in the apiserver log (one per
    // resource, e.g. 25+ at once).
    //
    // Instead, we accept the watch, skip the historical replay
    // (start_revision is in the future so the buffer has no matching
    // events), and register the watch for live events. The apiserver
    // will receive any new mutations at the next available rev and
    // process them normally; the previous List response is still
    // the source of truth for the current state.
    if start_revision > 0 {
        let compacted = store
            .compacted_rev
            .load(std::sync::atomic::Ordering::Relaxed) as i64;
        if start_revision <= compacted {
            tx.send(Ok(WatchResponse {
                header: header(compacted),
                watch_id: 0,
                compact_revision: compacted,
                ..Default::default()
            }))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            return Ok(());
        }
    }

    let watch_id = store.alloc_watch_id();
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel::<WatchEvent>();

    {
        let mut g = store.watches.lock();
        g.insert(
            watch_id,
            crate::store::WatchHandle {
                id: watch_id,
                key: key.clone(),
                range_end: range_end.clone(),
                prev_kv,
                sender: watch_tx,
            },
        );
    }
    active.lock().push(watch_id);

    tx.send(Ok(WatchResponse {
        header: header(store.current_rev() as i64),
        watch_id,
        created: true,
        ..Default::default()
    }))
    .await
    .map_err(|e| Status::internal(e.to_string()))?;

    if start_revision > 0 {
        let snap = store.history.snapshot();
        for m in snap {
            if (m.rev as i64) < start_revision {
                continue;
            }
            if !range_contains(&key, &range_end, &m.key) {
                continue;
            }
            let ev = WatchEvent::from_mutation(m, prev_kv);
            let resp = ev_to_response(&ev, watch_id);
            if tx.send(Ok(resp)).await.is_err() {
                break;
            }
        }
    }

    let tx2 = tx.clone();
    let active2 = active.clone();
    let store2 = store.clone();
    tokio::spawn(async move {
        while let Some(ev) = watch_rx.recv().await {
            let resp = ev_to_response(&ev, watch_id);
            if tx2.send(Ok(resp)).await.is_err() {
                let mut g = store2.watches.lock();
                g.remove(&watch_id);
                let mut a = active2.lock();
                a.retain(|x| *x != watch_id);
                break;
            }
        }
    });

    Ok(())
}

async fn handle_cancel(
    cancel: WatchCancelRequest,
    store: &Arc<Store>,
    tx: &mpsc::Sender<Result<WatchResponse, Status>>,
) -> Result<(), Status> {
    let id = cancel.watch_id;
    {
        let mut g = store.watches.lock();
        g.remove(&id);
    }
    tx.send(Ok(WatchResponse {
        header: header(store.current_rev() as i64),
        watch_id: id,
        canceled: true,
        ..Default::default()
    }))
    .await
    .ok();
    Ok(())
}

async fn handle_progress(
    store: &Arc<Store>,
    tx: &mpsc::Sender<Result<WatchResponse, Status>>,
) -> Result<(), Status> {
    tx.send(Ok(WatchResponse {
        header: header(store.current_rev() as i64),
        watch_id: 0,
        ..Default::default()
    }))
    .await
    .ok();
    Ok(())
}

fn ev_to_response(ev: &WatchEvent, watch_id: i64) -> WatchResponse {
    let event = Event {
        r#type: match ev.kind {
            MutationKind::Put => EventType::Put as i32,
            MutationKind::Delete | MutationKind::DeleteLease => EventType::Delete as i32,
        },
        kv: Some(mvccpb::KeyValue {
            key: ev.key.to_vec(),
            value: ev.value.to_vec(),
            create_revision: ev.create_rev as i64,
            mod_revision: ev.rev as i64,
            version: 1,
            lease: ev.lease_id.unwrap_or(0),
        }),
        prev_kv: ev.prev_value.clone().map(|b| mvccpb::KeyValue {
            key: ev.key.to_vec(),
            value: b.to_vec(),
            create_revision: ev.create_rev as i64,
            mod_revision: ev.rev as i64,
            version: 1,
            lease: ev.lease_id.unwrap_or(0),
        }),
    };
    WatchResponse {
        header: header(ev.rev as i64),
        watch_id,
        events: vec![event],
        ..Default::default()
    }
}

fn range_contains(key: &[u8], range_end: &[u8], target: &[u8]) -> bool {
    if target < key {
        return false;
    }
    if range_end.is_empty() {
        return target == key;
    }
    if range_end == b"\x00" {
        return true;
    }
    target < range_end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_contains_basic() {
        assert!(range_contains(b"a", b"c", b"b"));
        assert!(!range_contains(b"a", b"c", b"c"));
        assert!(range_contains(b"a", b"\x00", b"z"));
        assert!(range_contains(b"a", b"", b"a"));
        assert!(!range_contains(b"a", b"", b"b"));
    }
}
