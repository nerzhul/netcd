//! KV service: Range, Put, DeleteRange, Txn, Compact.
//!
//! The tonic-generated `Kv` trait requires each RPC method to return
//! `Result<Response<_>, tonic::Status>`. `tonic::Status` is ~176B
//! (carries a message, details, and metadata), which trips
//! `clippy::result_large_err`. The size is inherent to the trait
//! contract; boxing the error would require wrapping every response
//! and breaks the `?` ergonomics inside the handlers. The store
//! internally uses the small `StoreError` enum and converts to
//! `Status` only at the service boundary here.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use bytes::Bytes;
use tonic::{Request, Response, Status};

use crate::auth::{Authenticator, Identity};
use crate::error::internal;
use crate::proto::etcdserverpb::{
    self, request_op::Request as ReqOpVariant, response_op::Response as RespOpVariant,
    Compare, DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse, RequestOp, ResponseOp, TxnRequest, TxnResponse,
};
use crate::proto::mvccpb;
use crate::store::{MutationKind, Store};

pub fn identity_for(_auth: &Authenticator) -> Identity {
    _auth.current_identity()
}

pub struct KvService {
    pub store: Arc<Store>,
    pub auth: Authenticator,
}

impl KvService {
    pub fn new(store: Arc<Store>, auth: Authenticator) -> Self {
        Self { store, auth }
    }
}

fn header(rev: i64) -> Option<etcdserverpb::ResponseHeader> {
    Some(etcdserverpb::ResponseHeader {
        cluster_id: 0,
        member_id: 0,
        revision: rev,
        raft_term: 0,
    })
}

fn entry_to_kv(key: Bytes, e: &crate::store::Entry) -> mvccpb::KeyValue {
    mvccpb::KeyValue {
        key: key.to_vec(),
        value: e.value.to_vec(),
        create_revision: e.create_rev as i64,
        mod_revision: e.mod_rev as i64,
        version: 1,
        lease: e.lease_id.unwrap_or(0),
    }
}

#[tonic::async_trait]
impl etcdserverpb::kv_server::Kv for KvService {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        let id = identity_for(&self.auth);
        self.auth.authorise_range(&id, &req.key, &req.range_end)?;
        let out = self
            .store
            .range(&req.key, &req.range_end, req.limit, req.revision)?;
        let kvs: Vec<mvccpb::KeyValue> =
            out.iter().map(|(k, e)| entry_to_kv(k.clone(), e)).collect();
        let count = kvs.len() as i64;
        Ok(Response::new(RangeResponse {
            header: header(self.store.current_rev() as i64),
            kvs,
            more: false,
            count,
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let id = identity_for(&self.auth);
        self.auth.authorise_key(&id, &req.key)?;

        let key = Bytes::from(req.key);
        let value = Bytes::from(req.value);
        let lease_id = if req.lease == 0 {
            None
        } else {
            Some(req.lease)
        };

        if let Some(lid) = lease_id {
            if self.store.leases.time_to_live(lid).is_none() {
                return Err(Status::new(
                    tonic::Code::NotFound,
                    format!("lease {lid} not found"),
                ));
            }
        }

        let _ = self.store.put(key, value, lease_id)?;
        Ok(Response::new(PutResponse {
            header: header(self.store.current_rev() as i64),
            prev_kv: None,
        }))
    }

    // Note: store.put / store.delete / store.range now return
    // `Result<_, StoreError>`. The `?` operator below uses the
    // blanket `From<StoreError> for Status` impl in `crate::error`
    // to convert at the service boundary, avoiding returning the
    // 176-byte `tonic::Status` from internal storage methods.

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let req = request.into_inner();
        let id = identity_for(&self.auth);
        self.auth.authorise_range(&id, &req.key, &req.range_end)?;

        if req.range_end.is_empty() {
            let (rev, deleted) = self.store.delete(&req.key)?;
            return Ok(Response::new(DeleteRangeResponse {
                header: header(rev as i64),
                deleted,
                prev_kvs: Vec::new(),
            }));
        }
        let entries = self.store.range(&req.key, &req.range_end, 0, 0)?;
        let mut deleted = 0i64;
        let mut last_rev = self.store.current_rev();
        for (k, _) in entries {
            let (rev, d) = self.store.delete(&k)?;
            deleted += d;
            last_rev = rev;
        }
        Ok(Response::new(DeleteRangeResponse {
            header: header(last_rev as i64),
            deleted,
            prev_kvs: Vec::new(),
        }))
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let req = request.into_inner();
        let id = identity_for(&self.auth);

        for c in &req.compare {
            if !c.key.is_empty() || !c.range_end.is_empty() {
                self.auth.authorise_range(&id, &c.key, &c.range_end)?;
            }
        }
        for op in req.success.iter().chain(req.failure.iter()) {
            check_op_acl(&self.auth, &id, op)?;
        }

        let ok = req.compare.iter().try_fold(true, |acc, c| {
            if !acc {
                return Ok(false);
            }
            eval_compare(&self.store, c)
        })?;
        let branch: &[RequestOp] = if ok { &req.success } else { &req.failure };
        let mut responses = Vec::with_capacity(branch.len());
        for op in branch {
            responses.push(apply_op(&self.store, op)?);
        }
        Ok(Response::new(TxnResponse {
            header: header(self.store.current_rev() as i64),
            succeeded: ok,
            responses,
        }))
    }

    async fn compact(
        &self,
        request: Request<etcdserverpb::CompactionRequest>,
    ) -> Result<Response<etcdserverpb::CompactionResponse>, Status> {
        // Must return success so kube-apiserver's opportunistic
        // Compact call doesn't break.
        let req = request.into_inner();
        let new_rev = if req.revision <= 0 {
            self.store.current_rev() as i64
        } else {
            req.revision
        };
        use std::sync::atomic::Ordering;
        let cur = self.store.compacted_rev.load(Ordering::Relaxed) as i64;
        if new_rev < cur {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                format!("requested compaction rev {new_rev} is below current compacted_rev {cur}"),
            ));
        }
        self.store
            .compacted_rev
            .store(new_rev as u64, Ordering::Relaxed);
        Ok(Response::new(etcdserverpb::CompactionResponse {
            header: header(new_rev),
        }))
    }
}

fn check_op_acl(auth: &Authenticator, id: &Identity, op: &RequestOp) -> Result<(), Status> {
    match &op.request {
        Some(ReqOpVariant::RequestRange(r)) => auth.authorise_range(id, &r.key, &r.range_end),
        Some(ReqOpVariant::RequestPut(p)) => auth.authorise_key(id, &p.key),
        Some(ReqOpVariant::RequestDeleteRange(d)) => auth.authorise_range(id, &d.key, &d.range_end),
        Some(ReqOpVariant::RequestTxn(t)) => {
            for c in &t.compare {
                auth.authorise_range(id, &c.key, &c.range_end)?;
            }
            for sub in t.success.iter().chain(t.failure.iter()) {
                check_op_acl(auth, id, sub)?;
            }
            Ok(())
        }
        None => Ok(()),
    }
}

fn eval_compare(store: &Store, c: &Compare) -> Result<bool, Status> {
    use crate::proto::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
    let entry = store.get(&c.key)?;
    let cur_value: &[u8] = entry.as_ref().map(|e| e.value.as_ref()).unwrap_or(&[]);
    let cur_rev: i64 = entry.as_ref().map(|e| e.mod_rev as i64).unwrap_or(0);
    let cur_version: i64 = entry.as_ref().map(|_| 1i64).unwrap_or(0);
    let cur_lease: i64 = entry.as_ref().map(|e| e.lease_id.unwrap_or(0)).unwrap_or(0);
    let cur_create: i64 = entry.as_ref().map(|e| e.create_rev as i64).unwrap_or(0);
    let result = c.result;
    let target = c.target;
    // Decode target_union.
    let (target_int, target_bytes): (i64, &[u8]) = match &c.target_union {
        Some(TargetUnion::Version(v)) => (*v, &[]),
        Some(TargetUnion::CreateRevision(v)) => (*v, &[]),
        Some(TargetUnion::ModRevision(v)) => (*v, &[]),
        Some(TargetUnion::Lease(v)) => (*v, &[]),
        Some(TargetUnion::Value(b)) => (0, b.as_slice()),
        None => (0, &[]),
    };
    let result_kind = CompareResult::try_from(result).unwrap_or(CompareResult::Equal);
    let target_kind = CompareTarget::try_from(target).unwrap_or(CompareTarget::Value);
    let actual_int: i64 = match target_kind {
        CompareTarget::Version => cur_version,
        CompareTarget::Create => cur_create,
        CompareTarget::Mod => cur_rev,
        CompareTarget::Lease => cur_lease,
        CompareTarget::Value => 0, // unused
    };
    if target_kind == CompareTarget::Value {
        return Ok(match result_kind {
            CompareResult::Equal => cur_value == target_bytes,
            CompareResult::NotEqual => cur_value != target_bytes,
            CompareResult::Less => cur_value < target_bytes,
            CompareResult::Greater => cur_value > target_bytes,
        });
    }
    Ok(match result_kind {
        CompareResult::Equal => actual_int == target_int,
        CompareResult::NotEqual => actual_int != target_int,
        CompareResult::Less => actual_int < target_int,
        CompareResult::Greater => actual_int > target_int,
    })
}

fn apply_op(store: &Store, op: &RequestOp) -> Result<ResponseOp, Status> {
    let inner = op
        .request
        .as_ref()
        .ok_or_else(|| internal("empty request_op"))?;
    match inner {
        ReqOpVariant::RequestRange(r) => {
            let out = store.range(&r.key, &r.range_end, r.limit, 0)?;
            let kvs: Vec<mvccpb::KeyValue> =
                out.iter().map(|(k, e)| entry_to_kv(k.clone(), e)).collect();
            Ok(ResponseOp {
                response: Some(RespOpVariant::ResponseRange(RangeResponse {
                    header: header(store.current_rev() as i64),
                    kvs,
                    more: false,
                    count: out.len() as i64,
                })),
            })
        }
        ReqOpVariant::RequestPut(p) => {
            let key = Bytes::from(p.key.clone());
            let value = Bytes::from(p.value.clone());
            let lease = if p.lease == 0 { None } else { Some(p.lease) };
            let _ = store.put(key, value, lease)?;
            Ok(ResponseOp {
                response: Some(RespOpVariant::ResponsePut(PutResponse {
                    header: header(store.current_rev() as i64),
                    prev_kv: None,
                })),
            })
        }
        ReqOpVariant::RequestDeleteRange(d) => {
            let entries = store.range(&d.key, &d.range_end, 0, 0)?;
            let mut deleted = 0i64;
            let mut last_rev = store.current_rev();
            for (k, _) in entries {
                let (rev, n) = store.delete(&k)?;
                deleted += n;
                last_rev = rev;
            }
            Ok(ResponseOp {
                response: Some(RespOpVariant::ResponseDeleteRange(DeleteRangeResponse {
                    header: header(last_rev as i64),
                    deleted,
                    prev_kvs: Vec::new(),
                })),
            })
        }
        ReqOpVariant::RequestTxn(t) => {
            let ok = t.compare.iter().try_fold(true, |acc, c| {
                if !acc {
                    return Ok(false);
                }
                eval_compare(store, c)
            })?;
            let branch: &[RequestOp] = if ok { &t.success } else { &t.failure };
            let mut responses = Vec::with_capacity(branch.len());
            for op in branch {
                responses.push(apply_op(store, op)?);
            }
            Ok(ResponseOp {
                response: Some(RespOpVariant::ResponseTxn(TxnResponse {
                    header: header(store.current_rev() as i64),
                    succeeded: ok,
                    responses,
                })),
            })
        }
    }
}

#[allow(dead_code)]
pub fn _mutation_kind_is_used() -> MutationKind {
    MutationKind::Delete
}
