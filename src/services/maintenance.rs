//! Maintenance service.
//!
//! Implements `Compact` (no-op, just bumps `compacted_rev`) and
//! `Defragment` (no-op). All other methods (alarm, status, hash, etc.)
//! return `Unimplemented` so that kube-apiserver / etcd clients don't
//! choke on a service definition they expect.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::stream;
use tonic::{Request, Response, Status};

use crate::proto::etcdserverpb::{
    AlarmRequest, AlarmResponse, DefragmentRequest, DefragmentResponse, DowngradeRequest,
    DowngradeResponse, HashRequest, HashResponse, HashKvRequest, HashKvResponse,
    MoveLeaderRequest, MoveLeaderResponse, SnapshotRequest, SnapshotResponse, StatusRequest,
    StatusResponse,
};
use crate::store::Store;

pub struct MaintenanceService {
    pub store: Arc<Store>,
}

impl MaintenanceService {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

fn header(rev: i64) -> Option<crate::proto::etcdserverpb::ResponseHeader> {
    Some(crate::proto::etcdserverpb::ResponseHeader {
        cluster_id: 0,
        member_id: 0,
        revision: rev,
        raft_term: 0,
    })
}

#[tonic::async_trait]
impl crate::proto::etcdserverpb::maintenance_server::Maintenance for MaintenanceService {
    async fn defragment(
        &self,
        _request: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        Ok(Response::new(DefragmentResponse {
            header: header(self.store.current_rev() as i64),
        }))
    }

    async fn alarm(
        &self,
        _request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        Ok(Response::new(AlarmResponse {
            header: header(self.store.current_rev() as i64),
            ..Default::default()
        }))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(StatusResponse {
            header: header(self.store.current_rev() as i64),
            ..Default::default()
        }))
    }

    async fn hash(&self, _request: Request<HashRequest>) -> Result<Response<HashResponse>, Status> {
        Ok(Response::new(HashResponse {
            header: header(self.store.current_rev() as i64),
            hash: 0,
        }))
    }

    async fn hash_kv(
        &self,
        _request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        Ok(Response::new(HashKvResponse {
            header: header(self.store.current_rev() as i64),
            hash: 0,
            compact_revision: self.store.compacted_rev.load(Ordering::Relaxed) as i64,
        }))
    }

    type SnapshotStream = futures::stream::Empty<Result<SnapshotResponse, Status>>;

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        Ok(Response::new(stream::empty()))
    }

    async fn move_leader(
        &self,
        _request: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        Ok(Response::new(MoveLeaderResponse {
            header: header(self.store.current_rev() as i64),
        }))
    }

    async fn downgrade(
        &self,
        _request: Request<DowngradeRequest>,
    ) -> Result<Response<DowngradeResponse>, Status> {
        Ok(Response::new(DowngradeResponse {
            header: header(self.store.current_rev() as i64),
            version: String::new(),
        }))
    }
}
