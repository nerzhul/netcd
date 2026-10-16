//! Lease service: Grant, Revoke, KeepAlive (bidi), ListLeases, TimeToLive.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::auth::Authenticator;
use crate::proto::etcdserverpb::{
    self, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};
use crate::store::{MutationKind, Store};

pub struct LeaseService {
    pub store: Arc<Store>,
    pub auth: Authenticator,
    pub max_ttl: Duration,
}

impl LeaseService {
    pub fn new(store: Arc<Store>, auth: Authenticator, max_ttl: Duration) -> Self {
        Self {
            store,
            auth,
            max_ttl,
        }
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

#[tonic::async_trait]
impl etcdserverpb::lease_server::Lease for LeaseService {
    type LeaseKeepAliveStream =
        tokio_stream::wrappers::ReceiverStream<Result<LeaseKeepAliveResponse, Status>>;

    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let req = request.into_inner();
        let id = self.auth.current_identity();
        let requested = req.ttl.max(1);
        let clamped = requested.min(self.max_ttl.as_secs() as i64);
        let ttl = Duration::from_secs(clamped as u64);
        let owner = Some(id.tenant.name.clone());
        let lid = if req.id > 0 {
            // Honour requested id if free.
            if self.store.leases.time_to_live(req.id).is_some() {
                req.id
            } else {
                self.store.leases.grant_with_id(req.id, ttl, owner);
                req.id
            }
        } else {
            self.store.leases.grant(ttl, owner)
        };
        Ok(Response::new(LeaseGrantResponse {
            header: header(self.store.current_rev() as i64),
            id: lid,
            ttl: clamped,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let req = request.into_inner();
        let id = self.auth.current_identity();
        let owner = Some(id.tenant.name.as_str());
        let keys = self.store.leases.revoke(req.id, owner).ok_or_else(|| {
            Status::new(tonic::Code::PermissionDenied, "lease not owned by tenant")
        })?;
        let mut last_rev = self.store.current_rev();
        for k in keys {
            let (rev, _) = self.store.delete_with_event(k, MutationKind::DeleteLease)?;
            last_rev = rev;
        }
        Ok(Response::new(LeaseRevokeResponse {
            header: header(last_rev as i64),
        }))
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<LeaseKeepAliveResponse, Status>>(64);
        let store = self.store.clone();
        let max_ttl = self.max_ttl;
        tokio::spawn(async move {
            while let Some(req) = match in_stream.message().await {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "keepalive stream error");
                    None
                }
            } {
                // KeepAliveRequest has no `ttl` field - we use the
                // lease's original (max) TTL each call.
                let ok = store.leases.keep_alive(req.id, max_ttl, None);
                let resp = LeaseKeepAliveResponse {
                    header: header(store.current_rev() as i64),
                    id: req.id,
                    ttl: if ok { max_ttl.as_secs() as i64 } else { 0 },
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });
        let out: tokio_stream::wrappers::ReceiverStream<Result<LeaseKeepAliveResponse, Status>> =
            tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(out))
    }

    async fn lease_leases(
        &self,
        _request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let leases: Vec<etcdserverpb::LeaseStatus> = self
            .store
            .leases
            .list()
            .into_iter()
            .map(|id| etcdserverpb::LeaseStatus { id })
            .collect();
        Ok(Response::new(LeaseLeasesResponse {
            header: header(self.store.current_rev() as i64),
            leases,
        }))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let req = request.into_inner();
        let out = self.store.leases.time_to_live(req.id);
        let (_id, rem, keys) = match out {
            Some(v) => v,
            None => {
                return Err(Status::new(
                    tonic::Code::NotFound,
                    format!("lease {} not found", req.id),
                ));
            }
        };
        Ok(Response::new(LeaseTimeToLiveResponse {
            header: header(self.store.current_rev() as i64),
            id: req.id,
            ttl: rem.as_secs() as i64,
            granted_ttl: self.max_ttl.as_secs() as i64,
            keys: if req.keys {
                keys.iter().map(|b| b.to_vec()).collect()
            } else {
                Vec::new()
            },
        }))
    }
}
