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

/// Round a duration up to whole seconds, the way etcd reports lease
/// TTLs. Truncating would report 59 s for a lease that still has
/// 59.9 s left.
fn ceil_secs(d: Duration) -> i64 {
    let secs = d.as_secs();
    if d.subsec_nanos() > 0 {
        secs as i64 + 1
    } else {
        secs as i64
    }
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
            // Honour the requested id when it is free. etcd rejects a
            // taken id with `AlreadyExists`; silently handing it back
            // would give the caller a lease it does not own, carrying
            // someone else's TTL.
            if self.store.leases.time_to_live(req.id).is_some() {
                return Err(Status::new(
                    tonic::Code::AlreadyExists,
                    format!("lease {} already exists", req.id),
                ));
            }
            self.store.leases.grant_with_id(req.id, ttl, owner);
            req.id
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
        tokio::spawn(async move {
            while let Some(req) = match in_stream.message().await {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "keepalive stream error");
                    None
                }
            } {
                // `LeaseKeepAliveRequest` carries no TTL: the lease is
                // renewed with the TTL it was granted with (already
                // clamped to the server cap at grant time). Renewing
                // with the server cap instead turned every 60 s lease
                // into a 24 h one, so the objects bound to it were
                // never reaped.
                let remaining = store.leases.keep_alive(req.id, None);
                let resp = LeaseKeepAliveResponse {
                    header: header(store.current_rev() as i64),
                    id: req.id,
                    // etcd reports the time left before the lease
                    // expires, not the cap.
                    ttl: remaining.map(ceil_secs).unwrap_or(0),
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
        let status = self.store.leases.time_to_live(req.id).ok_or_else(|| {
            Status::new(tonic::Code::NotFound, format!("lease {} not found", req.id))
        })?;
        Ok(Response::new(LeaseTimeToLiveResponse {
            header: header(self.store.current_rev() as i64),
            id: status.id,
            ttl: ceil_secs(status.remaining),
            granted_ttl: status.granted_ttl.as_secs() as i64,
            keys: if req.keys {
                status.keys.iter().map(|b| b.to_vec()).collect()
            } else {
                Vec::new()
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::proto::etcdserverpb::lease_server::Lease;
    use arc_swap::ArcSwap;

    fn service(max_ttl: Duration) -> LeaseService {
        let store = Store::new(1024 * 1024, 100);
        let cfg = Arc::new(ArcSwap::from_pointee(Config::default_dev()));
        let auth = Authenticator::new(cfg, false);
        LeaseService::new(store, auth, max_ttl)
    }

    #[tokio::test]
    async fn grant_clamps_the_ttl_to_the_server_cap() {
        let svc = service(Duration::from_secs(60));
        let resp = svc
            .lease_grant(Request::new(LeaseGrantRequest { ttl: 3600, id: 0 }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.ttl, 60, "the TTL must be clamped to the cap");
    }

    #[tokio::test]
    async fn grant_keeps_a_ttl_below_the_cap() {
        let svc = service(Duration::from_secs(3600));
        let resp = svc
            .lease_grant(Request::new(LeaseGrantRequest { ttl: 60, id: 0 }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.ttl, 60, "a TTL below the cap must be honoured");
    }

    #[tokio::test]
    async fn time_to_live_reports_the_granted_ttl_not_the_cap() {
        // Regression: `granted_ttl` used to report the server-wide cap,
        // so a client could not tell what lifetime its lease actually
        // had.
        let svc = service(Duration::from_secs(3600));
        let id = svc
            .lease_grant(Request::new(LeaseGrantRequest { ttl: 60, id: 0 }))
            .await
            .unwrap()
            .into_inner()
            .id;

        let resp = svc
            .lease_time_to_live(Request::new(LeaseTimeToLiveRequest { id, keys: false }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.granted_ttl, 60);
        assert!(
            resp.ttl > 0 && resp.ttl <= 60,
            "the remaining TTL must follow the granted TTL, got {}",
            resp.ttl,
        );
    }

    #[tokio::test]
    async fn grant_with_a_taken_id_is_rejected() {
        // Regression: a taken id used to be handed back with a success
        // response, giving the caller a lease it did not own.
        let svc = service(Duration::from_secs(60));
        svc.lease_grant(Request::new(LeaseGrantRequest { ttl: 60, id: 7 }))
            .await
            .unwrap();
        let err = svc
            .lease_grant(Request::new(LeaseGrantRequest { ttl: 60, id: 7 }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn ceil_secs_rounds_up() {
        assert_eq!(ceil_secs(Duration::from_secs(60)), 60);
        assert_eq!(ceil_secs(Duration::from_millis(59_999)), 60);
        assert_eq!(ceil_secs(Duration::ZERO), 0);
    }
}
