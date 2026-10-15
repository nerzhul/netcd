//! gRPC health service. Standard
//! `grpc.health.v1.Health` gRPC protocol.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::warn;

use crate::proto::grpc::health::v1::health_check_response::ServingStatus;
use crate::proto::grpc::health::v1::{
    HealthCheckRequest, HealthCheckResponse, HealthListRequest, HealthListResponse,
};

pub struct HealthService {
    /// Tracked so the caller can see rev updates in their own code
    /// (we don't expose it on the wire per the standard health proto).
    #[allow(dead_code)]
    pub current_rev: Arc<AtomicU64>,
}

impl HealthService {
    pub fn new(current_rev: Arc<AtomicU64>) -> Self {
        Self { current_rev }
    }
}

#[tonic::async_trait]
impl crate::proto::grpc::health::v1::health_server::Health for HealthService {
    type WatchStream = HealthStream;

    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let _ = request.into_inner();
        let _ = self.current_rev.load(Ordering::Relaxed);
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthStream>, Status> {
        let (tx, rx) = mpsc::channel(16);
        let _ = tx
            .send(Ok(HealthCheckResponse {
                status: ServingStatus::Serving as i32,
            }))
            .await;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if tx
                    .send(Ok(HealthCheckResponse {
                        status: ServingStatus::Serving as i32,
                    }))
                    .await
                    .is_err()
                {
                    warn!("health watch stream closed");
                    break;
                }
            }
        });
        Ok(Response::new(HealthStream::new(rx)))
    }

    async fn list(
        &self,
        _request: Request<HealthListRequest>,
    ) -> Result<Response<HealthListResponse>, Status> {
        Ok(Response::new(HealthListResponse {
            statuses: Default::default(),
        }))
    }
}

pub type HealthStream = tokio_stream::wrappers::ReceiverStream<Result<HealthCheckResponse, Status>>;
