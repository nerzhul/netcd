//! Tonic status helpers and common error conversions.
//!
//! Maps internal errors to etcd-compatible gRPC status codes so that
//! `kube-apiserver` (and other etcd clients) get the semantics they expect.

use tonic::Status;

/// Internal error type for the storage layer.
///
/// Returned by `Store` methods instead of `tonic::Status` directly:
/// `tonic::Status` is ~176B and the `clippy::result_large_err` lint
/// discourages returning it from non-`tonic` modules. The service
/// layer converts `StoreError` to `Status` via `From`.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("out of range: {0}")]
    OutOfRange(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl From<StoreError> for Status {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::InvalidArgument(m) => {
                Status::new(tonic::Code::InvalidArgument, m)
            }
            StoreError::OutOfRange(m) => Status::new(tonic::Code::OutOfRange, m),
            StoreError::Internal(m) => Status::internal(m),
        }
    }
}

/// Map an anyhow error into a tonic `Status` with `Internal` code.
///
/// Use this only at the top of a request handler when no more specific
/// mapping makes sense.
pub fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(format!("{e}"))
}

/// `PermissionDenied` status with the given reason.
pub fn permission_denied(reason: impl Into<String>) -> Status {
    Status::new(tonic::Code::PermissionDenied, reason.into())
}

/// `InvalidArgument` status.
pub fn invalid_argument(reason: impl Into<String>) -> Status {
    Status::new(tonic::Code::InvalidArgument, reason.into())
}

/// `FailedPrecondition` status - the request is well-formed but the current
/// state of the server prevents fulfilling it (e.g. compacting an old
/// revision).
pub fn failed_precondition(reason: impl Into<String>) -> Status {
    Status::new(tonic::Code::FailedPrecondition, reason.into())
}

/// `OutOfRange` status - the requested revision has been compacted.
pub fn out_of_range(reason: impl Into<String>) -> Status {
    Status::new(tonic::Code::OutOfRange, reason.into())
}

/// `Unavailable` status.
pub fn unavailable(reason: impl Into<String>) -> Status {
    Status::new(tonic::Code::Unavailable, reason.into())
}
