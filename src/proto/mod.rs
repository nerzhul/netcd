//! Generated etcd v3 protobuf + gRPC bindings.
//!
//! `tonic-build` produces one `.rs` file per proto package in
//! `OUT_DIR`. Because the generated code references cross-package types
//! as `super::<other_package>::...`, we wrap each file in its own
//! `pub mod` inside this parent module so the `super::` paths resolve.

#![allow(clippy::all, clippy::pedantic)]

pub mod etcdserverpb {
    include!(concat!(env!("OUT_DIR"), "/etcdserverpb.rs"));
}

pub mod mvccpb {
    include!(concat!(env!("OUT_DIR"), "/mvccpb.rs"));
}

pub mod authpb {
    include!(concat!(env!("OUT_DIR"), "/authpb.rs"));
}

pub mod grpc {
    pub mod health {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/grpc.health.v1.rs"));
        }
    }
}
