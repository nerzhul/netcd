# syntax=docker/dockerfile:1.6
# Two-stage build for netcd.

FROM rust:1.85-bookworm AS builder
WORKDIR /build

# `build.rs` runs tonic-build/prost-build, which shells out to `protoc` to
# compile the .proto files. The stock rust image does not ship it, so install
# the Debian package before the cargo invocation (build time only, the final
# image does not need it).
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY build.rs ./
COPY src ./src
RUN cargo build --release --bin netcd

# Final image: distroless cc (no shell, no OpenSSL - we use rustls).
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/target/release/netcd /usr/local/bin/netcd
COPY config.example.json /etc/netcd/config.example.json
EXPOSE 2379 2378
ENTRYPOINT ["/usr/local/bin/netcd"]
CMD ["--config", "/etc/netcd/config.json"]
