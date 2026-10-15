# syntax=docker/dockerfile:1.6
# Two-stage build for netcd.

FROM rust:1.85-bookworm AS builder
WORKDIR /build

# Cache deps separately from sources.
COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY build.rs ./
RUN mkdir -p src && \
    echo "fn main() { println!(\"dep cache\"); }" > src/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release --bin netcd && \
    rm -rf src target/release/deps/netcd*

# Build the real binary.
COPY src ./src
RUN cargo build --release --bin netcd

# Final image: distroless cc (no shell, no OpenSSL - we use rustls).
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /build/target/release/netcd /usr/local/bin/netcd
COPY config.example.json /etc/netcd/config.example.json
EXPOSE 2379 2378
ENTRYPOINT ["/usr/local/bin/netcd"]
CMD ["--config", "/etc/netcd/config.json"]
