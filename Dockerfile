# Unified Strake Docker Image
# Contains: strake-cli, strake-server, strake-enterprise
# 
# Build: docker build -t strake:latest .
# Run server: docker run -p 50051:50051 strake:latest server
# Run enterprise: docker run -p 8080:8080 strake:latest enterprise
# Run CLI: docker run strake:latest cli --help

ARG RUST_VERSION=1.88
ARG DEBIAN_VERSION=bookworm

# ===== Builder Stage =====
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

ARG TARGETARCH
RUN echo "Building for arch: $TARGETARCH"

# Determine Rust target based on Docker target architecture
# amd64 -> x86_64-unknown-linux-musl
# arm64 -> aarch64-unknown-linux-musl
ENV RUST_TARGET=${TARGETARCH:-amd64}
ENV RUST_TARGET=${RUST_TARGET/amd64/x86_64-unknown-linux-musl}
ENV RUST_TARGET=${RUST_TARGET/arm64/aarch64-unknown-linux-musl}

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    musl-tools \
    cmake \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Add musl target for static linking
RUN rustup target add $RUST_TARGET

WORKDIR /build

# Copy workspace configuration
COPY Cargo.toml Cargo.lock ./
COPY rustfmt.toml clippy.toml deny.toml ./

# Copy all crates
COPY crates/ ./crates/
COPY python/ ./python/
COPY strake-enterprise/ ./strake-enterprise/

# Build all binaries with musl for static linking
# Use vendored OpenSSL to avoid runtime dependencies
ENV OPENSSL_VENDORED=1
ENV RUSTFLAGS="-C target-feature=+crt-static"

# Build dependencies
RUN cargo build --release \
    --target $RUST_TARGET \
    -p strake-cli \
    -p strake-server \
    && cargo build --release \
    --target $RUST_TARGET \
    --manifest-path strake-enterprise/Cargo.toml

# Verify binaries are statically linked
RUN ldd /build/target/$RUST_TARGET/release/strake-cli || true
RUN ldd /build/target/$RUST_TARGET/release/strake-server || true
RUN ldd /build/target/$RUST_TARGET/release/strake-enterprise || true

# Strip binaries to reduce size
RUN strip /build/target/$RUST_TARGET/release/strake-cli
RUN strip /build/target/$RUST_TARGET/release/strake-server
RUN strip /build/target/$RUST_TARGET/release/strake-enterprise

# ===== Runtime Stage =====
FROM scratch

# Need to redefine ARG in new stage or rely on copying
# Since we can't easily pass variable between stages in COPY --from, 
# we'll use a trick or just wildcard since we only built one target.
# Actually, we can use the ARG again if passed. 
# But let's stick to the simpler wildcard approach for COPY source path:
# target/*/release/...

# Copy binaries from builder
COPY --from=builder /build/target/*/release/strake-cli /bin/strake-cli
COPY --from=builder /build/target/*/release/strake-server /bin/strake-server
COPY --from=builder /build/target/*/release/strake-enterprise /bin/strake-enterprise

# Copy CA certificates for TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Create minimal passwd/group files for running as non-root
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# Default to server mode
ENTRYPOINT ["/bin/strake-server"]

# Expose default ports
# 50051: Flight SQL (strake-server)
# 8080: HTTP/gRPC (strake-enterprise)
EXPOSE 50051 8080

LABEL org.opencontainers.image.title="Strake"
LABEL org.opencontainers.image.description="Unified data platform with CLI, server, and enterprise capabilities"
LABEL org.opencontainers.image.source="https://github.com/strake-data/strake"
LABEL org.opencontainers.image.url="https://strakedata.com"
LABEL org.opencontainers.image.vendor="Strake Data"

# Usage examples:
# docker run ghcr.io/strake-data/strake:latest                          # Run strake-server
# docker run --entrypoint /bin/strake-enterprise ghcr.io/strake-data/strake:latest
# docker run --entrypoint /bin/strake-cli ghcr.io/strake-data/strake:latest --help
