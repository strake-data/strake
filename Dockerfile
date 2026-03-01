# Unified Strake Docker Image
# Contains: strake-cli, strake-server, strake-enterprise
# 
# Build: docker build -t strake:latest .
# Run server: docker run -p 50051:50051 strake:latest server
# Run enterprise: docker run -p 8080:8080 strake:latest enterprise
# Run CLI: docker run strake:latest cli --help

ARG RUST_VERSION=1.93
ARG ALPINE_VERSION=3.21

# ===== Builder Stage =====
FROM rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder

ARG TARGETARCH
RUN echo "Building natively for arch: $TARGETARCH on Alpine"

# Alpine natively builds static musl binaries
# Install build dependencies
RUN apk add --no-cache \
    pkgconfig \
    openssl-dev \
    musl-dev \
    cmake \
    clang \
    protoc \
    protobuf-dev \
    g++ \
    make \
    perl \
    linux-headers

WORKDIR /build

# Copy workspace configuration
COPY Cargo.toml Cargo.lock ./
COPY rustfmt.toml clippy.toml deny.toml ./

# Copy all crates
COPY crates/ ./crates/
COPY python/ ./python/
COPY strake-enterprise/ ./strake-enterprise/

# Create public key for enterprise build
RUN mkdir -p keys && \
    echo "-----BEGIN PUBLIC KEY-----" > keys/public.pem && \
    echo "MCowBQYDK2VwAyEAYTi9PtvXVWepOcLdKgBPe/TcZxat3g4f7nRR6My7QM0=" >> keys/public.pem && \
    echo "-----END PUBLIC KEY-----" >> keys/public.pem

ENV STRAKE_LICENSE_PUBKEY_PATH=/build/keys/public.pem

# Build all binaries with musl for static linking automatically via Alpine
ENV OPENSSL_VENDORED=1
ENV CARGO_TARGET_APPLIES_TO_HOST=false
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static"
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+crt-static"
ENV CC_x86_64_unknown_linux_musl=gcc
ENV CXX_x86_64_unknown_linux_musl=g++
ENV CC_aarch64_unknown_linux_musl=gcc
ENV CXX_aarch64_unknown_linux_musl=g++

RUN if [ "$TARGETARCH" = "arm64" ]; then \
        export TARGET=aarch64-unknown-linux-musl; \
    else \
        export TARGET=x86_64-unknown-linux-musl; \
    fi && \
    cargo build --release --target $TARGET \
    -p strake-cli \
    -p strake-server \
    && cargo build --release --target $TARGET \
    --manifest-path strake-enterprise/Cargo.toml && \
    mkdir -p /out && \
    cp /build/target/$TARGET/release/strake-cli /out/ && \
    cp /build/target/$TARGET/release/strake-server /out/ && \
    cp /build/target/$TARGET/release/strake-enterprise /out/

# Strip binaries to reduce size
RUN strip /out/strake-cli
RUN strip /out/strake-server
RUN strip /out/strake-enterprise

# ===== Runtime Stage =====
FROM scratch

# Copy binaries from builder for the native architecture
COPY --from=builder /out/strake-cli /bin/strake-cli
COPY --from=builder /out/strake-server /bin/strake-server
COPY --from=builder /out/strake-enterprise /bin/strake-enterprise

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
