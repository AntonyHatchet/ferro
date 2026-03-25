FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/* && \
    rustup target add aarch64-unknown-linux-musl x86_64-unknown-linux-musl

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

ARG TARGETARCH
RUN case "$TARGETARCH" in \
      arm64) TARGET=aarch64-unknown-linux-musl ;; \
      *)     TARGET=x86_64-unknown-linux-musl ;; \
    esac && \
    cargo build --release --bin ferro --target "$TARGET" && \
    cp target/"$TARGET"/release/ferro /build/ferro

FROM alpine:3.21

RUN apk add --no-cache bash ca-certificates curl

COPY --from=builder /build/ferro /usr/local/bin/ferro

RUN mkdir -p /etc/ferro/init/ready.d \
             /etc/ferro/init/boot.d \
             /etc/ferro/init/start.d \
             /etc/ferro/init/shutdown.d \
             /var/lib/ferro

ENV GATEWAY_LISTEN=:4566
ENV FERRO_INIT_DIR=/etc/ferro/init
ENV FERRO_DATA_DIR=/var/lib/ferro

VOLUME ["/etc/ferro/init", "/var/lib/ferro"]
EXPOSE 4566

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://127.0.0.1:4566/_ferro/health || exit 1

ENTRYPOINT ["ferro"]
