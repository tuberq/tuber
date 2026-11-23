FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /src/target/release/tuber /usr/local/bin/tuber
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD tuber stats -a "127.0.0.1:${TUBER_PORT:-11300}" >/dev/null 2>&1 || exit 1
ENTRYPOINT ["tuber"]
