FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /src/target/release/tuber /usr/local/bin/tuber
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD tuber stats >/dev/null 2>&1 || exit 1
ENTRYPOINT ["tuber"]
