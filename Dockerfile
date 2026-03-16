FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /src/target/release/tuber /usr/local/bin/tuber
ENTRYPOINT ["tuber"]
