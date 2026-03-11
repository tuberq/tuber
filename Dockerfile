FROM rust:1.84-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/tuber /usr/local/bin/tuber
EXPOSE 11300
ENTRYPOINT ["tuber"]
CMD ["-l", "0.0.0.0", "-p", "11300", "-V"]
