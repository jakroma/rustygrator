FROM rust:1-bookworm AS builder
RUN USER=root cargo new --bin rustygrator
WORKDIR /rustygrator
COPY ./ ./
RUN cargo build --release

FROM ubuntu:latest
COPY --from=builder /rustygrator/target/release/rustygrator /usr/local/bin/rustygrator

CMD ["rustygrator"]