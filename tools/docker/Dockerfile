# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.96
ARG DEBIAN_VERSION_CODENAME=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_VERSION_CODENAME} AS builder

WORKDIR /spider
COPY . .

RUN cargo build --release \
    --bin spider_storage_grpc_server \
    --bin spider_scheduler_grpc_server \
    --bin spider_execution_manager \
    --bin spider-task-executor

FROM debian:${DEBIAN_VERSION_CODENAME}-slim AS storage
COPY --from=builder /spider/target/release/spider_storage_grpc_server /usr/local/bin/

FROM debian:${DEBIAN_VERSION_CODENAME}-slim AS scheduler
COPY --from=builder /spider/target/release/spider_scheduler_grpc_server /usr/local/bin/

FROM debian:${DEBIAN_VERSION_CODENAME}-slim AS execution-manager-and-task-executor
COPY --from=builder /spider/target/release/spider_execution_manager /usr/local/bin/
COPY --from=builder /spider/target/release/spider-task-executor /usr/local/bin/
