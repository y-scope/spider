# syntax=docker/dockerfile:1

ARG UBUNTU_VERSION_CODENAME=jammy

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS builder

WORKDIR /spider
COPY . .

RUN tools/scripts/lib_install/ubuntu/install-dev-huntsman.sh
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
RUN task build:rust

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS storage
COPY --from=builder /spider/build/rust-targets/release/spider_storage_grpc_server /usr/local/bin/

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS scheduler
COPY --from=builder /spider/build/rust-targets/release/spider_scheduler_grpc_server /usr/local/bin/

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS execution-manager-and-task-executor
COPY --from=builder /spider/build/rust-targets/release/spider_execution_manager /usr/local/bin/
COPY --from=builder /spider/build/rust-targets/release/spider-task-executor /usr/local/bin/
