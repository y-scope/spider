# syntax=docker/dockerfile:1

ARG UBUNTU_VERSION_CODENAME=jammy

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS builder

WORKDIR /spider
COPY . .

RUN tools/scripts/lib_install/ubuntu/install-dev-huntsman.sh
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
RUN task build:rust

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS runtime-base
RUN useradd --create-home spider-user
USER spider-user

FROM runtime-base AS storage
COPY --from=builder /spider/build/rust-targets/release/spider_storage_grpc_server /usr/local/bin/

FROM runtime-base AS scheduler
COPY --from=builder /spider/build/rust-targets/release/spider_scheduler_grpc_server /usr/local/bin/

FROM runtime-base AS worker
COPY --from=builder /spider/build/rust-targets/release/spider_execution_manager /usr/local/bin/
COPY --from=builder /spider/build/rust-targets/release/spider-task-executor /usr/local/bin/
