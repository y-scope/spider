# syntax=docker/dockerfile:1

ARG UBUNTU_VERSION_CODENAME=jammy

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS builder

ARG GO_TASK_VERSION=3.49.1

WORKDIR /spider

COPY --link Cargo.toml Cargo.lock taskfile.yaml ./
COPY --link .cargo ./.cargo
COPY --link components ./components
COPY --link examples/huntsman ./examples/huntsman
COPY --link taskfiles ./taskfiles
COPY --link tests/huntsman ./tests/huntsman
COPY --link tools/scripts ./tools/scripts
COPY --link tools/yscope-dev-utils ./tools/yscope-dev-utils

RUN tools/scripts/lib_install/ubuntu/install-dev-huntsman.sh
RUN sh -c "$(curl --fail --location --silent --show-error https://taskfile.dev/install.sh)" -- \
    -d -b /usr/local/bin "v${GO_TASK_VERSION}"
RUN task build:rust

FROM ubuntu:${UBUNTU_VERSION_CODENAME} AS runtime-base
RUN useradd --create-home spider-user
USER spider-user

FROM runtime-base AS storage
COPY --from=builder --link /spider/build/rust-targets/release/spdier-storage /usr/local/bin/

FROM runtime-base AS scheduler
COPY --from=builder --link /spider/build/rust-targets/release/spider-scheduler /usr/local/bin/

FROM runtime-base AS worker
COPY --from=builder --link /spider/build/rust-targets/release/spider-execution-manager /usr/local/bin/
COPY --from=builder --link /spider/build/rust-targets/release/spider-task-executor /usr/local/bin/
