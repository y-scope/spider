#!/usr/bin/env bash

# Resolves the endpoint the Huntsman e2e test driver should use to reach `spider-storage`.
#
# Self-hosted runners mount the host's Docker socket, so containers started by `docker compose`
# are siblings on the host rather than children of the runner: a port `docker compose` publishes
# lands in the host's network namespace, unreachable from here. When that's detected, `connect`
# instead joins the runner into the Compose stack's own network, so its embedded DNS resolves
# `spider-storage` directly and host-port publishing is bypassed entirely. `disconnect` undoes
# that join during cleanup, before the network itself is removed.
#
# GitHub-hosted runners and local dev machines aren't containers on the daemon's own host, so
# detection fails there and the existing published-port behavior is used unchanged.
#
# Usage:
#   resolve-huntsman-e2e-endpoint.sh connect <compose-network> <storage-port> <published-port>
#   resolve-huntsman-e2e-endpoint.sh disconnect <compose-network>

# Exit on any error
set -e

# Error on undefined variable
set -u

mode="$1"
compose_network="$2"

# Docker defaults a container's hostname to its ID, and Compose sets it to the container's name;
# the daemon resolves either. This is empty (not an error) when we're not running inside a
# container the daemon can resolve, e.g. on a GitHub-hosted runner or a dev machine.
runner_id=""
if [ -f /.dockerenv ]; then
  runner_id="$(docker inspect -f '{{.Id}}' "$(hostname)" 2>/dev/null)" || runner_id=""
fi

case "$mode" in
  connect)
    storage_port="$3"
    published_port="$4"
    if [ -n "$runner_id" ]; then
      docker network connect "$compose_network" "$runner_id"
      echo "http://spider-storage:${storage_port}"
    else
      echo "http://127.0.0.1:${published_port}"
    fi
    ;;
  disconnect)
    if [ -n "$runner_id" ]; then
      # Safe to ignore failures: this is a no-op if `connect` never ran (e.g. GitHub-hosted
      # runners and dev machines), and cleanup shouldn't fail the task over it either way.
      docker network disconnect "$compose_network" "$runner_id" 2>/dev/null || true
    fi
    ;;
  *)
    echo "Unknown mode: '${mode}'. Expected 'connect' or 'disconnect'." >&2
    exit 1
    ;;
esac
