#!/usr/bin/env bash

# Common utilities for Helm chart set-up scripts
# Source this file from set-up-*.sh scripts

set -o errexit
set -o nounset
set -o pipefail

# Cleans up existing cluster and prepares environment
#
# @param {string} cluster_name Name of the kind cluster
prepare_environment() {
    local cluster_name=$1

    echo "Deleting existing cluster if present..."
    kind delete cluster --name "${cluster_name}" 2>/dev/null || true
}

# Loads a local Docker image into the kind cluster and returns the helm --set
# flags for using it. If image is not specified, returns empty string.
#
# @param {string} cluster_name Name of the kind cluster
# @param {string} component Image component name (e.g., "storage", "scheduler", "worker")
# @param {string} [image] Docker image (e.g., "spider-worker:dev")
# @return Prints helm --set flags to stdout
get_image_helm_args() {
    local cluster_name=$1
    local component=$2
    local image="${3:-}"

    if [[ -z "${image}" ]]; then
        return
    fi

    echo "Loading local image '${image}' into kind cluster..." >&2
    kind load docker-image "${image}" --name "${cluster_name}" >&2

    # Split "repo:tag" on the last colon whose right-hand side contains no '/'
    # (so registry ports like localhost:5000/repo are not mistaken for tags).
    if [[ "${image}" =~ ^(.+):([^:/]+)$ ]]; then
        local repo="${BASH_REMATCH[1]}"
        local tag="${BASH_REMATCH[2]}"
    else
        echo "Error: '${image}' is not a valid image reference (expected repo:tag)." >&2
        return 1
    fi
    echo "--set" "image.${component}.repository=${repo}" \
        "--set" "image.${component}.tag=${tag}" \
        "--set" "image.${component}.pullPolicy=Never"
}

# Parses common arguments shared across set-up scripts.
# Sets STORAGE_IMAGE, SCHEDULER_IMAGE, and WORKER_IMAGE global variables.
#
# @param {string[]} args Script arguments
parse_common_args() {
    STORAGE_IMAGE=""
    SCHEDULER_IMAGE=""
    WORKER_IMAGE=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --storage-image | --scheduler-image | --worker-image)
                if [[ $# -lt 2 || "$2" == --* ]]; then
                    echo "Error: '$1' requires a value." >&2
                    exit 1
                fi
                case "$1" in
                    --storage-image) STORAGE_IMAGE="$2" ;;
                    --scheduler-image) SCHEDULER_IMAGE="$2" ;;
                    --worker-image) WORKER_IMAGE="$2" ;;
                esac
                shift 2
                ;;
            *)
                echo "Unknown argument: $1" >&2
                exit 1
                ;;
        esac
    done
}

# Waits for all pods to be ready.
#
# NOTE: The Spider services fail fast when their dependencies (e.g., the database) are unreachable,
# so pods may go through a few restarts before the whole deployment converges.
#
# @param {int} timeout_seconds Overall timeout in seconds
# @param {int} poll_interval_seconds Interval between status checks
# @param {int} wait_timeout_seconds Timeout for each kubectl wait call
# @return {int} 0 on success, 1 on timeout
wait_for_pods() {
    local timeout_seconds=$1
    local poll_interval_seconds=$2
    local wait_timeout_seconds=$3

    echo "Waiting for all pods to be ready" \
        "(timeout=${timeout_seconds}s, poll=${poll_interval_seconds}s," \
        "wait=${wait_timeout_seconds}s)..."

    # Reset bash built-in SECONDS counter
    SECONDS=0

    while true; do
        sleep "${poll_interval_seconds}"
        kubectl get pods

        if kubectl wait pods \
            --all \
            --for=condition=Ready \
            --timeout="${wait_timeout_seconds}s" 2>/dev/null; then
            echo "All pods are ready."
            return 0
        fi

        if [[ ${SECONDS} -ge ${timeout_seconds} ]]; then
            echo "ERROR: Timed out waiting for pods to be ready"
            return 1
        fi

        echo "---"
    done
}
