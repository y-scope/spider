#!/usr/bin/env bash

# Single-node kind cluster set-up for testing the Helm chart
# TODO: Submit a job through the deployed stack once an end-to-end test scenario is available.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-spider-test}"
RELEASE_NAME="${RELEASE_NAME:-test}"

# shellcheck source=.set-up-common.sh
source "${script_dir}/.set-up-common.sh"

parse_common_args "$@"

echo "=== Single-node setup ==="
echo "Cluster: ${CLUSTER_NAME}"
echo ""

prepare_environment "${CLUSTER_NAME}"

echo "Creating kind cluster..."
kind create cluster --name "${CLUSTER_NAME}"

echo "Installing Helm chart..."
# Word splitting is intentional: get_image_helm_args returns multiple --set flags.
# shellcheck disable=SC2046
helm install "${RELEASE_NAME}" "${script_dir}" \
    $(get_image_helm_args "${CLUSTER_NAME}" "storage" "${STORAGE_IMAGE}") \
    $(get_image_helm_args "${CLUSTER_NAME}" "scheduler" "${SCHEDULER_IMAGE}") \
    $(get_image_helm_args "${CLUSTER_NAME}" "worker" "${WORKER_IMAGE}")

wait_for_pods 300 5 5
