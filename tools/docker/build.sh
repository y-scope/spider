#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

service="${1:?Usage: build.sh <storage|scheduler|worker>}"

remove_temp_file_and_prev_image() {
    rm -f "$temp_iid_file"

    if [[ -z "$new_image_id" ]]; then
        rm -f "$iid_file"
    elif [[ "$prev_image_id" == "$new_image_id" ]]; then
        return
    fi

    [[ -z "$prev_image_id" ]] && return

    docker image inspect "$prev_image_id" >/dev/null 2>&1 || return

    echo "Removing previous image $prev_image_id."
    docker image remove "$prev_image_id" || true
}
trap remove_temp_file_and_prev_image EXIT

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
repo_root="${script_dir}/../../"
iid_file="${repo_root}/build/spider-${service}-image.id"

prev_image_id=""
if [[ -f "$iid_file" ]]; then
    prev_image_id=$(<"$iid_file")
fi

temp_iid_file="$(mktemp)"
new_image_id=""

docker build \
    --pull \
    --target "$service" \
    --iidfile "$temp_iid_file" \
    --file "${script_dir}/Dockerfile" \
    "$repo_root"

if [[ -s "$temp_iid_file" ]]; then
    new_image_id="$(<"$temp_iid_file")"
    echo "$new_image_id" > "$iid_file"

    user="${USER:-$(whoami 2>/dev/null)}" \
        || user=$(id -un 2>/dev/null) \
        || user=$(id -u 2>/dev/null) \
        || user="unknown";
    short_id="${new_image_id#sha256:}"
    short_id="${short_id:0:4}"
    docker tag "$new_image_id" "spider-${service}:dev-${user}-${short_id}"
fi
