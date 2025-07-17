#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

echo "Checking for elevated privileges..."
privileged_command_prefix=""
if [ ${EUID:-$(id -u)} -ne 0 ] ; then
  sudo echo "Script can elevate privileges."
  privileged_command_prefix="${privileged_command_prefix} sudo"
fi
${privileged_command_prefix} apt-get update
DEBIAN_FRONTEND=noninteractive ${privileged_command_prefix} apt-get install --no-install-recommends -y \
    ca-certificates \
    checkinstall \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    jq \
    libcurl4 \
    libcurl4-openssl-dev \
    libmariadb-dev \
    libssl-dev \
    make \
    openjdk-11-jdk \
    pkg-config \
    python3 \
    python3-pip \
    python3-venv

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
lib_install_scripts_dir="$script_dir/.."
# TODO https://github.com/y-scope/spider/issues/86
"$lib_install_scripts_dir"/check-cmake-version.sh

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
