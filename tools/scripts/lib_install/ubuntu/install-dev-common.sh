#!/usr/bin/env bash

# Installs the dev dependencies shared by all versions of Spider.

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
DEBIAN_FRONTEND=noninteractive ${privileged_command_prefix} \
apt-get install --no-install-recommends -y \
    ca-certificates \
    curl \
    git \
    python3 \
    python3-pip \
    python3-venv

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
