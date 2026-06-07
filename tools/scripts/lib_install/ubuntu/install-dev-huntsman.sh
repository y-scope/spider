#!/usr/bin/env bash

# Installs the dev dependencies for Spider Huntsman.

# Exit on any error
set -e

# Error on undefined variable
set -u

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
"$script_dir"/install-dev-common.sh

echo "Checking for elevated privileges..."
privileged_command_prefix=""
if [ ${EUID:-$(id -u)} -ne 0 ] ; then
  sudo echo "Script can elevate privileges."
  privileged_command_prefix="${privileged_command_prefix} sudo"
fi

# `gcc` and `libc6-dev` are required by `rustc`, which invokes the system C compiler driver to
# link binaries against libc.
DEBIAN_FRONTEND=noninteractive ${privileged_command_prefix} \
apt-get install --no-install-recommends -y \
    gcc \
    libc6-dev
