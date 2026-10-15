#!/usr/bin/env bash

# Installs the dev dependencies for building, testing, and linting the C++ TDL compiler.

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

DEBIAN_FRONTEND=noninteractive ${privileged_command_prefix} \
apt-get install --no-install-recommends -y \
    g++ \
    gcc \
    jq \
    libssl-dev \
    make \
    openjdk-11-jdk

lib_install_scripts_dir="$script_dir/.."
${privileged_command_prefix} "$lib_install_scripts_dir"/install-cmake.sh 3.23.5
# TODO https://github.com/y-scope/spider/issues/86
"$lib_install_scripts_dir"/check-cmake-version.sh
