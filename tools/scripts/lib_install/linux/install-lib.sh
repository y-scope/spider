#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

# Install libmariadb
echo "Checking for elevated privileges..."
privileged_command_prefix=""
if [ ${EUID:-$(id -u)} -ne 0 ] ; then
  sudo echo "Script can elevate privileges."
  privileged_command_prefix="${privileged_command_prefix} sudo"
fi
${privileged_command_prefix} apt-get update
DEBIAN_FRONTEND=noninteractive ${privileged_command_prefix} apt-get install --no-install-recommends -y \
    libmariadb-dev

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
lib_install_scripts_dir=$script_dir/..

# TODO https://github.com/y-scope/spider/issues/86
"$lib_install_scripts_dir"/check-cmake-version.sh

"$lib_install_scripts_dir"/boost.sh 1.86.0
