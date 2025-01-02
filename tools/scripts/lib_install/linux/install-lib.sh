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

"$lib_install_scripts_dir"/fmtlib.sh 11.0.2
"$lib_install_scripts_dir"/spdlog.sh 1.15.0
"$lib_install_scripts_dir"/mariadb-connector-cpp.sh 1.1.5
"$lib_install_scripts_dir"/boost.sh 1.86.0
"$lib_install_scripts_dir"/msgpack.sh 7.0.0
