#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
lib_install_scripts_dir=$script_dir/..

"$lib_install_scripts_dir"/lib_install/fmtlib.sh 11.0.2
"$lib_install_scripts_dir"/lib_install/spdlog.sh 1.14.1
"$lib_install_scripts_dir"/lib_install/mariadb-connector-c.sh 3.4.1
