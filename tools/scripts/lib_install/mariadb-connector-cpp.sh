#!/bin/bash

# Dependencies:
# - cmake
# - g++
# - git
# NOTE: Dependencies should be installed outside the script to allow the script to be largely distro-agnostic

# Exit on any error
set -e

cUsage="Usage: ${BASH_SOURCE[0]} <version>[ <.deb output directory>]"
if [ "$#" -lt 1 ] ; then
    echo $cUsage
    exit
fi
version=$1

package_name=mariadb-connector-cpp
temp_dir=/tmp/${package_name}-installation
deb_output_dir=${temp_dir}
if [[ "$#" -gt 1 ]] ; then
  deb_output_dir="$(readlink -f "$2")"
  if [ ! -d ${deb_output_dir} ] ; then
    echo "${deb_output_dir} does not exist or is not a directory"
    exit
  fi
fi

# Check if already installed
set +e
dpkg -l ${package_name} | grep ${version}
installed=$?
set -e
if [ $installed -eq 0 ] ; then
  # Nothing to do
  exit
fi

# Get number of cpu cores
if [ "$(uname -s)" == "Darwin" ]; then
  num_cpus=$(sysctl -n hw.ncpu)
else
  num_cpus=$(grep -c ^processor /proc/cpuinfo)
fi

echo "Checking for elevated privileges..."
privileged_command_prefix=""
if [ ${EUID:-$(id -u)} -ne 0 ] ; then
  sudo echo "Script can elevate privileges."
  privileged_command_prefix="${privileged_command_prefix} sudo"
fi

# Download
mkdir -p $temp_dir
cd $temp_dir
git clone https://github.com/mariadb-corporation/mariadb-connector-cpp.git "mariadb-connector-cpp-${version}"
cd "mariadb-connector-cpp-${version}"
git checkout "${version}"

# Build
mkdir build
cd build
# Setting USE_SYSTEM_INSTALLED_LIB mess up the install prefix, so set it manually
cmake -DUSE_SYSTEM_INSTALLED_LIB=ON -DCMAKE_INSTALL_LIBDIR=/usr/local -DINSTALL_LAYOUT=RPM ..
make -j${num_cpus}

# Install
install_command_prefix="${privileged_command_prefix}"
${install_command_prefix} make install

# Clean up
rm -rf $temp_dir