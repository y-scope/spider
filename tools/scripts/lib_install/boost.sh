#!/usr/bin/env bash

# Exit on error
set -e

cUsage="Usage: ${BASH_SOURCE[0]} <version>"
if [ "$#" -lt 1 ] ; then
    echo $cUsage
    exit
fi
version=$1
version_with_underscores=${version//./_}

echo "Checking for elevated privileges..."
privileged_command_prefix=""
if [ ${EUID:-$(id -u)} -ne 0 ] ; then
  sudo echo "Script can elevate privileges."
  privileged_command_prefix="${privileged_command_prefix} sudo"
fi

# Get number of cpu cores
num_cpus=$(grep -c ^processor /proc/cpuinfo)

package_name=boost

# Create temp dir for installation
temp_dir=/tmp/${package_name}-installation
mkdir -p $temp_dir

cd $temp_dir

# Download source
echo "Downloading boost source..."
tar_filename=boost_${version_with_underscores}.tar.gz
curl -fsSL https://archives.boost.io/release/${version}/source/${tar_file} -o ${tar_filename}
echo "Extracting boost source..."
tar xzf ${tar_filename}
echo "Extracted boost source"
cd boost_${version_with_underscores}

# Build
./bootstrap.sh --with-libraries=filesystem,iostreams,process,program_options,regex,system
./b2 -j${num_cpus}

# Install
${privileged_command_prefix} ./b2 install

# Clean up
${privileged_command_prefix} rm -rf $temp_dir