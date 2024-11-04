#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    ca-certificates \
    checkinstall \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    libcurl4 \
    libcurl4-openssl-dev \
    libssl-dev \
    make \
    pkg-config \
    python3 \
    python3-pip \
    python3-venv
