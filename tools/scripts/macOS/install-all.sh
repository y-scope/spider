#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

brew update
brew install \
  boost \
  cmake \
  coreutils \
  fmt \
  gcc \
  go-task \
  mariadb-connector-c \
  spdlog \
  pkg-config