#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

brew update
brew install \
  boost \
  coreutils \
  fmt \
  mariadb-connector-c \
  spdlog \
  pkg-config