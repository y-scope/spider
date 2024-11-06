FROM ubuntu:jammy

WORKDIR /root

RUN mkdir -p ./tools/scripts/lib_install
COPY ./tools/scripts/lib_install ./tools/scripts/lib_install

RUN ./tools/scripts/lib_install/linux/install-dev.sh

RUN ./tools/scripts/lib_install/linux/install-lib.sh

# NOTE:
# - `task` doesn't have an apt/yum package so we use its install script.
# - We lock task's version since v3.40.0 and higher change the behaviour of how undefined variables
#   are treated in `ref` statements, causing yscope-dev-utils' `validate-checksum` task to fail.
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin v3.39.2

# Remove cached files
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
