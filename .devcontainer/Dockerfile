FROM ubuntu:jammy

WORKDIR /root

RUN mkdir -p ./tools/scripts/lib_install
COPY ./tools/scripts/lib_install ./tools/scripts/lib_install

RUN ./tools/scripts/lib_install/linux/install-dev.sh

RUN ./tools/scripts/lib_install/linux/install-lib.sh

# NOTE:
# `task` doesn't have an apt/yum package so we use its install script.
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin

# Remove cached files
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
