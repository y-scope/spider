FROM ubuntu:focal

WORKDIR /root

RUN mkdir -p ./tools/scripts/lib_install
COPY ./tools/scripts/lib_install ./tools/scripts/lib_install

RUN ./tools/scripts/lib_install/linux/install-dev.sh

# Set cpp, cc, and c++ to v10
RUN update-alternatives --set cc /usr/bin/gcc-10
RUN update-alternatives --set c++ /usr/bin/g++-10
RUN update-alternatives --set cpp /usr/bin/cpp-10

RUN ./tools/scripts/lib_install/linux/install-lib.sh

# NOTE:
# 1. `task` doesn't have an apt/yum package so we use its install script.
# 2. We don't want to install it using `install-prebuilt-packages.sh` since users may use that on
#    their own machines and it would change their environment in a way that can't easily be undone.
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin

# Remove cached files
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
