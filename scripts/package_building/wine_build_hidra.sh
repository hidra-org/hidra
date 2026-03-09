#!/bin/bash
set -uex

HIDRA_DIR=`pwd`

docker build -f "${HIDRA_DIR}/scripts/package_building/Dockerfile.build_wine" "${HIDRA_DIR}/scripts/package_building" -t wine_build

docker run --rm -v "${HIDRA_DIR}":/hidra wine_build \
    bash -c '
        cd /hidra \
        && git config --global --add safe.directory /hidra \
        && bash scripts/package_building/do_windows_build_hidra.sh
    '
