set -uex

HIDRA_DIR=`pwd`

# docker pull schooft/manylinux_freeze:3.7

docker run --rm -v ${HIDRA_DIR}:/hidra schooft/manylinux_freeze:3.7 \
    bash -c '
        cd /hidra \
        && bash scripts/package_building/do_manylinux_build_hidra.sh
    '