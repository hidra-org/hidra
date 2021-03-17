set -uex

vagrant rsync

vagrant ssh -c "
  cd /hidra \
  && bash scripts/package_building/manylinux_build_hidra.sh
"

HIDRA_DIR=$(pwd)/../..
. ${HIDRA_DIR}/scripts/package_building/build_utils.sh

get_hidra_version

filename=hidra-${HIDRA_VERSION}-x86_64-3.7-manylinux1.tar.gz

mkdir -p ${HIDRA_DIR}/build/freeze

vagrant ssh -c "ls -l /hidra/build/freeze/*"

vagrant ssh -c "
        cat /hidra/build/freeze/${filename}
    " \
    -- -T  > ${HIDRA_DIR}/build/freeze/${filename}
