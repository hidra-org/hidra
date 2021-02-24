set -uex

#!/bin/bash

set -uex

cd /hidra

if git show-ref --verify --quiet refs/heads/local_patches; then
    # a branch named local_patches exists locally
    # see https://stackoverflow.com/q/5167957
    CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
    git checkout local_patches
    git rebase "${CURRENT_BRANCH}"
fi

# freeze
PYBIN=/opt/python/cp27-cp27mu/bin/python
$PYBIN -m pip install cx_freeze==5.1.1
$PYBIN -m pip install -r requirements.txt
$PYBIN freeze_setup.py build

## set rpath to fix library paths (old rpath is "${ORIGIN}:${ORIGIN}/../lib")
# for file in build/exe.linux-x86_64-2.7/{datamanager,get_receiver_status,getsettings}; do
#     /usr/local/bin/patchelf --set-rpath '${ORIGIN}:${ORIGIN}/../lib:${ORIGIN}/lib' ${file}
# done
# zlib.so is dynamically linked against libpython but cx_freeze does not care
# set rpath tp workaround this
/usr/local/bin/patchelf --set-rpath '${ORIGIN}' build/exe.linux-x86_64-2.7/lib/zlib.so

if git show-ref --verify --quiet refs/heads/local_patches; then
    git checkout "${CURRENT_BRANCH}"
fi


# package
HIDRA_DIR=$(pwd)
. ${HIDRA_DIR}/scripts/package_building/build_utils.sh
get_hidra_version

rm -rf ${HIDRA_DIR}/build/hidra
mv ${HIDRA_DIR}/build/exe.linux-x86_64-2.7 ${HIDRA_DIR}/build/hidra
mkdir -p ${HIDRA_DIR}/build/freeze
pushd ${HIDRA_DIR}/build
tar -czf freeze/hidra-${HIDRA_VERSION}-x86_64-2.7-manylinux1.tar.gz hidra
popd
