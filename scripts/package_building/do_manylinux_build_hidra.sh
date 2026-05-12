set -uex

#!/bin/bash

set -uex

cd /hidra

if git -C desy show-ref --verify --quiet refs/heads/main; then
    # The DESY submodule exists
    # TODO: Check that repo is clean
    cp "desy/src/api/python/hidra/constants.py" \
       "src/api/python/hidra/constants.py"
fi

# freeze
PYBIN=/opt/python/cp37-cp37m/bin/python
$PYBIN -m pip install patchelf==0.17.2.1  # cx_freeze 6.15 requires patchelf >=0.14
rm /usr/local/bin/patchelf
ln -s /opt/_internal/cpython-3.7.10/bin/patchelf /usr/local/bin/patchelf
$PYBIN -m pip install cx_freeze==6.15
$PYBIN -m pip install --prefer-binary -r requirements.txt

# build inotifyx
git clone https://github.com/hidra-org/hidra-dependencies.git
pushd hidra-dependencies/inotifyx
    patch -ruN -p1 -d inotifyx-0.2.2 < 0001-python3-compatibility.patch
    patch -ruN -p1 -d inotifyx-0.2.2 < 0002-update-C-binding-for-python3.patch
    $PYBIN -m pip install ./inotifyx-0.2.2
popd


$PYBIN freeze_setup.py build

## set rpath to fix library paths (old rpath is "${ORIGIN}:${ORIGIN}/../lib")
# for file in build/exe.linux-x86_64-2.7/{datamanager,get_receiver_status,getsettings}; do
#     /usr/local/bin/patchelf --set-rpath '${ORIGIN}:${ORIGIN}/../lib:${ORIGIN}/lib' ${file}
# done
# zlib.so is dynamically linked against libpython but cx_freeze does not care
# set rpath tp workaround this
# /usr/local/bin/patchelf --set-rpath '${ORIGIN}' build/exe.linux-x86_64-3.7/lib/zlib.so

# package
HIDRA_DIR=$(pwd)
. "${HIDRA_DIR}/scripts/package_building/build_utils.sh"
get_hidra_version

rm -rf "${HIDRA_DIR}/build/hidra"
mv "${HIDRA_DIR}/build/exe.linux-x86_64-3.7" "${HIDRA_DIR}/build/hidra"
mkdir -p "${HIDRA_DIR}/build/freeze"
pushd "${HIDRA_DIR}/build"
tar -czf freeze/hidra-${HIDRA_VERSION}-x86_64-3.7-manylinux1.tar.gz hidra
popd
