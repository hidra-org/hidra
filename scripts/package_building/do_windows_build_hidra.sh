#!/bin/bash
# This script builds Hidra for Windows. It can be executed by Git Bash and
# should be run in a dedicated Python virtual environment.

set -uex

PYBIN=python
py_ver=$($PYBIN -c 'import platform; print(platform.python_version())')
py_ver=${py_ver%.*}
[[ $py_ver =~ 3\.(7|8|9|10|11|12) ]] || exit 1

zip=/c/Program\ Files/7-Zip/7z.exe

if git show-ref --verify --quiet refs/heads/local_patches; then
    # a branch named local_patches exists locally
    # see https://stackoverflow.com/q/5167957
    CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
    git checkout local_patches
    git rebase "${CURRENT_BRANCH}"
fi

# freeze

$PYBIN -m pip install cx_freeze
$PYBIN -m pip install --prefer-binary -r win-requirements.txt

$PYBIN freeze_setup.py build


if git show-ref --verify --quiet refs/heads/local_patches; then
    git checkout "${CURRENT_BRANCH}"
fi


# package
HIDRA_DIR="$(pwd)"
. "${HIDRA_DIR}"/scripts/package_building/build_utils.sh
get_hidra_version

rm -rf "${HIDRA_DIR}"/build/hidra
mv "${HIDRA_DIR}"/build/exe.win-amd64-${py_ver} "${HIDRA_DIR}"/build/hidra
mkdir -p "${HIDRA_DIR}"/build/freeze
pushd "${HIDRA_DIR}"/build
rm -f freeze/hidra-${HIDRA_VERSION}-amd64-${py_ver}-win.zip
"$zip" a freeze/hidra-${HIDRA_VERSION}-amd64-${py_ver}-win.zip hidra
popd
