#!/bin/bash
# This script builds Hidra for Windows. It can be executed on Windows by "Git
# Bash" or on Linux with Python installed under wine. It should be run in a
# dedicated Python virtual environment.

set -uex

# Detect the environment and set the Python binary accordingly
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    PYBIN=python
    zip=(/c/Program\ Files/7-Zip/7z.exe a)
else
    PYBIN="wine python"
    zip=("zip" -r)
fi

py_ver=$($PYBIN -c 'import platform; print(platform.python_version())')
py_ver=${py_ver%.*}
[[ $py_ver =~ 3\.(7|8|9|10|11|12) ]] || exit 1

if git -C desy show-ref --verify --quiet refs/heads/main; then
    # The DESY submodule exists
    # TODO: Check that repo is clean
    cp "desy/src/api/python/hidra/constants.py" \
       "src/api/python/hidra/constants.py"
fi
# freeze

$PYBIN -m pip install cx_freeze
$PYBIN -m pip install --prefer-binary -r win-requirements.txt

$PYBIN freeze_setup.py build

# package
HIDRA_DIR="$(pwd)"
. "${HIDRA_DIR}"/scripts/package_building/build_utils.sh
get_hidra_version

rm -rf "${HIDRA_DIR}"/build/hidra
mv "${HIDRA_DIR}"/build/exe.win-amd64-${py_ver} "${HIDRA_DIR}"/build/hidra
mkdir -p "${HIDRA_DIR}"/build/freeze
pushd "${HIDRA_DIR}"/build
rm -f freeze/hidra-${HIDRA_VERSION}-amd64-${py_ver}-win.zip
"${zip[@]}" freeze/hidra-${HIDRA_VERSION}-amd64-${py_ver}-win.zip hidra
popd
