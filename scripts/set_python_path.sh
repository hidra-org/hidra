SELF_ZERO="$0"
test -n "${BASH_VERSION}" && SELF_ZERO="${BASH_SOURCE[0]}" # Fix when bash is used
SELF_PATH="$(readlink --canonicalize-existing -- "${SELF_ZERO}")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

BASE_DIR=$(dirname "$SELF_DIR")
export PYTHONPATH="${BASE_DIR}"/src/api/python:$PYTHONPATH
export PYTHONPATH="${BASE_DIR}"/src/hidra/sender:$PYTHONPATH
export PYTHONPATH="${BASE_DIR}"/src/hidra/receiver:$PYTHONPATH
export PYTHONPATH="${BASE_DIR}"/src/hidra/hidra_control:$PYTHONPATH
