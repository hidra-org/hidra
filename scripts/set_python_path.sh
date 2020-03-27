CURRENTFILE="$(readlink --canonicalize-existing -- "$0")"
CURRENTDIR="${CURRENTFILE%/*}"
BASEDIR=$(dirname "$CURRENTDIR")
export PYTHONPATH=${BASEDIR}/src/api/python:$PYTHONPATH
export PYTHONPATH=${BASEDIR}/src/hidra/sender:$PYTHONPATH
export PYTHONPATH=${BASEDIR}/src/hidra/receiver:$PYTHONPATH
export PYTHONPATH=${BASEDIR}/src/hidra/hidra_control:$PYTHONPATH
