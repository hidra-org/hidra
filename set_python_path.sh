CURRENTFILE="$(readlink --canonicalize-existing -- "$0")"
CURRENTDIR="${CURRENTFILE%/*}"
export PYTHONPATH=${CURRENTDIR}/api/python:$PYTHONPATH
export PYTHONPATH=${CURRENTDIR}/app/hidra/:$PYTHONPATH
export PYTHONPATH=${CURRENTDIR}/app/hidra_control/:$PYTHONPATH
