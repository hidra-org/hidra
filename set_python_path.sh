CURRENTFILE="$(readlink --canonicalize-existing -- "$0")"
CURRENTDIR="${CURRENTFILE%/*}"
export PYTHONPATH=${CURRENTDIR}/src/APIs:$PYTHONPATH
