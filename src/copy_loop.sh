#/bin/sh

BASEPATH=/space/projects/live-viewer
#BASEPATH=/home/p11user/live-viewer

TARGET=${BASEPATH}/data/source/local
LIMIT=10

usage() { echo "Usage: $0 [-f <cbf|tif>] [-t <targetpath>] [-n <number of files>]" 1>&2; exit 1; }


while getopts ':f:t:n:' OPTION ; do
    case "${OPTION}" in
        f) FORMAT=${OPTARG}
            if [ "${FORMAT}" != "cbf" ] && [ "${FORMAT}" != "tif" ]; then usage; fi
#           ((${FORMAT} == "cbf" || ${FORMAT} == "tif")) || usage
            ;;
        t) TARGET=${OPTARG}
            if [ ! -d ${TARGET} ]; then echo "${TARGET} does not exist"; exit 1; fi
            ;;
        n) LIMIT=${OPTARG}
            if echo ${LIMIT} | grep -q '^[0-9]+$'; then echo "${LIMIT} is not a number"; exit 1; fi
            ;;
        *) usage
    esac
done
shift $((OPTIND-1))


#if [ -z "${t}" ] || [ -z "${p}" ]; then
if [ -z "${FORMAT}" ]; then
    usage
fi

case "${FORMAT}" in
    cbf) FILES=${BASEPATH}/test_015_00001.cbf ;;
    tif) FILES=${BASEPATH}/bf_00000.tif
esac

i=1

while [ "$i" -le $LIMIT ]
do
    TARGET_FILE="$TARGET/$i.$FORMAT"
    echo $TARGET_FILE
    cp $FILES "$TARGET_FILE"
    i=$(($i+1))
done
