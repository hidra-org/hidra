#/bin/sh

SOURCE=/opt/HiDRA

TARGET=${SOURCE}/data/source/local
LIMIT=10

usage() { echo "Usage: $0 [-f <cbf|tif>] [-s <sourcepath>] [-t <targetpath>] [-n <number of files>]" 1>&2; exit 1; }


while getopts ':f:s:t:n:' OPTION ; do
    case "${OPTION}" in
        f) FORMAT=${OPTARG}
            if [ "${FORMAT}" != "cbf" ] && [ "${FORMAT}" != "tif" ]; then echo "${FORMAT} not supported"; exit 1; fi
#           ((${FORMAT} == "cbf" || ${FORMAT} == "tif")) || usage
            ;;
        s) SOURCE=${OPTARG}
            if [ ! -d ${SOURCE} ]; then echo "${SOURCE} does not exist"; exit 1; fi
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
    cbf) FILES=${SOURCE}/test_file.cbf ;;
    tif) FILES=${SOURCE}/bf_00000.tif
esac

i=1

while [ "$i" -le $LIMIT ]
do
    TARGET_FILE="$TARGET/$i.$FORMAT"
    echo $TARGET_FILE
    cp $FILES "$TARGET_FILE"
    i=$(($i+1))
done
