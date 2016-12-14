#/bin/sh

usage() { echo "Usage: $0 [-r <major|minor|patch>] [-d]" 1>&2; exit 1; }

while getopts 'r:d' OPTION ; do
    case "${OPTION}" in
        r) RELEASE=${OPTARG}
            if [ "${RELEASE}" != "major" ] && [ "${RELEASE}" != "minor" ] && [ "${RELEASE}" != "patch" ]
                then
                    echo "${RELEASE} not supported"
                    exit 1
            fi
            ;;
        d) DRYRUN="--dry-run --verbose"
            ;;
        *) usage
    esac
done
shift $((OPTIND-1))

if [ -z "${RELEASE}" ]; then
    usage
fi

bumpversion ${RELEASE} ${DRYRUN} --config-file .bumpversion_prework.cfg
#git add -u
bumpversion ${RELEASE} ${DRYRUN} --allow-dirty
