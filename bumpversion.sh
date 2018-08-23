#!/bin/sh

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
bumpversion ${RELEASE} ${DRYRUN} --allow-dirty

# fix manually because bumversion does not work properly with %z
echo "Fix timezone"
current_time=$(date "+%a, %d %b %Y %H")
timezone=$(date "+%z")
sed -i -e "s/$current_time\(:[0-9][0-9]:[0-9][0-9]\) $/$current_time\1 $timezone/g" debian/changelog

#TODO add to git via bumpversion or
#git add -u

# VERSION=$(cat ./src/shared/_version.py)
# remove "__version__ = '" at the beginning and "'" at the end
# VERSION=${VERSION:15:-1}
# $BRANCH=$(git branch | sed -n '/\* /s///p')
# git checkout master
# git pull
# git merge --no-ff ${BRANCH}
# git tag -a v${VERSION} -m "Version ${VERSION}"
# git checkout develop
# git pull
# git merge --no-ff ${BRANCH}
