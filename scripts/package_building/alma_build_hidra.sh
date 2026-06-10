#!/bin/bash
set -ex

get_hidra_version()
{
    NEXT_HIDRA_VERSION=$(cat ${HIDRA_DIR}/src/api/python/hidra/utils/_version.py)

    # cut of the first characters
    NEXT_HIDRA_VERSION=${NEXT_HIDRA_VERSION:15}
    NEXT_HIDRA_VERSION=${NEXT_HIDRA_VERSION%?}

    # get the current git version (based on *last* tag)
    GIT_VERSION=$(git describe)

    if [[ "${GIT_VERSION}" =~ - ]]; then
        HIDRA_VERSION=${NEXT_HIDRA_VERSION}.dev${GIT_VERSION#*-}
    else
        HIDRA_VERSION=${NEXT_HIDRA_VERSION}
    fi

    # according to PEP440, local part separator is "+"
    HIDRA_VERSION=${HIDRA_VERSION/-/+}
}


prepare_build()
{
    mkdir -p "$BUILD_DIR"

    # create rpm structure
    mkdir -p "${BUILD_DIR}/BUILD"
    mkdir -p "${BUILD_DIR}/BUILDROOT"
    mkdir -p "${BUILD_DIR}/RPMS"
    mkdir -p "${BUILD_DIR}/SOURCES"
    mkdir -p "${BUILD_DIR}/SPECS"
    mkdir -p "${BUILD_DIR}/SRPMS"
}


build_docker_image()
{
    DOCKER_DIR="${HIDRA_DIR}"
    DOCKER_IMAGE=alma${ALMA_VERSION}_build
    DOCKER_CONTAINER=hidra_build_alma${ALMA_VERSION}
    DOCKER_FILE="${HIDRA_DIR}/scripts/package_building/Dockerfile.build_alma${ALMA_VERSION}"

    cd "${DOCKER_DIR}" || exit 1
    if [[ "$(docker images -q "${DOCKER_IMAGE}" 2> /dev/null)" == "" ]]; then
        echo "Building docker image"
        docker build -f "${DOCKER_FILE}" -t "${DOCKER_IMAGE}" .
    fi
}

build_package()
{
    echo "Building package"
    cmd="cd ~/rpmbuild/SPECS; rpmbuild -ba hidra.spec"

    PASSWD_FILE=/tmp/passwd_x
    GROUP_FILE=/tmp/group_x

    UIDGID="$(id -u "$USER"):$(id -g "$USER")"

    getent passwd "$USER" > "$PASSWD_FILE"
    echo "$(id -gn):*:$(id -g):$USER" > "$GROUP_FILE"
    docker create -it \
        -v "$PASSWD_FILE":/etc/passwd \
        -v "$GROUP_FILE":/etc/group \
        --userns=host \
        --net=host \
        --security-opt no-new-privileges \
        --privileged \
        -v "${BUILD_DIR}":"$IN_DOCKER_DIR" \
        --user "${UIDGID}" \
        --name "${DOCKER_CONTAINER}" \
        "${DOCKER_IMAGE}" \
        bash
    docker start "${DOCKER_CONTAINER}" > /dev/null && echo "Started container"
    docker exec --user="${UIDGID}" "${DOCKER_CONTAINER}" sh -c "$cmd" && success=true
    docker stop "${DOCKER_CONTAINER}" > /dev/null && echo "Stopped container"
    docker rm "${DOCKER_CONTAINER}" > /dev/null && echo "Removed container"

    rm "$PASSWD_FILE"
    rm "$GROUP_FILE"
}


HIDRA_DIR=$(pwd)

get_hidra_version

ALMA_VERSION=9

BUILD_DIR="${HIDRA_DIR}/build/alma${ALMA_VERSION}/${HIDRA_VERSION}/rpmbuild"
IN_DOCKER_DIR=~/rpmbuild

prepare_build


HIDRA_DESY_DIR=${HIDRA_DIR}/desy

if git -C ${HIDRA_DESY_DIR} show-ref --verify --quiet refs/heads/main; then
    # The DESY submodule exists
    # TODO: Check that repo is clean
    ADD_CONSTANTS="--prefix=hidra-${HIDRA_VERSION}/src/api/python/hidra/ --add-file ${HIDRA_DESY_DIR}/src/api/python/hidra/constants.py"
else
    ADD_CONSTANTS=""
fi

git archive --format tar ${ADD_CONSTANTS} --prefix="hidra-${HIDRA_VERSION}/" -o "${BUILD_DIR}/SOURCES/hidra-${HIDRA_VERSION}.tar.gz" HEAD
cp package/hidra.spec "${BUILD_DIR}/SPECS"

sed -i "s/${NEXT_HIDRA_VERSION}/${HIDRA_VERSION}/" "${BUILD_DIR}/SPECS/hidra.spec"

build_docker_image
build_package

# clean up
#docker rmi "${DOCKER_IMAGE}"
#rm -rf "${MAPPED_DIR}/hidra"

if [ "$success" == "true" ]; then
    echo "RPM packages can be found in ${BUILD_DIR}"
else
    echo "Building packages failed"
    exit 1
fi
