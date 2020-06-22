#!/bin/bash

get_hidra_version()
{
    URL="https://raw.githubusercontent.com/hidra-org/hidra/master/src/api/python/hidra/utils/_version.py"
    HIDRA_VERSION=$(curl -L $URL)

    # cut of the first characters
    HIDRA_VERSION=${HIDRA_VERSION:15}
    HIDRA_VERSION=${HIDRA_VERSION%?}
}

download_hidra()
{
    # clean up old download
    if [ -d "$MAPPED_DIR/hidra" ]; then
        rm -rf "$MAPPED_DIR/hidra"
    fi

    # get sources
    git clone --branch "v$HIDRA_VERSION" https://github.com/hidra-org/hidra.git
}

prepare_build()
{
    cd "${MAPPED_DIR}" || exit 1

    # create rpm structure
    mkdir -p "${MAPPED_DIR}/BUILD"
    mkdir -p "${MAPPED_DIR}/BUILDROOT"
    mkdir -p "${MAPPED_DIR}/RPMS"
    mkdir -p "${MAPPED_DIR}/SOURCES"
    mkdir -p "${MAPPED_DIR}/SPECS"
    mkdir -p "${MAPPED_DIR}/SRPMS"
}

build_docker_image()
{
    DOCKER_DIR=$(pwd)
    DOCKER_IMAGE=centos${CENTOS_VERSION}_build
    DOCKER_CONTAINER=hidra_build_centos${CENTOS_VERSION}
    DOCKER_FILE="${MAPPED_DIR}/hidra/scripts/package_building/Dockerfile.build_centos${CENTOS_VERSION}"

    cd "${DOCKER_DIR}" || exit 1
    if [[ "$(docker images -q "${DOCKER_IMAGE}" 2> /dev/null)" == "" ]]; then
        echo "Building docker image"
        docker build -f "${DOCKER_FILE}" -t "${DOCKER_IMAGE}" .
    fi
}

build_package()
{
    echo "Building package"
    cmd="cd ~/rpmbuild/SPECS; rpmbuild -ba -D 'dist .el7' hidra.spec"

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
        -v "${MAPPED_DIR}":"$IN_DOCKER_DIR" \
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


DOCKER_DIR=$(pwd)

get_hidra_version

CENTOS_VERSION=7

MAPPED_DIR=/tmp/hidra_builds/centos${CENTOS_VERSION}/${HIDRA_VERSION}/rpmbuild
IN_DOCKER_DIR=~/rpmbuild

if [ ! -d "$MAPPED_DIR" ]; then
    mkdir -p "$MAPPED_DIR"
fi

prepare_build

# get sources
cd "${MAPPED_DIR}" || exit 1
download_hidra

cd hidra || exit 1
git archive --format tar --prefix="hidra-${HIDRA_VERSION}/" -o "hidra-${HIDRA_VERSION}.tar.gz" "v${HIDRA_VERSION}"
mv "hidra-${HIDRA_VERSION}.tar.gz" "${MAPPED_DIR}/SOURCES"
cp package/hidra.spec "${MAPPED_DIR}/SPECS"

build_docker_image
build_package

# clean up
#docker rmi "${DOCKER_IMAGE}"
#rm -rf "${MAPPED_DIR}/hidra"

if [ "$success" == "true" ]; then
    echo "RPM packages can be found in ${MAPPED_DIR}"
else
    echo "Building packages failed"
    exit 1
fi
