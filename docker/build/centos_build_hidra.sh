#!/bin/bash

VERSION=$(curl -l "https://stash.desy.de/projects/HIDRA/repos/hidra/raw/src/APIs/hidra/_version.py?at=refs%2Fheads%2Fmaster")
# cut of the first characters
VERSION=${VERSION:16}
VERSION=${VERSION%?}

MAPPED_DIR=/tmp/hidra_builds/${VERSION}/centos/rpmbuild
IN_DOCKER_DIR=~/rpmbuild
DOCKER_DIR=$(pwd)

if [ ! -d "$MAPPED_DIR" ]; then
    mkdir -p $MAPPED_DIR
fi

if [ -d "$MAPPED_DIR/hidra" ]; then
    rm -rf $MAPPED_DIR/hidra
fi

cd ${MAPPED_DIR}
# create rpm structure
mkdir -p ${MAPPED_DIR}/BUILD
mkdir -p ${MAPPED_DIR}/BUILDROOT
mkdir -p ${MAPPED_DIR}/RPMS
mkdir -p ${MAPPED_DIR}/SOURCES
mkdir -p ${MAPPED_DIR}/SPECS
mkdir -p ${MAPPED_DIR}/SRPMS

# get sources
git clone --branch "v$VERSION" https://stash.desy.de/scm/hidra/hidra.git
cd hidra
git archive --format zip -o hidra-v${VERSION}.zip v${VERSION}
mv hidra-v${VERSION}.zip ${MAPPED_DIR}/SOURCES
cp hidra.spec ${MAPPED_DIR}/SPECS

# DOCKER
DOCKER_IMAGE=centos_build
DOCKER_CONTAINER=hidra_build_centos

cd ${DOCKER_DIR}
if [[ "$(docker images -q ${DOCKER_IMAGE} 2> /dev/null)" == "" ]]; then
    echo "Creating container"
    docker build -f ./Dockerfile.build_centos -t ${DOCKER_IMAGE} .
fi

cmd="cd ~/rpmbuild/SPECS; rpmbuild -ba -D 'dist .el7' hidra.spec"

PASSWD_FILE=/tmp/passwd_x
GROUP_FILE=/tmp/group_x

getent passwd $USER > $PASSWD_FILE
echo "$(id -gn):*:$(id -g):$USER" > $GROUP_FILE
docker create -it \
    -v $PASSWD_FILE:/etc/passwd \
    -v $GROUP_FILE:/etc/group \
    --userns=host \
    --net=host \
    --security-opt no-new-privileges \
    --privileged \
    -v ${MAPPED_DIR}:$IN_DOCKER_DIR \
    --user=$(id -u $USER):$(id -g $USER) \
    --name ${DOCKER_CONTAINER} \
    ${DOCKER_IMAGE} \
    bash
docker start ${DOCKER_CONTAINER} > /dev/null && echo "Started container"
docker exec --user=$(id -u $USER):$(id -g $USER) ${DOCKER_CONTAINER} sh -c "$cmd" && success=true
docker stop ${DOCKER_CONTAINER} > /dev/null && echo "Stopped container"
docker rm ${DOCKER_CONTAINER} > /dev/null && echo "Removed container"

rm $PASSWD_FILE
rm $GROUP_FILE

#docker rmi ${DOCKER_IMAGE}
#rm -rf ${MAPPED_DIR}/hidra

if [ "$success" == "true" ]; then
    echo "RPM packages can be found in ${MAPPED_DIR}"
else
    echo "Building packages failed"
    exit 1
fi
