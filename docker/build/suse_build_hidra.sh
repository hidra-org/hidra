#!/bin/bash

VERSION=$(curl -L "https://stash.desy.de/projects/HIDRA/repos/hidra/raw/src/shared/_version.py?at=refs%2Fheads%2Fmaster")
# cut of the first 16 characters
VERSION=${VERSION:16}
VERSION=${VERSION%?}

MAPPED_DIR=/tmp/hidra_builds/${VERSION}/suse10
IN_DOCKER_DIR=/external
DOCKER_DIR=$(pwd)

MY_UID=$(id -u $USER)
MY_GID=$(id -g $USER)
MY_GROUP=$(id -g --name $USER)

if [ ! -d "$MAPPED_DIR" ]; then
    mkdir -p $MAPPED_DIR
fi

if [ -d "$MAPPED_DIR/hidra" ]; then
    rm -rf $MAPPED_DIR/hidra
fi

if [ -d "$MAPPED_DIR/build" ]; then
    rm -rf $MAPPED_DIR/build
fi

cd ${MAPPED_DIR}
git clone https://stash.desy.de/scm/hidra/hidra.git

# DOCKER
DOCKER_IMAGE=suse_build
DOCKER_CONTAINER=hidra_build

cd ${DOCKER_DIR}
if [[ "$(docker images -q ${DOCKER_IMAGE} 2> /dev/null)" == "" ]]; then
    echo "Creating container"
    docker build -f ./Dockerfile.build_suse10-2 -t ${DOCKER_IMAGE} .
fi

# TODO exit if build fails

cmd="export LD_LIBRARY_PATH=/usr/local/lib/:\$LD_LIBRARY_PATH; \
    cd ${IN_DOCKER_DIR}; \
    /usr/bin/python ${IN_DOCKER_DIR}/hidra/freeze_setup.py build"

docker create -it -v ${MAPPED_DIR}:${IN_DOCKER_DIR}:Z --user=$MY_UID:$MY_GID --name ${DOCKER_CONTAINER} ${DOCKER_IMAGE} bash
docker start ${DOCKER_CONTAINER}
docker exec ${DOCKER_CONTAINER} sh -c "$cmd"
docker stop ${DOCKER_CONTAINER}
docker rm ${DOCKER_CONTAINER}

# build tar
cd ${MAPPED_DIR}/build
mv exe.linux-x86_64-2.7 hidra
tar -czf hidra-v${VERSION}-x86_64-2.7-suse10.2.tar.gz hidra
echo "Frozen hidra version can be found in ${MAPPED_DIR}/build"
#scp hidra-v${VERSION}-x86_64-2.7-suse10.2.tar.gz $USER@bastion.desy.de:/afs/desy.de/products/hidra/freeze/linux
#rm hidra-v${VERSION}-x86_64-2.7-suse10.2.tar.gz
