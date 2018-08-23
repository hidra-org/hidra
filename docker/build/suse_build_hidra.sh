#!/bin/bash

MAPPED_DIR=/tmp/hidra_builds
IN_DOCKER_DIR=/iso
DOCKER_DIR=$(pwd)

MY_UID=$(id -u $USER)
MY_GID=$(id -g $USER)
MY_GROUP=$(id -g --name $USER)

if [ ! -d "$MAPPED_DIR" ]; then
    mkdir $MAPPED_DIR
fi

if [ -d "$MAPPED_DIR/hidra" ]; then
    rm -rf $MAPPED_DIR/hidra
fi

if [ -d "$MAPPED_DIR/build" ]; then
    rm -rf $MAPPED_DIR/build
fi

cd ${MAPPED_DIR}
git clone https://stash.desy.de/scm/hidra/hidra.git
VERSION=$(cat ./hidra/src/shared/_version.py)
VERSION=${VERSION:15}
VERSION=${VERSION%?}

# workaround to fix freeze_setup
#TODO fix this in repo
cp /home/kuhnm/projects/hidra/freeze_setup_fix.py ${MAPPED_DIR}/hidra/freeze_setup.py

cmd="export LD_LIBRARY_PATH=/usr/local/lib/:\$LD_LIBRARY_PATH; \
    cd /iso; \
    /usr/bin/python $IN_DOCKER_DIR/hidra/freeze_setup.py build"

cd ${DOCKER_DIR}
docker create -it -v ${MAPPED_DIR}:$IN_DOCKER_DIR --user=$MY_UID:$MY_GID --name hidra_build suse_build bash
docker start hidra_build
docker exec hidra_build sh -c "$cmd"
docker stop hidra_build
docker rm hidra_build
#docker run -it -v ${MAPPED_DIR}:/iso suse_build /usr/bin/python /iso/hidra/freeze_setup.py build
#docker run -it -v ${MAPPED_DIR}:/iso --name suse_build suse_build /usr/bin/python /iso/hidra/freeze-setup.py build


cd ${MAPPED_DIR}/build
mv exe.linux-x86_64-2.7 hidra
tar -czf hidra-v${VERSION}-x86_64-2.7-suse10.2.tar.gz hidra
echo "Frozen hidra version can be found in ${MAPPED_DIR}/build"
#scp hidra-v${VERSION}-x86_64-2.7-suse10.2.tar.gz $USER@bastion.desy.de:/afs/desy.de/products/hidra/freeze/linux
#rm hidra-v${VERSION}-x86_64-2.7-suse10.2.tar.gz
