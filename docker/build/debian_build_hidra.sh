#!/bin/bash

usage()
{
    printf "Usage: $SCRIPTNAME --version <debian_version>\n" >&2
}

action=
while test $# -gt 0
do
    #convert to lower case
    input_value=$(echo "$1" | tr '[:upper:]' '[:lower:]')

    case $input_value in
        --version)
            #convert to lower case
            version=$(echo "$2" | tr '[:upper:]' '[:lower:]')
            shift
            ;;
        -h | --help ) usage
            exit
            ;;
        * ) break;  # end of options
    esac
    shift
done

if [ -z ${version+x} ]
then
    usage
    exit 1
fi

if [ "$version" == "9" -o "$version" == "stretch" ]
then
    DEBIAN_NAME=stretch
    DEBIAN_VERSION=9

elif [ "$version" == "8" -o "$version" == "jessie" ]
then
    DEBIAN_NAME=jessie
    DEBIAN_VERSION=8

else
    echo "Not supported version"
    exit 1
fi

VERSION=$(curl -L "https://stash.desy.de/projects/HIDRA/repos/hidra/raw/src/shared/_version.py?at=refs%2Fheads%2Fmaster")
# cut of the first 16 characters
VERSION=${VERSION:16}
VERSION=${VERSION%?}

MAPPED_DIR=/tmp/hidra_builds/${VERSION}/debian${DEBIAN_VERSION}
IN_DOCKER_DIR=/external
DOCKER_DIR=$(pwd)

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
git clone --branch "v$VERSION" https://stash.desy.de/scm/hidra/hidra.git

mv hidra/debian .
if [ "$DEBIAN_NAME" == "jessie" ]
then
    sed -i -e "s/+deb9u5\~fsec) stretch/+deb8u11\~fsec) jessie/g" debian/changelog
    sed -i -e "s/stretch/jessie/g" debian/changelog
    sed -i -e "s/Standards-Version: [0-9.]*/Standards-Version: 3.9.4/g" debian/control
fi
tar czf hidra_${VERSION}.orig.tar.gz hidra
mv debian hidra/debian

# DOCKER
DOCKER_IMAGE=debian_${DEBIAN_NAME}_build
DOCKER_CONTAINER=hidra_build_${DEBIAN_NAME}

cd ${DOCKER_DIR}
if [[ "$(docker images -q ${DOCKER_IMAGE} 2> /dev/null)" == "" ]]; then
    echo "Creating container"
    docker build -f ./Dockerfile.build_debian${DEBIAN_VERSION} -t ${DOCKER_IMAGE} .
fi

cmd="cd /external/hidra; dpkg-buildpackage -us -uc"

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
docker start ${DOCKER_CONTAINER}
docker exec --user=$(id -u $USER):$(id -g $USER) ${DOCKER_CONTAINER} sh -c "$cmd"
docker stop ${DOCKER_CONTAINER}
docker rm ${DOCKER_CONTAINER}

rm $PASSWD_FILE
rm $GROUP_FILE

#docker rmi ${DOCKER_IMAGE}
#rm -rf ${MAPPED_DIR}/hidra

echo "Debian ${DEBIAN_VERSION} packages can be found in ${MAPPED_DIR}/build"
