#!/bin/bash

fix_debian_version()
{
    # debian 8
    if [ "$DEBIAN_NAME" == "jessie" ]
    then
        sed -i -e "s/+deb9u5\~fsec) stretch/+deb8u11\~fsec) jessie/g" debian/changelog
        sed -i -e "s/stretch/jessie/g" debian/changelog
        sed -i -e "s/Standards-Version: [0-9.]*/Standards-Version: 3.9.4/g" debian/control
    fi
}

check_arguments()
{
    if [ "${version}" == "" ]
    #if [ -z ${version+x} -o "${version}" == "" ]
    then
        # set default to debian 9
        DEBIAN_NAME=stretch
        DEBIAN_VERSION=9
        printf "Create packages for debian $DEBIAN_VERSION. "
        printf "If you want a different version use --version\n"
    elif [ "$version" == "9" -o "$version" == "stretch" ]
    then
        DEBIAN_NAME=stretch
        DEBIAN_VERSION=9

    elif [ "$version" == "8" -o "$version" == "jessie" ]
    then
        DEBIAN_NAME=jessie
        DEBIAN_VERSION=8

    else
        echo "Not supported debian version"
        exit 1
    fi

    if [ "${TAG}" == "" ]
    then
        # set default
        TAG="master"
    fi

    if [ "${HIDRA_LOCATION}" != "" ]; then
        if [ -d "$HIDRA_LOCATION" ]; then
            echo "Hidra is not downloaded. Using $HIDRA_LOCATION"
        else
            echo "ERROR: No hidra found at $HIDRA_LOCATION. Abort."
            exit 1
        fi
    fi
}

get_hidra_version()
{
    if [ "${HIDRA_LOCATION}" != "" ]; then
        HIDRA_VERSION=$(cat ${HIDRA_LOCATION}/src/APIs/hidra/utils/_version.py)
    else
        URL="https://raw.githubusercontent.com/hidra-org/hidra/$TAG/src/APIs/hidra/utils/_version.py"
        HIDRA_VERSION=$(curl -L $URL)
    fi
    # cut of the first characters
    HIDRA_VERSION=${HIDRA_VERSION:15}
    HIDRA_VERSION=${HIDRA_VERSION%?}
}

download_hidra()
{
    if [ "$HIDRA_LOCATION" != "" ]; then
        cp -r $HIDRA_LOCATION $MAPPED_DIR/hidra
        return
    fi

    # clean up old download
    if [ -d "$MAPPED_DIR/hidra" ]; then
        rm -rf $MAPPED_DIR/hidra
    fi

    BRANCH="v${HIDRA_VERSION}"
    git clone --branch $BRANCH https://github.com/hidra-org/hidra.git
}

build_docker_image()
{
    DOCKER_DIR=$(pwd)
    DOCKER_IMAGE=debian_${DEBIAN_NAME}_build
    DOCKER_CONTAINER=hidra_build_${DEBIAN_NAME}

    cd ${DOCKER_DIR}
    if [[ "$(docker images -q ${DOCKER_IMAGE} 2> /dev/null)" == "" ]]; then
        echo "Creating container"
        docker build -f ./Dockerfile.build_debian${DEBIAN_VERSION} -t ${DOCKER_IMAGE} .
    fi
}

build_package()
{
    cmd="cd /external/hidra; dpkg-buildpackage -us -uc -sa"

    IN_DOCKER_DIR=/external

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
}

usage()
{
    printf "Usage: $SCRIPTNAME --version <debian version> --tag <hidra tag> --hidra-location <path>\n" >&2
}

action=
TAG=
HIDRA_LOCATION=
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
        --tag)
            TAG="$2"
            shift
            ;;
        --hidra-location)
            HIDRA_LOCATION="$2"
            shift
            ;;
        -h | --help ) usage
            exit
            ;;
        * ) break;  # end of options
    esac
    shift
done

check_arguments
get_hidra_version

MAPPED_DIR=/tmp/hidra_builds/debian${DEBIAN_VERSION}/${HIDRA_VERSION}

echo "Create packages for hidra tag $TAG for version $HIDRA_VERSION"

if [ ! -d "$MAPPED_DIR" ]; then
    mkdir -p $MAPPED_DIR
fi

cd ${MAPPED_DIR}
download_hidra

mv hidra/debian .
fix_debian_version
tar czf hidra_${HIDRA_VERSION}.orig.tar.gz hidra
mv debian hidra/debian

build_docker_image
build_package

# clean up
#docker rmi ${DOCKER_IMAGE}
rm -rf $MAPPED_DIR/hidra

echo "Debian ${DEBIAN_VERSION} packages can be found in ${MAPPED_DIR}"
