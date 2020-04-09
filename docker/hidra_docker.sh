#!/bin/bash

# This script only work in the DESY environment with the DESY specific docker i
# setup.

OVERWRITE_IMAGE=false
SELF_ZERO="$0"
test -n "${BASH_VERSION}" && SELF_ZERO="${BASH_SOURCE[0]}" # Fix when bash is used
SELF_PATH="$(readlink --canonicalize-existing -- "${SELF_ZERO}")"
CURRENTDIR="${SELF_PATH%/*}"
SCRIPTNAME="${SELF_PATH##*/}"
BASEDIR="${CURRENTDIR%/*}"

usage()
{
    printf "Usage: %s" "$SCRIPTNAME"
    printf " [--dockerfile <dockerfile to use>]"
    printf " [--hidradir <hidra location>]"
    printf " [--hidratype <hidra type to use (sender, receiver, api,..)>]"
    printf " [--overwrite]"
    printf "\n"
}

dockerfile=
hidradir=
hidratype=
while test $# -gt 0
do
    #convert to lower case
    input_value=$(echo "$1" | tr '[:upper:]' '[:lower:]')

    case $input_value in
        --dockerfile)
            dockerfile=$2
            shift
            ;;
        --hidradir)
            hidradir=$2
            shift
            ;;
        --hidratype)
            hidratype=$2
            shift
            ;;
        --overwrite)
            OVERWRITE_IMAGE=true
            ;;
        -h | --help ) usage
            exit
            ;;
        * ) break;  # end of options
    esac
    shift
done

set_defaults()
{
    if [ -z ${dockerfile+x} ] || [ "${dockerfile}" = "" ]
    then
        if [ -z ${hidratype+x} ] || [ "${hidratype}" = "" ]
        then
            echo "No dockerfile and/or hidratype set. Abort"
            usage
            exit 1
        else
            dockerfile=${CURRENTDIR}/Dockerfile.${hidratype}
            echo "No dockerfile set: using $dockerfile"
        fi
    fi

    if [ ! -f "$dockerfile" ]
    then
        echo "Dockerfile $dockerfile does not exist. Abort"
        exit 1
    fi

    if [ -z ${hidratype+x} ] || [ "${hidratype}" = "" ]
    then
        filename=$(basename "$dockerfile")
        hidratype="${filename##*.}"
        echo "No hidratype set: using $hidratype"
    fi

    if [ -z ${hidradir+x} ] || [ "${hidradir}" = "" ]
    then
        hidradir=${BASEDIR}
        echo "No hidradir set: using $hidradir"
    fi
}

set_hidra_type_specifics()
{
    DOCKER_OPTIONS="-v ${hidradir}:/opt/hidra"

#    echo "Using hidra type $hidratype"
#    if [[ "$hidratype" == "api" ]]
#    then
#        DOCKER_OPTIONS="${DOCKER_OPTIONS} \
#            -v ${hidradir}/src/APIs/hidra:/usr/local/lib/python2.7/dist-packages/hidra"
#
#    else
    if [[ "$hidratype" != "api" ]]
    then
        DOCKER_OPTIONS="${DOCKER_OPTIONS} \
           -v ${hidradir}/data/source/:/ramdisk \
           -v $hidradir/data/target/:/target"
    fi
}

run_hidra_docker()
{
    set_defaults
    set_hidra_type_specifics

    # build image
    DOCKER_IMAGE=hidra_build_${hidratype}
    DOCKER_CONTAINER=hidra_container_${hidratype}

    if [[ "$(docker images -q "${DOCKER_IMAGE}" 2> /dev/null)" == "" || "$OVERWRITE_IMAGE" = true ]]
    then

        echo "Creating image"
        if [ ! -f "${dockerfile}" ]
        then
            echo "dockerfile $dockerfile does not exist. Abort"
            exit 1
        fi

        docker build -f "${dockerfile}" -t "${DOCKER_IMAGE}" .
    else
        echo "Image already available, no building required."
    fi

    # run container
    PASSWD_FILE=/tmp/passwd_x
    GROUP_FILE=/tmp/group_x

    UIDGID="$(id -u "$USER"):$(id -g "$USER")"

    getent passwd "$USER" > "$PASSWD_FILE"
    echo "$(id -gn):*:$(id -g):$USER" > "$GROUP_FILE"

    docker run -it \
        --rm \
        -v $PASSWD_FILE:/etc/passwd \
        -v $GROUP_FILE:/etc/group \
        --userns=host \
        --net=host \
        --security-opt no-new-privileges \
        --user "${UIDGID}" \
        "${DOCKER_OPTIONS}" \
        --name "${DOCKER_CONTAINER}" \
        "${DOCKER_IMAGE}"

    rm "$PASSWD_FILE"
    rm "$GROUP_FILE"

    #docker rmi "${DOCKER_IMAGE}"
    #rm -rf "${MAPPED_DIR}/hidra"

}

run_hidra_docker
