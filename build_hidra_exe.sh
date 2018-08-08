#!/bin/sh

# to run this building script the environment has be set up in advance
# see https://confluence.desy.de/display/hidra/SuSE+10%3A+Set+up+and+freeze+HiDRA

VERSION=
USER=
usage() { echo "Usage: $0 -v <version> -u <user> " 1>&2; exit 1; }

while getopts ':v:u:' OPTION
do
    case "${OPTION}" in
        v) VERSION=${OPTARG}
            ;;
        u) USER=${OPTARG}
            ;;
        *) usage
    esac
done
shift $((OPTIND-1))

if [ -z $VERSION ] || [ -z $USER ]
then
     usage
     exit 1
fi

cd ~/Desktop
#wget or clone repo
wget -O hidra-v${VERSION}.zip "https://stash.desy.de/rest/archive/latest/projects/HIDRA/repos/hidra/archive?at=refs%2Ftags%2Fv${VERSION}&format=zip" --no-check-certificate

unzip hidra-v${VERSION} -d hidra
rm hidra-v${VERSION}
cd hidra
/usr/bin/python freeze_setup.py build
cd build
mv exe.linux-x86_64-2.7 hidra
tar -czf hidra_v${VERSION}-x86_64-2.7-suse10.2.tar.gz hidra
scp hidra_v${VERSION}-x86_64-2.7-suse10.2.tar.gz $USER@bastion.desy.de:/afs/desy.de/products/hidra/
rm hidra_v${VERSION}-x86_64-2.7-suse10.2.tar.gz
