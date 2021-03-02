set -uex

. scripts/package_building/build_utils.sh

HIDRA_DIR=`pwd`
get_hidra_version
HIDRA_VERSION_DEBIAN=${HIDRA_VERSION%.dev*}

export HIDRA_TESTDIR
export UID

[[ -z "${HIDRA_TESTDIR}" ]] && exit 1

# prepare hidra environment
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/support/hidra
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/current/raw
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/current/processed
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/current/shared
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/current/scratch_bl
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/commissioning/raw
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/commissioning/processed
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/commissioning/shared
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/commissioning/scratch_bl
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/local/raw
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/local/processed
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/local/shared
mkdir -p ${HIDRA_TESTDIR}/receiver/beamline/p00/local/scratch_bl
chmod -R 777 ${HIDRA_TESTDIR}/receiver/beamline/p00
mkdir -p ${HIDRA_TESTDIR}/sender-freeze/ramdisk
chmod -R 777 ${HIDRA_TESTDIR}/sender-freeze/ramdisk
mkdir -p ${HIDRA_TESTDIR}/sender-debian/ramdisk
chmod -R 777 ${HIDRA_TESTDIR}/sender-debian/ramdisk
mkdir -p ${HIDRA_TESTDIR}/sender-debian10/ramdisk
chmod -R 777 ${HIDRA_TESTDIR}/sender-debian10/ramdisk
mkdir -p ${HIDRA_TESTDIR}/sender-suse/ramdisk
chmod -R 777 ${HIDRA_TESTDIR}/sender-suse/ramdisk

docker-compose up --build -d

# setup receiver
docker-compose exec receiver yum install -y \
    build/centos7/${HIDRA_VERSION}*/rpmbuild/RPMS/noarch/python3-hidra-${HIDRA_VERSION}*-?.el7.noarch.rpm \
    build/centos7/${HIDRA_VERSION}*/rpmbuild/RPMS/noarch/hidra-${HIDRA_VERSION}*-?.el7.noarch.rpm

docker-compose exec receiver bash /scripts/post_install.sh

docker-compose exec receiver systemctl start hidra-receiver@p00
docker-compose exec receiver systemctl start hidra-control-server@p00
sleep 2
docker-compose exec receiver systemctl is-active hidra-receiver@p00
docker-compose exec receiver systemctl is-active hidra-control-server@p00


# setup sender-freeze
docker-compose exec sender-freeze mkdir -p /opt/hidra
docker-compose exec sender-freeze tar -xf \
    build/freeze/hidra-${HIDRA_VERSION}*-x86_64-2.7-manylinux1.tar.gz \
    --directory /opt/hidra --strip-components=1

docker-compose exec sender-freeze cp /conf/datamanager_p00.yaml /opt/hidra/conf

# A small initial test
# -T is necessary for the started processes to survive after the command finishes
# --env TERM=linux is necessary for hidra.sh because of -T
docker-compose exec -T --env TERM=linux sender-freeze /opt/hidra/hidra.sh start --beamline p00
docker-compose exec -T --env TERM=linux sender-freeze /opt/hidra/hidra.sh status --beamline p00
docker-compose exec -T --env TERM=linux sender-freeze /opt/hidra/hidra.sh stop --beamline p00

chmod -R 777 ${HIDRA_TESTDIR}/sender-freeze/ramdisk


# setup sender-debian
docker-compose exec sender-debian apt update
docker-compose exec sender-debian apt install -y \
    ./build/debian9/${HIDRA_VERSION_DEBIAN}/*.deb

docker-compose exec sender-debian cp /conf/datamanager_p00.yaml /opt/hidra/conf

# A small initial test
# -T is necessary for the started processes to survive after the command finishes
# --env TERM=linux is necessary for hidra.sh because of -T
docker-compose exec -T --env TERM=linux sender-debian systemctl start hidra@p00
docker-compose exec -T --env TERM=linux sender-debian systemctl status hidra@p00
docker-compose exec -T --env TERM=linux sender-debian systemctl stop hidra@p00

chmod -R 777 ${HIDRA_TESTDIR}/sender-debian/ramdisk


# setup sender-debian10
docker-compose exec sender-debian10 apt update
docker-compose exec sender-debian10 apt install -y \
    ./build/debian10/${HIDRA_VERSION_DEBIAN}/*.deb

docker-compose exec sender-debian10 cp /conf/datamanager_p00.yaml /opt/hidra/conf

# A small initial test
# -T is necessary for the started processes to survive after the command finishes
# --env TERM=linux is necessary for hidra.sh because of -T
docker-compose exec -T --env TERM=linux sender-debian10 systemctl start hidra@p00
docker-compose exec -T --env TERM=linux sender-debian10 systemctl status hidra@p00
docker-compose exec -T --env TERM=linux sender-debian10 systemctl stop hidra@p00

chmod -R 777 ${HIDRA_TESTDIR}/sender-debian10/ramdisk


# setup sender-suse
docker-compose exec sender-suse mkdir -p /opt/hidra
docker-compose exec sender-suse tar -xf \
    build/freeze/hidra-${HIDRA_VERSION}*-x86_64-2.7-manylinux1.tar.gz \
    --directory /opt/hidra --strip-components=1

docker-compose exec sender-suse cp /conf/datamanager_p00.yaml /opt/hidra/conf

# A small initial test
# -T is necessary for the started processes to survive after the command finishes
# --env TERM=linux is necessary for hidra.sh because of -T
docker-compose exec -T --env TERM=linux sender-suse /opt/hidra/hidra.sh start --beamline p00
docker-compose exec -T --env TERM=linux sender-suse /opt/hidra/hidra.sh status --beamline p00
docker-compose exec -T --env TERM=linux sender-suse /opt/hidra/hidra.sh stop --beamline p00

chmod -R 777 ${HIDRA_TESTDIR}/sender-suse/ramdisk


# setup control client
docker-compose exec control-client yum install -y \
    build/centos7/${HIDRA_VERSION}*/rpmbuild/RPMS/noarch/python3-hidra-${HIDRA_VERSION}*-?.el7.noarch.rpm \
    build/centos7/${HIDRA_VERSION}*/rpmbuild/RPMS/noarch/hidra-control-client-${HIDRA_VERSION}*-?.el7.noarch.rpm


# setup transfer client
docker-compose exec transfer-client yum install -y \
    build/centos7/${HIDRA_VERSION}*/rpmbuild/RPMS/noarch/python2-hidra-${HIDRA_VERSION}*-?.el7.noarch.rpm

echo $HIDRA_TESTDIR