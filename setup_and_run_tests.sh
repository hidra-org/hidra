set -uex

#python test/unittests/test_suite.py --suite all --case all
#python3 test/unittests/test_suite.py --suite all --case all

export RECEIVER_VERSION=alma9

docker-compose down

export HIDRA_TESTDIR=$(mktemp -d --tmpdir hidra_test_dir.XXXXXX)

source test/docker/install.sh && python3 -m pytest test/docker -vv

echo $HIDRA_TESTDIR