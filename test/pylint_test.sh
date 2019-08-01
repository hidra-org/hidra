#/bin/sh

pylint -r no \
    *.py \
    src/sender/*.py \
    src/sender/eventdetectors/*.py \
    src/sender/datafetchers/*.py \
    src/APIs \
    src/receiver/*.py \
    src/hidra_control/*.py \
    test/unittests/*.py \
    test/unittests/api/*.py \
    test/unittests/core/*.py \
    test/unittests/datafetcher/*.py \
    test/unittests/eventdetector/*.py \
    test/unittests/receiver/*.py \
    examples/*.py
