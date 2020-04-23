#!/bin/sh

pylint -r no \
    ./*.py \
    src/hidra/sender/*.py \
    src/hidra/sender/eventdetectors/*.py \
    src/hidra/sender/datafetchers/*.py \
    src/api/python \
    src/hidra/receiver/*.py \
    src/hidra/hidra_control/*.py \
    test/unittests/*.py \
    test/unittests/api/*.py \
    test/unittests/core/*.py \
    test/unittests/datafetcher/*.py \
    test/unittests/eventdetector/*.py \
    test/unittests/receiver/*.py \
    examples/*.py
