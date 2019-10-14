#/bin/sh

pylint -r no \
    *.py \
    api/python/hidra \
    app/hidra/hidra_receiver \
    app/hidra/hidra_sender \
    app/hidra_control/client \
    app/hidra_control/server \
    test/unittests/*.py \
    test/unittests/api/*.py \
    test/unittests/core/*.py \
    test/unittests/datafetcher/*.py \
    test/unittests/eventdetector/*.py \
    test/unittests/receiver/*.py \
    examples/*.py
