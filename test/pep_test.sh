#/bin/sh

flake8 --ignore=W503,E123 *.py src/sender/*.py src/sender/eventdetectors/*.py src/sender/datafetchers/*.py src/shared/*.py src/hidra_control/*.py src/APIs/*.py src/receiver/*.py test/API/*.py test/unittests/*.py test/unittests/core/*.py test/unittests/eventdetectors/*.py test/unittests/datafetchers/*.py
