#!/bin/sh

flake8 --ignore=W503,E123 ./*.py src/hidra/sender/*.py src/hidra/sender/eventdetectors/*.py src/hidra/sender/datafetchers/*.py src/hidra/hidra_control src/api/python src/hidra/receiver test/unittests examples
