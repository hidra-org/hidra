#!/bin/sh

flake8 --ignore=W503,E123 ./*.py src/sender src/hidra_control src/APIs src/receiver test/API test/unittests examples
