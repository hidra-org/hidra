#/bin/sh

flake8 --ignore=W503,E123 *.py app api test/API test/unittests examples
