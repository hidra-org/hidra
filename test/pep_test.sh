#/bin/sh

flake8 --ignore=W503,E123 src/sender/eventDetectors/watchdog_detector.py src/sender/eventDetectors/zmq_detector.py src/shared/*.py src/hidra_control/*.py src/APIs/*.py src/receiver/*.py test/API/*.py
