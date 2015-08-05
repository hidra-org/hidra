#/bin/sh

BASE_DIR="/space/projects/live-viewer/"

python ../src/ZeroMQTunnel/watcher.py --watchFolder "${BASE_DIR}/data/source/" --logfilePath "${BASE_DIR}/logs" --verbose
