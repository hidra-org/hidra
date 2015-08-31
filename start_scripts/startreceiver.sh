#/bin/sh

BASE_DIR="/space/projects/live-viewer/"

python ../src/ZeroMQTunnel/receiver.py --outputDir "${BASE_DIR}/data/zmq_target" --bindingIpForDataStream "*" --logfile ../logs/receiver.log --maxRingBufferSize 10 --verbose
