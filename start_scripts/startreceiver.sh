#/bin/sh

BASE_DIR="/space/projects/live-viewer/"

python ../src/ZeroMQTunnel/receiver.py --outputDir "${BASE_DIR}/data/zmq_target" --tcpPortDataStream 6061 --bindingIpForDataStream 127.0.01 --logfile ../logs/receiver.log --maxRingBufferSize 20 --verbose
