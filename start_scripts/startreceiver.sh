#/bin/sh

BASE_DIR="/space/projects/live-viewer"

python ../src/ZeroMQTunnel/receiver.py --targetDir "${BASE_DIR}/data/zmq_target" --dataStreamIp "*" --logfilePath "${BASE_DIR}/logs" --logfileName "receiver.log" --maxRingBufferSize 10 --verbose
