#/bin/sh

BASE_DIR="/space/projects/live-viewer"

#python ../src/ZeroMQTunnel/sender.py --logfilePath="${BASE_DIR}/logs" --parallelDataStreams 16 --dataStreamIp="131.169.185.34" --dataStreamPort="6061" --cleanerTargetPath="${BASE_DIR}/data/target/" --verbose
python ../src/ZeroMQTunnel/sender.py --logfilePath="${BASE_DIR}/logs" --parallelDataStreams 1 --dataStreamIp="127.0.0.1" --cleanerTargetPath="${BASE_DIR}/data/target" --verbose
