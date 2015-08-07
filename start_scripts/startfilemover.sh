#/bin/sh

BASE_DIR="/space/projects/live-viewer/"

python ../src/ZeroMQTunnel/fileMover.py --logfilePath="${BASE_DIR}/logs" --bindingIpForSocket="127.0.0.1" --logfileName="fileMover.log" --parallelDataStreams 16 --dataStreamIp="131.169.185.34" --dataStreamPort="6061" --cleanerTargetPath="${BASE_DIR}/data/target/" --verbose
#python ../src/ZeroMQTunnel/fileMover.py --logfilePath="${BASE_DIR}/logs" --bindingIpForSocket="127.0.0.1" --logfileName="fileMover.log" --parallelDataStreams 1 --dataStreamIp="127.0.0.1" --dataStreamPort="6061" --cleanerTargetPath="${BASE_DIR}/data/target/" --verbose