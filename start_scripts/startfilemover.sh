#/bin/sh

python ../src/ZeroMQTunnel/fileMover.py --logfilePath="/space/projects/live-viewer/logs" --bindingIpForSocket="127.0.0.1" --logfileName="fileMover.log" --parallelDataStreams 1 --dataStreamIp="127.0.0.1" --dataStreamPort="6061" --verbose
