#/bin/sh

python ../ZeroMQTunnel/fileMover.py --logfilePath="/space/projects/Live_Viewer/logs" --bindingIpForSocket="127.0.0.1" --logfileName="fileMover.log" --parallelDataStreams 1 --dataStreamIp="127.0.0.1" --dataStreamPort="6061" --verbose
