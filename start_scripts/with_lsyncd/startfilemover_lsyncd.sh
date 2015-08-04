#/bin/sh

python ../src/ZeroMQTunnel/with_lsyncd/fileMover_lsyncd.py --logfilePath="/space/projects/live-viewer/logs" --bindingIpForSocket="127.0.0.1" --logfileName="fileMover.log" --parallelDataStreams 1 --dataStreamIp="127.0.0.1" --dataStreamPort="6061" --verbose
