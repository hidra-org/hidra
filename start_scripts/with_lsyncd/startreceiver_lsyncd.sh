#/bin/sh

python ../src/ZeroMQTunnel/with_lsyncd/receiver.py --outputDir /space/projects/live-viewer/data/zmq_target --tcpPortDataStream 6061 --bindingIpForDataStream 127.0.01 --logfile ../logs/receiver.log --verbose
