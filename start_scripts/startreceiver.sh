#/bin/sh

python ../ZeroMQTunnel/receiver.py --outputDir /space/projects/Live_Viewer/zmq_target --tcpPortDataStream 6061 --bindingIpForDataStream 127.0.0.1 --logfile ../logs/receiver.log --verbose
