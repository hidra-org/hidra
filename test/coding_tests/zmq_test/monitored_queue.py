from __future__ import print_function

import time
import zmq
from zmq.devices.monitoredqueuedevice import MonitoredQueue
from zmq.utils.strtypes import asbytes
from multiprocessing import Process
import os
import tempfile


def monitordevice(in_con_id, out_con_id, mon_con_id):
    in_prefix = asbytes('in')
    out_prefix = asbytes('out')

    monitoringdevice = MonitoredQueue(zmq.PUSH,  # in
                                      zmq.PULL,  # out
                                      zmq.PUB,   # mon
                                      in_prefix, out_prefix)

    monitoringdevice.bind_in(in_con_id)
    monitoringdevice.bind_out(out_con_id)
    monitoringdevice.bind_mon(mon_con_id)

    monitoringdevice.start()
    print("Program: Monitoring device has started")


def server(out_con_id):
    print("Program: Server connecting to device")
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(out_con_id)
    for request_num in range(2):
        socket.send("Request #{0} from server".format(request_num))


def client(in_con_id, client_id):
    print("Program: Worker #%s connecting to device" % client_id)
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect(in_con_id)
    while True:
        message = socket.recv()
        print("Client #{0}: Received - {1}".format(client_id, message))


def monitor(mon_con_id):
    print("Starting monitoring process")
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    print("Collecting updates from server...")
    socket.connect(mon_con_id)
    socket.setsockopt(zmq.SUBSCRIBE, "")
    while True:
        string = socket.recv_multipart()
        print("Monitoring Client: %s" % string)


def main():
    ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
    current_pid = os.getpid()
    frontend_port = 5559

    if not os.path.exists(ipc_dir):
        os.mkdir(ipc_dir)
        os.chmod(ipc_dir, 0o777)
        tmp_created = True
    else:
        tmp_created = False

    in_con_id = "tcp://127.0.0.1:{0}".format(frontend_port)
    out_con_id = "ipc://{0}/{1}_{2}".format(ipc_dir, current_pid, "out")
    mon_con_id = "ipc://{0}/{1}_{2}".format(ipc_dir, current_pid, "mon")

    print("in_con_id", in_con_id)
    print("out_con_id", out_con_id)
    print("mon_con_id", mon_con_id)

    number_of_workers = 2

    monitoring_p = Process(target=monitordevice,
                           args=(in_con_id, out_con_id, mon_con_id))
    monitoring_p.start()
    server_p = Process(target=server, args=(out_con_id,))
    server_p.start()
    monitorclient_p = Process(target=monitor, args=(mon_con_id,))
    monitorclient_p.start()
    time.sleep(1)

    client_p = []
    for client_id in range(number_of_workers):
        p = Process(target=client, args=(in_con_id, client_id,))
        p.start()
        client_p.append(p)

    time.sleep(2)
    for proc in client_p:
        proc.terminate()
#    server_p.terminate()
    monitorclient_p.terminate()
    monitoring_p.terminate()

    if tmp_created:
        os.rmdir(ipc_dir)


if __name__ == "__main__":
    main()

