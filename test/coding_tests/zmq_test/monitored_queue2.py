from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import time
import zmq
from zmq.devices.monitoredqueuedevice import MonitoredQueue
from zmq.utils.strtypes import asbytes
from multiprocessing import Process
import os
import tempfile

class MonitorDevice():
    def __init__ (self, in_endpoint, out_endpoint, mon_endpoint):

        self.in_prefix = asbytes('in')
        self.out_prefix = asbytes('out')

        self.context = zmq.Context()

        self.in_socket = self.context.socket(zmq.PULL)
        self.in_socket.bind(in_endpoint)

        self.out_socket = self.context.socket(zmq.PUSH)
        self.out_socket.bind(out_endpoint)

        self.mon_socket = self.context.socket(zmq.PUSH)
        self.mon_socket.bind(mon_endpoint)

        self.run()

    def run (self):
        while True:
            msg = self.in_socket.recv_multipart()
            print("[MonitoringDevice] In: Received message {}".format(msg))

            mon_msg = [self.in_prefix] + msg
            self.mon_socket.send_multipart(mon_msg)
            print("[MonitoringDevice] Mon: Send message {}".format(mon_msg))

            self.out_socket.send_multipart(msg)
            print("[MonitoringDevice] Out: Send message {}".format(msg))

            mon_msg = [self.out_prefix] + msg
            self.mon_socket.send_multipart(mon_msg)
            print("[MonitoringDevice] Mon: Send message {}".format(mon_msg))


def server(in_endpoint):
    print "[Server] connecting to device"
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(in_endpoint)
    for request_num in range(2):
        socket.send("Request #{} from server".format(request_num))


def client(out_endpoint, mon_endpoint, client_id):
    print("[Client #{}] Worker #%s connecting to device".format(client_id))
    context = zmq.Context()
    data_socket = context.socket(zmq.PULL)
    data_socket.connect(out_endpoint)

    print("[Client #{}] Starting monitoring process".format(client_id))
    mon_socket = context.socket(zmq.PULL)
    print("[Client #{}] Collecting updates from server...".format(client_id))
    mon_socket.connect (mon_endpoint)
#    mon_socket.setsockopt(zmq.SUBSCRIBE, "")

    mon_socket.connect (mon_endpoint)

    while True:
        print("[Client #{}] Monitoring waiting".format(client_id))
        string = mon_socket.recv_multipart()
        print("[Client #{}] Monitoring received: {}".format(client_id, string))
        string = mon_socket.recv_multipart()
        print("[Client #{}] Monitoring received: {}".format(client_id, string))
#        time.sleep(2)
        message = data_socket.recv()
        print("[Client #{}] Received - {}".format(client_id, message))


if __name__ == "__main__":
    ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")
    current_pid = os.getpid()
    frontend_port = 5559

    if not os.path.exists(ipc_dir):
        os.mkdir(ipc_dir)
        os.chmod(ipc_dir, 0o777)
        tmp_created = True
    else:
        tmp_created = False

    in_endpoint = "tcp://127.0.0.1:{}".format(frontend_port)
    out_endpoint = "ipc://{}/{}_{}".format(ipc_dir, current_pid, "out")
    mon_endpoint = "ipc://{}/{}_{}".format(ipc_dir, current_pid, "mon")

    print("in_endpoint", in_endpoint)
    print("out_endpoint", out_endpoint)
    print("mon_endpoint", mon_endpoint)

    number_of_workers = 2

    """
    in_prefix = asbytes('in')
    out_prefix = asbytes('out')

    monitoring_p = MonitorDevice2(zmq.PULL, zmq.PUSH, zmq.PUB,
                                  in_prefix, out_prefix)

    monitoring_p.bind_in(in_endpoint)
    monitoring_p.bind_out(out_endpoint)
    monitoring_p.bind_mon(mon_endpoint)
    """
    monitoring_p = Process(target=MonitorDevice,
                           args=(in_endpoint, out_endpoint, mon_endpoint))
    monitoring_p.start()
    server_p = Process(target=server, args=(in_endpoint,))
    server_p.start()

    client_p = []
    for client_id in range(number_of_workers):
        p = Process(target=client,
                    args=(out_endpoint, mon_endpoint, client_id,))
        p.start()
        client_p.append(p)

    try:
        for proc in client_p:
            proc.join()
    except KeyboardInterrupt:
        for proc in client_p:
            proc.terminate()
        monitoring_p.terminate()

    if tmp_created:
        os.rmdir(ipc_dir)
