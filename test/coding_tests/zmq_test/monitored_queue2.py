import time
import zmq
from zmq.devices.monitoredqueuedevice import MonitoredQueue
from zmq.utils.strtypes import asbytes
from multiprocessing import Process
import os
import tempfile

class MonitorDevice():
    def __init__ (self, in_con_str, out_con_str, mon_con_str):

        self.in_prefix = asbytes('in')
        self.out_prefix = asbytes('out')

        self.context = zmq.Context()

        self.in_socket = self.context.socket(zmq.PULL)
        self.in_socket.bind(in_con_str)

        self.out_socket = self.context.socket(zmq.PUSH)
        self.out_socket.bind(out_con_str)

        self.mon_socket = self.context.socket(zmq.PUSH)
        self.mon_socket.bind(mon_con_str)

        self.run()

    def run (self):
        while True:
            msg = self.in_socket.recv_multipart()
            print "[MonitoringDevice] In: Received message {0}".format(msg)

            mon_msg = [self.in_prefix] + msg
            self.mon_socket.send_multipart(mon_msg)
            print "[MonitoringDevice] Mon: Send message {0}".format(mon_msg)

            self.out_socket.send_multipart(msg)
            print "[MonitoringDevice] Out: Send message {0}".format(msg)

            mon_msg = [self.out_prefix] + msg
            self.mon_socket.send_multipart(mon_msg)
            print "[MonitoringDevice] Mon: Send message {0}".format(mon_msg)


def server(in_con_str):
    print "[Server] connecting to device"
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect(in_con_str)
    for request_num in range(2):
        socket.send ("Request #{0} from server".format(request_num))


def client(out_con_str, mon_con_str, client_id):
    print "[Client #{0}] Worker #%s connecting to device".format(client_id)
    context = zmq.Context()
    data_socket = context.socket(zmq.PULL)
    data_socket.connect(out_con_str)

    print "[Client #{0}] Starting monitoring process".format(client_id)
    mon_socket = context.socket(zmq.PULL)
    print "[Client #{0}] Collecting updates from server...".format(client_id)
    mon_socket.connect (mon_con_str)
#    mon_socket.setsockopt(zmq.SUBSCRIBE, "")

    mon_socket.connect (mon_con_str)

    while True:
        print "[Client #{0}] Monitoring waiting".format(client_id)
        string = mon_socket.recv_multipart()
        print "[Client #{0}] Monitoring received: {1}".format(client_id, string)
        string = mon_socket.recv_multipart()
        print "[Client #{0}] Monitoring received: {1}".format(client_id, string)
#        time.sleep(2)
        message = data_socket.recv()
        print "[Client #{0}] Received - {1}".format(client_id, message)


if __name__ == "__main__":
    ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
    current_pid = os.getpid()
    frontend_port = 5559

    if not os.path.exists(ipc_path):
        os.mkdir(ipc_path)
        os.chmod(ipc_path, 0o777)
        tmp_created = True
    else:
        tmp_created = False

    in_con_str = "tcp://127.0.0.1:{0}".format(frontend_port)
    out_con_str = "ipc://{0}/{1}_{2}".format(ipc_path, current_pid, "out")
    mon_con_str = "ipc://{0}/{1}_{2}".format(ipc_path, current_pid, "mon")

    print "in_con_str", in_con_str
    print "out_con_str", out_con_str
    print "mon_con_str", mon_con_str

    number_of_workers = 2

    """
    in_prefix = asbytes('in')
    out_prefix = asbytes('out')

    monitoring_p = MonitorDevice2(zmq.PULL, zmq.PUSH, zmq.PUB,
                                  in_prefix, out_prefix)

    monitoring_p.bind_in(in_con_str)
    monitoring_p.bind_out(out_con_str)
    monitoring_p.bind_mon(mon_con_str)
    """
    monitoring_p = Process(target=MonitorDevice, args=(in_con_str, out_con_str, mon_con_str))
    monitoring_p.start()
    server_p = Process(target=server, args=(in_con_str,))
    server_p.start()

    client_p = []
    for client_id in range(number_of_workers):
        p = Process(target=client, args=(out_con_str, mon_con_str, client_id,))
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
        os.rmdir(ipc_path)
