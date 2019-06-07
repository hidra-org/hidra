from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from multiprocessing import Process
import zmq

# using zmq in multiprocessing Process classes can result in problem
# https://stackoverflow.com/questions/44257579/zeromq-hangs-in-a-python-multiprocessing-class-object-solution

# -> even when moving the context and socket creation into run it hangs 
# sometime (1 out of 5 times)


def server(endpoint):
	print("Server bind to device")
	context = zmq.Context()
	socket = context.socket(zmq.PUSH)
	socket.bind(endpoint)

	for i in range(2):
		socket.send("Request #{} from server".format(i).encode())


def client(endpoint):
	print("Client connecting to device")
	context = zmq.Context()
	socket = context.socket(zmq.PULL)
	socket.connect(endpoint)

	for i in range(2):
		message = socket.recv().decode()
		print("Client: Received - {}".format(message))


class Server(Process):
    def __init__(self, endpoint):
        Process.__init__(self)

        self.endpoint = endpoint

    def run(self):
        print("Server bind to device")

        # zmq breaks if the context and socket are created inside the
        # constructor
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.bind(self.endpoint)

        print("server run")
        for i in range(2):
            self.socket.send("Request #{} from server".format(i).encode())


class Client(Process):
    def __init__(self, endpoint):

        Process.__init__(self)

        self.endpoint = endpoint

    def run(self):
        print("Client connecting to device")

        # zmq breaks if the context and socket are created inside the
        # constructor
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.connect(self.endpoint)

        print("client run")
        for i in range(2):
            message = self.socket.recv().decode()
            print("Client: Received - {}".format(message))


def use_class(endpoint):
    server_p = Server(endpoint)
    server_p.start()

    client_p = Client(endpoint)
    client_p.start()

    server_p.join()
    client_p.join()


def use_function(endpoint):
    server_p = Process(target=server, args=(endpoint,))
    server_p.start()

    client_p = Process(target=client, args=(endpoint,))
    client_p.start()

    server_p.join()
    client_p.join()

if __name__ =="__main__":
    endpoint = "ipc:///tmp/test_zmq_with_processes"

    use_class(endpoint)
    #use_function(endpoint)
