import zmq
import cPickle

context = zmq.Context()

comSocket = context.socket(zmq.REQ)
comSocket.connect("tcp://zitpcx19282.desy.de:50000")

dataSocket = context.socket(zmq.PULL)
dataSocket.bind("tcp://131.169.185.121:50101")

requestSocket = context.socket(zmq.PUSH)
requestSocket.connect("tcp://zitpcx19282.desy.de:50001")


comSocket.send_multipart(["0.0.1",  "START_QUERY_NEXT", cPickle.dumps([["zitpcx19282.desy.de:50101", 1]])])
print "Signal responded: ", comSocket.recv()

try:
    requestSocket.send_multipart(["NEXT", "zitpcx19282.desy.de:50101"])

    multipartMessage = dataSocket.recv_multipart()
    metadata = cPickle.loads(multipartMessage[0])
    payload = multipartMessage[1:]

    print "metadata: ", metadata

finally:
    comSocket.send_multipart(["0.0.1",  "STOP_QUERY_NEXT", cPickle.dumps([["zitpcx19282.desy.de:50101", 1]])])

    comSocket.close()
    dataSocket.close()
    requestSocket.close()

    context.destroy()

