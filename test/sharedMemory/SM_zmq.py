import zmq
from multiprocessing import Process
import time

def f1():

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:9999")

#    print "p1: sockets created"
    try:
        for i in range(1,11):
            filename = "/space/projects/live-viewer/data/source/local/raw/" + str(i) + ".cbf"
            time.sleep(0.2)
            f = open(filename, "rb")
            fileObject = f.read()
#            print "p1: file " + filename + " read"
            f.close()

            socket.send(fileObject)
    finally:
        socket.close(1)
        context.destroy()

def f2():

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:9999")

#    print "p2: sockets created"
    try:
        for j in range(11,21):
            filename = "/space/projects/live-viewer/data/source/local/raw/" + str(j) + ".cbf"
            time.sleep(0.1)
            f = open(filename, "rb")
            fileObject = f.read()
#            print "p2: file " + filename + " read"
            f.close()

            socket.send(fileObject)
    finally:
        socket.close(1)
        context.destroy()

def f3():
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind("tcp://127.0.0.1:9999")

    i = 1
    try:
        t1 = time.time()
        while i<=20:

#            print "main: receive"
            res = socket.recv()
#            print res[:10]

            time.sleep(0.01)
            i += 1
        t2 = time.time()
        print "time needed", t2-t1
    finally:
        socket.close(1)
        context.destroy()


if __name__ == '__main__':

    p1 = Process(target=f1)
    p2 = Process(target=f2)
    p3 = Process(target=f3)

    p3.start()
    p1.start()
    p2.start()

    p1.join()
    p2.join()
    p3.join()
