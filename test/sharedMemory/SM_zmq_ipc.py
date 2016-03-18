import zmq
from multiprocessing import Process
import time

def f1():

    context1 = zmq.Context()
    socket1 = context1.socket(zmq.PUSH)
    socket1.connect("ipc://test.ipc")

#    print "p1: sockets created"
    try:
        for i in range(1,11):
            filename = "/space/projects/zeromq-data-transfer/data/source/local/raw/" + str(i) + ".cbf"
            time.sleep(0.2)
            f = open(filename, "rb")
            fileObject = f.read()
#            print "p1: file " + filename + " read"
            f.close()

            socket1.send_multipart([str(i),fileObject])
    finally:
        time.sleep(1)
#        print "p1: close socket"
        socket1.close()
#        print "p1: destroy context"
        context1.destroy()

def f2():

    context2 = zmq.Context()
    socket2 = context2.socket(zmq.PUSH)
    socket2.connect("ipc://test.ipc")

#    print "p2: sockets created"
    try:
        for j in range(11,21):
            filename = "/space/projects/zeromq-data-transfer/data/source/local/raw/" + str(j) + ".cbf"
            time.sleep(0.1)
            f = open(filename, "rb")
            fileObject = f.read()
#            print "p2: file " + filename + " read"
            f.close()

            socket2.send_multipart([str(j),fileObject])
    finally:
        time.sleep(1)
#        print "p2: close socket"
        socket2.close()
#        print "p2: destroy context"
        context2.destroy()

def f3():
    context3 = zmq.Context()
    socket3 = context3.socket(zmq.PULL)
    socket3.bind("ipc://test.ipc")

    i = 1
    try:
        t1 = time.time()
        while i<=20:

#            print "main: receive"
            res = socket3.recv_multipart()
#            print i, res[0][:10], res[1][:10]

            time.sleep(0.01)
            i += 1
        t2 = time.time()
        print "time needed", t2-t1
    finally:
#        print "p3: close socket"
        socket3.close()
#        print "p3: destroy context"
        context3.destroy()


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
