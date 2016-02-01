from multiprocessing import Manager, Process, Queue
import time

def f1(q):
    for i in range(1,11):
        filename = "/space/projects/live-viewer/data/source/local/raw/" + str(i) + ".cbf"
#        print "f1:", filename
        time.sleep(0.2)
        f = open(filename, "rb")
        fileObject = f.read()
        f.close()

        q.put(fileObject)
#        q.put([42, None, 'hello'])

def f2(q):
    for j in range(11,21):
        filename = "/space/projects/live-viewer/data/source/local/raw/" + str(j) + ".cbf"
#        print "f2: ", filename
        time.sleep(0.1)
        f = open(filename, "rb")
        fileObject = f.read()
        f.close()

        q.put(fileObject)
#        q.put([43, None, 'hell'])

def f3(q):
    i = 1
    while i<=20:
        res = q.get()
        print res[:10]
        time.sleep(0.01)
        i += 1



if __name__ == '__main__':
    q = Queue()

    p1 = Process(target=f1, args=(q, ))
    p2 = Process(target=f2, args=(q, ))
    p3 = Process(target=f3, args=(q, ))

    p1.start()
    p2.start()
    p3.start()


    p1.join()
    p2.join()
    p3.join()
