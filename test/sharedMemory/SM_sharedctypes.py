from multiprocessing import Process, Lock
from multiprocessing.sharedctypes import Value, Array, RawArray
from ctypes import Structure, c_double, c_char, create_string_buffer, pointer, POINTER, addressof, byref, c_char_p, c_wchar_p

class Point(Structure):
    _fields_ = [('x', c_double), ('y', c_double)]

#class Testclass(Structure):
#    _fields_ = [('metadata', c_char), ('data', c_char)]
#    _fields_ = [('data', create_string_buffer(10))]

Buffer = c_char_p
#Buffer = c_char * 6229487

class FileBuffer(Structure):
    _fields_ = [('data', Buffer)]
#    _fields_ = [('data', POINTER(Buffer))]

def modify(n, x, s, A):
    n.value **= 2
    x.value **= 2
    s.value = s.value.upper()
    for a in A:
        print a
        a.x **= 2
        a.y **= 2

def worker(e):
    print "e"
    print len(e)
    e[1] = "a"
    print [ i for i in e]

def worker2(B):
    print "B"
    print B

    f = open("/space/projects/zeromq-data-transfer/test_015_00001.cbf", "rb")
    fileObject = f.read()
#    print len(fileObject)
    f.close()


    for b in B:
        b.data = fileObject
        print
        print "================================================================"
        print "================================================================"
        print "================================================================"
        print
        print "b.data"
        print len(b.data)


if __name__ == '__main__':
    lock = Lock()

#    n = Value('i', 7)
#    x = Value(c_double, 1.0/3.0, lock=False)
#    s = Array('c', 'hello world', lock=lock)
#    A = Array(Point, [(1.875,-6.25), (-5.75,2.0), (2.375,9.5)], lock=lock)

#    p = Process(target=modify, args=(n, x, s, A))

    blubb = create_string_buffer(10)

    D = Array('c', create_string_buffer(10))
#    p = Process(target=worker, args=(D, ))

    arr = FileBuffer()

    B = Array(FileBuffer, [arr])

    p = Process(target=worker2, args=(B, ))
    p.start()
    p.join()

#    print "===="
#    print n.value
#    print x.value
#    print s.value
#    print [(a.x, a.y) for a in A]
