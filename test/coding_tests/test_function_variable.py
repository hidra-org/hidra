from __future__ import print_function

import threading
import time


# class Test(threading.Thread):
#    def __init__(self, my_var):
#        self.my_var = my_var
#        self.keep_running = True
#        threading.Thread.__init__(self)
#
#    def set_var(self, my_var):
#        self.my_var = my_var
#
#    def run(self):
#        while self.keep_running:
#            print("my_var=", self.my_var)
#            time.sleep(1)
#
#    def stop(self):
#        self.keep_running = False


class Subtest(object):
    def __init__(self):
        self.my_var = "test_var"
        self.keep_running = True

    def run(self):
        while self.keep_running:
            print("my_var=", self.my_var)
            time.sleep(1)

        print("stop subtest")


class Test(threading.Thread):
    def __init__(self, my_var):
        self.keep_running = True
        self.subtest = Subtest()
        threading.Thread.__init__(self)

    def run(self):
        while self.keep_running:
            self.subtest.run()
            time.sleep(1)

    def stop(self):
        self.keep_running = False


if __name__ == "__main__":

    my_list = [1, 2, 3]

    t = Test(my_list)
    t.start()
    print("after run")
    t.subtest.keep_running = False

    time.sleep(3)
    t.stop()

