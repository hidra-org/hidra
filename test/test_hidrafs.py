import os
import time

path = "/tmp/fs_test"

"""
for f in os.listdir(path):
    filename = os.path.join(path, f)
    if os.path.isfile(filename):
        print "open", filename
        f = open(filename, "w")
        f.write("ABC")
        f.close()
"""
while True:

    for f in os.listdir(path):
        print f
        """
        filename = os.path.join(path, f)
        if os.path.isfile(filename):
            print "open", filename
            f = open(filename, "r")
            read_lines = f.read()
            f.close()
        """

#    time.sleep(0.1)
