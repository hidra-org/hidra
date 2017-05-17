from __future__ import print_function
import os

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

    try:
        for f in os.listdir(path):
            print(f)
            """
            filename = os.path.join(path, f)
            if os.path.isfile(filename):
                print "open", filename
                f = open(filename, "r")
                read_lines = f.read()
                print read_lines[:50]
                f.close()
            """
    except KeyboardInterrupt:
        break
