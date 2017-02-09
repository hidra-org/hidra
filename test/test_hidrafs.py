import os
import time

path = "/space/tmp"

while True:
    for f in os.listdir(path):
        filename = os.path.join(path, f)
        if os.path.isfile(filename):
            print "open", filename
            f = open(filename, "r")
            read_lines = f.read()
            f.close()

    time.sleep(0.1)
