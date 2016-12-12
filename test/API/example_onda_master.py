from __future__ import print_function
#from __future__ import unicode_literals

import os

from __init__ import BASE_PATH
import helpers

from hidra import Transfer


# enable logging
logfile_path = os.path.join(BASE_PATH, "logs")
logfile = os.path.join(logfile_path, "test_onda.log")
helpers.init_logging(logfile, True, "DEBUG")

if __name__ == "__main__":

    signal_host = "zitpcx19282.desy.de"

    # a list of targets of the form [<host>, <port, <priority>]
    targets = [["zitpcx19282.desy.de", "50101", 1],
               ["zitpcx19282.desy.de", "50102", 1],
               ["zitpcx19282.desy.de", "50103", 1],
               ["lsdma-lab04.desy.de", "50104", 1]]

    query = Transfer("queryNext", signal_host, use_log=True)
    query.initiate(targets)

    try:
        while True:
            pass
    finally:
        query.stop()
