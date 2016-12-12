from __future__ import print_function
#from __future__ import unicode_literals

import os

from __init__ import BASE_PATH
import helpers

from hidra import Transfer


# enable logging
logfile_path = os.path.join(BASE_PATH, "logs")
logfile = os.path.join(logfile_path, "example_force_stop.log")
helpers.init_logging(logfile, True, "DEBUG")

if __name__ == "__main__":

    signal_host = "zitpcx19282.desy.de"
#    signal_host = "lsdma-lab04.desy.de"
#    signal_host = "asap3-bl-prx07.desy.de"

#    targets = [["asap3-bl-prx07.desy.de", "50101", 1, [".cbf"]],
#               ["asap3-bl-prx07.desy.de", "50102", 1, [".cbf"]],
#               ["asap3-bl-prx07.desy.de", "50103", 1, [".cbf"]]]
#    targets = [["zitpcx19282.desy.de", "50101", 1, [".cbf"]]]
    targets = [["zitpcx19282.desy.de", "50100", 1, [".cbf"]],
               ["zitpcx19282.desy.de", "50101", 1, [".cbf"]],
               ["zitpcx19282.desy.de", "50102", 1, [".cbf"]]]
#    targets = [["zitpcx19282.desy.de", "50101", 1],
#               ["zitpcx19282.desy.de", "50102", 1],
#               ["zitpcx19282.desy.de", "50103", 1]]
#    targets = [["zitpcx19282.desy.de", "50101", 1, [".cbf"]],
#               ["zitpcx19282.desy.de", "50102", 1, [".cbf"]],
#               ["zitpcx19282.desy.de", "50103", 1, [".cbf"]],
#               ["lsdma-lab04.desy.de", "50104", 1, [".cbf"]]]

    transfer_type = "queryNext"
#    transfer_type = "stream"
#    transfer_type = "streamMetadata"
#    transfer_type = "queryMetadata"

    query = Transfer(transfer_type, signal_host, use_log=True)
    query.force_stop(targets)
