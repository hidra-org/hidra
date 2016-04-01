# System imports
import logging
import logging.handlers
try:
    import Queue as queue
except ImportError:
    import queue

import os
import sys

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) ))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH


# Custom imports
from logutils.queue import QueueHandler, QueueListener


class CustomQueueListener(QueueListener):
    def __init__(self, queue, *handlers):
        super(CustomQueueListener, self).__init__(queue, *handlers)
        """
        Initialise an instance with the specified queue and
        handlers.
        """
        # Changing this to a list from tuple in the parent class
        self.handlers = list(handlers)

    def handle(self, record):
        """
        Override handle a record.

        This just loops through the handlers offering them the record
        to handle.

        :param record: The record to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            if record.levelno >= handler.level: # This check is not in the parent class
                handler.handle(record)

    def addHandler(self, hdlr):
        """
        Add the specified handler to this logger.
        """
        if not (hdlr in self.handlers):
            self.handlers.append(hdlr)

    def removeHandler(self, hdlr):
        """
        Remove the specified handler from this logger.
        """
        if hdlr in self.handlers:
            hdlr.close()
            self.handlers.remove(hdlr)


# Get queue
q = queue.Queue(-1)

# Setup stream handler 1 to output WARNING to console
h1 = logging.StreamHandler()
f1 = logging.Formatter('STREAM 1 WARNING: %(threadName)s: %(message)s')
h1.setFormatter(f1)
h1.setLevel(logging.WARNING) # NOT WORKING. This should log >= WARNING

# Setup stream handler 2 to output INFO to console
h2 = logging.StreamHandler()
f2 = logging.Formatter('STREAM 2 INFO: %(threadName)s: %(message)s')
h2.setFormatter(f2)
h2.setLevel(logging.INFO) # NOT WORKING. This should log >= WARNING

# Start queue listener using the stream handler above
ql = CustomQueueListener(q, h1, h2)
ql.start()

# Create log and set handler to queue handle
root = logging.getLogger()
root.setLevel(logging.DEBUG) # Log level = DEBUG
qh = QueueHandler(q)
root.addHandler(qh)

root.info('Look out!') # Create INFO message

root.warning('Look out2!') # Create WARNING message

ql.stop()
