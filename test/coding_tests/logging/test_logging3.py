
# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

# System imports
import multiprocessing
import logging
import logging.handlers

from logutils.queue import QueueHandler, QueueListener

# Next two import lines for this demo only
from random import choice, random
import time

# Arrays used for random selections in this demo
MESSAGES = [
    'Random message #1',
    'Random message #2',
    'Random message #3',
]


class CustomQueueListener(QueueListener):
    def __init__(self, queue, *handlers):
        super().__init__(queue, *handlers)
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
            # This check is not in the parent class
            if record.levelno >= handler.level:
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


def log_configuration():
    # Set format
    datef = "%Y-%m-%d %H:%M:%S"
    f = ("[%(asctime)s] [%(module)s:%(funcName)s:%(lineno)d] "
         "[%(name)s] [%(levelname)s] %(message)s")
#    f = "%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s"

    # Setup stream handler 1 to output WARNING to file
    h1 = logging.handlers.RotatingFileHandler("mptest.log", 'a', 300000, 5)
    f1 = logging.Formatter(datefmt=datef, fmt=f)
    h1.setFormatter(f1)
    h1.setLevel(logging.WARNING)

    # Setup stream handler 2 to output INFO to console
    h2 = logging.StreamHandler()
    f2 = logging.Formatter(
        datefmt=datef,
        fmt="[%(asctime)s] > [%(filename)s:%(lineno)d] %(message)s"
    )
    h2.setFormatter(f2)
    h2.setLevel(logging.INFO)

    return h1, h2


# The worker configuration is done at the start of the worker process run.
# Note that on Windows you can't rely on fork semantics, so each process
# will run the logging configuration code when it starts.
def worker_configurer(queue):
    # Create log and set handler to queue handle
    h = QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)


def worker_process1(queue, configurer):
    configurer(queue)
    name = multiprocessing.current_process().name
    print("Worker started: %s" % name)
    for i in range(10):
        time.sleep(random())
        logger = logging.getLogger("stream 1")
        level = logging.INFO
        message = choice(MESSAGES)
        logger.log(level, message)
    print("Worker finished: %s" % name)


def worker_process2(queue, configurer):
    configurer(queue)
    name = multiprocessing.current_process().name
    print("Worker started: %s" % name)
    for i in range(10):
        time.sleep(random())
        logger = logging.getLogger("stream 2")
        level = logging.WARNING
        message = choice(MESSAGES)
        logger.log(level, message)
    print("Worker finished: %s" % name)


def main():
    # Get queue
    q = multiprocessing.Queue(-1)

    # Get the log Configuration for the listener
    h1, h2 = log_configuration()

    # Start queue listener using the stream handler above
    ql = CustomQueueListener(q, h1, h2)
    ql.start()

    workers = []

    worker = multiprocessing.Process(target=worker_process1,
                                     args=(q, worker_configurer))
    workers.append(worker)
    worker.start()

    worker = multiprocessing.Process(target=worker_process2,
                                     args=(q, worker_configurer))
    workers.append(worker)
    worker.start()

    for w in workers:
        w.join()
    q.put_nowait(None)

    ql.stop()


if __name__ == '__main__':
    main()
