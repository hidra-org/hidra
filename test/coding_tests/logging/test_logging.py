""" Test how logging with a QueueHandler and multiple processes work.
"""

from __future__ import print_function

import logging
import logging.handlers
import multiprocessing

# Next two import lines for this demo only
from random import choice, random
import sys
import time
import traceback

from logutils.queue import QueueHandler


def listener_configurer():
    """
    Because you'll want to define the logging configurations for listener and
    workers, the listener and worker process functions take a configurer
    parameter which is a callable for configuring logging for that process.
    These functions are also passed the queue, which they use for
    communication.

    In practice, you can configure the listener however you want, but note that
    in this simple example, the listener does not apply level or filter logic
    to received records. In practice, you would probably want to do this logic
    in the worker processes, to avoid sending events which would be filtered
    out between processes.

    The size of the rotated files is made small so you can see the results
    easily.
    """

    root = logging.getLogger()
    handle = logging.handlers.RotatingFileHandler(
        filename='mptest.log', mode='a', maxBytes=300000, backupCount=5
    )
    f = logging.Formatter("'%(asctime)s "
                          "%(processName)-10s "
                          "%(name)s "
                          "%(levelname)-8s "
                          "%(message)s")
    handle.setFormatter(f)
    root.addHandler(handle)


def listener_process(queue, configurer):
    """
    This is the listener process top-level loop: wait for logging events
    (LogRecords)on the queue and handle them, quit when you get a None for a
    LogRecord.
    """

    configurer()
    while True:
        try:
            record = queue.get()
            # We send this as a sentinel to tell the listener to quit.
            if record is None:
                break
            logger = logging.getLogger(record.name)
            # No level or filter logic applied - just do it!
            logger.handle(record)
        except Exception:
            print("Whoops! Problem:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


# Arrays used for random selections in this demo
LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING,
          logging.ERROR, logging.CRITICAL]

LOGGERS = ['a.b.c', 'd.e.f']

MESSAGES = [
    'Random message #1',
    'Random message #2',
    'Random message #3',
]


def worker_configurer(queue):
    """
    The worker configuration is done at the start of the worker process run.
    Note that on Windows you can't rely on fork semantics, so each process
    will run the logging configuration code when it starts.
    """

    handler = QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(handler)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)


def worker_process(queue, configurer):
    """
    This is the worker process top-level loop, which just logs ten events with
    random intervening delays before terminating.
    The print messages are just so you know it's doing something!
    """

    configurer(queue)
    name = multiprocessing.current_process().name
    print("Worker started: %s" % name)
    for _ in range(10):
        time.sleep(random())
        logger = logging.getLogger(choice(LOGGERS))
        level = choice(LEVELS)
        message = choice(MESSAGES)
        logger.log(level, message)
    print("Worker finished: %s" % name)


def main():
    """
    Here's where the demo gets orchestrated. Create the queue, create and start
    the listener, create ten workers and start them, wait for them to finish,
    then send a None to the queue to tell the listener to finish.
    """

    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=listener_process,
                                       args=(queue, listener_configurer))
    listener.start()
    workers = []
    for _ in range(10):
        worker = multiprocessing.Process(target=worker_process,
                                         args=(queue, worker_configurer))
        workers.append(worker)
        worker.start()
    for i in workers:
        i.join()
    queue.put_nowait(None)
    listener.join()


if __name__ == '__main__':
    main()
