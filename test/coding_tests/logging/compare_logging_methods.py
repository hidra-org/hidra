from __future__ import print_function

import logging
import logging.handlers
import logbook
import time
import sys


def run_logging(loglevel, n_iter, n_call):
    logger = logging.getLogger("logging")
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(loglevel)

    for i in range(n_iter):
        logger.debug('logging message {}, {}'.format(i, n_call))
    logger.info("print: finished {}".format(n_call))

    logger.removeHandler(handler)

def run_logging_with_if(loglevel, n_iter):
    logger = logging.getLogger("logging_with_if")
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(loglevel)
    if loglevel == logging.DEBUG:
        debug = True
    else:
        debug = False

    for i in range(n_iter):
        if debug:
            logger.debug('logging message {}'.format(i))
    logger.info("logging with if: finished")

    logger.removeHandler(handler)

#    {'extra': defaultdict(<function <lambda> at 0x7f344f75bb90>, {}), 'frame_correction': 0, 'level': 10, 'process': 19178, 'frame': <frame object at 0x7f3450022b90>, 'args': (), 'heavy_initialized': True, 'time': datetime.datetime(2018, 6, 15, 10, 16, 38, 345634), 'kwargs': {}, 'msg': 'logbook message', 'exc_info': None, '_dispatcher': <weakref at 0x7f34513c35d0; to 'Logger' at 0x7f344f75c210>, 'channel': 'logbook'}

def run_logbook(loglevel):
    #def my_formatter(record, handler):
    #    print(vars(record))
    handler = logbook.StreamHandler(sys.stdout)
    #handler.formatter = my_formatter
    #handler.format_string = '{record.time} {record.channel} {record.message}'
    handler.push_application()
    log = logbook.Logger('logbook', loglevel)
    for i in range(n_iter):
        log.debug('logbook message {}'.format(i))
    log.info("logbook: finished")

def run_logbook_with_if(loglevel):
    handler = logbook.StreamHandler(sys.stdout)
    #handler.format_string = '{record.time} {record.level} {record.channel} {record.message}'
    handler.push_application()
    log = logbook.Logger('logbook with if', loglevel)

    if loglevel == logbook.DEBUG:
        debug = True
    else:
        debug = False

    for i in range(n_iter):
        if debug:
            log.debug('logbook with if message {}'.format(i))
    log.info("logbook with if: finished")

def run_print(loglevel, n_iter):
    for i in range(n_iter):
        if loglevel == "debug":
            print('print: logging message {}'.format(i))
    print("print: finished")

if __name__ == "__main__":

    #n_iter = 1
    n_iter = 100000

    output = ""

    # DEBUG

    n_call = 1
    logging_start_time_debug = time.time()
    run_logging(logging.DEBUG, n_iter, n_call)
    logging_stop_time_debug = time.time()

    logging_w_if_start_time_debug = time.time()
    run_logging_with_if(logging.DEBUG, n_iter)
    logging_w_if_stop_time_debug = time.time()

    print_start_time_debug = time.time()
    run_print("debug", n_iter)
    print_stop_time_debug = time.time()

    logbook_start_time_debug = time.time()
    run_logbook(logbook.DEBUG)
    logbook_stop_time_debug = time.time()

    logbook_w_if_start_time_debug = time.time()
    run_logbook_with_if(logbook.DEBUG)
    logbook_w_if_stop_time_debug = time.time()

    # INFO

    n_call = 2
    logging_start_time_info = time.time()
    run_logging(logging.INFO, n_iter, n_call)
    logging_stop_time_info = time.time()

    logging_w_if_start_time_info = time.time()
    run_logging_with_if(logging.INFO, n_iter)
    logging_w_if_stop_time_info = time.time()

    print_start_time_info = time.time()
    run_print("info", n_iter)
    print_stop_time_info = time.time()

    logbook_start_time_info = time.time()
    run_logbook(logbook.INFO)
    logbook_stop_time_info = time.time()

    logbook_w_if_start_time_info = time.time()
    run_logbook_with_if(logbook.INFO)
    logbook_w_if_stop_time_info = time.time()


    logging_time_debug = logging_stop_time_debug - logging_start_time_debug
    logging_with_if_time_debug = logging_w_if_stop_time_debug - logging_w_if_start_time_debug
    print_time_debug = print_stop_time_debug - print_start_time_debug
    logbook_time_debug = logbook_stop_time_debug - logbook_start_time_debug
    logbook_with_if_time_debug = logbook_w_if_stop_time_debug - logbook_w_if_start_time_debug

    logging_time_info = logging_stop_time_info - logging_start_time_info
    logging_with_if_time_info = logging_w_if_stop_time_info - logging_w_if_start_time_info
    print_time_info = print_stop_time_info - print_start_time_info
    logbook_time_info = logbook_stop_time_info - logbook_start_time_info
    logbook_with_if_time_info = logbook_w_if_stop_time_info - logbook_w_if_start_time_info

    output += "\n\n"
    output += "log level             \tDEBUG" + " " * 12 + "\tINFO\n"
    output += "logging needed         \t{} \t{}\n".format(logging_time_debug, logging_time_info)
    output += "logging with if needed \t{} \t{}\n".format(logging_with_if_time_debug, logging_with_if_time_info)
    output += "print needed           \t{} \t{}\n".format(print_time_debug, print_time_info)
    output += "logbook needed         \t{} \t{}\n".format(logbook_time_debug, logbook_time_info)
    output += "logbook with if needed \t{} \t{}\n".format(logbook_with_if_time_debug, logbook_with_if_time_info)

    print(output)
