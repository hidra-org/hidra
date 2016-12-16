from __future__ import unicode_literals

import zmq
import json

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    pass


def send_to_targets(log, targets, source_file, target_file, open_connections,
                      metadata, payload, context, timeout=-1):

    for target, prio, suffixes, send_type in targets:

        # send data to the data stream to store it in the storage system
        if prio == 0:
            # socket not known
            if target not in open_connections:
                # open socket
                try:
                    socket = context.socket(zmq.PUSH)
                    connection_str = "tcp://{0}".format(target)

                    socket.connect(connection_str)
                    log.info("Start socket (connect): '{0}'"
                             .format(connection_str))

                    # register socket
                    open_connections[target] = socket
                except:
                    raise DataHandlingError("Failed to start socket (connect):"
                                            " '{0}'".format(connection_str))

            # send data
            try:
                if send_type == "data":
                    tracker = open_connections[target].send_multipart(
                        payload, copy=False, track=True)
                    log.info("Sending message part from file '{0}' to '{1}' "
                             "with priority {2}"
                             .format(source_file, target, prio))

                elif send_type == "metadata":
                    # json.dumps(None) is 'N.'
                    tracker = open_connections[target].send_multipart(
                        [json.dumps(metadata).encode("utf-8"),
                         json.dumps(None).encode("utf-8")],
                        copy=False, track=True)
                    log.info("Sending metadata of message part from file "
                             "'{0}' to '{1}' with priority {2}"
                             .format(source_file, target, prio))
                    log.debug("metadata={0}".format(metadata))

                if not tracker.done:
                    log.debug("Message part from file '{0}' has not been sent "
                              "yet, waiting...".format(source_file))
                    tracker.wait(timeout)
                    log.debug("Message part from file '{0}' has not been sent "
                              "yet, waiting...done".format(source_file))

            except:
                raise DataHandlingError("Sending (metadata of) message part "
                                        "from file '{0}' to '{1}' with "
                                        "priority {2} failed."
                                        .format(source_file, target, prio))

        else:
            # socket not known
            if target not in open_connections:
                # open socket
                socket = context.socket(zmq.PUSH)
                connection_str = "tcp://{0}".format(target)

                socket.connect(connection_str)
                log.info("Start socket (connect): '{0}'"
                         .format(connection_str))

                # register socket
                open_connections[target] = socket

            # send data
            if send_type == "data":
                open_connections[target].send_multipart(payload, zmq.NOBLOCK)
                log.info("Sending message part from file '{0}' to '{1}' with "
                         "priority {2}".format(source_file, target, prio))

            elif send_type == "metadata":
                open_connections[target].send_multipart(
                    [json.dumps(metadata).encode("utf-8"),
                     json.dumps(None).encode("utf-8")],
                    zmq.NOBLOCK)
                log.info("Sending metadata of message part from file '{0}' "
                         "to '{1}' with priority {2}"
                         .format(source_file, target, prio))
                log.debug("metadata={0}".format(metadata))
