from __future__ import unicode_literals

import zmq
import json

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataHandlingError(Exception):
    pass


def __send_to_targets(log, targets, sourceFile, targetFile, openConnections,
                      metadata, payload, context, timeout=-1):

    for target, prio, suffixes, sendType in targets:

        # send data to the data stream to store it in the storage system
        if prio == 0:
            # socket not known
            if target not in openConnections:
                # open socket
                try:
                    socket = context.socket(zmq.PUSH)
                    connectionStr = "tcp://{0}".format(target)

                    socket.connect(connectionStr)
                    log.info("Start socket (connect): '{0}'"
                             .format(connectionStr))

                    # register socket
                    openConnections[target] = socket
                except:
                    raise DataHandlingError("Failed to start socket (connect):"
                                            " '{0}'".format(connectionStr))

            # send data
            try:
                if sendType == "data":
                    tracker = openConnections[target].send_multipart(
                        payload, copy=False, track=True)
                    log.info("Sending message part from file '{0}' to '{1}' "
                             "with priority {2}"
                             .format(sourceFile, target, prio))

                elif sendType == "metadata":
                    #json.dumps(None) is 'N.'
                    tracker = openConnections[target].send_multipart(
                        [json.dumps(metadata).encode("utf-8"),
                         json.dumps(None).encode("utf-8")],
                        copy=False, track=True)
                    log.info("Sending metadata of message part from file "
                             "'{0}' to '{1}' with priority {2}"
                             .format(sourceFile, target, prio))
                    log.debug("metadata={0}".format(metadata))

                if not tracker.done:
                    log.debug("Message part from file '{0}' has not been sent "
                              "yet, waiting...".format(sourceFile))
                    tracker.wait(timeout)
                    log.debug("Message part from file '{0}' has not been sent "
                              "yet, waiting...done".format(sourceFile))

            except:
                raise DataHandlingError("Sending (metadata of) message part "
                                        "from file '{0}' to '{1}' with "
                                        "priority {2} failed."
                                        .format(sourceFile, target, prio))

        else:
            # socket not known
            if target not in openConnections:
                # open socket
                socket = context.socket(zmq.PUSH)
                connectionStr = "tcp://{0}".format(target)

                socket.connect(connectionStr)
                log.info("Start socket (connect): '{0}'".format(connectionStr))

                # register socket
                openConnections[target] = socket

            # send data
            if sendType == "data":
                openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                log.info("Sending message part from file '{0}' to '{1}' with "
                         "priority {2}".format(sourceFile, target, prio))

            elif sendType == "metadata":
                openConnections[target].send_multipart(
                    [json.dumps(metadata).encode("utf-8"),
                     json.dumps(None).encode("utf-8")],
                    zmq.NOBLOCK)
                log.info("Sending metadata of message part from file '{0}' "
                         "to '{1}' with priority {2}"
                         .format(sourceFile, target, prio))
                log.debug("metadata={0}".format(metadata))
