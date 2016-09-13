__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq
import cPickle


class DataHandlingError(Exception):
    pass


def __sendToTargets(log, targets, sourceFile, targetFile, openConnections, metadata, payload, context, timeout = -1):

    for target, prio, suffixes, sendType in targets:

        # send data to the data stream to store it in the storage system
        if prio == 0:
            # socket not known
            if target not in openConnections:
                # open socket
                try:
                    socket        = context.socket(zmq.PUSH)
                    connectionStr = "tcp://{t}".format(t=target)

                    socket.connect(connectionStr)
                    log.info("Start socket (connect): '{s}'".format(s=connectionStr))

                    # register socket
                    openConnections[target] = socket
                except:
                    raise DataHandlingError("Failed to start socket (connect): '{s]'".format(s=connectionStr))

            # send data
            try:
                if sendType == "data":
                    tracker = openConnections[target].send_multipart(payload, copy=False, track=True)
                    log.info("Sending message part from file '{s}' to '{t}' with priority {p}".format(s=sourceFile, t=target, p=prio))

                elif sendType == "metadata":
                    #cPickle.dumps(None) is 'N.'
                    tracker = openConnections[target].send_multipart([cPickle.dumps(metadata), cPickle.dumps(None)], copy=False, track=True)
                    log.info("Sending metadata of message part from file '{s}' to '{t}' with priority {p}".format(s=sourceFile, t=target, p=prio))
                    log.debug("metadata={m}".format(m=metadata))

                if not tracker.done:
                    log.debug("Message part from file '{f}' has not been sent yet, waiting...".format(f=sourceFile))
                    tracker.wait(timeout)
                    log.debug("Message part from file '{f}' has not been sent yet, waiting...done".format(f=sourceFile))

            except:
                raise DataHandlingError("Sending (metadata of) message part from file '{s}' to '{t}' with priority {p} failed.".format(s=sourceFile, t=target, p=prio))


        else:
            # socket not known
            if target not in openConnections:
                # open socket
                socket        = context.socket(zmq.PUSH)
                connectionStr = "tcp://{t}".format(t=target)

                socket.connect(connectionStr)
                log.info("Start socket (connect): '{s}'".format(s=connectionStr))

                # register socket
                openConnections[target] = socket

            # send data
            if sendType == "data":
                openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                log.info("Sending message part from file '{s}' to '{t}' with priority {p}".format(s=sourceFile, t=target, p=prio))

            elif sendType == "metadata":
                openConnections[target].send_multipart([cPickle.dumps(metadata), cPickle.dumps(None)], zmq.NOBLOCK)
                log.info("Sending metadata of message part from file '{s}' to '{t}' with priority {p}".format(s=sourceFile, t=target, p=prio) )
                log.debug("metadata={m}".format(m=metadata))

