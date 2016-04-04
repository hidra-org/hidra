__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq

def __sendToTargets(log, targets, sourceFile, targetFile, openConnections, metadata, payload, context):

    for target, prio, sendType in targets:

        # send data to the data stream to store it in the storage system
        if prio == 0:
            # socket not known
            if target not in openConnections:
                # open socket
                socket        = context.socket(zmq.PUSH)
                connectionStr = "tcp://" + str(target)

                socket.connect(connectionStr)
                log.info("Start socket (connect): '" + str(connectionStr) + "'")

                # register socket
                openConnections[target] = socket

            # send data
            if sendType == "data":
                tracker = openConnections[target].send_multipart(payload, copy=False, track=True)
                log.info("Sending message part from file " + str(sourceFile) +
                         " to '" + target + "' with priority " + str(prio) )

            elif sendType == "metadata":
                tracker = openConnections[target].send_multipart(metadata, copy=False, track=True)
                log.info("Sending metadata of message part from file " + str(sourceFile) +
                         " to '" + target + "' with priority " + str(prio) )
                log.debug("metadata=" + str(metadata))


            if not tracker.done:
                log.info("Message part from file " + str(sourceFile) +
                         " has not been sent yet, waiting...")
                tracker.wait()
                log.info("Message part from file " + str(sourceFile) +
                         " has not been sent yet, waiting...done")

        else:
            # socket not known
            if target not in openConnections:
                # open socket
                socket        = context.socket(zmq.PUSH)
                connectionStr = "tcp://" + str(target)

                socket.connect(connectionStr)
                log.info("Start socket (connect): '" + str(connectionStr) + "'")

                # register socket
                openConnections[target] = socket

            # send data
            if sendType == "data":
                openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                log.info("Sending message part from file " + str(sourceFile) +
                         " to " + target)

            elif sendType == "metadata":
                openConnections[target].send_multipart(metadata, zmq.NOBLOCK)
                log.info("Sending metadata of message part from file " + str(sourceFile) +
                         " to " + target)
                log.debug("metadata=" + str(metadata))

