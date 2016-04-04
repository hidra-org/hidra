__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'

import zmq

def __sendToTargets(log, targets, sourceFile, openConnections, payload, context, properties):

    for target, prio, sendType in targets:

        # send data to the data stream to store it in the storage system
        if prio == 0:
            # socket already known
            if target in openConnections:
                tracker = openConnections[target].send_multipart(payload, copy=False, track=True)
                log.info("Sending message part from file " + str(sourceFile) +
                         " to '" + target + "' with priority " + str(prio) )
            else:
                # open socket
                socket        = context.socket(zmq.PUSH)
                connectionStr = "tcp://" + str(target)

                socket.connect(connectionStr)
                log.info("Start socket (connect): '" + str(connectionStr) + "'")

                # register socket
                openConnections[target] = socket

                # send data
                tracker = openConnections[target].send_multipart(payload, copy=False, track=True)
                log.info("Sending message part from file " + str(sourceFile) +
                         " to '" + target + "' with priority " + str(prio) )

            # socket not known
            if not tracker.done:
                log.info("Message part from file " + str(sourceFile) +
                         " has not been sent yet, waiting...")
                tracker.wait()
                log.info("Message part from file " + str(sourceFile) +
                         " has not been sent yet, waiting...done")

        else:
            # socket already known
            if target in openConnections:
                # send data
                openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                log.info("Sending message part from file " + str(sourceFile) +
                         " to " + target)
            # socket not known
            else:
                # open socket
                socket        = context.socket(zmq.PUSH)
                connectionStr = "tcp://" + str(target)

                socket.connect(connectionStr)
                log.info("Start socket (connect): '" + str(connectionStr) + "'")

                # register socket
                openConnections[target] = socket

                # send data
                openConnections[target].send_multipart(payload, zmq.NOBLOCK)
                log.info("Sending message part from file " + str(sourceFile) +
                         " to " + target)

