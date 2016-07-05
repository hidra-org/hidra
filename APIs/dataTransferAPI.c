// API to ingest data into a data transfer unit

#include <dataTransferAPI.h>
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>


// helper functions for sending and receiving messages
// source: Pieter Hintjens: ZeroMQ; O'Reilly
static char* s_recv (void *socket)
{
    zmq_msg_t message;
    zmq_msg_init (&message);
    int size = zmq_msg_recv (&message, socket, 0);
    if (size == -1)
        return NULL;
    char *string = malloc (size + 1);
    memcpy (string, zmq_msg_data (&message), size);
    printf("recieved string: %s\n", string);
    zmq_msg_close (&message);
    string [size] = 0;
    return string;
};


static int s_send (void * socket, char* string)
{
    printf("Sending: %s\n", string);
    int size = zmq_send (socket, string, strlen (string), 0);
    if (size == -1) {
        printf("Sending failed\n");
//        fprintf (stderr, "ERROR: Sending failed\n");
//        perror("");
    };
    return size;

};


struct dataTransfer {

    char *localhost;
    char *extIp;
    char *ipcPath;

    char *signalHost;
    char *signalPort;
    char *requestPort;
    char *fileOpPort;
    char *dataHost;
    char *dataPort;

    //  Prepare our context and socket
    void *context;
    void *signalSocket;
    void *requestSocket;
    void *fileOpSocket;
    void *dataSocket;

    //  Initialize poll set
//    zmq::pollitem_t items [];

//        void *auth

    char **targets;

    char **supportedConnections;

    int signalExchanged;

    char *streamStarted;
    char *queryNextStarted;
    char *nexusStarted;

    int socketResponseTimeout;

    int numberOfStreams;
    char **recvdCloseFrom;
    char *replyToSignal;
//    int replyToSignal;
    int allCloseRecvd;

//    char **fileDescriptors;

    int fileOpened;
    int callbackParams;
    int openCallback;
    int readCallback;
    int closeCallback;

    char *connectionType;

};


int dataTransfer_init (dataTransfer **dT, char *connectionType)
{
    dataTansfer* dI = malloc(sizeof(dataTransfer));

    *out = NULL;

    char currentPID[128] = "1234"

    dT->localhost   = "localhost";
    dT->extIp       = "0.0.0.0";
    dT->ipcPath     = "/tmp/HiDRA";

    dT->signalHost  = "zitpcx19282";
    dT->signalPort  = "50000";
    dT->requestPort = "50001";
    dT->fileOpPort  = "50050";
    dT->dataHost    = "zitpcx19282"
    dT->dataPort    = "50100";

    dT->targets     = NULL;

    dT->supportedConnections = NULL;

    dT->signalExchanged  = 0;

    dT->streamStarted    = NULL;
    dT->queryNextStarted = NULL;
    dT->nexusStarted     = NULL;

    dT->socketResponseTimeout = 1000;

    dT->numberOfStreams  = 0;
    dT->recvdCloseFrom   = NULL;
    dT->replyToSignal    = NULL;
//    dT->replyToSignal = 0;
    dT->allCloseRecvd    = 0;

//    dT->fileDescriptors = NULL;

    dT->fileOpened       = 0;
    dT->callbackParams   = 0;
    dT->openCallback     = 0;
    dT->readCallback     = 0;
    dT->closeCallback    = 0;

    dT->connectionType   = connectionType;

    if ( (dT->context = zmq_ctx_new ()) == NULL )
    {
		perror("ERROR: Cannot create 0MQ context: ");
		exit(9);
	}


    dT->replyToSignal = ""
    dT->allCloseRecvd = false

    *out = dT;

    return 0;

}

int dataTransfer_initiate (dataTransfer *dT, char **targets)
{
    char *signal;
    char *signalPort;
    char signalConId[128];

    if ( dT->connectionType == "nexus" )
    {
        printf ("There is no need for a signal exchange for connection type 'nexus'");
        return 1;
    }

    // Signal exchange
    if ( dT->connectionType == "stream" )
    {
        signalPort = dT->signalPort;
        signal     = "START_STREAM";
    }
    else if ( dT->connectionType == "streamMetadata" )
    {
        signalPort = dT->signalPort;
        signal     = "START_STREAM_METADATA";
    }
    else if ( dT->connectionType == "queryNext")
    {
        signalPort = dT->signalPort;
        signal     = "START_QUERY_NEXT";
    }
    else if ( self.connectionType == "queryMetadata")
    {
        signalPort = dT->signalPort;
        signal     = "START_QUERY_METADATA";
    }

    printf ("Create socket for signal exchange...");


    if ( dT->signalHost == NULL )
    {
        if ( (dT->signalSocket = zmq_socket (dT->context, ZMQ_REQ)) == NULL )
        {
            perror("ERROR: Could not create 0MQ signalSocket: ");
            exit(9);
        }

        snprintf(signalConId, sizeof(signalConId), "tcp://%s:%s", dI->signalHost, dI->signalPort);
        if ( zmq_connect(dT->signalSocket, connectionStr) )
        {
            perror("ERROR: Failed to start signalSocket of type %s (bind): '%s'", dT->connectionType, connectionStr);
        }
        else
        {
            printf("SignalSocket of type %s started (bind) for '%s'", dT->connectionType, connectionStr);
        }
    }
    else
    {
        dataTransfer_stop();
        printf ("No host to send signal to specified." );
        return 1;
    }

    setTargets (targets);

    char *message;
    message = sendSignal(signal);

    if ( message == "VERSION_CONFLICT" )
    {
        dataTransfer_stop();
//        raise VersionError("Versions are conflicting.")
        printf ("VersionError: Versions are conflicting.");
        return 1;
    }
    else if ( message == "NO_VALID_HOST" )
    {
        dataTransfer_stop();
//        raise AuthenticationFailed("Host is not allowed to connect.")
        print ("AuthenticationFailed: Host is not allowed to connect.");
        return 1;
    }
    else if ( message == "CONNECTION_ALREADY_OPEN" )
    {
        dataTransfer_stop();
//        raise CommunicationFailed("Connection is already open.")
        prints ("CommunicationFailed: Connection is already open.");
        return 1;
    }
    else if ( message == "NO_VALID_SIGNAL" )
    {
        dataTransfer_stop();
//        raise CommunicationFailed("Connection type is not supported for this kind of sender.")
        print ("CommunicationFailed: Connection type is not supported for this kind of sender.");
        return 1;
    }
    // if there was no response or the response was of the wrong format, the receiver should be shut down
    else if ( message.startswith(signal) )
    {
        printf ("Received confirmation ...");
        dT->signalExchanged = signal;
    }
    else
    {
//        raise CommunicationFailed("Sending start signal ...failed.")
        printf ("CommunicationFailed: Sending start signal ...failed.");
        return 1;
    }
}


int dataTransfer_start (dataTransfer *dT)
{

    char *socketIdToBind;
    char connectionStr[128];

    if ( !nT->streamStarted )
    {
        socketIdToBind = nT->streamStarted;
    }
    else if
    {
        socketIdToBind = dT->queryNextStarted;
    }
    else if
    {
        socketIdToBind = dT->nexusStarted;
    }

    if ( !socketIdToBind )
    {
        printf ("Reopening already started connection.");
    }
    else
    {
        snprintf(socketIdToBind, sizeof(socketIdToBind), "%s:%s", dT->extIp, dT->dataPort);
    }

    if ( (dT->dataSocket = zmq_socket (dT->context, ZMQ_PULL)) == NULL )
    {
		perror("ERROR: Could not create 0MQ dataSocket: ");
		exit(9);
	}

    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s", socketidToBind);
    if ( zmq_bind(dT->dataSocket, connectionStr) )
    {
        perror("ERROR: Failed to start Socket of type %s (bind): '%s'", dT->connectionType, connectionStr);
    }
    else
    {
        printf("Data socket of type %s started (bind) for '%s'", dT->connectionType, connectionStr);
    }


    if (dT->connectionType == "queryNext" || dT->connectionType == "queryMetadata")
    {

        if ( (dT->requestSocket = zmq_socket (dT->context, ZMQ_PUSH)) == NULL )
        {
            perror("ERROR: Could not create 0MQ requestSocket: ");
            exit(9);
        }

        snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", dI->signalHost, dI->requestPort);
        if ( zmq_connect(dT->requestSocket, connectionStr) )
        {
            perror(stderr, "ERROR: Failed to start Socket of type %s (connect): '%s'", dT->connectionType, connectionStr)
        }
        else
        {
            printf("Request socket started (connect) for '%s'", connectionStr)
        }

        dT->queryNextStarted = socketIdToBind
    }
    else if ( dT->connectionType == "nexus" )
    {

        if ( (dT->fileOpSocket = zmq_socket (dT->context, ZMQ_REP)) == NULL )
        {
            perror("ERROR: Could not create 0MQ fileOpSocket: ");
            exit(9);
        }

        snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", dT->extlHost, dT->fileOpPort);
        if ( zmq_bind(dT->fileOpSocket, connectionStr) )
        {
            perror("ERROR: Failed to start Socket of type %s (bind): '%s'", dT->connectionType, connectionStr);
        }
        else
        {
            printf("File operation socket started (bind) for '%s'", connectionStr)
        }

        dT->nexusStarted = socketIdToBind;
    }
    else
    {
        dT->streamStarted    = socketIdToBind;
    }



int dataTransfer_read (dataTransfer *dT, char *data, int size)
{

    zmq_pollitem_t items [] = {
        { dT->fileOpSocket,   0, ZMQ_POLLIN, 0 },
        { dT->dataSocket, 0, ZMQ_POLLIN, 0 }
    };

    while (1)
    {
        printf ("polling");
        zmq_poll (items, 2, -1);

        if (items [0].revents & ZMQ_POLLIN)
        {
            printf ("fileOpSocket is polling\n");

            data = s_recv (dI->fileOpSocket);
            printf ("fileOpSocket recv: '%s'\n", message);

            if (data == b"CLOSE_FILE" && dT->allCloseRecvd == false):
                dT->replyToSignal = data;
            else:
                rc = zmq_send (dT-fileOpSocket, data, strlen(data), 0);
                printf ("fileOpSocket send: '%s'\n", data);

                return 0;
        }

        if (items [1].revents & ZMQ_POLLIN) {

            printf ("dataSocket is polling\n");
            char *multipartMessage[2];
            int j;

            rc = getMultipartMessage(dI->dataSocket, multipartMessage)

            for (j = 0; j < 2; j++)
            {
                free(multipartMessage[j]);
            };
        }

};


int recv_multipartMessage (void *socket, char **multipartMessage, int *len)
{
    int i = 0;
    int size;
    int more;
    size_t more_size = sizeof(more);
    while (1)
    {

        //  Wait for next request from client
        zmq_msg_t message;
        zmq_msg_init (&message);
        m_size = zmq_msg_recv (&message, socket, 0);

        //Process message
        multipartMessage[i] = malloc (m_size + 1);
        memcpy (multipartMessage[i], zmq_msg_data (&message), m_size);
        printf("recieved string: %s\n", multipartMessage[i]);
        multipartMessage[i] [m_size] = 0;

        i++;

        zmq_msg_close(&message);
        zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (more == 0)
        {
            printf("last received\n");
            *len = i;
            break;
        };

    }

    return 0;


};



int getMultipartMessage (void *socket, char *data)
{
    char* mulitpartMessage[2];
    int len;
    int rc;

    //TODO multipart
    rc = recv_multipartMessage (dI-dataSocket, multipartMessage, len);
    printf ("multipartMessage[0]=%s\nmultipartMessage[1]=%s\n", multipartMessage[0], multipartMessage[1]);
    //self.log.error("Could not receive data due to unknown error.", exc_info=True)

    if (len < 2)
    {
        printf ("Received mutipart-message is too short. Either config or file content is missing.");
//            self.log.debug("multipartMessage=" + str(mutipartMessage))
        //TODO coorect errorcode
        return -1;


    }

    if (multipartMessage[0] == "CLOSE_FILE")
    {
/*
            id = multipartMessage[1]
            self.recvdCloseFrom.append(id)
            self.log.debug("Received close-file-signal from DataDispatcher-" + id)

            # get number of signals to wait for
            if not self.numberOfStreams:
                self.numberOfStreams = int(id.split("/")[1])

            # have all signals arrived?
            if len(self.recvdCloseFrom) == self.numberOfStreams:
                self.log.info("All close-file-signals arrived")
                self.allCloseRecvd = True
                if self.replyToSignal:
                    self.fileOpSocket.send(self.replyToSignal)
                    self.log.debug("fileOpSocket send: " + self.replyToSignal)
                    self.replyToSignal = False
                else:
                    pass

*/
        data = "CLOSE_FILE"
        return 0;

    } else {
        data = multipartMessage[1];
        return 0;
    }
};


int dataTransfer_stop (dataTransfer *dT)
{

    printf ("closing fileOpSocket...\n");
    zmq_close(dT->fileOpSocket);
    printf ("closing dataSocket...\n");
    zmq_close(dT->dataSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dT->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

//    free (dT);
    printf ("Cleanup finished.\n");

    return 0;
};

