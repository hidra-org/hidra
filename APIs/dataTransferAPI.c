// API to ingest data into a data transfer unit

#include <dataTransferAPI.h>
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <glib.h>
#include <json.h>


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
//    printf("recieved string: %s\n", string);
    zmq_msg_close (&message);
    string [size] = 0;
    return string;
}


static int s_send (char* socketName, void * socket, char* string)
{
//    printf("Sending: %s\n", string);
    int size = zmq_send (socket, string, strlen (string), 0);
    if (size == -1)
    {
        fprintf(stderr, "Failed to send message via %s (message: '%s'): %s\n",
                socketName, string, strerror( errno ));
//        fprintf (stderr, "ERROR: Sending failed\n");
//        perror("");
    }
    else
    {
        printf("%s send: %s\n", socketName, string);
    }

    return size;
}



DATATRANSFERAPI_ERROR recv_multipartMessage (void *socket, char **multipartMessage, int *len)
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
        size = zmq_msg_recv (&message, socket, 0);

        //Process message
        multipartMessage[i] = malloc (size + 1);
        memcpy (multipartMessage[i], zmq_msg_data (&message), size);
//        printf("recieved string: %s\n", multipartMessage[i]);
        multipartMessage[i] [size] = 0;

        i++;

        zmq_msg_close(&message);
        zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (more == 0)
        {
//            printf("last received\n");
            *len = i;
            break;
        };
    }

    return SUCCESS;

}


struct dataTransfer {

    char *localhost;
    char *extIp;
    char *ipcPath;

    char *conType;

    char *signalHost;
    char *fileOpPort;
    char *dataHost;
    char *dataPort;

    //  Prepare our context and socket
    void *context;
    void *fileOpSocket;
    void *dataSocket;

    char *nexusStarted;

    int socketResponseTimeout;

    int numberOfStreams;
    char **recvdCloseFrom;
    char *replyToSignal;
    int allCloseRecvd;

    int runLoop;

//    char **fileDescriptors;

    int fileOpened;
    int callbackParams;
    int openCallback;
    int readCallback;
    int closeCallback;

};


DATATRANSFERAPI_ERROR dataTransfer_init (dataTransfer **out, char *connectionType)
{
//    assert( strcmp(connectionType, "nexus") == 0 )
//    printf("conType: %s, comparison: %i\n", connectionType, strcmp(connectionType, "nexus"));
/*    if (strcmp(connectionType, "nexus") == 0 )
    {
        fprintf(stderr, "Chosen type of connection is not supported.\n");
//        return NOTSUPPORTED;
    }
*/
    dataTransfer* dT = malloc(sizeof(dataTransfer));

    *out = NULL;

    dT->localhost   = "localhost";
    dT->extIp       = "0.0.0.0";
    dT->ipcPath     = "/tmp/HiDRA";

    dT->conType     = connectionType;

    dT->signalHost  = "zitpcx19282";
    dT->fileOpPort  = "50050";
    dT->dataHost    = "zitpcx19282";
    dT->dataPort    = "50100";

    dT->nexusStarted = NULL;

    dT->socketResponseTimeout = 1000;

    dT->numberOfStreams  = 0;
    dT->recvdCloseFrom   = NULL;
    dT->replyToSignal    = NULL;
    dT->allCloseRecvd    = 0;

    dT->runLoop          = 1;
//    dT->fileDescriptors = NULL;

    dT->fileOpened       = 0;
    dT->callbackParams   = 0;
    dT->openCallback     = 0;
    dT->readCallback     = 0;
    dT->closeCallback    = 0;

    if ( (dT->context = zmq_ctx_new ()) == NULL )
    {
		perror("Cannot create 0MQ context");
		return ZMQERROR;
	}

    dT->allCloseRecvd = 0;

    *out = dT;

    return SUCCESS;

}


DATATRANSFERAPI_ERROR dataTransfer_initiate (dataTransfer *dT, char **targets)
{
    char *signal;
    char *signalPort;
    char signalConId[128];

    if (dT->conType == "nexus")
    {
        printf ("There is no need for a signal exchange for connection type 'nexus'");
        return SUCCESS;
    }

    return SUCCESS;
}


DATATRANSFERAPI_ERROR dataTransfer_start (dataTransfer *dT)
{

    char *socketIdToBind = NULL;
    char connectionStr[128];

    if ( dT->nexusStarted != NULL)
    {
        socketIdToBind = dT->nexusStarted;
        printf ("Reopening already started connection.\n");
    }
    else
    {
        socketIdToBind = malloc(strlen(dT->extIp)+strlen(dT->dataPort)+1);
        sprintf(socketIdToBind, "%s:%s", dT->extIp, dT->dataPort);
    }

    // Create data socket
    if ( (dT->dataSocket = zmq_socket (dT->context, ZMQ_PULL)) == NULL )
    {
		perror("Could not create 0MQ dataSocket");
		return ZMQERROR;
	}

    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s", socketIdToBind);
    if ( zmq_bind(dT->dataSocket, connectionStr) )
    {
        fprintf(stderr, "Failed to start data socket (bind) for '%s': %s\n",
                connectionStr, strerror( errno ));
        return ZMQERROR;
    }
    else
    {
        printf("Data socket of type %s started (bind) for '%s'\n", dT->conType, connectionStr);
    }

    // Create socket for file operation exchanging
    if ( (dT->fileOpSocket = zmq_socket (dT->context, ZMQ_REP)) == NULL )
    {
        perror("Could not create 0MQ fileOpSocket");
        return ZMQERROR;
    }

    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", dT->extIp, dT->fileOpPort);
    if ( zmq_bind(dT->fileOpSocket, connectionStr) )
    {
        fprintf(stderr, "Failed to start file operation socket (bind) for '%s': %s\n",
                connectionStr, strerror( errno ));
        return ZMQERROR;
    }
    else
    {
        printf("File operation socket started (bind) for '%s'\n", connectionStr);
    }

    dT->nexusStarted = socketIdToBind;

    return SUCCESS;
}


DATATRANSFERAPI_ERROR reactOnMessage (dataTransfer *dT, char **multipartMessage)
{
    char *id = NULL;
    int idNum = 0;
    char **splitRes = NULL;
    int i = 0;
    int rc = 0;
    int totalRecvd = 0;
    struct json_object *metadata = NULL;

    if (strcmp(multipartMessage[0], "CLOSE_FILE") == 0)
    {
        id = multipartMessage[1];

        splitRes = g_strsplit (id, "/", 2);
        idNum = atoi(splitRes[0]);

        // get number of signals to wait for
        if (dT->numberOfStreams == 0)
        {
            dT->numberOfStreams = atoi(splitRes[1]);

            dT->recvdCloseFrom = malloc(sizeof(char*) * dT->numberOfStreams);
            for (i = 0; i < dT->numberOfStreams; i++)
            {
                dT->recvdCloseFrom[i] = NULL;
            }
        }

        dT->recvdCloseFrom[idNum] = strdup(id);
        printf("Received close-file-signal from DataDispatcher-%s\n", id);

        // have all been signals arrived?
        printf("numberOfStreams=%i\n", dT->numberOfStreams);
        for (i = 0; i < dT->numberOfStreams; i++)
        {
            printf("recvdCloseFrom[%i]=%s\n", i, dT->recvdCloseFrom[i]);
            if (dT->recvdCloseFrom[i] != NULL)
            {
                totalRecvd++;
            }
            printf ("totalRecvd=%i\n", totalRecvd);
        }

        if ( totalRecvd >= dT->numberOfStreams)
        {
            printf("All close-file-signals arrived\n");
            dT->allCloseRecvd = 1;

            printf("replyToSignal: %s\n", dT->replyToSignal);
            if (dT->replyToSignal != NULL)
            {
                rc = s_send ("fileOpSocket", dT->fileOpSocket, dT->replyToSignal);
                if (rc == -1) return COMMUNICATIONFAILED;

                dT->replyToSignal = NULL;
                if (dT->recvdCloseFrom) free(dT->recvdCloseFrom);
                dT->recvdCloseFrom = NULL;
                dT->allCloseRecvd = 0;
                dT->runLoop = 0;
            }

//            dT->closeCallback(dT->callbackParams, dT->multipartMessage)
        }
        else
        {
            printf("Still close events missing\n");
        }
    }
    else
    {

//        printf("load JSON: %s\n", multipartMessage[0]);

        //extract multipart message
        metadata = json_tokener_parse(multipartMessage[0]);
//        printf("metadata:\n%s\n", json_object_to_json_string_ext(metadata, JSON_C_TO_STRING_SPACED | JSON_C_TO_STRING_PRETTY));

//        perror("Could not extract metadata from the multipart-message.");
//        metadata = NULL;

        char *payload = multipartMessage[1];
//        perror("An empty file was received within the multipart-message");
//        payload = NULL;

//        self.readCallback(self.callbackParams, [metadata, payload])
    }

    return SUCCESS;
}


DATATRANSFERAPI_ERROR dataTransfer_read (dataTransfer *dT, char *data, int size)
{
    zmq_pollitem_t items [] = {
        { dT->fileOpSocket,   0, ZMQ_POLLIN, 0 },
        { dT->dataSocket, 0, ZMQ_POLLIN, 0 }
    };
    char *message = NULL;
    int rc = 0;
    char *multipartMessage[2];
    int i = 0;
    int len = 0;

    dT->runLoop = 1;

    while (dT->runLoop)
    {
//        printf ("polling\n");
        zmq_poll (items, 2, -1);

        if (items [0].revents & ZMQ_POLLIN)
        {
            printf ("fileOpSocket is polling\n");

            message = s_recv (dT->fileOpSocket);
            printf ("fileOpSocket recv: '%s'\n", message);

            if (strcmp(message,"CLOSE_FILE") == 0)
            {
                if ( dT->allCloseRecvd )
                {
                    rc = s_send ("fileOpSocket", dT->fileOpSocket, message);
                    if (rc == -1) return COMMUNICATIONFAILED;

                    dT->runLoop = 0;
                    dT->allCloseRecvd = 0;
                    break;
                }
                else
                {
                    dT->replyToSignal = message;
                }
            }
            else if (strcmp(message,"OPEN_FILE") == 0)
            {
                rc = s_send ("fileOpSocket", dT->fileOpSocket, message);
                if (rc == -1) return COMMUNICATIONFAILED;

                dT->allCloseRecvd = 0;
//                dT->openCallback(dT->callbackParams, message);
//                dT->fileOpened = 1;
            }
            else
            {
                printf("Not supported message received\n");

                rc = s_send ("fileOpSocket", dT->fileOpSocket, "ERROR");
                if (rc == -1) return COMMUNICATIONFAILED;
            }

            free (message);
        }

        if (items [1].revents & ZMQ_POLLIN)
        {
            printf ("dataSocket is polling\n");

            if (recv_multipartMessage (dT->dataSocket, multipartMessage, &len))
            {
                perror("Failed to receive data");
                return COMMUNICATIONFAILED;
            }
            else
            {
                printf ("multipartMessage[0]=%s\nmultipartMessage[1]=%s\n",
                        multipartMessage[0], multipartMessage[1]);
            }

            if (len < 2)
            {
                perror ("Received mutipart-message is too short");
                return FORMATERROR;
            }

            if (multipartMessage[1] == "ALIVE_TEST")
            {
                continue;
            }


            rc = reactOnMessage (dT, multipartMessage);

            for (i = 0; i < 2; i++)
            {
                free(multipartMessage[i]);
            };

            if (rc)
            {
                printf("reactOnMessage failed\n");
//                return rc;
            }

        }
    }

    return SUCCESS;
}


DATATRANSFERAPI_ERROR dataTransfer_stop (dataTransfer *dT)
{

    printf ("closing fileOpSocket...\n");
    zmq_close(dT->fileOpSocket);
    printf ("closing dataSocket...\n");
    zmq_close(dT->dataSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dT->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

    if (dT->nexusStarted) free (dT->nexusStarted);
    if (dT->recvdCloseFrom != NULL) free (dT->recvdCloseFrom);
    free (dT);
    printf ("Cleanup finished.\n");

    return SUCCESS;
}

