// API to ingest data into a data transfer unit

#include <dataTransferAPI.h>
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <json.h>


inline void free_array (char ***array, int *len)
{
    int i;

    if (*array != NULL)
    {
        for ( i = 0; i < *len; i++)
        {
            if ((*array)[i] != NULL) free((*array)[i]);
        }
        free (*array);
        *array = NULL;
    }
}


// helper functions for sending and receiving messages
// source: Pieter Hintjens: ZeroMQ; O'Reilly
static char* s_recv (void *socket)
{
    zmq_msg_t message;
    zmq_msg_init (&message);
    int bytesRecvd = zmq_msg_recv (&message, socket, 0);
    if (bytesRecvd == -1)
        return NULL;
    char *string = malloc (bytesRecvd + 1);
    memcpy (string, zmq_msg_data (&message), bytesRecvd);
//    printf("recieved string: %s\n", string);
    zmq_msg_close (&message);
    string [bytesRecvd] = 0;
    return string;
}


static int s_send_ (char* socketName, void * socket, char* string)
{
//    printf("Sending: %s\n", string);
    int bytesSent = zmq_send (socket, string, strlen (string), 0);
    if (bytesSent == -1)
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

    return bytesSent;
}


// flags could be 0 or ZMQ_SNDMORE
static int s_send (void * socket, const char* string, int len, int flags)
{
    int bytesSent;
    int rc;

    /* Create a new message, allocating bytes for message content */
    zmq_msg_t message;
    printf("Sent message of size: %i\n", len);
    rc = zmq_msg_init_size (&message, len);
    assert (rc == 0);

    /* Fill in message content */
    memcpy (zmq_msg_data (&message), string, len);

    /* Send the message to the socket */
    bytesSent = zmq_msg_send (&message, socket, flags);
    assert (bytesSent == len);

    return bytesSent;
}


HIDRA_ERROR s_recv_multipart (void *socket, char ***multipartMessage, int *len, int **messageSize)
{
    int i = 0;
    int j;
    int more;
    size_t more_size = sizeof(more);
    zmq_msg_t message;

    char **temp;
    int *temp2;

    *multipartMessage = malloc (sizeof(char*));
    *messageSize = malloc (sizeof(int));

    while (1)
    {
        //  Wait for next request from client
        zmq_msg_init (&message);
        (*messageSize)[i] = zmq_msg_recv (&message, socket, 0);

        //Process message
        printf("Received message of size: %d\n", (*messageSize)[i]);
        (*multipartMessage)[i] = malloc ((*messageSize)[i] + 1);



        memcpy ((*multipartMessage)[i], zmq_msg_data (&message), (*messageSize)[i]);
        (*multipartMessage)[i] [(*messageSize)[i]] = 0;
//        printf("recieved string: %s\n", (*multipartMessage)[i]);

        i++;

        zmq_msg_close(&message);
        zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size);
        if (more == 0)
        {
//            printf("last received\n");
            *len = i;
            break;
        }
        else
        {
            temp=realloc((*multipartMessage),(i+2)*sizeof(char*)); // give the pointer some memory
            temp2=realloc((*messageSize),(i+2)*sizeof(int)); // give the pointer some memory

            if (temp != NULL && temp2 != NULL)
            {
                (*multipartMessage)=temp;
                (*messageSize)=temp2;
            }
            else
            {
                free_array (multipartMessage, &i);
                free (messageSize);

                printf("Error allocating memory!\n");
                return DATASAVINGERROR;
            }
        }
    }

    return SUCCESS;
}

inline void print_array (char **array, int *len)
{
    int i;

    for ( i = 0; i < *len; i++)
    {
        printf("%s, ", array[i]);
    }
    printf("\n");
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
    char **replyToSignal;
    int replyToSigLen;
    int allCloseRecvd;

    int runLoop;

//    char **fileDescriptors;

    char *filename;

    int fileOpened;
    metadata_t *metadata;
    params_cb_t *cbParams;
    open_cb_t openCb;
    read_cb_t readCb;
    close_cb_t closeCb;

};


HIDRA_ERROR dataTransfer_init (dataTransfer_t **out, char *connectionType)
{
//    assert( strcmp(connectionType, "nexus") == 0 );
    if (strcmp(connectionType, "nexus") != 0 )
    {
        fprintf(stderr, "Chosen type of connection is not supported.\n");
        return NOTSUPPORTED;
    }

    dataTransfer_t *dT = malloc(sizeof(dataTransfer_t));

    *out = NULL;

    dT->localhost    = "localhost";
    dT->extIp        = "0.0.0.0";
    dT->ipcPath      = "/tmp/HiDRA";

    dT->conType      = connectionType;

    dT->signalHost   = "zitpcx19282";
    dT->fileOpPort   = "50050";
    dT->dataHost     = "zitpcx19282";
    dT->dataPort     = "50100";

    dT->nexusStarted = NULL;

    dT->socketResponseTimeout = 1000;

    dT->numberOfStreams = 0;
    dT->recvdCloseFrom  = NULL;
    dT->replyToSignal   = NULL;
    dT->replyToSigLen   = 0;
    dT->allCloseRecvd   = 0;

    dT->runLoop         = 1;

    dT->filename        = NULL;

    dT->fileOpened      = 0;
    dT->metadata        = malloc(sizeof(metadata_t));
    dT->cbParams        = NULL;
    dT->openCb          = NULL;
    dT->readCb          = NULL;
    dT->closeCb         = NULL;

    if ( (dT->context = zmq_ctx_new ()) == NULL )
    {
		perror("Cannot create 0MQ context");
		return ZMQERROR;
	}

    dT->allCloseRecvd = 0;

    *out = dT;

    return SUCCESS;

}


HIDRA_ERROR dataTransfer_initiate (dataTransfer_t *dT, char **targets)
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


HIDRA_ERROR dataTransfer_start (dataTransfer_t *dT)
{

    char *socketIdToBind = NULL;
    char connectionStr[128];

    if ( dT->nexusStarted != NULL)
    {
        socketIdToBind = strdup(dT->nexusStarted);
        printf ("Reopening already started connection.\n");
    }
    else
    {
        socketIdToBind = malloc(strlen(dT->extIp)+strlen(dT->dataPort)+2);
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

    dT->nexusStarted = strdup(socketIdToBind);
    free(socketIdToBind);

    return SUCCESS;
}


HIDRA_ERROR reactOnMessage (dataTransfer_t *dT, char **multipartMessage, int *messageSize)
{
    char *filename = NULL;
    char *id = NULL;
    int idNum = 0;
    char **splitRes = NULL;
    int i = 0;
    int rc = 0;
    int totalRecvd = 0;
    struct json_object *metadata_json = NULL;
    char delimiter[] = "/";
    char *token;
    int numStream;


    if (strcmp(multipartMessage[0], "CLOSE_FILE") == 0)
    {
        filename = multipartMessage[1];
        id = multipartMessage[2];

        //TODO do this correctly
        char recv_filename[strlen(dT->filename)];
        strcpy(recv_filename, dT->filename);

        // check if received close call belongs to the opened file
        if (strcmp(recv_filename, dT->filename) != 0)
        {
            perror("Close event for different file received.");
            //TODO react
        }

        char id_temp[strlen(id)];
        strcpy(id_temp, id);

        // initialisation and get the identifier of the DataDispatcher
        token = strtok(id_temp, delimiter);
        idNum = atoi(token);
        printf("idNum=%i\n", idNum);

        if (token != NULL)
        {
            //  get the number of total streams
            token = strtok(NULL, delimiter);
            numStream = atoi(token);
            printf("numStream=%i\n", numStream);
        }

        if (token != NULL)
        {
            token = strtok(NULL, delimiter);
        }

        while (token != NULL)
        {
            printf("Additional unexpected information received with DataDispatcher id: %s\n", token);
            // generate next token
            token = strtok(NULL, delimiter);
        }

        // get number of signals to wait for
        if (dT->numberOfStreams == 0)
        {
            dT->numberOfStreams = numStream;

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

            if (dT->replyToSignal != NULL)
            {
                printf("replyToSignal: ");
                print_array (dT->replyToSignal, &dT->replyToSigLen);

                rc = s_send (dT->fileOpSocket, dT->replyToSignal[0], strlen(dT->replyToSignal[0]), ZMQ_SNDMORE);
                rc = s_send (dT->fileOpSocket, dT->replyToSignal[1], strlen(dT->replyToSignal[1]), 0);
                if (rc == -1) return COMMUNICATIONFAILED;

                if (dT->filename != NULL) free (dT->filename);
                free_array (&dT->replyToSignal, &dT->replyToSigLen);
                free_array (&dT->recvdCloseFrom, &dT->numberOfStreams);

                dT->filename = NULL;
                dT->allCloseRecvd = 0;
                dT->runLoop = 0;

                dT->closeCb(dT->cbParams);
            }
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
        metadata_json = json_tokener_parse(multipartMessage[0]);
//        printf("metadata:\n%s\n", json_object_to_json_string_ext(metadata_json, JSON_C_TO_STRING_SPACED | JSON_C_TO_STRING_PRETTY));

        json_object *val;

        json_object_object_get_ex(metadata_json, "filename", &val);
        dT->metadata->filename = json_object_get_string(val);

        json_object_object_get_ex(metadata_json, "filePart", &val);
        dT->metadata->filePart = json_object_get_int(val);

        json_object_object_get_ex(metadata_json, "fileCreateTime", &val);
        dT->metadata->fileCreateTime = json_object_get_int(val);

        json_object_object_get_ex(metadata_json, "fileModTime", &val);
        dT->metadata->fileModTime = json_object_get_int(val);

        json_object_object_get_ex(metadata_json, "filesize", &val);
        dT->metadata->filesize = json_object_get_int(val);

        json_object_object_get_ex(metadata_json, "chunkSize", &val);
        dT->metadata->chunkSize = json_object_get_int(val);

        json_object_object_get_ex(metadata_json, "chunkNumber", &val);
        dT->metadata->chunkNumber = json_object_get_int(val);

//        perror("Could not extract metadata from the multipart-message.");
//        metadata = NULL;

//        perror("An empty file was received within the multipart-message");
//        payload = NULL;

        dT->readCb(dT->cbParams, dT->metadata, multipartMessage[1], messageSize[1]);

        json_object_put ( metadata_json );
    }

    return SUCCESS;
}

HIDRA_ERROR dataTransfer_read (dataTransfer_t *dT, params_cb_t *cbp, open_cb_t openFunc, read_cb_t readFunc, close_cb_t closeFunc)
{
    zmq_pollitem_t items [] = {
        { dT->fileOpSocket,   0, ZMQ_POLLIN, 0 },
        { dT->dataSocket, 0, ZMQ_POLLIN, 0 }
    };
    char *message = NULL;
    int rc = 0;
    char **multipartMessage;
    int *messageSize;
    int i = 0;
    int len = 0;

    dT->runLoop = 1;

    dT->cbParams = cbp;
    dT->openCb   = openFunc;
    dT->readCb   = readFunc;
    dT->closeCb  = closeFunc;

    while (dT->runLoop)
    {
        printf ("polling\n");
        zmq_poll (items, 2, -1);

        // fileOpSocket is polling
        if (items [0].revents & ZMQ_POLLIN)
        {
            printf ("fileOpSocket is polling\n");

            if (s_recv_multipart (dT->fileOpSocket, &multipartMessage, &len, &messageSize))
            {
                perror("Failed to receive data");
                for (i = 0; i < len; i++)
                {
                    free(multipartMessage[i]);
                };
                return COMMUNICATIONFAILED;
            }
            printf ("fileOpSocket recv: '%s' for file '%s'\n", multipartMessage[0], multipartMessage[1]);

            if (strcmp(multipartMessage[0],"OPEN_FILE") == 0)
            {
                //TODO for loop over len
                rc = s_send (dT->fileOpSocket, multipartMessage[0], strlen(multipartMessage[0]), ZMQ_SNDMORE);
                rc = s_send (dT->fileOpSocket, multipartMessage[1], strlen(multipartMessage[1]), 0);
                if (rc == -1) return COMMUNICATIONFAILED;

                dT->filename = strdup(multipartMessage[1]);

                dT->allCloseRecvd = 0;
//                dT->fileOpened = 1;

                dT->openCb(dT->cbParams, dT->filename);
            }
            else if (strcmp(multipartMessage[0],"CLOSE_FILE") == 0)
            {
                if ( dT->allCloseRecvd )
                {
                    //TODO for loop over len
                    rc = s_send (dT->fileOpSocket, multipartMessage[0], strlen(multipartMessage[0]), ZMQ_SNDMORE);
                    rc = s_send (dT->fileOpSocket, multipartMessage[1], strlen(multipartMessage[1]), 0);
                    if (rc == -1) return COMMUNICATIONFAILED;

                    // check if received close call belongs to the opened file
                    if (strcmp(multipartMessage[1], dT->filename) == 0)
                    {
                        dT->closeCb(dT->cbParams);
                    }
                    else
                    {
                        perror("Close event for different file received.");
                        //TODO react
                    }

                    if (dT->filename != NULL) free (dT->filename);
                    free_array (&dT->replyToSignal, &dT->replyToSigLen);
                    free_array (&dT->recvdCloseFrom, &dT->numberOfStreams);

                    dT->filename = NULL;
                    dT->allCloseRecvd = 0;
                    dT->runLoop = 0;

                    break;
                }
                else
                {
                    dT->replyToSigLen = len;
                    dT->replyToSignal = malloc(sizeof(char*) * len);

                    printf("Set replyToSignal: ");
                    for (i = 0; i < len; i++)
                    {
                        dT->replyToSignal[i] = strdup(multipartMessage[i]);
                        printf("%s ", dT->replyToSignal[i]);
                    }
                    printf("\n");
                }
            }
            else
            {
                printf("Not supported message received\n");

                message = "ERROR";
                rc = s_send (dT->fileOpSocket, message, strlen(message), ZMQ_SNDMORE);
                rc = s_send (dT->fileOpSocket, multipartMessage[1], strlen(multipartMessage[1]), 0);
                if (rc == -1) return COMMUNICATIONFAILED;
            }


            free_array (&multipartMessage, &len);
            free (messageSize);

        }

        // dataSocket is polling
        if (items [1].revents & ZMQ_POLLIN)
        {
            printf ("dataSocket is polling\n");

            if (s_recv_multipart (dT->dataSocket, &multipartMessage, &len, &messageSize))
            {
                perror("Failed to receive data");
                free_array (&multipartMessage, &len);
                free (messageSize);
                return COMMUNICATIONFAILED;
            }

            if (strcmp(multipartMessage[0],"ALIVE_TEST") == 0)
            {
                free_array (&multipartMessage, &len);
                free (messageSize);
                continue;
            }

            if (len < 2)
            {
                perror ("Received mutipart-message is too short");
                free_array (&multipartMessage, &len);
                free (messageSize);
                return FORMATERROR;
            }

            print_array (multipartMessage, &len);
            rc = reactOnMessage (dT, multipartMessage, messageSize);

            free_array (&multipartMessage, &len);
            free (messageSize);

            if (rc)
            {
                printf("reactOnMessage failed\n");
//                return rc;
            }

        }
    }

    return SUCCESS;
}


HIDRA_ERROR dataTransfer_stop (dataTransfer_t *dT)
{

    printf ("closing fileOpSocket...\n");
    zmq_close(dT->fileOpSocket);
    printf ("closing dataSocket...\n");
    zmq_close(dT->dataSocket);
//  perror("closing ZMQ Sockets...failed")

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dT->context);
//  perror("Closing ZMQ context...failed")

    if (dT->nexusStarted) free (dT->nexusStarted);
    free_array (&dT->recvdCloseFrom, &dT->numberOfStreams);

    free(dT->metadata);
    free (dT);
    printf ("Cleanup finished.\n");

    return SUCCESS;
}

