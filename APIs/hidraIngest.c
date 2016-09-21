// API to ingest data into a HiDRA unit

#include <hidraIngest.h>
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <json.h>


const char* PATH_SEPARATOR =
#ifdef _WIN32
                            "\\";
#else
                            "/";
#endif


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


HIDRA_ERROR s_recv_multipart (void *socket, char **multipartMessage, int *len, int *messageSize)
{
    int i = 0;
    int more;
    size_t more_size = sizeof(more);
    zmq_msg_t message;

    while (1)
    {
        //  Wait for next request from client
        zmq_msg_init (&message);
        messageSize[i] = zmq_msg_recv (&message, socket, 0);

        //Process message
        printf("Received message of size: %d\n", messageSize[i]);
        multipartMessage[i] = malloc (messageSize[i] + 1);

        memcpy (multipartMessage[i], zmq_msg_data (&message), messageSize[i]);
        multipartMessage[i] [messageSize[i]] = 0;
//        printf("recieved string: %s\n", multipartMessage[i]);

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


struct hidraIngest {

    char *localhost;
    char *extIp;
    char *ipcPath;

    char *signalHost;
    char *signalPort;

    char *eventDetPort;
    char *dataFetchPort;

    //  Prepare our context and socket
    void *context;
    void *fileOpSocket;
    void *eventDetSocket;
    void *dataFetchSocket;

    //  Initialize poll set
//    zmq::pollitem_t items [];

    char *filename;
    char *openFile;
    int  filePart;

    int  responseTimeout;

};


HIDRA_ERROR hidraIngest_init (hidraIngest **out)
{
    hidraIngest* dI = malloc(sizeof(hidraIngest));

    *out = NULL;

    dI->localhost       = "localhost";
    dI->extIp           = "0.0.0.0";
    dI->ipcPath         = "/tmp/HiDRA";

    dI->signalHost      = "zitpcx19282";
    dI->signalPort      = "50050";

    // has to be the same port as configured in dataManager.conf as eventDetPort
    dI->eventDetPort    = "50003";
    // has to be the same port as configured in dataManager.conf as dataFetchPort
    dI->dataFetchPort   = "50010";

    char signalConId[128];
    char eventDetConId[128];
    char dataFetchConId[128];

    snprintf(signalConId, sizeof(signalConId), "tcp://%s:%s", dI->signalHost, dI->signalPort);

    // if plattform is Windows:
//    snprintf(eventDetConId, sizeof(eventDetConId), "tcp://%s:%s", dI->localhost, dI->eventDetPort);
//    snprintf(dataFetchConId, sizeof(dataFetchConId), "tcp://%s:%s", dI->localhost, dI->dataFetchPort);
    // else:
    snprintf(eventDetConId, sizeof(eventDetConId), "ipc://%s/eventDet", dI->ipcPath);
    snprintf(dataFetchConId, sizeof(dataFetchConId), "ipc://%s/dataFetch", dI->ipcPath);

    // Initialize sockets
    if ( (dI->context = zmq_ctx_new ()) == NULL )
    {
		perror("Cannot create 0MQ context: ");
        return ZMQERROR;
	}

	if ( (dI->fileOpSocket = zmq_socket (dI->context, ZMQ_REQ)) == NULL )
    {
		perror("Could not create 0MQ fileOpSocket");
        return ZMQERROR;
	}

	if ( (dI->eventDetSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
    {
		perror("Could not create 0MQ eventDetSocket");
        return ZMQERROR;
	}

    if ( (dI->dataFetchSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
    {
		perror("Could not create 0MQ dataFetchSocket: ");
        return ZMQERROR;
	}

    dI->filePart        = 0;
    dI->responseTimeout = 1000;

    char connectionStr[128];
    int rc;

    // Create sockets
    if ( zmq_connect(dI->fileOpSocket, signalConId) )
    {
        fprintf(stderr, "Failed to start fileOpSocket (connect) for '%s': %s\n",
                signalConId, strerror( errno ));
        //TODO stop socket
        return ZMQERROR;
    }
    else
    {
        printf("fileOpSocket started (connect) for '%s'\n", signalConId);
    }

    if ( zmq_connect(dI->eventDetSocket, eventDetConId) )
    {
        fprintf (stderr, "Failed to start eventDetSocket (connect) for '%s': %s\n",
                eventDetConId, strerror( errno ));
        //TODO stop socket
        return ZMQERROR;
    }
    else
    {
        printf("eventDetSocket started (connect) for '%s'\n", eventDetConId);
    }

    if ( zmq_connect(dI->dataFetchSocket, dataFetchConId) )
    {
        fprintf (stderr, "Failed to start dataFetchSocket (connect) for '%s': %s\n",
                dataFetchConId, strerror( errno ));
        //TODO stop socket
        return ZMQERROR;
    }
    else
    {
        printf("dataFetchSocket started (connect) for '%s'\n", dataFetchConId);
    }

    *out = dI;

    return SUCCESS;

}


HIDRA_ERROR hidraIngest_createFile (hidraIngest *dI, char *fileName)
{

//        if self.openFile and self.openFile != filename:
//            raise Exception("File " + str(filename) + " already opened.")

    char *message = "OPEN_FILE";
    int rc;
    char *token;
    char *openFile;
    char filename[strlen(fileName)];
    char *multipartMessage[2];
    int messageSize[2];
    int len = 0;
    int i;

    strcpy(filename, fileName);

    /* get the first token */
    token = strtok(filename, PATH_SEPARATOR);

    /* walk through other tokens */
    while( token != NULL )
    {
        openFile = token;
        token = strtok(NULL, PATH_SEPARATOR);
    }
    dI->openFile = strdup(openFile);
    printf( "openFile %s\n", dI->openFile );

    // Send notification to receiver
    rc = s_send (dI->fileOpSocket, message, strlen(message), ZMQ_SNDMORE);
    if (rc == -1) return COMMUNICATIONFAILED;
    rc = s_send (dI->fileOpSocket, dI->openFile, strlen(dI->openFile), 0);
    if (rc == -1) return COMMUNICATIONFAILED;
    printf ("Sending signal to open a new file.\n");

    s_recv_multipart (dI->fileOpSocket, multipartMessage, &len, messageSize);
    printf ("Received responce: '%s' for file '%s'\n", multipartMessage[0], multipartMessage[1]);
    //TODO check if received signal is correct

    dI->filename = fileName;
    dI->filePart = 0;

    for (i = 0; i < len; i++)
    {
        free(multipartMessage[i]);
    };


    return SUCCESS;
}


HIDRA_ERROR hidraIngest_write (hidraIngest *dI, char *data, int size)
{

    int rc;
    json_object * metadata_json;
    const char *metadata_string;


    metadata_json = json_object_new_object();

    json_object_object_add(metadata_json,"filename", json_object_new_string(dI->filename));
    json_object_object_add(metadata_json,"filePart", json_object_new_int(dI->filePart));
    json_object_object_add(metadata_json,"chunkSize", json_object_new_int(size));

    metadata_string = json_object_to_json_string ( metadata_json );

    // Send event to eventDetector
    rc = s_send (dI->eventDetSocket, metadata_string, strlen(metadata_string), 0);
    if (rc == -1) return COMMUNICATIONFAILED;

    printf ("Writing: %s\n", metadata_string);
    json_object_put ( metadata_json );

    // Send data to ZMQ-Queue
    rc = s_send (dI->dataFetchSocket, data, size, 0);
    if (rc == -1) return COMMUNICATIONFAILED;


    dI->filePart += 1;

    return SUCCESS;
};


HIDRA_ERROR hidraIngest_closeFile (hidraIngest *dI)
{

    char *message = "CLOSE_FILE";
    char *multipartMessage[2];
    int messageSize[2];
    int len = 0;
    int i;

    int rc;

    // Send close-signal to signal socket
    rc = s_send (dI->fileOpSocket, message, strlen(message), ZMQ_SNDMORE);
    if (rc == -1) return COMMUNICATIONFAILED;
    rc = s_send (dI->fileOpSocket, dI->openFile, strlen(dI->openFile), 0);
    if (rc == -1) return COMMUNICATIONFAILED;
    //        perror("Sending signal to close the file to fileOpSocket...failed.")
    printf ("Sending signal to close the file to fileOpSocket  (sendMessage=%s)\n", message);


    // send close-signal to event Detector
    rc = s_send (dI->eventDetSocket, message, strlen(message), 0);
    if (rc == -1) return COMMUNICATIONFAILED;
//        perror("Sending signal to close the file to eventDetSocket...failed.)")
    printf ("Sending signal to close the file to eventDetSocket (sendMessage=%s)\n", message);


//    try:
//        socks = dict(self.poller.poll(10000)) # in ms
//    except:
//        socks = None
//        self.log.error("Could not poll for signal", exc_info=True)
//
//    // if there was a response
//    if socks and self.fileOpSocket in socks and socks[self.fileOpSocket] == zmq.POLLIN:
    // Get the reply.
    s_recv_multipart (dI->fileOpSocket, multipartMessage, &len, messageSize);
    printf ("Received answer to signal: '%s' for file '%s'\n", multipartMessage[0], multipartMessage[1]);

    //TODO compare whole message
    if ( strcmp(message,multipartMessage[0]) != 0 )
    {
        perror ("Something went wrong while notifying to close the file");
        printf ("recieved message: %s\n", multipartMessage[0]);
        printf ("send message: %s\n", message);
    };

    dI->filename = "";
    dI->filePart = 0;
    free (dI->openFile);

    for (i = 0; i < len; i++)
    {
        free(multipartMessage[i]);
    };

    return SUCCESS;
};

HIDRA_ERROR hidraIngest_stop (hidraIngest *dI)
{

    printf ("closing fileOpSocket...\n");
    zmq_close(dI->fileOpSocket);
    printf ("closing eventDetSocket...\n");
    zmq_close(dI->eventDetSocket);
    printf ("closing dataFetchSocket...\n");
    zmq_close(dI->dataFetchSocket);
//    perror("closing ZMQ Sockets...failed.")

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dI->context);
//    perror("Closing ZMQ context...failed.")

    free (dI);
    printf ("Cleanup finished.\n");

    return SUCCESS;
};
