// API to ingest data into a data transfer unit

#include <dataIngestAPI.h>
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
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
};


static int s_send (void * socket, const char* string, int len)
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
    bytesSent = zmq_msg_send (&message, socket, 0);
    assert (bytesSent == len);

    return bytesSent;
}


/*
static int s_send (void * socket, char* string)
{
//    printf("Sending: %s\n", string);
    int size = zmq_send (socket, string, strlen (string), 0);
    if (size == -1)
    {
        fprintf(stderr, "Failed to send message (message: '%s'): %s\n",
                string, strerror( errno ));
//        fprintf (stderr, "ERROR: Sending failed\n");
//        perror("");
    }

    return size;
}
*/

struct dataIngest {

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
    int  openFile;
    int  filePart;

    int  responseTimeout;

};


HIDRA_ERROR dataIngest_init (dataIngest **out)
{
    dataIngest* dI = malloc(sizeof(dataIngest));

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


HIDRA_ERROR dataIngest_createFile (dataIngest *dI, char *fileName)
{

//        if self.openFile and self.openFile != filename:
//            raise Exception("File " + str(filename) + " already opened.")

    char *message;
    int rc;

    // Send notification to receiver
    message = "OPEN_FILE";
    rc = s_send (dI->fileOpSocket, message, strlen(message));
    if (rc == -1) return COMMUNICATIONFAILED;
    printf ("Sending signal to open a new file.\n");

    message = s_recv (dI->fileOpSocket);
    printf ("Received responce: '%s'\n", message);

    dI->filename = fileName;
    dI->filePart = 0;

    free (message);

    return SUCCESS;
}


HIDRA_ERROR dataIngest_write (dataIngest *dI, char *data, int size)
{

    char message[128];
    int rc;
    json_object * metadata_json;
    const char *metadata_string;


    metadata_json = json_object_new_object();

    json_object_object_add(metadata_json,"filename", json_object_new_string(dI->filename));
    json_object_object_add(metadata_json,"filePart", json_object_new_int(dI->filePart));
    json_object_object_add(metadata_json,"chunkSize", json_object_new_int(size));

    metadata_string = json_object_to_json_string ( metadata_json );

    // Send event to eventDetector
    rc = s_send (dI->eventDetSocket, metadata_string, strlen(metadata_string));
    if (rc == -1) return COMMUNICATIONFAILED;

    json_object_put ( metadata_json );

    // Send data to ZMQ-Queue
    rc = s_send (dI->dataFetchSocket, data, size);
    if (rc == -1) return COMMUNICATIONFAILED;

    printf ("Writing: %s\n", message);

    dI->filePart += 1;

    return SUCCESS;
};


HIDRA_ERROR dataIngest_closeFile (dataIngest *dI)
{

    char *message = "CLOSE_FILE";
    char *answer;

    int rc;

    // Send close-signal to signal socket
    rc = s_send (dI->fileOpSocket, message, strlen(message));
    if (rc == -1) return COMMUNICATIONFAILED;
    //        perror("Sending signal to close the file to fileOpSocket...failed.")
    printf ("Sending signal to close the file to fileOpSocket  (sendMessage=%s)\n", message);


    // send close-signal to event Detector
    rc = s_send (dI->eventDetSocket, message, strlen(message));
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
    answer = s_recv (dI->fileOpSocket);
    printf ("Received answer to signal: %s\n", answer);

    if ( strcmp(message,answer) != 0 )
    {
        perror ("Something went wrong while notifying to close the file");
        printf ("recieved message: %s\n", answer);
        printf ("send message: %s\n", message);
    };

    dI->filename = "";
    dI->filePart = 0;

    free (answer);

    return SUCCESS;
};

HIDRA_ERROR dataIngest_stop (dataIngest *dI)
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
