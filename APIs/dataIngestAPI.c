// API to ingest data into a data transfer unit

#include <dataIngestAPI.h>
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
//    printf("recieved string: %s\n", string);
    zmq_msg_close (&message);
    string [size] = 0;
    return string;
};


static int s_send (void * socket, char* string)
{
    int bytesSent;

    /* Create a new message, allocating bytes for message content */
    zmq_msg_t message;
    int rc = zmq_msg_init_size (&message, strlen(string));
    assert (rc == 0);

    /* Fill in message content */
    memcpy (zmq_msg_data (&message), string, strlen(string));

    /* Send the message to the socket */
    bytesSent = zmq_msg_send (&message, socket, 0);
    assert (bytesSent == strlen(string));

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
    rc = s_send (dI->fileOpSocket, "OPEN_FILE");
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

    snprintf(message, sizeof(message), "{ \"filePart\": %d, \"chunkSize\": %d, \"filename\": \"%s\" }", dI->filePart, size, dI->filename);

    // Send event to eventDetector
    rc = s_send (dI->eventDetSocket, message);
    if (rc == -1) return COMMUNICATIONFAILED;

    // Send data to ZMQ-Queue
    rc = s_send (dI->dataFetchSocket, data);
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
    rc = s_send (dI->fileOpSocket, message);
    if (rc == -1) return COMMUNICATIONFAILED;
    printf ("Sending signal to close the file to fileOpSocket  (sendMessage=%s)\n", message);
    //        perror("Sending signal to close the file to fileOpSocket...failed.")


    // send close-signal to event Detector
    rc = s_send (dI->eventDetSocket, message);
    if (rc == -1) return COMMUNICATIONFAILED;
    printf ("Sending signal to close the file to eventDetSocket (sendMessage=%s)\n", message);
//        perror("Sending signal to close the file to eventDetSocket...failed.)")


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
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dI->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

    free (dI);
    printf ("Cleanup finished.\n");

    return SUCCESS;
};
