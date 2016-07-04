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
    void *signalSocket;
    void *eventDetSocket;
    void *dataFetchSocket;

    //  Initialize poll set
//    zmq::pollitem_t items [];

    char *filename;
    int  openFile;
    int  filePart;

    int  responseTimeout;

};


int dataIngest_init (dataIngest **out)
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
    snprintf(eventDetConId, sizeof(eventDetConId), "tcp://%s:%s", dI->localhost, dI->eventDetPort);
    snprintf(dataFetchConId, sizeof(dataFetchConId), "tcp://%s:%s", dI->localhost, dI->dataFetchPort);
    // else:
    snprintf(eventDetConId, sizeof(eventDetConId), "ipc://%s/eventDet", dI->ipcPath);
    snprintf(dataFetchConId, sizeof(dataFetchConId), "ipc://%s/dataFetch", dI->ipcPath);

    // Initialize sockets
    if ( (dI->context = zmq_ctx_new ()) == NULL )
    {
		perror("ERROR: Cannot create 0MQ context: ");
		exit(9);
	}

	if ( (dI->signalSocket = zmq_socket (dI->context, ZMQ_REQ)) == NULL )
    {
		perror("ERROR: Could not create 0MQ signalSocket: ");
		exit(9);
	}

	if ( (dI->eventDetSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
    {
		perror("ERROR: Could not create 0MQ eventDetSocket: ");
		exit(9);
	}

    if ( (dI->dataFetchSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
    {
		perror("ERROR: Could not create 0MQ dataFetchSocket: ");
		exit(9);
	}

    dI->filePart        = 0;
    dI->responseTimeout = 1000;

    char connectionStr[128];
    int rc;

    // Create sockets
//    rc = zmq_connect(dI->signalSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_connect(dI->signalSocket, signalConId) )
    {
        fprintf (stderr, "ERROR: Could not start signalSocket (connect) for '%s'\n", signalConId);
        perror("");
    } else {
        printf("signalSocket started (connect) for '%s'\n", signalConId);
    }

//    rc = zmq_connect(dI->eventDetSocket, connectionStr);
//    assert (rc == 0);
//    if ( zmq_connect(dI->eventDetSocket, connectionStr) )
    if ( zmq_connect(dI->eventDetSocket, eventDetConId) )
    {
        fprintf (stderr, "ERROR: Could not start eventDetSocket (connect) for '%s'\n", eventDetConId);
        perror("");
    } else {
        printf("eventDetSocket started (connect) for '%s'\n", eventDetConId);
    }

//    rc = zmq_connect(dI->dataFetchSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_connect(dI->dataFetchSocket, dataFetchConId) )
    {
        fprintf (stderr, "ERROR: Could not start dataFetchSocket (connect) for '%s'\n", dataFetchConId);
        perror("");
    } else {
        printf("dataFetchSocket started (connect) for '%s'\n", dataFetchConId);
    }

    *out = dI;

    return 0;

}


int dataIngest_createFile (dataIngest *dI, char *fileName)
{

//        if self.openFile and self.openFile != filename:
//            raise Exception("File " + str(filename) + " already opened.")

    char *message;
    int rc;

    // Send notification to receiver
    rc = s_send (dI->signalSocket, "OPEN_FILE");
    printf ("Sending signal to open a new file.\n");

//    s_recv (dI->signalSocket, message);
    message = s_recv (dI->signalSocket);
    printf ("Received responce: '%s'\n", message);

    dI->filename = fileName;
    dI->filePart = 0;

    free (message);

    return 0;
}


int dataIngest_write (dataIngest *dI, char *data, int size)
//int dataIngest_write (dataIngest *dI, void *data, int &size)
{

    char message[128];
    int rc;

    snprintf(message, sizeof(message), "{ \"filePart\": %d, \"filename\": \"%s\" }", dI->filePart, dI->filename);

    // Send event to eventDetector
    rc = s_send (dI->eventDetSocket, message);

    // Send data to ZMQ-Queue
    rc = s_send (dI->dataFetchSocket, data);

    dI->filePart += 1;

    return 0;
};


int dataIngest_closeFile (dataIngest *dI)
{

    char *message = "CLOSE_FILE";
    char *answer;

    int rc;

    // Send close-signal to signal socket
    rc = s_send (dI->signalSocket, message);
    printf ("Sending signal to close the file to signalSocket.\n");
    //        self.log.error("Sending signal to close the file to signalSocket...failed.")


    // send close-signal to event Detector
    rc = s_send (dI->eventDetSocket, message);
    printf ("Sending signal to close the file to eventDetSocket.(sendMessage=%s)\n", message);
//        self.log.error("Sending signal to close the file to eventDetSocket...failed.)")


//    try:
//        socks = dict(self.poller.poll(10000)) # in ms
//    except:
//        socks = None
//        self.log.error("Could not poll for signal", exc_info=True)
//
//    // if there was a response
//    if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
    // Get the reply.
//    s_recv (dI->signalSocket, answer);
    answer = s_recv (dI->signalSocket);
    printf ("Received answer to signal: %s\n", answer);
//    else:
//        recvMessage = None


    if ( message != answer )
    {
        printf ("recieved message: %s\n", answer);
        printf ("send message: %s\n", message);
//            raise Exception("Something went wrong while notifying to close the file")
    };

    dI->filename = "";
    dI->filePart = 0;

    free (answer);

    return 0;
};

int dataIngest_stop (dataIngest *dI)
{

    printf ("closing signalSocket...\n");
    zmq_close(dI->signalSocket);
    printf ("closing eventDetSocket...\n");
    zmq_close(dI->eventDetSocket);
    printf ("closing dataFetchSocket...\n");
    zmq_close(dI->dataFetchSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dI->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

//    free (dI);
    printf ("Cleanup finished.\n");

    return 0;
};
