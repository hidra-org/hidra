// API to ingest data into a data transfer unit

//#ifndef DATAINGEST_H
#define DATAINGEST_H

#define version "0.0.1"

#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>


typedef int bool;
#define true 1
#define false 0

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


typedef struct {
    char *signalHost;
    char *extHost;

    char *signalPort;
    // has to be the same port as configured in dataManager.conf as eventPort
    char *eventPort;
    char *dataPort;

    //  Prepare our context and socket
    void *context;
    void *signalSocket;
    void *eventSocket;
    void *dataSocket;

    //  Initialize poll set
//    zmq::pollitem_t items [];

    char *filename;
    bool openFile;
    int  filePart;
    int  responseTimeout;
} dataIngest;


int dataIngest_init (dataIngest *dI)
{
    dI->signalHost = "zitpcx19282";
    dI->extHost    = "0.0.0.0";

    dI->signalPort = "50050";
    dI->eventPort  = "50003";
    dI->dataPort   = "50010";

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

	if ( (dI->eventSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
    {
		perror("ERROR: Could not create 0MQ eventSocket: ");
		exit(9);
	}

    if ( (dI->dataSocket = zmq_socket (dI->context, ZMQ_PUSH)) == NULL )
    {
		perror("ERROR: Could not create 0MQ dataSocket: ");
		exit(9);
	}

    dI->filePart = 0;
    dI->responseTimeout = 1000;

    char connectionStr[128];
    int rc;

    // Create sockets
    snprintf(connectionStr, sizeof(connectionStr), "tcp://%s:%s", dI->signalHost, dI->signalPort);
//    rc = zmq_connect(dI->signalSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_connect(dI->signalSocket, connectionStr) )
    {
        fprintf (stderr, "ERROR: Could not start signalSocket (connect) for '%s'\n", connectionStr);
        perror("");
    } else {
        printf("signalSocket started (connect) for '%s'\n", connectionStr);
    }

    snprintf(connectionStr, sizeof(connectionStr), "tcp://localhost:%s", dI->eventPort);
//    rc = zmq_connect(dI->eventSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_connect(dI->eventSocket, connectionStr) )
    {
        fprintf (stderr, "ERROR: Could not start eventSocket (connect) for '%s'\n", connectionStr);
        perror("");
    } else {
        printf("eventSocket started (connect) for '%s'\n", connectionStr);
    }

    snprintf(connectionStr, sizeof(connectionStr), "tcp://localhost:%s", dI->dataPort);
//    rc = zmq_connect(dI->dataSocket, connectionStr);
//    assert (rc == 0);
    if ( zmq_connect(dI->dataSocket, connectionStr) )
    {
        fprintf (stderr, "ERROR: Could not start dataSocket (connect) for '%s'\n", connectionStr);
        perror("");
    } else {
        printf("dataSocket started (connect) for '%s'\n", connectionStr);
    }


    return 0;

}


int dataIngest_createFile (dataIngest *dI, char *fileName)
{

//        if self.openFile and self.openFile != filename:
//            raise Exception("File " + str(filename) + " already opened.")

    char *message;
    char *message2;
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
    free (message2);

    return 0;
}


int dataIngest_write (dataIngest *dI, char *data, int size)
//int dataIngest_write (dataIngest *dI, void *data, int &size)
{

    char message[128];
    int rc;

    snprintf(message, sizeof(message), "{ \"filePart\": %d, \"filename\": \"%s\" }", dI->filePart, dI->filename);

    // Send event to eventDetector
    rc = s_send (dI->eventSocket, message);

    // Send data to ZMQ-Queue
    rc = s_send (dI->dataSocket, data);

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
    rc = s_send (dI->eventSocket, message);
    printf ("Sending signal to close the file to eventSocket.(sendMessage=%s)\n", message);
//        self.log.error("Sending signal to close the file to eventSocket...failed.)")


//    try:
//        socks = dict(self.poller.poll(10000)) # in ms
//    except:
//        socks = None
//        self.log.error("Could not poll for signal", exc_info=True)
//
//    if socks and self.signalSocket in socks and socks[self.signalSocket] == zmq.POLLIN:
    //  Get the reply.
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
    printf ("closing eventSocket...\n");
    zmq_close(dI->eventSocket);
    printf ("closing dataSocket...\n");
    zmq_close(dI->dataSocket);
//            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(dI->context);
//                self.log.error("Closing ZMQ context...failed.", exc_info=True)

//    free (dI);
    printf ("Cleanup finished.\n");

    return 0;
};

//#endif


int main ()
{
    dataIngest *obj;
    obj = malloc(sizeof(dataIngest));

    char *data;
    int i;
    int size;
    int rc;

    rc = dataIngest_init (obj);

    rc = dataIngest_createFile (obj, "1.h5");

    for (i=0; i < 5; i++)
    {
        data = "asdfasdasdfasd";
        size = strlen(data);
        rc = dataIngest_write (obj, data, size);
        printf ("write\n");
    };

    rc = dataIngest_closeFile (obj);

    printf ("Stopping\n");
    rc = dataIngest_stop(obj);

    free (obj);

    return 0;
};


