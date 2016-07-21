// API to ingest data into a data transfer unit

#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <json.h>


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


static int s_send (char* socketName, void * socket, char* string)
{
//    printf("Sending: %s\n", string);
    int bytesSend = zmq_send (socket, string, strlen (string), 0);
    if (bytesSend == -1)
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

    return bytesSend;
}


int recv_multipartMessage (void *socket, char **multipartMessage, int *len)
{
    int i = 0;
    int bytesRecvd;
    int more;
    size_t more_size = sizeof(more);
    zmq_msg_t message;

    while (1)
    {
        //  Wait for next request from client
        zmq_msg_init (&message);
        bytesRecvd = zmq_msg_recv (&message, socket, 0);

        //Process message
        printf("Received message of size: %d\n", bytesRecvd);
        multipartMessage[i] = malloc (bytesRecvd + 1);
        memcpy (multipartMessage[i], zmq_msg_data (&message), bytesRecvd);
        multipartMessage[i] [bytesRecvd] = 0;
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

    return 0;

}


int send_multipartMessage (void *socket, const char **multipartMessage, int len, int chunksize)
{
    int i = 0;
    int bytesSent;
    zmq_msg_t message;

    printf("len before loop=%i\n", len);

    for (i = 0; i < len - 1; i++)
    {
        printf("len=%i\n", len);
        //Process message
        zmq_msg_init_size (&message, chunksize);
        memcpy (zmq_msg_data (&message), multipartMessage[i], strlen(multipartMessage[i]));
        printf("sent string: %s\n", multipartMessage[i]);

        // Send message part
        bytesSent = zmq_msg_send (&message, socket, ZMQ_SNDMORE);
        if (bytesSent == -1)
        {
            perror("Sending message failed\n");
        }
        printf("Sent message of size: %d\n", bytesSent);

        zmq_msg_close(&message);
        printf("close message");
    }

    printf("message of size %i", chunksize);
    //Process message
    zmq_msg_init_size (&message, chunksize);
    memcpy (zmq_msg_data (&message), multipartMessage[len], chunksize);
    printf("sent string: %s\n", multipartMessage[len]);

    /* Final part; no more parts to follow */
    bytesSent = zmq_msg_send (&message, socket, 0);
    if (bytesSent == -1)
    {
        perror("Sending message failed\n");
    }
    printf("Sent message of size: %d\n", bytesSent);

    zmq_msg_close(&message);

    return bytesSent;
}


int main()
{

    void *context;
    void *fileOpSocket;
    void *dataSocket;
    char connectionStr1[128] = "tcp://zitpcx19282:50100";
    char connectionStr2[128] = "tcp://zitpcx19282:50050";

    int chunksize=524288; //1024*512
    int rc;
    char file_name[25] = "/opt/HiDRA/test_file.cbf";
    FILE *fp;
    char *buffer = malloc(chunksize);
    int bytesRead;

    FILE *fp_local;
    char *filepath = "/opt/HiDRA/data/source/local";
    char *filename = "test.cbf";
    int filePart = 0;
    char abs_filename[128];

    json_object * metadata_json;
    const char *metadata_string;

    zmq_msg_t msg;
    int bytesSent;
    char *message;
    char *multipartMessage[2];

    char *printBuf = malloc(100);
    int i = 0;
    char *fileOp;

    if ( (context = zmq_ctx_new ()) == NULL )
    {
		perror("Cannot create 0MQ context");
		exit(-9);
	}

    /*
     * Set up ZMQ
     */

    // Create socket for file operation exchanging
    if ( (fileOpSocket = zmq_socket (context, ZMQ_REQ)) == NULL )
    {
        perror("Could not create 0MQ fileOpSocket");
		exit(-9);
    }

    if ( zmq_connect(fileOpSocket, connectionStr2) )
    {
        fprintf(stderr, "Failed to start file operation socket (connect) for '%s': %s\n",
                connectionStr2, strerror( errno ));
		exit(-9);
    }
    else
    {
        printf("File operation socket started (connect) for '%s'\n", connectionStr2);
    }

    // Create data socket
    if ( (dataSocket = zmq_socket (context, ZMQ_PUSH)) == NULL )
    {
		perror("Could not create 0MQ dataSocket");
		exit(-9);
	}

    if ( zmq_connect(dataSocket, connectionStr1) )
    {
        fprintf(stderr, "Failed to start data socket (connect) for '%s': %s\n",
                connectionStr1, strerror( errno ));
		exit(-9);
    }
    else
    {
        printf("Data socket started (connect) for '%s'\n", connectionStr1);
    }


    /*
     * Open file
     */

    fp = fopen(file_name,"r"); // read mode

    // Send notification to receiver
    rc = s_send ("fileOpSocket", fileOpSocket, "OPEN_FILE");
    printf ("Sending signal to open a new file.\n");

    message = s_recv (fileOpSocket);
    printf ("Received responce: '%s'\n", message);

    /*
     * Read file
     */

    if (fp != NULL)
    {
        // read up to sizeof(buffer) bytes
        while ((bytesRead = fread(buffer, 1, chunksize, fp)) > 0)
        {

            printf ("The content of file %s:\n", file_name);
//            memcpy(printBuf, buffer, 100);
//            printf("%s\n",printBuf);

            /*
             * Build and send metadata message
             */

            metadata_json = json_object_new_object();

            json_object_object_add(metadata_json,"filename", json_object_new_string(filename));
            json_object_object_add(metadata_json,"filePart", json_object_new_int(filePart));
            json_object_object_add(metadata_json,"chunkSize", json_object_new_int(chunksize));

            metadata_string = json_object_to_json_string ( metadata_json );

            char metadata_string2[128];
            snprintf(metadata_string2, sizeof(metadata_string2), "{ \"filePart\": %d, \"chunkSize\": %d, \"filename\": \"%s\" }", filePart, chunksize, filename);


//            printf ("The json object created: %s\n", metadata_string);
            printf ("The json object created: %s\n", metadata_string2);

            // Send event to eventDetector
//            rc = send_multipartMessage (dataSocket, multipartMessage, 2, chunksize);

            //Process message
            rc = zmq_msg_init_size (&msg, strlen(metadata_string2));
            if (rc != 0) perror("ERROR when init msg");
//            memcpy (zmq_msg_data (&msg), metadata_string, strlen(metadata_string));
//            printf("sent string: %s\n", metadata_string);
            memcpy (zmq_msg_data (&msg), metadata_string2, strlen(metadata_string2));
//            printf("sent string: %s, len=%zu\n", metadata_string2, strlen(metadata_string2));
            // Send message part
            bytesSent = zmq_msg_send (&msg, dataSocket, ZMQ_SNDMORE);
            if (bytesSent == -1)
            {
                perror("Sending message failed\n");
            }
            else
            {
                printf("Sent message of size: %d\n", bytesSent);
            }

            rc = zmq_msg_close(&msg);
            if (rc != 0) perror("ERROR when closing msg");
            printf("close message\n");


            /*
             * Send data
             */

            //Process message
            rc = zmq_msg_init_size (&msg, bytesRead);
            if (rc != 0) perror("ERROR when init msg");
            memcpy (zmq_msg_data (&msg), buffer, bytesRead);
//            printf("sent string: %s\n", buffer);

            /* Final part; no more parts to follow */
            bytesSent = zmq_msg_send (&msg, dataSocket, 0);
            if (bytesSent == -1)
            {
                perror("Sending message failed\n");
            }
            printf("Sent message of size: %d\n", bytesSent);

            rc = zmq_msg_close(&msg);
            if (rc != 0) perror("ERROR when closing msg");


            printf ("Writing: %s\n", message);

            filePart++;

            snprintf(abs_filename, sizeof(abs_filename), "%s/%s_%d", filepath, filename, i);
            printf ("abs_filename %s\n", abs_filename);

            //write controlling file
/*            fp_local = fopen(abs_filename,"w");
            fwrite(buffer, chunksize, 1, fp_local);
            fclose(fp_local);
*/
            i++;

        }
        free(printBuf);

        fclose(fp);

    }

    /*
     * Close file
     */
    message = "CLOSE_FILE";
    //Process message
    rc = zmq_msg_init_size (&msg, strlen(message));
    if (rc != 0) perror("ERROR when init msg");
    memcpy (zmq_msg_data (&msg), message, strlen(message));
    printf("sent string: %s, len=%zu\n", message, strlen(message));

    // Send message part
    bytesSent = zmq_msg_send (&msg, dataSocket, ZMQ_SNDMORE);
    if (bytesSent == -1) perror("Sending message failed\n");
    printf("Sent message of size: %d\n", bytesSent);

    rc = zmq_msg_close(&msg);
    if (rc != 0) perror("ERROR when closing msg");
    printf("close message\n");


    message = "0/1";
    //Process message
    rc = zmq_msg_init_size (&msg, strlen(message));
    if (rc != 0) perror("ERROR when init msg");
    memcpy (zmq_msg_data (&msg), message, strlen(message));
    printf("sent string: %s\n", message);

    /* Final part; no more parts to follow */
    bytesSent = zmq_msg_send (&msg, dataSocket, 0);
    if (bytesSent == -1) perror("Sending message failed\n");
    printf("Sent message of size: %d\n", bytesSent);

    rc = zmq_msg_close(&msg);
    if (rc != 0) perror("ERROR when closing msg");


    // Send notification to receiver
    rc = s_send ("fileOpSocket", fileOpSocket, "CLOSE_FILE");
    printf ("Sent signal to close file.\n");

    message = s_recv (fileOpSocket);
    printf ("Received responce: '%s'\n", message);


    /*
     * Clean up ZMQ
     * */

    printf ("closing fileOpSocket...\n");
    zmq_close(fileOpSocket);
    printf ("closing dataSocket...\n");
    zmq_close(dataSocket);

    printf ("Closing ZMQ context...\n");
    zmq_ctx_destroy(context);

    printf ("Cleanup finished.\n");

    return 0;
}

