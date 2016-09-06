#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <json.h>
#include <assert.h>


int main()
{
    char *sourceFile = "/opt/HiDRA/test_file.cbf";
//    int chunkSize = 10485760; // 1024*1024*10 = 10MB
    int chunkSize = 524288;
    int filepart = 0;
    FILE *source_fp;
    void *context;
    void *socket;
    char connectionStr[128] = "ipc:///tmp/HiDRA/file_sending_test";
//    char connectionStr[128] = "tcp://zitpcx19282:55555";
    char *buffer = malloc(chunkSize);
    int bytesRead;
    int bytesSent;
    zmq_msg_t msg;
    int rc;
    json_object * metadata_json;
    const char *metadata_string;


    // Set up ZMQ
    if ( (context = zmq_ctx_new ()) == NULL )
    {
		perror("Cannot create 0MQ context");
		exit(-9);
	}

    if ( (socket = zmq_socket (context, ZMQ_PUSH)) == NULL )
    {
		perror("Could not create 0MQ dataSocket");
		exit(-9);
	}

    if ( zmq_connect(socket, connectionStr) )
    {
        fprintf(stderr, "Failed to start socket (connect) for '%s': %s\n", connectionStr, strerror( errno ));
		exit(-9);
    }
    else
    {
        printf("Socket started (connect) for '%s'\n", connectionStr);
    }

    // Open file
    source_fp = fopen(sourceFile,"rb");
    assert(source_fp != NULL);
    printf ("Opened file: %s\n", sourceFile);

    // Read file content
    while ((bytesRead = fread(buffer, 1, chunkSize, source_fp)) > 0)
    {
        printf ("Read file content of size: %i\n", bytesRead);

        // Build metadata
        metadata_json = json_object_new_object();

        json_object_object_add(metadata_json,"filename", json_object_new_string(sourceFile));
        json_object_object_add(metadata_json,"filePart", json_object_new_int(filepart));
        json_object_object_add(metadata_json,"chunkSize", json_object_new_int(chunkSize));

        metadata_string = json_object_to_json_string ( metadata_json );

        //Process message
        rc = zmq_msg_init_size (&msg, strlen(metadata_string));
        if (rc != 0) perror("ERROR when init msg");
        memcpy (zmq_msg_data (&msg), metadata_string, strlen(metadata_string));
        printf("memcpy metadata: %s\n", metadata_string);

        // Send message part
        bytesSent = zmq_msg_send (&msg, socket, ZMQ_SNDMORE);
        if (bytesSent == -1) perror("Sending message failed\n");
        printf("Sent message of size: %d\n", bytesSent);

        rc = zmq_msg_close(&msg);
        if (rc != 0) perror("ERROR when closing msg");
        printf("close message\n");

        //Process message
        rc = zmq_msg_init_size (&msg, bytesRead);
        if (rc != 0) perror("ERROR when init msg");
        memcpy (zmq_msg_data (&msg), buffer, bytesRead);
        printf("bytesRead: %i\n", bytesRead);
//        printf("memcpy data: %s\n", (char *) zmq_msg_data (&msg));

        /* Final part; no more parts to follow */
        bytesSent = zmq_msg_send (&msg, socket, 0);
        if (bytesSent == -1) perror("Sending message failed\n");
        printf("Sent message of size: %d\n", bytesSent);

        rc = zmq_msg_close(&msg);
        if (rc != 0) perror("ERROR when closing msg");

        filepart++;
        json_object_put ( metadata_json );
    }

    free(buffer);


    // Close file
    fclose(source_fp);
    printf ("Closed file: %s\n", sourceFile);

    // Clean up ZMQ
    zmq_close(socket);
    zmq_ctx_destroy(context);

    return 0;
}
