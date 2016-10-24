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

    char *targetFile = "/opt/hidra/data/target/local/test.cbf";
    FILE *target_fp;
    void *context;
    void *socket;
    char connectionStr[128] = "ipc:///tmp/hidra/file_sending_test";
    int runLoop = 1;
    zmq_msg_t msg;
    int rc;
    json_object * metadata_json;
    char *metadata_string;
    char *data;
    int metadataSize;
    int dataSize;
    int chunkSize;
    int filePart;
    char *filename;


    // Set up ZMQ
    if ( (context = zmq_ctx_new ()) == NULL )
    {
		perror("Cannot create 0MQ context");
		exit(-9);
	}

    if ( (socket = zmq_socket (context, ZMQ_PULL)) == NULL )
    {
		perror("Could not create 0MQ dataSocket");
		exit(-9);
	}

    if ( zmq_bind(socket, connectionStr) )
    {
        fprintf(stderr, "Failed to start socket (connect) for '%s': %s\n", connectionStr, strerror( errno ));
		exit(-9);
    }
    else
    {
        printf("Socket started (connect) for '%s'\n", connectionStr);
    }

    // Open file
    target_fp = fopen(targetFile,"wb");
    assert(target_fp != NULL);
    printf ("Opened file: %s\n", targetFile);

    // Read file content
    while (runLoop)
    {
        // Receive message
        rc = zmq_msg_init (&msg);
        if (rc != 0) perror("ERROR when init msg");
        metadataSize = zmq_msg_recv (&msg, socket, 0);

        // Process message
        printf("Received message of size: %d\n", metadataSize);
        metadata_string = malloc (metadataSize + 1);
        memcpy (metadata_string, zmq_msg_data (&msg), metadataSize);
        metadata_string [metadataSize] = 0;

        rc = zmq_msg_close(&msg);
        if (rc != 0) perror("ERROR when closing msg");
        printf("close message\n");

        // Receive message
        rc = zmq_msg_init (&msg);
        if (rc != 0) perror("ERROR when init msg");
        dataSize = zmq_msg_recv (&msg, socket, 0);

        // Process message
        printf("Received message of size: %d\n", dataSize);
        data = malloc (dataSize + 1);
        memcpy (data, zmq_msg_data (&msg), dataSize);
        data [dataSize] = 0;

        rc = zmq_msg_close(&msg);
        if (rc != 0) perror("ERROR when closing msg");
        printf("close message\n");

        // Decode metadata
        metadata_json = json_tokener_parse(metadata_string);
        printf("metadata:\n%s\n", json_object_to_json_string_ext(metadata_json, JSON_C_TO_STRING_SPACED | JSON_C_TO_STRING_PRETTY));

        json_object *val;

//        json_object_object_get_ex(metadata_json, "filename", &val);
//        filename = json_object_get_string(val);

        json_object_object_get_ex(metadata_json, "filePart", &val);
        filePart = json_object_get_int(val);

        json_object_object_get_ex(metadata_json, "chunkSize", &val);
        chunkSize = json_object_get_int(val);

        // Write file
        fwrite(data, dataSize, 1, target_fp);

        free (metadata_string);
        free (data);
        json_object_put ( metadata_json );

        if (dataSize < chunkSize)
        {
            break;
        }
    }

    // Close file
    fclose(target_fp);
    printf ("Closed file: %s\n", targetFile);

    // Clean up ZMQ
    zmq_close(socket);
    zmq_ctx_destroy(context);

    return 0;
}
