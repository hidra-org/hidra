//
#include "hidraIngest.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
//#include <unistd.h> //needed if sleep is used

int main()
{
    hidraIngest *obj;

    int chunksize=524288; //1024*512
    int rc;
    char source_file[25] = "/opt/hidra/test_file.cbf";
    FILE *fp;
    char *buffer = malloc(chunksize);
    int bytesRead;
    int i = 0;

    printf ("source_file %s\n", source_file);


    rc = hidraIngest_init (&obj);
    if (rc) exit(-9);

    fp = fopen(source_file,"rb"); // read mode
    assert(fp != NULL);

    rc = hidraIngest_createFile (obj, "test/test_file.cbf");
    if (rc)
    {
        perror ("createFile failed");
        rc = hidraIngest_stop(obj);
        assert(rc==0);
        exit(-9);
    }

    // read up to sizeof(buffer) bytes
    while ((bytesRead = fread(buffer, 1, chunksize, fp)) > 0)
    {
        printf ("The content of file %s:\n", source_file);
        printf ("Read file content of size: %i\n", bytesRead);
        rc = hidraIngest_write (obj, buffer, bytesRead);
        i++;
    }

    fclose(fp);

//    sleep(5);
    rc = hidraIngest_closeFile (obj);

    printf ("Stopping\n");
    rc = hidraIngest_stop(obj);

    free (buffer);

    return 0;


};
