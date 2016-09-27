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
//    char ch;
    char source_file[25] = "/opt/HiDRA/test_file.cbf";
    FILE *fp;
    char *buffer = malloc(chunksize);
    int bytesRead;

    printf ("source_file %s\n", source_file);


    rc = hidraIngest_init (&obj);
    if (rc) exit(-9);

    fp = fopen(source_file,"rb"); // read mode
    assert(fp != NULL);

    rc = hidraIngest_createFile (obj, "test/test_file.cbf");

    char *printBuf = malloc(100);
    char c;
    int i = 0;
    // read up to sizeof(buffer) bytes
//        while( ( ch = fgetc(fp) ) != EOF )
    while ((bytesRead = fread(buffer, 1, chunksize, fp)) > 0)
    {

        printf ("The content of file %s:\n", source_file);
        printf ("Read file content of size: %i\n", bytesRead);
//        memcpy(printBuf, buffer, 100);
//        printf("%s\n",printBuf);
//        printf("%c",ch);
        rc = hidraIngest_write (obj, buffer, bytesRead);
        i++;

    }
    free(printBuf);

    fclose(fp);

//    sleep(5);
    rc = hidraIngest_closeFile (obj);

    printf ("Stopping\n");
    rc = hidraIngest_stop(obj);

    free (buffer);

    return 0;


};
