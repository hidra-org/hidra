//
#include "dataIngestAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main()
{
    dataIngest *obj;

    int chunksize=524288; //1024*512
    int rc;
//    char ch;
    char file_name[25] = "/opt/HiDRA/test_file.cbf";
    FILE *fp;
    char *buffer = malloc(chunksize);
    int bytesRead;

    rc = dataIngest_init (&obj);
    if (rc) exit(-9);

    fp = fopen(file_name,"r"); // read mode

    rc = dataIngest_createFile (obj, file_name);

    if (fp != NULL)
    {
        char *printBuf = malloc(100);
        char c;
        // read up to sizeof(buffer) bytes
//        while( ( ch = fgetc(fp) ) != EOF )
        while ((bytesRead = fread(buffer, 1, chunksize, fp)) > 0)
        {

            printf ("The content of file %s:\n", file_name);
            memcpy(printBuf, buffer, 100);
//            printf("%s\n",printBuf);
//            printf("%c",ch);
            rc = dataIngest_write (obj, buffer, chunksize);
        }
        free(printBuf);

        fclose(fp);
    }
    else
    {
        perror ("Error while opening the file.\n");
        exit(-9);
    }

    rc = dataIngest_closeFile (obj);

    printf ("Stopping\n");
    rc = dataIngest_stop(obj);

    free (buffer);
//    free (obj);

    return 0;


};
