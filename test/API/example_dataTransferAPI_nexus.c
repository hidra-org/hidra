// API to ingest data into a data transfer unit

#include "dataTransferAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

struct params_cb
{
    int num;
    FILE *fp;
};


int open_cb (params_cb_t *cbp, char *filename)
{
    char *filepath = "/opt/HiDRA/data/target/local";
    char abs_filename[128];
    char *lastSlash = NULL;
    char *parent = NULL;
    struct stat st = {0};
    int rc;

    snprintf(abs_filename, sizeof(abs_filename), "%s/%s", filepath, filename);

    printf ("abs_filename %s\n", abs_filename);


    lastSlash = strrchr(abs_filename, '/'); // you need escape character
    //lastSlash = strrchr(abs_filename, '\\'); // you need escape character
    printf("lastSlash= %s\n", lastSlash);
    parent = strndup(abs_filename, strlen(abs_filename) - (strlen(lastSlash)));
    printf ("filepath = %s\n", parent);


    if (stat(parent, &st) == -1)
    {
        printf ("Create directory %s\n", parent);
        if (mkdir(parent, 0700))
        {
            fprintf(stderr, "Failed to create directory '%s': %s\n", parent, strerror(errno));
        }
    }

    printf ("execute openCall_cb for file: %s\n", filename);

    cbp->fp = fopen(abs_filename,"wb"); // read mode

    free (parent);
}


int read_cb (params_cb_t *cbp, metadata_t *metadata, char *payload, int payloadSize)
{
    printf("execute readCall_cb\n");
/*
    FILE *fp_local;
    char *filepath = "/opt/HiDRA/data/target/local";
    char abs_filename[128];
    snprintf(abs_filename, sizeof(abs_filename), "%s/%s_%d", filepath, "test.cbf", metadata->filePart);
    printf ("abs_filename %s\n", abs_filename);

    fp_local = fopen(abs_filename,"w"); // read mode

    fwrite(payload, payloadSize, 1, fp_local);
*/
/*
    char *printBuf = malloc(100);
    memcpy(printBuf, payload, 100);
    printf("%s\n",printBuf);
*/
/*
    printf ("filename: %s\n", metadata->filename);
    printf ("filePart: %i\n", metadata->filePart);
    printf ("fileCreateTime: %i\n", metadata->fileCreateTime);
    printf ("fileModTime: %i\n", metadata->fileModTime);
    printf ("filesize: %i\n", metadata->filesize);
    printf ("chunkNumber: %i\n", metadata->chunkNumber);
*/
//    fclose(fp_local);

    fwrite(payload, payloadSize, 1, cbp->fp);
}


int close_cb (params_cb_t *cbp)
{
    printf("execute closeCall_cb\n");
    fclose(cbp->fp);
}


int main ()
{
    dataTransfer_t *obj;

    char *data;
    int i;
    int size;
    int rc;
    params_cb_t *cbp = malloc(sizeof(params_cb_t));


    rc = dataTransfer_init (&obj, "nexus");
    if (rc) exit(-9);
    printf ("dataTransfer_init returned: %i\n", rc);

    rc = dataTransfer_start (obj);
    printf ("dataTransfer_start returned: %i\n", rc);

//    rc = dataTransfer_read (obj, data, size);
    rc = dataTransfer_read (obj, cbp, open_cb, read_cb, close_cb);
    printf ("dataTransfer_read returned: %i\n", rc);

    printf ("Stopping\n");
    rc = dataTransfer_stop(obj);
    printf ("dataTransfer_stop returned: %i\n", rc);

    free (cbp);

    return 0;
};


