// API to ingest data into a data transfer unit

#include "dataTransferAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>


struct params_cb
{
    int num;
    FILE *fp;
};


int open_cb (params_cb_t *cbp, char *filename)
{
    char *filepath = "/opt/HiDRA/data/target/local";
    char abs_filename[128];
    snprintf(abs_filename, sizeof(abs_filename), "%s/%s", filepath, filename);

    printf ("abs_filename %s\n", abs_filename);

    printf("execute openCall_cb for file: %s\n", filename);

    cbp->fp = fopen(abs_filename,"w"); // read mode
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

    return 0;
};


