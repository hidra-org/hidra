// API to ingest data into a data transfer unit

#ifndef DATATRANSFER_H
#define DATATRANSFER_H

#define version "0.0.1"

#include <json.h>

typedef struct dataTransfer dataTransfer_t;
typedef struct params_cb params_cb_t;

typedef struct metadata
{
    const char *filename;
    int filePart;
    int fileCreateTime;
    int fileModTime;
    int filesize;
    int chunkSize;
    int chunkNumber;
} metadata_t;


typedef int (*open_cb_t)(params_cb_t *cbp, char *filename);
typedef int (*read_cb_t)(params_cb_t *cbp, metadata_t *metadata, char *payload, int payloadSize);
typedef int (*close_cb_t)(params_cb_t *cbp);


typedef enum { SUCCESS, NOTSUPPORTED, USAGEERROR, FORMATERROR, ZMQERROR, CONNECTIONFAILED, VERSIONERROR, AUTHENTICATIONFAILED, COMMUNICATIONFAILED, DATASAVINGERROR } HIDRA_ERROR;

HIDRA_ERROR dataTransfer_init (dataTransfer_t **dT, char *connectionType);

HIDRA_ERROR dataTransfer_initiate (dataTransfer_t *dT, char **targets);

HIDRA_ERROR dataTransfer_read (dataTransfer_t *dT, params_cb_t *cbp, open_cb_t openFunc, read_cb_t readFunc, close_cb_t closeFunc);

HIDRA_ERROR dataTransfer_stop (dataTransfer_t *dT);

#endif
