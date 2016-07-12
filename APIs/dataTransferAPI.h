// API to ingest data into a data transfer unit

#ifndef DATATRANSFER_H
#define DATATRANSFER_H

#define version "0.0.1"

typedef struct dataTransfer dataTransfer;

typedef enum { SUCCESS, NOTSUPPORTED, USAGEERROR, FORMATERROR, ZMQERROR, CONNECTIONFAILED, VERSIONERROR, AUTHENTICATIONFAILED, COMMUNICATIONFAILED, DATASAVINGERROR } DATATRANSFERAPI_ERROR;

DATATRANSFERAPI_ERROR dataTransfer_init (dataTransfer **dT, char *connectionType);

DATATRANSFERAPI_ERROR dataTransfer_initiate (dataTransfer *dT, char **targets);

DATATRANSFERAPI_ERROR dataTransfer_read (dataTransfer *dT, char *data, int size);

DATATRANSFERAPI_ERROR dataTransfer_stop (dataTransfer *dT);

#endif
