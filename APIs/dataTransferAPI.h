// API to ingest data into a data transfer unit

#ifndef DATATRANSFER_H
#define DATATRANSFER_H

#define version "0.0.1"

typedef struct dataTransfer dataTransfer;

typedef enum { SUCCESS, NOTSUPPORTED, USAGEERROR, FORMATERROR, ZMQERROR, CONNECTIONFAILED, VERSIONERROR, AUTHENTICATIONFAILED, COMMUNICATIONFAILED, DATASAVINGERROR } HIDRA_ERROR;

HIDRA_ERROR dataTransfer_init (dataTransfer **dT, char *connectionType);

HIDRA_ERROR dataTransfer_initiate (dataTransfer *dT, char **targets);

HIDRA_ERROR dataTransfer_read (dataTransfer *dT, char *data, int size);

HIDRA_ERROR dataTransfer_stop (dataTransfer *dT);

#endif
