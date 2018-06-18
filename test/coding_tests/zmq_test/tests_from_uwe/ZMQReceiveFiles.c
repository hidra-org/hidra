/*
 * ZMQReceiveFiles.c
 *
 *  Created on: 12.11.2014
 *      Author: ensslin
 */


/*	ZMQReceiveFiles  - Receive zeromq messages from SourceHost and store them as files into
 * 					   Arguments.TargetDirectory.
 * 				  	   The file name is derived from the first ZMQ message part.
 *
 *
 *
 *
 *  Modification History.
 *
 *  2014-11-12,		Uwe Ensslin.		V 14.11.12.01	- Initial version.
 *  2014-12-17,		Uwe Ensslin.		V 14.12.17.01	- Variable cleanup.
 *  2015-01-06,		Uwe Ensslin.		V 15.01.06.01	- Get NFS4 access bits from stream.
 *
 *
 *
 *
 *	Notes:
 *
 *	Use #define _GNU_SOURCE to compile for dcache.
 */

#include <zmq.h>
#include <zmq_utils.h>
#include <assert.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include "nfs-acls.h"

#define PROGNAME		"ZMQReceiveFiles"
#define VERSION			"15.01.06.01"

// Sequence of things to do in WriteFile as messages come in. Adapt as necessary.
#define STATE_OPEN_FILE		0		// Initial message received. Need to open file to receive.
#define STATE_PROCESS_ACL	1		// Message is supposed to contain the NFS4 ACL
#define STATE_WRITE_CONTENT	2		// Message is supposed to contain file content
#define STATE_CLOSE_FILE	3		// Last message, terminate file reception


void Usage();	// Froward declaration,,

struct Arguments_t {
	int nThreads;
	size_t ReceiveBufferSize;
	char *SourceHost;
	char *TargetDirectory;
	long ZMQ_High_Water_Mark;
} Arguments;

struct Statistics_t {
	int64_t	BytesWritten, BytesReceived;
} Statistics;


long Atol(char *s) {
	long int l;

	if ( !sscanf(s, "%16ld",&l) ) {
		fprintf(stderr,"ERROR: Cannot convert '%s' to long.\n",s);
		exit(9);
	}

	return l;
}


double GetMTime() {
	// Return microseconds accurate time.
	struct timeval tp;
	double Time;

	gettimeofday(&tp,NULL);

	Time = tp.tv_sec + (double)tp.tv_usec/1000000.0;

	return Time;
}

void SetArgumentsFromCommandLine (int argc, char *argv[]) {
	static int Help, help;

	Help = 0; help = 0;	// Better way ??
	while (1) {
		static struct option long_options[] =
		{
				{"Help", 	  			no_argument, &Help,1},
				{"help", 	  			no_argument, &help,1},

				{"nThreads",  			required_argument, 0, 'a'},
				{"ReceiveBufferSize",	required_argument, 0, 'b'},
				{"SourceHost",			required_argument, 0, 'c'},
				{"TargetDirectory",		required_argument, 0, 'd'},
				{"ZMQ_High_Water_Mark",	required_argument, 0, 'e'},
				{0,0,0,0}
		};
		int option_index = 0;
		int c;
		c = getopt_long(argc, argv, "a:b:c:d:e:", long_options, &option_index);
		if ( c == -1 ) {
			break;
		}
		switch (c) {
			case 0:		// Flag type option
				if ( long_options[option_index].flag ) {
					if (! strncmp(long_options[option_index].name,"Help",4) ) {
						// If help requested ..
						Usage();
						exit(0);
					}
					if (! strncmp(long_options[option_index].name,"help",4) ) {
						// If help requested ..
						Usage();
						exit(0);
					}
					break;
				}
			break;

			case 'a':	// Key for nThreads ..
				Arguments.nThreads = Atol(optarg);
				if ( Arguments.nThreads < 0 ) {
					fprintf(stderr,"ERROR: nThreads must be >= 0.\n");
					exit(9);
				}
			break;

			case 'b':	// Key for ReceiveBufferSize ..
				Arguments.ReceiveBufferSize = Atol(optarg);
				if ( Arguments.ReceiveBufferSize < 0 ) {
					fprintf(stderr,"ERROR: ReceiveBufferSize must be >= 0.\n");
					exit(9);
				}
			break;

			case 'c':	// SourceHost
				Arguments.SourceHost = optarg;
			break;

			case 'd':	// TargetDirectory
				Arguments.TargetDirectory = optarg;
			break;

			case 'e':	// Key for ZMQ high water
				Arguments.ZMQ_High_Water_Mark = Atol(optarg);
			break;

			case '?':
				fprintf(stderr,"ERROR: Unknown option encountered.\n");
				exit(9);
			break;
		}
	}
}


void WriteFile (int BytesToWrite, char *Buffer, int PartialMessage, long MessageNr) {
	static char *Path = NULL;
	static int File = -1;
	static int State = STATE_OPEN_FILE;
	ssize_t bw;
	static double TimeRcvStart, TimeRcvEnd, TimeSpent;
	static double MBytesInFile = 0;
	NFS4_ACL_t NFS4_ACL;
	char *cp;
	int i;


	switch ( State ) {
	case STATE_OPEN_FILE:
		if ( (Path = malloc(strlen(Arguments.TargetDirectory)+1+strlen(Buffer)+1)) == NULL ) {
			perror("ERROR: Cannot malloc file path: ");
			exit(9);
		}
		if ( sprintf(Path,"%s/%s",Arguments.TargetDirectory,Buffer) < 0 ) {
			perror("ERROR: Cannot assemble file path: ");
			exit(9);
		}
		TimeRcvStart = GetMTime();
		if ( (File = open(Path,O_WRONLY|O_CREAT|O_TRUNC,0644)) < 0  ) {
		   fprintf(stderr,"ERROR: WriteImage: Could not open/write %s.\n",Path);
		   perror("");
		   exit(9);
		}
		MBytesInFile = 0;
		State = STATE_PROCESS_ACL;
		break;
	case STATE_PROCESS_ACL:
		cp = NFS4_ACL.c;
		for (i=0; i < BytesToWrite; i++) {	// Copy the ACL. NFS4_ACL must have enough space !
			*cp++ = *(Buffer+i);
		}
		fprintf(stdout,"DEBUG: ACL %d m.c: %s, btw: %d\n",NFS4_ACL.i,NFS4_ACL.c,BytesToWrite);
		State = STATE_WRITE_CONTENT;
		break;
	case STATE_WRITE_CONTENT:
		// File is already open; write (part of) content.
		if ( (bw = write(File,(Buffer), BytesToWrite)) < 0 ) {
			fprintf(stderr,"ERROR: WriteFile: Cannot write partial message to file %s\n",Path);
	        perror("");
	        exit(9);
		}
		MBytesInFile += bw;
		if ( ! PartialMessage ) { // Last part of file. Close up.
			if ( close(File) ) {
				fprintf(stderr,"ERROR: WriteFile: Cannot close %s\n",Path);
				perror("");
			}
			TimeRcvEnd = GetMTime();
			TimeSpent = TimeRcvEnd - TimeRcvStart;
			MBytesInFile /= (1024*1024);
			fprintf(stdout,"Received file %8.8ld with %10.2f MB in %10.2fs (%10.2f MB/s). Path %s\n",
					MessageNr,
					MBytesInFile,
					TimeSpent,
					MBytesInFile/TimeSpent,
					Path);
			free(Path);
			File = -1;		// Mark the output file "closed".
			State = STATE_OPEN_FILE;	// Prepare for next file.
		}
		break;
	}
}


char* TimeStamp(void) {
/*  Return current time as "YYYY-MM-DD, HH:MM:SS".
 */
	static char *TimeStrBuf = NULL;
	time_t AbsTime;
	struct tm *LocalTime;

	if ( TimeStrBuf == NULL ) {
		TimeStrBuf = malloc(22);	/* YYYY-MM-DD, HH:MM:SS */
	}
	AbsTime = time(NULL);
	LocalTime = localtime(&AbsTime);
	strftime(TimeStrBuf,22,"%Y-%m-%d, %H:%M:%S",
			LocalTime);
	return TimeStrBuf;
}

void Usage () {
	fprintf(stdout,"usage: %s \n",PROGNAME);
	fprintf(stdout,"\t--Help\n");
	fprintf(stdout,"\t--nThreads\t\t- Fork n children to do the work. Default: No children.\n");
	fprintf(stdout,"\t--ReceiveBufferSize\t- Size (bytes) of ZMQ receive buffer. Default 500MB.\n");
	fprintf(stdout,"\t--SourceHost\t\t- Host to listen to. Default: tcp://zitpcx21708.desy.de:5556\n");
	fprintf(stdout,"\t--TargetDirectory\t- Directory to write files to. Default: ./tmp.\n");
	fprintf(stdout,"\t--ZMQ_High_Water_Mark\t-ZMQ Receive buffer high water mark.\n");
}

int main (int argc, char *argv [])
{
	long Messages = 0;
	char *ReceiveBuffer;
	struct stat StatBuf;
	int MoreFiles;

	Arguments.nThreads				= 0;
	Arguments.ReceiveBufferSize 	= 1024*1024*500; 	// 500 MB.
	Arguments.SourceHost 			= "tcp://zitpcx21708.desy.de:5556";
	Arguments.TargetDirectory 		= "./tmp";
	Arguments.ZMQ_High_Water_Mark 	= 0;	// Use the default..

	SetArgumentsFromCommandLine(argc,argv);

	printf ("%10.10d: %s (V %s): Receiving from %s (max size: %d)…\n",
			getpid(),PROGNAME,VERSION,Arguments.SourceHost,Arguments.ReceiveBufferSize);
	printf("ZMQ RCV high water mark:\t%ld\n",Arguments.ZMQ_High_Water_Mark);

	// Create output dir, if not present. Do it here to avoid child races ..
	if ( stat(Arguments.TargetDirectory,&StatBuf) < 0 ) {
		// Create missing directory.
		if ( mkdir(Arguments.TargetDirectory,0755) < 0 ) {
			fprintf(stderr,"ERROR: Cannot create directory %s:\n",Arguments.TargetDirectory);
			perror("");
			exit(9);
		}
		fprintf(stdout,"%10.10d: %s: INFO: Target directory %s created.\n",getpid(),TimeStamp(),Arguments.TargetDirectory);fflush(stdout);
	}

	// If requested, we fork a couple of receivers. Parent waits and exits; childs do the work.
	if ( Arguments.nThreads ) {
        fprintf (stdout, "%10.10d: %s: Starting %d receiver processes.\n", getpid(), TimeStamp(),Arguments.nThreads);

        int i,StatLoc;
        pid_t pid;
        for(i = 0; i < Arguments.nThreads; i++) {
        	pid = fork();
            if (pid < 0) {
               perror("can't fork");
               exit(11);
            }
            if ( ! pid ) { // Child, start receiving ..
            	break;
            }
        }

        if ( pid ) {	// Parent, wait for childs, then exit..
        	for(i = 0; i < Arguments.nThreads; i++) {
        		wait(&StatLoc);
        	}
        	exit(0);
        }
	}

	void *context = zmq_ctx_new ();

	if ( (ReceiveBuffer = malloc(Arguments.ReceiveBufferSize)) == NULL ) {
		perror("ERROR: Could not malloc receive buffer: ");
		exit(9);
	}
	// Process files ..
	while ( 1 ) {
		int BytesRcved, MoreMessageParts;
		size_t MoreMessagePartsSize;
		double TotalBytesRcved;

		MoreMessagePartsSize = sizeof(MoreMessageParts);

		void *subscriber = zmq_socket (context, ZMQ_PULL);
// Start workaround for SL 6
		int hwm = Arguments.ZMQ_High_Water_Mark;
		if ( (zmq_setsockopt(subscriber,ZMQ_RCVHWM, &hwm, sizeof(hwm)) ) ) {
// End workaround.. Delete above 2 lines and uncomment following one to get rid of this..
//		if ( (zmq_setsockopt(zmq_publisher,ZMQ_SNDHWM, &Arguments.ZMQ_High_Water_Mark, sizeof(long)) ) ) {
					perror("ERROR: Cannot set ZMQ socket options: ");
					exit(9);
				}
		if ( zmq_connect (subscriber, Arguments.SourceHost) ) {
			perror("ERROR: Cannot connect to 0MQ socket: ");
			exit(9);
		}

		Messages=1;
		TotalBytesRcved = 0;
		MoreFiles = 1;
		while ( MoreFiles ) {
			MoreMessageParts = 0;
			double MBytesInFile = 0;;
			do {
				BytesRcved = zmq_recv (subscriber, ReceiveBuffer, Arguments.ReceiveBufferSize,0);

				if (zmq_getsockopt(subscriber, ZMQ_RCVMORE, &MoreMessageParts, &MoreMessagePartsSize) ) {
					perror("ERROR: Cannot get socket options: ");
					exit(9);
				}

				if ( BytesRcved ) {
//				printf("Got %d bytes  \n",BytesRcved);
					TotalBytesRcved += BytesRcved;
					MBytesInFile += BytesRcved;
					WriteFile(BytesRcved,ReceiveBuffer,MoreMessageParts,Messages);
				}
			} while (MoreMessageParts);
			Messages++;
		}
		fprintf(stdout,"%10.10d: %s: %ld messages/files with %.2f MBytes rcved.\n",getpid(),TimeStamp(),
				Messages,TotalBytesRcved/(1024*1024)
				);
		zmq_close (subscriber);
		fprintf(stdout,"\n"); fflush(stdout);
		Statistics.BytesReceived = 0;
		Statistics.BytesWritten = 0;
	}
	zmq_ctx_destroy (context);
	return 0;
}
