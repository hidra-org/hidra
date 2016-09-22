/*
 ============================================================================
 Name        : C0MQSendFiles.c
 Author      : Uwe Ensslin (DESY)
 Version     :
 Copyright   : DESY
 Description : Send a list of files via 0MQs push algorithm.
 ============================================================================
 */

/*
 * C0MQSendFiles - Send a list of files from stdin via the 0MQ 'push' mechanism.
 *
 *
 *
 * 		Synopsis.
 *
 * 		C0MQSendFiles
 * 			--DryRun
 * 			--PartialMessageSize
 *      	--WaitBeforeRetry
 *      	--ZMQ_High_Water_Mark
 *
 *
 *
 * Note:
 *
 *
 * This assumed input (from inotifywait) of the form
 *
 * inotifywait -e close_write -m /Pool-3/1/DeleteMe --format "%w%f %e" -r -q
 *
 * On 32 bit systems use -D_FILE_OFFSET_BITS=64 to compile to allow stat to
 * work on files > 2GB..
 *
 *
 *
 *
 * 	Modification History.
 *
 * 	2014-11-11, 	Uwe Ensslin.	V 14.11.11.01 		- Cloned from C0MQSimulator-push
 * 	2014-11-17,		Uwe Ensslin		V 14.11.17.01		- Statistics improved,
 * 	2015-01-06,		Uwe Ensslin.	V 15.01.06.01		- Experiments to pass NFS4 ACL bits via partial message.
 *
 */

#define VERSION 	"15.01.08.01"
#define PROGNAME	"C0MQSendFiles"
#define MAX_RETRIES	10			// Max attempts to retry sending a message.

// #define I64PRINT	%" PRIi64 "

#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <sched.h>				// For setting CPU affinity..

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/stat.h>
#include <getopt.h>
#include <unistd.h>
#include <zmq.h>
#include <zmq_utils.h>
#ifdef D_GPFS_NFS4_ACLS
#include <gpfs.h>
#endif

#include "nfs-acls.h"

// int64_t support ..
#define __STDC_FORMAT_MACROS
#include <inttypes.h>


struct Arguments_t {
	int	 	DryRun;						// Don't process any data; just run the loop
	size_t	PartialMessageSize;			// Chop the image into chunks and write them as partial msgs.
	int	 	WaitBeforeRetry;			// Wait n seconds before retry sending a failed message.
	long 	ZMQ_High_Water_Mark;		// Passed to zmq_setsockopt.
} Arguments;

struct ZMQPartialMessages_t {
	long long  	FileSize;
	long long	nChunks;		// Nr. of chunks to write
	size_t	Chunksize;
	size_t	Remainder;		// ImageSize % Chunksize ..
} ZMQPartialMessages;

struct Statistics_t {
	int64_t BytesRead, BytesWritten;
	int64_t FilesProcessed;
	double StartTime,EndTime, RunTime, SleepTime;
} Statistics;

// Forward declarations..
void WriteMessageChunk(char* Buffer, long Size, void* zmq_publisher);
void WriteWholeMessage(char* ImageBuffer, size_t ImageSz, void* zmq_publisher);
void Usage();


long Atol(char *s) {
	long int l;

	if ( !sscanf(s, "%16ld",&l) ) {
		fprintf(stderr,"ERROR: Cannot convert '%s' to long.\n",s);
		exit(9);
	}

	return l;
}

#ifdef D_GPFS_NFS$_ACLS
void Get_NFS4_ACL_from_GPFS(char *Path, NFS4_ACL_t *ACL) {

	ACL.c = 'X';

}
#else
void Get_NFS4_ACL_from_GPFS(char *Path, NFS4_ACL_t *ACL) {

	*ACL->c = '\0';
	strcat(ACL->c,"Howdy!\tUnlimited access for everyone.");
}
#endif

double GetMTime() {
	// Return microseconds accurate time.
	struct timeval tp;
	double Time;

	gettimeofday(&tp,NULL);

	Time = tp.tv_sec + (double)tp.tv_usec/1000000.0;

	return Time;
}

int OpenFile(char *Path) {
	struct stat StatBuf;
	int File;

	if ( stat(Path,&StatBuf) ) {
		fprintf(stderr,"ERROR: Could stat input file %s:\n",Path);
		perror(" ");
		exit(9);
	}

	ZMQPartialMessages.FileSize = StatBuf.st_size;

	if ( (File = open(Path,O_RDONLY)) == -1 ) {
		fprintf(stderr,"ERROR: Could not open %s.\n",Path);
		perror(" ");
		exit(9);
	}

//	fprintf(stdout,"%s open as %d. Size %ld.\n",Path,File,ZMQPartialMessages.FileSize/(1024*1024));
	// Do some preparations for partial ZMQ messages here, to avoid repeated computation later..
	if ( Arguments.PartialMessageSize ) {	// We are going to write partial msgs.
		ZMQPartialMessages.Chunksize	= Arguments.PartialMessageSize;
		ZMQPartialMessages.Remainder 	= ZMQPartialMessages.FileSize % Arguments.PartialMessageSize;
		ZMQPartialMessages.nChunks	 	= ZMQPartialMessages.FileSize / Arguments.PartialMessageSize;
		if ( ZMQPartialMessages.Remainder ) {
			ZMQPartialMessages.nChunks++;
		}
		else {
			ZMQPartialMessages.Remainder = ZMQPartialMessages.Chunksize;
		}
/*		fprintf(stdout,"ZMQ image size\t\t%lldB\n",ZMQPartialMessages.FileSize);
		fprintf(stdout,"ZMQ chunk size\t\t%dB\n",ZMQPartialMessages.Chunksize);
		fprintf(stdout,"ZMQ remainder\t\t%dB\n",ZMQPartialMessages.Remainder);
*/		fprintf(stdout,"ZMQ nchunks\t\t%lld\n",ZMQPartialMessages.nChunks);
	}

	return(File);
}

void SendFile(int File, char *Path, void *zmq_publisher) {
	char *Buffer;
	long BufferSize;
	ssize_t BytesRead;
	long Chunk;
	NFS4_ACL_t NFS4_ACL;

	// Send file name and attributes
	BufferSize = strlen(Path)+1;
	if ( (Buffer = malloc(BufferSize)) == NULL ) {
		perror("ERROR: Cannot allocate memory for 1st message part: ");
		exit(9);
	}
	strcpy(Buffer, Path);
	// Add attributes here. Adapt malloc as needed !
	WriteMessageChunk(Buffer, BufferSize, zmq_publisher);
	free(Buffer); BufferSize = 0;

	// Send ACL  ..
	Get_NFS4_ACL_from_GPFS(Path, &NFS4_ACL);
	BufferSize = sizeof(NFS4_ACL_t);
	fprintf(stdout,"DEBUG: ACL = %d, m.c = <%s>, bs=%ld\n",NFS4_ACL.i,NFS4_ACL.c,BufferSize);
	WriteMessageChunk(NFS4_ACL.c, BufferSize, zmq_publisher);
	BufferSize = 0;

	// Send file content.
	BufferSize = ZMQPartialMessages.Chunksize;
	if ( (Buffer = malloc(BufferSize)) == NULL ) {
		fprintf(stderr,"ERROR: Cannot allocate read buffer for file %s.\n",Path);
		perror(" ");
		exit(9);
	}

	for (Chunk = 1 ; Chunk <= ZMQPartialMessages.nChunks; Chunk++ ) {
		BytesRead = read(File, Buffer, BufferSize);
//		fprintf(stdout,"Read %d of %ld bytes. Chunk %ld of %ld.\n",BytesRead,BufferSize,Chunk,ZMQPartialMessages.nChunks);fflush(stdout);
		if ( BytesRead < 0 ) {
			fprintf(stderr,"ERROR: read failed for file %s.\n",Path);
			perror(" ");
			exit(9);
		}
		if ( Chunk < ZMQPartialMessages.nChunks ) {
			WriteMessageChunk(Buffer, BufferSize, zmq_publisher);
		} else {
			WriteWholeMessage(Buffer, BytesRead, zmq_publisher);
		}
	}

	free(Buffer);
}

void SetArgumentsFromCommandLine (int argc, char *argv[]) {
	static int Help, help;

	Help = 0; help = 0;	// Better way ??
	while (1) {
		static struct option long_options[] =
		{
				{"DryRun",				no_argument, &Arguments.DryRun, 1},
				{"Help", 	  			no_argument, &Help,1},
				{"help", 	  			no_argument, &help,1},
				{"PartialMessageSize",	required_argument, 0, 'a'},
				{"WaitBeforeRetry",		required_argument, 0, 'b'},
				{"ZMQHighWaterMark",	required_argument, 0, 'c'},
				{0,0,0,0}
		};
		int option_index = 0;
		int c;
		c = getopt_long(argc, argv, "a:b:c:", long_options, &option_index);
		if ( c == -1 ) {
			break;
		}
		switch (c) {
			case 0:		// Flag type option
				if ( long_options[option_index].flag ) {
					if (! strncmp(long_options[option_index].name,"Help",4) ) {
						// If help requested ..
						Usage();
					}
					if (! strncmp(long_options[option_index].name,"help",4) ) {
						// If help requested ..
						Usage();
					}
					break;
				}
			break;

			case 'a':	// Key for Partial Message Size
				Arguments.PartialMessageSize = Atol(optarg);
				if ( strstr(optarg,"k") != NULL ) { Arguments.PartialMessageSize *= 1024;}
				if ( strstr(optarg,"m") != NULL ) { Arguments.PartialMessageSize *= 1024*1024;}
			break;

			case 'b':	// Key for message retry. ..
				Arguments.WaitBeforeRetry = Atol(optarg);
			break;

			case 'c':	// Key for ZMQ high water
				Arguments.ZMQ_High_Water_Mark = Atol(optarg);
			break;

			case '?':
				fprintf(stderr,"ERROR: Unknown option encountered.\n");
				exit(9);
			break;
		}
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
	fprintf(stdout,"%s V %s:\n",PROGNAME, VERSION);
	fprintf(stdout,"usage: %s \\\n",PROGNAME);
	fprintf(stdout,"\t--DryRun\n");
	fprintf(stdout,"\t--PartialMessageSize <size>[km] Write partial msgs of size <size>.\n");
	fprintf(stdout,"\t--WaitBeforeRetry <seconds to wait before reseinding message>\n");
	fprintf(stdout,"\t--ZMQ_High_water_Mark <nr messages>.\n");
	exit(0);
}


void WriteMessageChunk(char* Buffer, long Size, void* zmq_publisher) {
	int Retries = MAX_RETRIES;
	while ( Retries ) {
		Retries--;
		if ( zmq_send(zmq_publisher,Buffer, Size, ZMQ_SNDMORE) == -1 ) {
			perror("ERROR: Cannot send 0MQ message: ");
			if ( Retries ) {
				fprintf(stderr,"%s: INFO: Retrying message nr. %jd. Retry Count: %d.\n",TimeStamp(),Statistics.FilesProcessed+1,Retries);
				sleep(Arguments.WaitBeforeRetry);
				fprintf(stderr,"%s: INFO: Continuing.\n",TimeStamp());
			} else {
				fprintf(stderr,"%s: Retry count exceeded. Aborting.\n",TimeStamp());
				fprintf(stderr,"Images sent: %jd\n",Statistics.FilesProcessed);
				exit(9);
			}
		}
		else {
			Retries = 0;
			Statistics.BytesWritten += Size;
		}
	}
}


void WriteWholeMessage (char* Buffer, size_t Size, void* zmq_publisher) {
	int Retries = MAX_RETRIES;

	while ( Retries ) {
		Retries--;
		if ( zmq_send(zmq_publisher,Buffer, Size, ZMQ_DONTWAIT) == -1 ) {
			perror("ERROR: Cannot send 0MQ message: ");
			if ( Retries ) {
				fprintf(stderr,"%s: INFO: Retrying message nr. %jd. Retry Count: %d.\n",TimeStamp(),Statistics.FilesProcessed+1,Retries);
				sleep(Arguments.WaitBeforeRetry);
				fprintf(stderr,"%s: INFO: Continuing.\n",TimeStamp());
			} else {
				fprintf(stderr,"%s: Retry count exceeded. Aborting.\n",TimeStamp());
				fprintf(stderr,"Images sent: %jd\n",Statistics.FilesProcessed);
				exit(9);
			}
		}
		else {
			Retries = 0;
			Statistics.BytesWritten += Size;
		}
	}

}

int main(int argc, char *argv[]) {
	char *InputLine;
	size_t LineLength;

	double Thruput, Runtime, MBytesProcessed;
	struct tms Clock;

	void *zmq_context, *zmq_publisher;

	// 	*** Initializations.
	srand(time(NULL));

	// Parse command arguments.
	Arguments.DryRun				= 0;
	Arguments.WaitBeforeRetry		= 1;		// 10 seconds.
	Arguments.PartialMessageSize	= 04*1024;	// 4k..
	Arguments.ZMQ_High_Water_Mark	= 0;

	SetArgumentsFromCommandLine(argc,argv);

	fprintf(stdout,"%s: %s V %s argument summary:\n",TimeStamp(),PROGNAME,VERSION);

	// Set up 0MQ
	fprintf(stdout,"ZMQ high water mark\t%ld\n",Arguments.ZMQ_High_Water_Mark);
	if ( (zmq_context = zmq_ctx_new ()) == NULL ) {
		perror("ERROR: Cannot create 0MQ context: ");
		exit(9);
	}
	if ( (zmq_publisher = zmq_socket (zmq_context, ZMQ_PUSH)) == NULL ) {
		perror("ERROR: Could not create 0MQ socket: ");
		exit(9);
	}
// Start workaround for SL 6
	int hwm = Arguments.ZMQ_High_Water_Mark;
	if ( (zmq_setsockopt(zmq_publisher,ZMQ_SNDHWM, &hwm, sizeof(hwm)) ) ) {
// End workaround.. Delete above 2 lines and uncomment following one to get rid of this..
//		if ( (zmq_setsockopt(zmq_publisher,ZMQ_SNDHWM, &Arguments.ZMQ_High_Water_Mark, sizeof(long)) ) ) {
		perror("ERROR: Cannot set ZMQ socket options: ");
		exit(9);
	}
	if ( zmq_bind (zmq_publisher, "tcp://*:5556") ) {
		perror("ERROR: Could not bind to 0MQ tcp socket:");
		exit(9);
	}

	fprintf(stdout,"Wait before resend\t%d\n",Arguments.WaitBeforeRetry);
	fprintf(stdout,"Partial message size\t%dB\n",Arguments.PartialMessageSize);
	if ( Arguments.DryRun ) {
		fprintf(stdout,"\n\t*** Dry run requested. NO read/write operations are done. ***\n\n");
	}

	fprintf(stdout,"Waiting for receivers to get ready..");	fflush(stdout);
	sleep(1);
	fprintf(stdout," Running..\n");fflush(stdout);

	// Initialize statistics
	Statistics.BytesRead 		= 0;
	Statistics.BytesWritten 	= 0;
	Statistics.FilesProcessed 	= 0;
	Statistics.StartTime 		= GetMTime();
	Statistics.RunTime			= 0.0;
	Statistics.SleepTime		= 0.0;

	InputLine = NULL; LineLength = 0;
	while ( getline(&InputLine,&LineLength,stdin) > 0 ) {
		int InFile;
		char *Path;

//		fprintf(stdout,"Got <%s>\n",InputLine);
		Path = strtok(InputLine," \n");

		InFile = OpenFile(Path);
		SendFile(InFile, Path, zmq_publisher);

		close(InFile);
//		fprintf(stdout,"Closing %s\n",Path);
		free(InputLine); // Implicitely frees Path, too ..
		InputLine = NULL; LineLength = 0; // Reset getline..
		Statistics.FilesProcessed++;
	}

	Statistics.EndTime = GetMTime();

	int i;
	for ( i = 0 ; i < 50; i++ ) {
		if ( zmq_send(zmq_publisher,NULL, 0, ZMQ_DONTWAIT) < 0 ) {
			perror("ERROR: Cannot send final 0MQ message: ");
			sleep(Arguments.WaitBeforeRetry);
		}
	}

	zmq_close (zmq_publisher);
	zmq_ctx_destroy (zmq_context);

	// Print statistics.
	Runtime = Statistics.EndTime - Statistics.StartTime;
	Thruput = ((double)Statistics.BytesWritten/(1024.0*1024.0)) / Runtime;
	fprintf(stdout,"\n %" PRIi64 " Files processed in %f seconds (%.2f Hz).\n",
			Statistics.FilesProcessed,
			Statistics.EndTime-Statistics.StartTime,
			(double)Statistics.FilesProcessed/(Statistics.EndTime-Statistics.StartTime)
			);
	fprintf(stdout,"Average thruput \t:\t%14.4f MB/s\n",Thruput);
	MBytesProcessed = (double)Statistics.BytesWritten/(1024.0*1024.0);
	fprintf(stdout,"Bytes written\t\t:\t%14.4f MB\n",MBytesProcessed);
	fprintf(stdout,"Files processed\t\t:\t%14.4f\n",(double)Statistics.FilesProcessed);
	fprintf(stdout,"Avg file size\t\t:\t%14.4f MB\n",MBytesProcessed/(double)Statistics.FilesProcessed);

	fprintf(stdout,"Seconds spent sleeping  :\t%14.4f s (%.2f%% of run)\n",
			Statistics.SleepTime,
			100.0*Statistics.SleepTime/Runtime);

	if ( times(&Clock) == (clock_t)(-1) ) {
		fprintf(stderr,"WARNING: Could not get processor time (times call): %d\n",errno);
	} else {
		long ClockTicks;
		double CPUtime;
		ClockTicks = sysconf(_SC_CLK_TCK);
		CPUtime = ((double)Clock.tms_utime + (double)Clock.tms_stime) / (double)ClockTicks;
		fprintf(stdout,"CPU time used\t\t:\t%14.4f s (%.2f%% of total)\n",CPUtime, 100.0*CPUtime/Runtime);
	}

	fprintf(stdout,"\n%s: %s V %s complete.\n",TimeStamp(),PROGNAME,VERSION);
	return EXIT_SUCCESS;
}
