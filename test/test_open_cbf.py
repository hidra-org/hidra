import logging
#import fabio
#import fabio.cbfimage
#from fabio.cbfimage import cbfimage

def initLogging(filenameFullPath, verbose):
    #@see https://docs.python.org/2/howto/logging-cookbook.html

    #more detailed logging if verbose-option has been set
    loggingLevel = logging.INFO
    if verbose:
        loggingLevel = logging.DEBUG

    #log everything to file
    logging.basicConfig(level=loggingLevel,
                        format='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d_%H:%M:%S',
                        filename=filenameFullPath,
                        filemode="a")

    #log info to stdout, display messages with different format than the file output
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter("%(asctime)s >  %(message)s")
    console.setFormatter(formatter)

    logging.getLogger("").addHandler(console)



#enable logging
initLogging("/space/projects/live-viewer/test/cbf_test.log", True)
logging.info("Test")

#path = "/space/projects/live-viewer/jan_015_00001.cbf"
#fileFormat           = path.rsplit(".", 2)[1]
#print fileFormat

#cbf_file = fabio.cbfimage.cbfimage()
##cbf_file = fabio.cbfimage.cbfimage(fname=path)
#
#cbf_file.read(path)
#
#dict_to_send = cbf_file.header
#dict_to_send[u"data"] = cbf_file.data
#print dict_to_send

