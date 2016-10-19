from cx_Freeze import setup, Executable
import zmq
from distutils.sysconfig import get_python_lib
import os

#libzmq_path = os.path.join(get_python_lib(), "zmq", "libzmq.so")
libzmq_path = "/usr/local/lib/python2.7/dist-packages/zmq"


basepath = "/opt/HiDRA"
senderpath = "{0}/src/sender".format(basepath)
sharedpath = "{0}/src/shared".format(basepath)
confpath = "{0}/conf".format(basepath)

# Dependencies are automatically detected, but it might need
# fine tuning.
buildOptions = {
    # zmq.backend.cython seems to be left out by default
    "packages": ["zmq", "zmq.backend.cython", "logging.handlers", "inotifyx", "setproctitle"],
    # libzmq.pyd is a vital dependency
#    "include_files": [zmq.libzmq.__file__, ],
    "include_files": [
#        libzmq_lib,
        libzmq_path,
        "{0}/SignalHandler.py".format(senderpath),
        "{0}/TaskProvider.py".format(senderpath),
        "{0}/DataDispatcher.py".format(senderpath),
        "{0}/eventDetectors/InotifyxDetector.py".format(senderpath),
        "{0}/eventDetectors/WatchdogDetector.py".format(senderpath),
        "{0}/eventDetectors/HttpDetector.py".format(senderpath),
        "{0}/eventDetectors/ZmqDetector.py".format(senderpath),
        "{0}/dataFetchers/getFromFile.py".format(senderpath),
        "{0}/dataFetchers/getFromHttp.py".format(senderpath),
        "{0}/dataFetchers/getFromZmq.py".format(senderpath),
        "{0}/dataFetchers/send_helpers.py".format(senderpath),
        "{0}/logutils/".format(sharedpath),
        "{0}/helpers.py".format(sharedpath),
        "{0}/version.py".format(sharedpath),
        "{0}/".format(confpath),
        ],
}

executables = [
    Executable("{0}/DataManager.py".format(senderpath))
]

copyDependentFiles=True

setup(name='HiDRA',
      version = '0.0.1',
      description = '',
      options = {"build_exe" : buildOptions},
      executables = executables
      )
