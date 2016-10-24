from cx_Freeze import setup, Executable
import zmq
from distutils.sysconfig import get_python_lib
import os

#libzmq_path = os.path.join(get_python_lib(), "zmq", "libzmq.so")
libzmq_path = "/usr/local/lib/python2.7/dist-packages/zmq"


basepath = "/opt/hidra"
senderpath = os.path.join(basepath, "src", "sender")
sharedpath = os.path.join(basepath, "src", "shared")
confpath = os.path.join(basepath, "conf")

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
        os.path.join(senderpath, "SignalHandler.py"),
        os.path.join(senderpath, "TaskProvider.py"),
        os.path.join(senderpath, "DataDispatcher.py"),
        os.path.join(senderpath, "eventDetectors", "InotifyxDetector.py"),
        os.path.join(senderpath, "eventDetectors", "WatchdogDetector.py"),
        os.path.join(senderpath, "eventDetectors", "HttpDetector.py"),
        os.path.join(senderpath, "eventDetectors", "ZmqDetector.py"),
        os.path.join(senderpath, "dataFetchers", "getFromFile.py"),
        os.path.join(senderpath, "dataFetchers", "getFromHttp.py"),
        os.path.join(senderpath, "dataFetchers", "getFromZmq.py"),
        os.path.join(senderpath, "dataFetchers", "send_helpers.py"),
        os.path.join(sharedpath, "logutils") + "/",
        os.path.join(sharedpath, "helpers.py"),
        os.path.join(sharedpath, "version.py"),
        confpath + "/",
        ],
}

executables = [
    Executable(os.path.join(senderpath, "DataManager.py"))
]

copyDependentFiles=True

setup(name='HiDRA',
      version = '0.0.1',
      description = '',
      options = {"build_exe" : buildOptions},
      executables = executables
      )
