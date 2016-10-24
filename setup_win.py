from cx_Freeze import setup, Executable
import zmq
from distutils.sysconfig import get_python_lib
import os

#libzmq_path = os.path.join(get_python_lib(), "zmq", "libzmq.so")
libzmq_path = "C:\Python27\Lib\site-packages\zmq"


basepath = "D:\hidra"
senderpath = os.path.join(basepath, "src", "sender")
sharedpath = os.path.join(basepath, "src", "shared")
confpath = os.path.join(basepath, "conf")

# Dependencies are automatically detected, but it might need
# fine tuning.
buildOptions = {
    # zmq.backend.cython seems to be left out by default
    "packages": ["zmq", "zmq.backend.cython", "logging.handlers", "watchdog", "setproctitle"],
    # libzmq.pyd is a vital dependency
#    "include_files": [zmq.libzmq.__file__, ],
    "include_files": [
#        libzmq_lib,
        (libzmq_path, "zmq"),
        (os.path.join(senderpath, "SignalHandler.py"), "SignalHandler.py"),
        (os.path.join(senderpath, "TaskProvider.py"), "TaskProvider.py"),
        (os.path.join(senderpath, "DataDispatcher.py"), "DataDispatcher.py"),
        (os.path.join(senderpath, "eventDetectors", "InotifyxDetector.py"), os.path.join("eventDetectors", "InotifyxDetector.py")),
        (os.path.join(senderpath, "eventDetectors", "WatchdogDetector.py"), os.path.join("eventDetectors", "WatchdogDetector.py")),
        (os.path.join(senderpath, "eventDetectors", "HttpDetector.py"), os.path.join("eventDetectors", "HttpDetector.py")),
        (os.path.join(senderpath, "eventDetectors", "ZmqDetector.py"), os.path.join("eventDetectors", "ZmqDetector.py")),
        (os.path.join(senderpath, "dataFetchers", "getFromFile.py"), os.path.join("dataFetchers", "getFromFile.py")),
        (os.path.join(senderpath, "dataFetchers", "getFromHttp.py"), os.path.join("dataFetchers", "getFromHttp.py")),
        (os.path.join(senderpath, "dataFetchers", "getFromZmq.py"), os.path.join("dataFetchers", "getFromZmq.py")),
        (os.path.join(senderpath, "dataFetchers", "send_helpers.py"), os.path.join("dataFetchers", "send_helpers.py")),
        (os.path.join(sharedpath, "logutils"), "logutils"),
        (os.path.join(sharedpath, "helpers.py"), "helpers.py"),
        (os.path.join(sharedpath, "version.py"), "version.py"),
        (confpath, "conf"),
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
