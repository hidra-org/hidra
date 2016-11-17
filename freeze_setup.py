# For Linux:
# Tested with cx_Freeze 4.3.4 after fixing the this installation bug:
# http://stackoverflow.com/questions/25107697/compiling-cx-freeze-under-ubuntu
# For Windows:
# Tested with cx_Freeze 4.3.3 installed with pip

from cx_Freeze import setup, Executable
import zmq
from distutils.sysconfig import get_python_lib
import os
import platform

basepath = os.path.dirname(os.path.abspath(__file__))

if platform.system() == "Windows":
    libzmq_path = "C:\Python27\Lib\site-packages\zmq"
#    basepath = "D:\hidra"
    senderpath = os.path.join(basepath, "src", "sender")

    platform_specific_packages = ["watchdog"]
    platform_specific_files = [
        (os.path.join(senderpath, "eventDetectors", "InotifyxDetector.py"),
            os.path.join("eventDetectors", "InotifyxDetector.py")),
        (os.path.join(senderpath, "eventDetectors", "WatchdogDetector.py"),
            os.path.join("eventDetectors", "WatchdogDetector.py")),
        (os.path.join(senderpath, "eventDetectors", "HttpDetector.py"),
            os.path.join("eventDetectors", "HttpDetector.py")),
        (os.path.join(senderpath, "eventDetectors", "ZmqDetector.py"),
            os.path.join("eventDetectors", "ZmqDetector.py")),
        (os.path.join(senderpath, "dataFetchers", "getFromFile.py"),
            os.path.join("dataFetchers", "getFromFile.py")),
        (os.path.join(senderpath, "dataFetchers", "getFromHttp.py"),
            os.path.join("dataFetchers", "getFromHttp.py")),
        (os.path.join(senderpath, "dataFetchers", "getFromZmq.py"),
            os.path.join("dataFetchers", "getFromZmq.py")),
        (os.path.join(senderpath, "dataFetchers", "send_helpers.py"),
            os.path.join("dataFetchers", "send_helpers.py"))
        ]

else:
    libzmq_path = "/usr/local/lib/python2.7/dist-packages/zmq"
#    basepath = "/opt/hidra"
    senderpath = os.path.join(basepath, "src", "sender")

    platform_specific_packages = ["inotifyx"]
    platform_specific_files = [
        (os.path.join(senderpath, "eventDetectors", "InotifyxDetector.py"),
            "InotifyxDetector.py"),
        (os.path.join(senderpath, "eventDetectors", "WatchdogDetector.py"),
            "WatchdogDetector.py"),
        (os.path.join(senderpath, "eventDetectors", "HttpDetector.py"),
            "HttpDetector.py"),
        (os.path.join(senderpath, "eventDetectors", "ZmqDetector.py"),
            "ZmqDetector.py"),
        (os.path.join(senderpath, "dataFetchers", "getFromFile.py"),
            "getFromFile.py"),
        (os.path.join(senderpath, "dataFetchers", "getFromHttp.py"),
            "getFromHttp.py"),
        (os.path.join(senderpath, "dataFetchers", "getFromZmq.py"),
            "getFromZmq.py"),
        (os.path.join(senderpath, "dataFetchers", "send_helpers.py"),
            "send_helpers.py")
        ]

    dist = platform.dist()
    if dist[0].lower() == "suse" and dist[1].startswith("10"):
        setproctitle_egg_path = os.path.join(os.path.expanduser("~"), ".cache/Python-Eggs/setproctitle-1.1.10-py2.7-linux-i686.egg-tmp/setproctitle.so")
        platform_specific_files += [(setproctitle_egg_path, "setproctitle.so")]



sharedpath = os.path.join(basepath, "src", "shared")
confpath = os.path.join(basepath, "conf")
#libzmq_path = os.path.join(get_python_lib(), "zmq")

# Dependencies are automatically detected, but it might need fine tuning.
buildOptions = {
    # zmq.backend.cython seems to be left out by default
    "packages": ["zmq", "zmq.backend.cython", "logging.handlers", "setproctitle"] + platform_specific_packages,
    # libzmq.pyd is a vital dependency
#    "include_files": [zmq.libzmq.__file__, ],
    "include_files": [
        (libzmq_path, "zmq"),
        (os.path.join(senderpath, "SignalHandler.py"), "SignalHandler.py"),
        (os.path.join(senderpath, "TaskProvider.py"), "TaskProvider.py"),
        (os.path.join(senderpath, "DataDispatcher.py"), "DataDispatcher.py"),
        (os.path.join(sharedpath, "logutils"), "logutils"),
        (os.path.join(sharedpath, "helpers.py"), "helpers.py"),
        (os.path.join(sharedpath, "version.py"), "version.py"),
        (confpath, "conf"),
        ] + platform_specific_files,
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
