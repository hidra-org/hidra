# For Linux:
# Tested with cx_Freeze 4.3.4 after fixing the this installation bug:
# http://stackoverflow.com/questions/25107697/compiling-cx-freeze-under-ubuntu
# For Windows:
# Tested with cx_Freeze 4.3.3 installed with pip

from cx_Freeze import setup, Executable
# import zmq
from distutils.sysconfig import get_python_lib
import os
import sys
import platform

basepath = os.path.dirname(os.path.abspath(__file__))
senderpath = os.path.join(basepath, "src", "sender")
sharedpath = os.path.join(basepath, "src", "shared")
confpath = os.path.join(basepath, "conf")
libzmq_path = os.path.join(get_python_lib(), "zmq")
platform_specific_files = []

if platform.system() == "Windows":
#    libzmq_path = "C:\Python27\Lib\site-packages\zmq"
    platform_specific_packages = ["watchdog"]

else:
#    libzmq_path = "/usr/local/lib/python2.7/dist-packages/zmq"
    platform_specific_packages = ["inotifyx"]

    # Workaround for including setproctitle when building on SuSE 10
    dist = platform.dist()
    if dist[0].lower() == "suse" and dist[1].startswith("10"):
        architecture_type = platform.architecture()[0]
        if architecture_type == "64bit":
            archi_t = "x86_64"
        else:
            archi_t = "i686"
        setproctitle_egg_path = (
            os.path.join(
                os.path.expanduser("~"),
                ".cache/Python-Eggs/"
                "setproctitle-1.1.10-py2.7-linux-" + archi_t + ".egg-tmp/"
                "setproctitle.so"))
        platform_specific_files += [(setproctitle_egg_path, "setproctitle.so")]

# Some packages differ in Python 3
# TODO windows compatible?
if sys.version_info >= (3,0):
    version_specific_packages = ["configparser"]
else:
    version_specific_packages = ["ConfigParser"]

# Dependencies are automatically detected, but it might need fine tuning.
buildOptions = {
    # zmq.backend.cython seems to be left out by default
    "packages": (["zmq", "zmq.backend.cython",
                  "logging.handlers",
                  "setproctitle",
                  "six",
                  "ast"]
                 + version_specific_packages
                 + platform_specific_packages),
    # libzmq.pyd is a vital dependency
    # "include_files": [zmq.libzmq.__file__, ],
    "include_files": [
        (libzmq_path, "zmq"),
        (os.path.join(basepath, "initscripts", "hidra.sh"), "hidra.sh"),
        (os.path.join(senderpath, "__init__.py"), "__init__.py"),
        (os.path.join(senderpath, "taskprovider.py"), "taskprovider.py"),
        (os.path.join(senderpath, "signalhandler.py"), "signalhandler.py"),
        (os.path.join(senderpath, "datadispatcher.py"), "datadispatcher.py"),
        (os.path.join(sharedpath, "logutils"), "logutils"),
        (os.path.join(sharedpath, "helpers.py"), "helpers.py"),
        (os.path.join(sharedpath, "cfel_optarg.py"), "cfel_optarg.py"),
        (os.path.join(sharedpath, "_version.py"), "_version.py"),
        # config
        (os.path.join(confpath, "datamanager_pilatus.conf"),
            os.path.join("conf", "datamanager.conf")),
        # event detectors
        (os.path.join(senderpath, "eventdetectors", "inotifyx_events.py"),
            os.path.join("eventdetectors", "inotifyx_events.py")),
        (os.path.join(senderpath, "eventdetectors", "watchdog_events.py"),
            os.path.join("eventdetectors", "watchdog_events.py")),
        (os.path.join(senderpath, "eventdetectors", "http_events.py"),
            os.path.join("eventdetectors", "http_events.py")),
        (os.path.join(senderpath, "eventdetectors", "zmq_events.py"),
            os.path.join("eventdetectors", "zmq_events.py")),
        # data fetchers
        (os.path.join(senderpath, "datafetchers", "file_fetcher.py"),
            os.path.join("datafetchers", "file_fetcher.py")),
        (os.path.join(senderpath, "datafetchers", "http_fetcher.py"),
            os.path.join("datafetchers", "http_fetcher.py")),
        (os.path.join(senderpath, "datafetchers", "zmq_fetcher.py"),
            os.path.join("datafetchers", "zmq_fetcher.py")),
        (os.path.join(senderpath, "datafetchers", "send_helpers.py"),
            os.path.join("datafetchers", "send_helpers.py"))
    ] + platform_specific_files,
}

executables = [
    Executable(os.path.join(senderpath, "datamanager.py")),
    Executable(os.path.join(sharedpath, "getsettings.py"))
]

setup(name='HiDRA',
      version='3.0.2',
      description='',
      options={"build_exe": buildOptions},
      executables=executables
)
