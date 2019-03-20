# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

# For Linux:
# Tested with cx_Freeze 4.3.4 after fixing the this installation bug:
# http://stackoverflow.com/questions/25107697/compiling-cx-freeze-under-ubuntu
#
# For Windows:
# Tested with cx_Freeze 4.3.3 installed with pip


"""
This module freezes hidra to be able to run in on systems without installing
the dependencies.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# from distutils.sysconfig import get_python_lib
import os
import sys
import platform
import zmq

from cx_Freeze import setup, Executable

BASEPATH = os.path.dirname(os.path.abspath(__file__))
SENDERPATH = os.path.join(BASEPATH, "src", "sender")
APIPATH = os.path.join(BASEPATH, "src", "APIs", "hidra")
UTILSPATH = os.path.join(APIPATH, "utils")
CONFPATH = os.path.join(BASEPATH, "conf")


def get_zmq_path():
    """Find the correct zmq path
    """

    path = os.path.dirname(zmq.__file__)
#    path = os.path.join(get_python_lib(), "zmq")
#    LIBZMQ_PATH = "C:\Python27\Lib\site-packages\zmq"
#    LIBZMQ_PATH = "/usr/local/lib/python2.7/dist-packages/zmq"

    return path


def windows_specific():
    """Set Windows specific packages and config
    """
    packages = ["watchdog"]

    files = [
        # config
        (os.path.join(CONFPATH, "datamanager_windows.conf"),
         os.path.join("conf", "datamanager.conf"))
    ]

    return packages, files


def linux_specific():
    """Set Linux specific packages and config
    """

    packages = ["inotifyx"]

    files = [
        # config
        (os.path.join(CONFPATH, "datamanager_pilatus.conf"),
         os.path.join("conf", "datamanager.conf"))
    ]

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
                "setproctitle.so"
            )
        )

        if not os.path.exists(setproctitle_egg_path):
            import setproctitle
            setproctitle_egg_path = setproctitle.__file__

        files += [(setproctitle_egg_path, "setproctitle.so")]

    return packages, files


def get_init():
    """Reuse the init file for installed HiDRA to reduce amount of maintenance
    """

    initscript = os.path.join(BASEPATH, "initscripts", "hidra.sh")
    exescript = os.path.join(BASEPATH, "initscripts", "hidra_exe.sh")
    with open(initscript, "r") as f:
        with open(exescript, "w") as f_exe:
            for line in f:
                if line == "USE_EXE=false\n":
                    f_exe.write("USE_EXE=true\n")
                else:
                    f_exe.write(line)
    os.chmod(exescript, 0o755)

    return exescript


def get_environment():
    """Adapts environment to executables.

    BASE_DIR is different for executables because directories are ordered
    differently.
    """

    env = os.path.join(SENDERPATH, "_environment.py")
    exe_env = os.path.join(SENDERPATH, "_environment_exe.py")
    with open(env, "r") as f:
        with open(exe_env, "w") as f_exe:
            for line in f:
                ref_line = (
                    "BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))"
                    "\n"
                )
                if line == ref_line:
                    f_exe.write("BASE_DIR = CURRENT_DIR\n")
                else:
                    f_exe.write(line)

    return exe_env


if platform.system() == "Windows":
    PLATFORM_SPECIFIC_PACKAGES, PLATFORM_SPECIFIC_FILES = windows_specific()
else:
    PLATFORM_SPECIFIC_PACKAGES, PLATFORM_SPECIFIC_FILES = linux_specific()

# Some packages differ in Python 3
# TODO windows compatible?
if sys.version_info >= (3, 0):
    VERSION_SPECIFIC_PACKAGES = ["configparser"]
else:
    VERSION_SPECIFIC_PACKAGES = ["ConfigParser"]

# Dependencies are automatically detected, but it might need fine tuning.
BUILD_EXE_OPTIONS = {
    "packages": (
        [
            "ast",
            "logging.handlers",
            "logutils",
            "setproctitle",
            "six",
            "zmq",
            # zmq.backend.cython seems to be left out by default
            "zmq.backend.cython",
        ]
        + VERSION_SPECIFIC_PACKAGES
        + PLATFORM_SPECIFIC_PACKAGES
    ),
    # libzmq.pyd is a vital dependency
    # "include_files": [zmq.libzmq.__file__, ],
    "include_files": [
        (get_zmq_path(), "zmq"),
        (os.path.join(BASEPATH, "logs/.gitignore"),
         os.path.join("logs", ".gitignore")),
        (get_init(), "hidra.sh"),
        (os.path.join(CONFPATH, "base_sender.conf"),
         os.path.join("conf", "base_sender.conf")),
        (os.path.join(SENDERPATH, "__init__.py"), "__init__.py"),
        (get_environment(), "_environment.py"),
        (os.path.join(SENDERPATH, "base_class.py"), "base_class.py"),
        (os.path.join(SENDERPATH, "taskprovider.py"), "taskprovider.py"),
        (os.path.join(SENDERPATH, "signalhandler.py"), "signalhandler.py"),
        (os.path.join(SENDERPATH, "datadispatcher.py"), "datadispatcher.py"),
        # only for readability (not for actual code)
        (os.path.join(UTILSPATH, "_version.py"), "_version.py"),
        # event detectors
        (os.path.join(SENDERPATH, "eventdetectors", "eventdetectorbase.py"),
         os.path.join("eventdetectors", "eventdetectorbase.py")),
        (os.path.join(SENDERPATH, "eventdetectors", "inotifyx_events.py"),
         os.path.join("eventdetectors", "inotifyx_events.py")),
        (os.path.join(SENDERPATH, "eventdetectors", "watchdog_events.py"),
         os.path.join("eventdetectors", "watchdog_events.py")),
        (os.path.join(SENDERPATH, "eventdetectors", "http_events.py"),
         os.path.join("eventdetectors", "http_events.py")),
        (os.path.join(SENDERPATH, "eventdetectors", "zmq_events.py"),
         os.path.join("eventdetectors", "zmq_events.py")),
        # data fetchers
        (os.path.join(SENDERPATH, "datafetchers", "datafetcherbase.py"),
         os.path.join("datafetchers", "datafetcherbase.py")),
        (os.path.join(SENDERPATH, "datafetchers", "file_fetcher.py"),
         os.path.join("datafetchers", "file_fetcher.py")),
        (os.path.join(SENDERPATH, "datafetchers", "http_fetcher.py"),
         os.path.join("datafetchers", "http_fetcher.py")),
        (os.path.join(SENDERPATH, "datafetchers", "zmq_fetcher.py"),
         os.path.join("datafetchers", "zmq_fetcher.py")),
        (os.path.join(SENDERPATH, "datafetchers", "cleanerbase.py"),
         os.path.join("datafetchers", "cleanerbase.py")),
        # apis
        (APIPATH, "hidra"),
    ] + PLATFORM_SPECIFIC_FILES,
}

BDIS_MSI_OPTIONS = {
    # the upgrade code for the package that is created;
    # this is used to force removal of any packages created with the same
    # upgrade code prior to the installation of this one
    "upgrade_code": "{3bce61b3-96da-42af-99e7-a080130539aa}"
}

EXECUTABLES = [
    Executable(os.path.join(SENDERPATH, "datamanager.py")),
    Executable(os.path.join(UTILSPATH, "getsettings.py")),
    Executable(os.path.join(UTILSPATH, "get_receiver_status.py"))
]

setup(name='HiDRA',
      version='4.0.19',
      description='',
      options={"build_exe": BUILD_EXE_OPTIONS,
               "bdist_msi": BDIS_MSI_OPTIONS},
      executables=EXECUTABLES)
