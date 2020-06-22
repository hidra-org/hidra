# Copyright (C) 2015 DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
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
#
# Important: dependencies have to be installed with pip and not setup.py
#            because e.g. installing setproctitle will result in an
#            .egg-file which cx_Freeze cannot handle.

"""
This module freezes hidra to be able to run in on systems without installing
the dependencies.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from distutils.sysconfig import get_python_lib
import os
import sys
import platform
import zmq

from cx_Freeze import setup, Executable

BASEPATH = os.path.dirname(os.path.abspath(__file__))
SENDERPATH = os.path.join(BASEPATH, "src", "hidra", "sender")
APIPATH = os.path.join(BASEPATH, "src", "api", "python", "hidra")
UTILSPATH = os.path.join(APIPATH, "utils")
CONFPATH = os.path.join(BASEPATH, "conf")
PYTHON_PATH = os.path.dirname(get_python_lib())


def get_zmq_path():
    """Find the correct zmq path
    """

    path = os.path.dirname(zmq.__file__)
#    path = os.path.join(get_python_lib(), "zmq")
#    LIBZMQ_PATH = "C:\Python27\Lib\site-packages\zmq"
#    LIBZMQ_PATH = "/usr/local/lib/python2.7/dist-packages/zmq"

#    libzmq.pyd is a vital dependency
#    "include_files": [zmq.libzmq.__file__, ],

    return path


def windows_specific():
    """Set Windows specific packages and config
    """
    packages = ["watchdog"]

    files = [
        # config
        (os.path.join(CONFPATH, "datamanager_windows.yaml"),
         os.path.join("conf", "datamanager.yaml"))
    ]

    return packages, files


def linux_specific():
    """Set Linux specific packages and config
    """

    packages = ["inotifyx"]

    files = [
        # config
        (os.path.join(CONFPATH, "datamanager_pilatus.yaml"),
         os.path.join("conf", "datamanager.yaml"))
    ]

    return packages, files


def get_init():
    """Reuse the init file for installed HiDRA to reduce amount of maintenance
    """

    script_dir = os.path.join(BASEPATH, "scripts", "init_scripts")
    initscript = os.path.join(script_dir, "hidra.sh")
    exescript = os.path.join(script_dir, "hidra_exe.sh")
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
                if line.startswith("BASE_DIR = os.path.dirname"):
                    f_exe.write("BASE_DIR = CURRENT_DIR\n")
                elif line.startswith("API_DIR = os.path.join"):
                    f_exe.write("API_DIR = BASE_DIR\n")
                else:
                    f_exe.write(line)

    return exe_env


if platform.system() == "Windows":
    PLATFORM_SPECIFIC_PACKAGES, PLATFORM_SPECIFIC_FILES = windows_specific()
else:
    PLATFORM_SPECIFIC_PACKAGES, PLATFORM_SPECIFIC_FILES = linux_specific()

# Some packages differ in Python 3
# TODO windows compatible?
if sys.version_info.major >= 3:
    VERSION_SPECIFIC_PACKAGES = [
        # "future",  # building with python 3.5 does not include this
        # otherwise zmq.auth.thread cannot be found:
        # ImportError: No module named zmq.auth.thread
        # but if the whole zmq module is added asyncio is missed
        # ImportError: No module named 'zmq.auth.asyncio'
        "zmq.auth.thread",
        # otherwise yaml cannot be found:
        # ModuleNotFoundError: No module named 'yaml'
        "yaml",
        # otherwise ldap3 cannot be found
        # ModuleNotFoundError: No module named 'ldap3'
        "ldap3"
    ]
else:
    VERSION_SPECIFIC_PACKAGES = [
        "ConfigParser",
        # otherwise logutils cannot be found
        # ImportError: No module named logutils.queue
        "logutils",
        # otherwise zmq.auth.thread cannot be found:
        # ImportError: No module named auth.thread
        "zmq",
        # otherwise uuid cannot be found:
        # ImportError: No module named uuid
        "ldap3",
        # otherwise yaml cannot be found:
        # ImportError: No module named yaml
        "yaml"
    ]


# Dependencies are automatically detected, but it might need fine tuning.
BUILD_EXE_OPTIONS = {
    "packages": (
        # prefer putting packages in VERSION_SPECIFIC_PACKAGES since problems
        # might be fixed with new cx-Freeze versions
        []
        + VERSION_SPECIFIC_PACKAGES
        + PLATFORM_SPECIFIC_PACKAGES
    ),
    "include_files": [
        (os.path.join(BASEPATH, "logs/.gitignore"),
         os.path.join("logs", ".gitignore")),
        (get_init(), "hidra.sh"),
        (os.path.join(CONFPATH, "base_sender.yaml"),
         os.path.join("conf", "base_sender.yaml")),
        (os.path.join(SENDERPATH, "__init__.py"), "__init__.py"),
        (get_environment(), "_environment.py"),
        (os.path.join(SENDERPATH, "base_class.py"), "base_class.py"),
        (os.path.join(SENDERPATH, "taskprovider.py"), "taskprovider.py"),
        (os.path.join(SENDERPATH, "signalhandler.py"), "signalhandler.py"),
        (os.path.join(SENDERPATH, "datadispatcher.py"), "datadispatcher.py"),
        (os.path.join(SENDERPATH, "statserver.py"), "statserver.py"),
        # only for readability (not for actual code)
        (os.path.join(UTILSPATH, "_version.py"), "_version.py"),
        # event detectors
        (os.path.join(SENDERPATH, "eventdetectors"), "eventdetectors"),
        # data fetchers
        (os.path.join(SENDERPATH, "datafetchers"), "datafetchers"),
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
      version='4.2.0',
      description='',
      options={"build_exe": BUILD_EXE_OPTIONS,
               "bdist_msi": BDIS_MSI_OPTIONS},
      executables=EXECUTABLES)
