""" Installing hidra """

import ast
import io
import os

from setuptools import setup  # , find_packages

# Package meta-data.
NAME = "hidra"
DESCRIPTION = "High performance data multiplexing tool"
URL = "https://github.com/hidra-org/hidra"
EMAIL = "manuela.kuhn@desy.de"
AUTHOR = "Manuela Kuhn"
REQUIRES_PYTHON = ">=2.7.0"
VERSION = ""

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))


def get_long_description():
    """ Retrive the desciption """

    # Import the README and use it as the long-description.
    # Note: this will only work if 'README.md' is present in your MANIFEST.in
    #       file!
    try:
        fname = os.path.join(CURRENT_DIR, "README.md")
        with io.open(fname, encoding="utf-8") as f:
            long_description = '\n' + f.read()
    except IOError:
        long_description = DESCRIPTION

    return long_description


def get_version():
    """ Retrive the version """

    # Load the package's _version.py module as a dictionary.
    about = {}
    if not VERSION:
        version_file = os.path.join(CURRENT_DIR, "src", "api", "python",
                                    "hidra", "utils", "_version.py")
        with open(version_file) as f:
            # exec(f.read(), about)
            about["__version__"] = ast.parse(f.read()).body[0].value.s
    else:
        about["__version__"] = VERSION

    return about["__version__"]

# CONFDIR = "/etc/hidra"

# puts dir into $HOME/.local/
# data_files = [
#    (CONFDIR, [
#        "conf/base_receiver.yaml",
#        "conf/base_sender.yaml",
#        "conf/datamanager.yaml",
#        "conf/datareceiver.yaml"])]


setup(
    name=NAME,
    version=get_version(),
    description=DESCRIPTION,
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    install_requires=[
        "future",
        "pyzmq>=14.6.0",
        "pyyaml",
        "setproctitle",
        "logutils; python_version<'3'",
    ],
    extras_require={
        "sender": [
            # python3 specific
            "pathlib; python_version>='3'",
            "inotify; python_version>='3'",
            # python2 specific
            "pathlib2; python_version<'3.4'",
            "inotifyx; python_version<'3'",
            # windows specific
            "watchdog;platform_system=='Windows'",
            "requests"
        ],
        "receiver": [
            "pathlib2; python_version<'3.4'",
        ],
        "control_client": []
    },
    entry_points={
        # TODO only installed if corresponding extras is installed
        "console_scripts": [
            "hidra_receiver=hidra.receiver.datareceiver:DataReceiver [receiver]",
            "hidra_sender=hidra.sender.datamanager:main [sender]",
            "hidra_control_server=hidra.hidra_control.server:main [sender]",
            "hidra_control_client=hidra.hidra_control.client:main [control_client]",
        ],
    },
    #    packages=find_packages(where="./src") + find_packages(where="./src/api/python"),
    packages=[
        "hidra",
        "hidra.utils",
        "hidra.sender",
        "hidra.sender.eventdetectors",
        "hidra.sender.datafetchers",
        "hidra.receiver",
        "hidra.hidra_control",
        # define the config as package to include it
        "hidra.conf",
    ],
    package_data={
        "hidra.conf": [
            "base_receiver.yaml",
            "base_sender.yaml",
            "datamanager.yaml",
            "datareceiver.yaml",
            "control_server.yaml"
            "control_client.yaml"
        ]
    },
    include_package_data=True,
    # use distutils util.convert_path to make paths windows compatible
    package_dir={
        # attempt to make "python setup.py develop" work (bug in setuptools)
        "": "src/api/python",
        "hidra": "./src/api/python/hidra",
        "hidra.utils": "./src/api/python/hidra/utils",
        "hidra.sender": "./src/hidra/sender",
        "hidra.sender.eventdetectors": "./src/hidra/sender/eventdetectors",
        "hidra.sender.datafetcher": "./src/hidra/sender/datafetchers",
        "hidra.receiver": "./src/hidra/receiver",
        "hidra.hidra_control": "./src/hidra/hidra_control",
        "hidra.conf": "./conf",
    },
    # data_files=data_files,
    license="AGPLv3",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        (
            "License :: OSI Approved :: "
            "GNU Affero General Public License v3 or later (AGPLv3+)"
        ),
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Development Status :: 5 - Production/Stable"
    ],
)
