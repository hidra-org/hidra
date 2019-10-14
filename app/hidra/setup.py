import io
import os

from setuptools import setup, find_packages

# Package meta-data.
NAME = 'hidra'
DESCRIPTION = 'High performance data multiplexing tool'
URL = 'https://github.com/hidra-org/hidra'
EMAIL = 'manuela.kuhn@desy.de'
AUTHOR = 'Mauela Kuhn'
REQUIRES_PYTHON = '>=2.7.0'
VERSION = '4.1.0'

REQUIRED = [
    "future",
    "inotify; python_version>='3'",
    "inotifyx; python_version<'3'",
    "setproctitle",
    "python-hidra"
]

here = os.path.abspath(os.path.dirname(__file__))


# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except IOError:
    long_description = DESCRIPTION


# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    with open(os.path.join(here, NAME, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION

CONFDIR = "/etc/hidra"

data_files = [
    (CONFDIR, [
        "conf/base_receiver.yaml",
        "conf/base_sender.yaml",
        "conf/datamanager.yaml",
        "conf/datareceiver.yaml"])]


setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'hidra_receiver=hidra_receiver.datareceiver:DataReceiver',
            'hidra_sender=hidra_sender.datamanager:main',
        ],
    },
    install_requires=REQUIRED,
    include_package_data=True,
    data_files=data_files,
    license='AGPLv3',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        (
            'License :: OSI Approved :: '
            'GNU Affero General Public License v3 or later (AGPLv3+)'
        ),
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Development Status :: 5 - Production/Stable'
    ],
)